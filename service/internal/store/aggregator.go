package store

import (
	"bapi/internal/pb"
	"sync"
)

type aggregator interface {
	aggregate(ctx *aggCtx, filterResults []*BlockQueryResult)
}

type aggCtx struct {
	op             pb.AggOp
	firstAggIntCol int
	intColCnt      int
	// for timeline query
	isTimelineQuery bool
	tsCol           int
	startTs         int64
	gran            uint64
}

type basicAggregator struct {
	aggBuckets sync.Map // map[uint64]*aggBucket
}

func newBasicAggregator() *basicAggregator {
	return &basicAggregator{
		aggBuckets: sync.Map{},
	}
}

func (a *basicAggregator) aggregate(ctx *aggCtx, filterResults []*BlockQueryResult) map[uint64][]aggOpResult[int64] {
	tableIntAggPartialRes := make(map[uint64][]aggOp[int64])

	for _, result := range filterResults {
		blockIntAggPartialRes := a.aggregateBlock(ctx, result)

		for hash, blockResForCol := range blockIntAggPartialRes {
			tableResForCol, ok := tableIntAggPartialRes[hash]
			if !ok {
				// if this is the first time seeing this hash, we just initialize the table level
				// results with the block level results
				tableIntAggPartialRes[hash] = blockResForCol
				continue
			}

			// merge block level results for each aggregated column for this hash
			for i, tableAggOp := range tableResForCol {
				tableAggOp.consume(blockResForCol[i])
			}
		}
	}

	intAggResults := make(map[uint64][]aggOpResult[int64])
	for hash, partialResForCol := range tableIntAggPartialRes {
		intAggResults[hash] = make([]aggOpResult[int64], len(partialResForCol))
		for i, aggOp := range partialResForCol {
			intAggResults[hash][i] = aggOp.finalize()
		}
	}

	return intAggResults
}

func (a *basicAggregator) aggregateBlock(c *aggCtx, r *BlockQueryResult) map[uint64][]aggOp[int64] {
	hasher := buildHasherForBlock(c, r)
	hashes := hasher.getHashes()

	intAggResult := make(map[uint64][]aggOp[int64], 0)

	for _, hash := range hashes {
		_, ok := intAggResult[hash]
		if ok {
			continue
		}
		// First time seeting this hash in this block, so initialize the aggResult for it.
		intAggResult[hash], _ = getAggOpSlice[int64](c.op, c.intColCnt-c.firstAggIntCol)

		// Also initialize the global aggbucket for it if needed. we do this here instead of
		// when all blocks are aggregated since the hasher knows the row of the hash.
		if _, ok := a.aggBuckets.Load(hash); !ok {
			aggBucket, _ := hasher.getAggBucket(hash)
			a.aggBuckets.Store(hash, aggBucket)
		}
	}

	for colIdx := c.firstAggIntCol; colIdx < c.intColCnt; colIdx++ {
		intHasVal := r.IntResult.hasValue[colIdx]
		intVals := r.IntResult.matrix[colIdx]

		for rowIdx, hash := range hashes {
			if !intHasVal[rowIdx] {
				continue
			}
			intAggResult[hash][colIdx-c.firstAggIntCol].addValue(intVals[rowIdx])
		}
	}

	return intAggResult
}
