package store

import (
	"bapi/internal/pb"
	"sort"
	"sync"
)

type aggCtx struct {
	query            *pb.TableQuery
	op               pb.AggOp
	groupbyIntColCnt int
	intColCnt        int
	groupbyStrColCnt int
	strColCnt        int
	// for timeline query
	isTimelineQuery bool
	tsCol           int
	startTs         int64
	gran            uint64
}

type aggregator struct {
	ctx        *aggCtx
	aggBuckets sync.Map // map[uint64]*aggBucket
}

type aggResultMap[T OrderedNumeric] struct {
	m               map[uint64][]accResult[T]
	intResIdxes     []int
	floatResIdxes   []int
	genericResIdxes []int
}

func newAggResultMap[T OrderedNumeric]() aggResultMap[T] {
	return aggResultMap[T]{
		m:               make(map[uint64][]accResult[T]),
		intResIdxes:     make([]int, 0),
		floatResIdxes:   make([]int, 0),
		genericResIdxes: make([]int, 0),
	}
}

func (a aggResultMap[T]) addAggResForBucket(hash uint64, partialResForBucket []accumulator[T]) {
	a.m[hash] = make([]accResult[T], len(partialResForBucket))
	for i, accumulator := range partialResForBucket {
		a.m[hash][i] = accumulator.finalize()
		switch a.m[hash][i].valType {
		case accIntRes:
			a.intResIdxes = append(a.intResIdxes, i)
		case accFloatRes:
			a.floatResIdxes = append(a.floatResIdxes, i)
		case accGenericRes:
			a.genericResIdxes = append(a.genericResIdxes, i)
		default:
			continue
		}
	}
}

func newAggregator(c *aggCtx) *aggregator {
	return &aggregator{
		ctx:        c,
		aggBuckets: sync.Map{},
	}
}

func (a *aggregator) aggregate(filterResults []*BlockQueryResult) (*pb.TableQueryResult, bool) {
	tableIntAggPartialRes := make(map[uint64][]accumulator[int64])

	for _, result := range filterResults {
		blockIntAggPartialRes := a.aggregateBlock(result)

		for hash, blockResForCol := range blockIntAggPartialRes {
			tableResForCol, ok := tableIntAggPartialRes[hash]
			if !ok {
				// if this is the first time seeing this hash, we just initialize the table level
				// results with the block level results
				tableIntAggPartialRes[hash] = blockResForCol
				continue
			}

			// merge block level results for each aggregated column for this hash
			for i, tableaccumulator := range tableResForCol {
				tableaccumulator.consume(blockResForCol[i])
			}
		}
	}

	intAggResultMaps := newAggResultMap[int64]()
	for hash, partialResForCol := range tableIntAggPartialRes {
		intAggResultMaps.addAggResForBucket(hash, partialResForCol)
	}

	return a.buildResult(intAggResultMaps)
}

func (a *aggregator) buildResult(intAggResultMap aggResultMap[int64]) (*pb.TableQueryResult, bool) {
	buckets := make([]*aggBucket, 0)
	a.aggBuckets.Range(
		func(k, bucket interface{}) bool {
			buckets = append(buckets, bucket.(*aggBucket))
			return true
		})

	if a.ctx.isTimelineQuery {
		sort.Slice(buckets, func(i, j int) bool {
			left, right := buckets[i], buckets[j]
			return left.tsBucket <= right.tsBucket
		})
	}

	return a.toPbTableQueryResult(buckets, intAggResultMap)
}

func (a *aggregator) toPbTableQueryResult(buckets []*aggBucket, intAggResultMap aggResultMap[int64]) (*pb.TableQueryResult, bool) {
	bucketCount := len(buckets)
	intResultLen := bucketCount * a.ctx.groupbyIntColCnt
	intResult := make([]int64, intResultLen)
	intHasValue := make([]bool, intResultLen)
	for colIdx := 0; colIdx < a.ctx.groupbyIntColCnt; colIdx++ {
		for i, bucket := range buckets {
			idx := i*a.ctx.groupbyIntColCnt + colIdx
			intResult[idx] = bucket.intVals[colIdx]
			intHasValue[idx] = bucket.intHasVal[colIdx]
		}
	}

	// TODO: add geneirc result
	colCnt := len(intAggResultMap.intResIdxes)
	aggIntColumnNames := make([]string, 0)
	aggIntResultLen := bucketCount * colCnt
	aggIntResult := make([]int64, aggIntResultLen)
	aggIntHasValue := make([]bool, aggIntResultLen)
	for colIdx, accumulatorIdx := range intAggResultMap.intResIdxes {
		aggIntColumnNames = append(aggIntColumnNames, a.ctx.query.AggIntColumnNames[accumulatorIdx])

		for i, bucket := range buckets {
			idx := i*colCnt + colIdx
			accumulator := intAggResultMap.m[bucket.hash][accumulatorIdx]
			aggIntResult[idx] = accumulator.intVal
			aggIntHasValue[idx] = accumulator.hasValue
		}
	}

	return &pb.TableQueryResult{
		Count:             int32(bucketCount),
		IntColumnNames:    a.ctx.query.AggIntColumnNames,
		IntResult:         intResult,
		IntHasValue:       intHasValue,
		AggIntColumnNames: aggIntColumnNames,
		AggIntResult:      aggIntResult,
		AggIntHasValue:    aggIntHasValue,

		// TODO: support cols
		StrColumnNames: make([]string, 0),
		StrIdMap:       make(map[uint32]string),
		StrResult:      make([]uint32, 0),
		StrHasValue:    make([]bool, 0),
	}, true
}

func (a *aggregator) aggregateBlock(r *BlockQueryResult) map[uint64][]accumulator[int64] {
	hasher := buildHasherForBlock(a.ctx, r)
	hashes := hasher.getHashes()

	intAggResultMap := make(map[uint64][]accumulator[int64], 0)

	for _, hash := range hashes {
		_, ok := intAggResultMap[hash]
		if ok {
			continue
		}
		// First time seeting this hash in this block, so initialize the aggResult for it.
		intAggResultMap[hash], _ = getAccumulatorSlice[int64](a.ctx.op, a.ctx.intColCnt-a.ctx.groupbyIntColCnt)

		// Also initialize the global aggbucket for it if needed. we do this here instead of
		// when all blocks are aggregated since the hasher knows the row of the hash.
		if _, ok := a.aggBuckets.Load(hash); !ok {
			aggBucket, _ := hasher.getAggBucket(hash)
			a.aggBuckets.Store(hash, aggBucket)
		}
	}

	for colIdx := a.ctx.groupbyIntColCnt; colIdx < a.ctx.intColCnt; colIdx++ {
		intHasVal := r.IntResult.hasValue[colIdx]
		intVals := r.IntResult.matrix[colIdx]

		for rowIdx, hash := range hashes {
			if !intHasVal[rowIdx] {
				continue
			}
			intAggResultMap[hash][colIdx-a.ctx.groupbyIntColCnt].addValue(intVals[rowIdx])
		}
	}

	return intAggResultMap
}
