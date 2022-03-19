package store

import (
	"bapi/internal/pb"
	"sort"
	"sync"
)

type aggCtx struct {
	query          *pb.TableQuery
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
	ctx        *aggCtx
	aggBuckets sync.Map // map[uint64]*aggBucket
}

type aggResultMap[T OrderedNumeric] struct {
	m            map[uint64][]aggOpResult[T]
	intAggOp     []int
	floatAggOp   []int
	genericAggOp []int
}

func newAggResultMap[T OrderedNumeric]() aggResultMap[T] {
	return aggResultMap[T]{
		m:            make(map[uint64][]aggOpResult[T]),
		intAggOp:     make([]int, 0),
		floatAggOp:   make([]int, 0),
		genericAggOp: make([]int, 0),
	}
}

func (a aggResultMap[T]) addAggResForBucket(hash uint64, partialResForBucket []aggOp[T]) {
	a.m[hash] = make([]aggOpResult[T], len(partialResForBucket))
	for i, aggOp := range partialResForBucket {
		a.m[hash][i] = aggOp.finalize()
		switch a.m[hash][i].valType {
		case aggOpIntRes:
			a.intAggOp = append(a.intAggOp, i)
		case aggOpFloatRes:
			a.floatAggOp = append(a.floatAggOp, i)
		case aggOpGenericRes:
			a.genericAggOp = append(a.genericAggOp, i)
		default:
			continue
		}
	}
}

func newBasicAggregator(c *aggCtx) *basicAggregator {
	return &basicAggregator{
		ctx:        c,
		aggBuckets: sync.Map{},
	}
}

func (a *basicAggregator) aggregate(filterResults []*BlockQueryResult) aggResultMap[int64] {
	tableIntAggPartialRes := make(map[uint64][]aggOp[int64])

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
			for i, tableAggOp := range tableResForCol {
				tableAggOp.consume(blockResForCol[i])
			}
		}
	}

	intAggResults := newAggResultMap[int64]()
	for hash, partialResForCol := range tableIntAggPartialRes {
		intAggResults.addAggResForBucket(hash, partialResForCol)
	}

	return intAggResults
}

func (a *basicAggregator) buildResult(intAggResult aggResultMap[int64]) (*pb.TableQueryResult, bool) {
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

	return a.toPbTableQueryResult(buckets, intAggResult)
}

func (a *basicAggregator) toPbTableQueryResult(buckets []*aggBucket, intAggResult aggResultMap[int64]) (*pb.TableQueryResult, bool) {
	bucketCount := len(buckets)
	intResultLen := bucketCount * a.ctx.firstAggIntCol
	intResult := make([]int64, intResultLen)
	intHasValue := make([]bool, intResultLen)
	for colIdx := 0; colIdx < a.ctx.firstAggIntCol; colIdx++ {
		for i, bucket := range buckets {
			idx := i*a.ctx.firstAggIntCol + colIdx
			intResult[idx] = bucket.intVals[colIdx]
			intHasValue[idx] = bucket.intHasVal[colIdx]
		}
	}

	// TODO: add geneirc result
	colCnt := len(intAggResult.intAggOp)
	aggIntColumnNames := make([]string, 0)
	aggIntResultLen := bucketCount * colCnt
	aggIntResult := make([]int64, aggIntResultLen)
	aggIntHasValue := make([]bool, aggIntResultLen)
	for colIdx, aggOpIdx := range intAggResult.intAggOp {
		aggIntColumnNames = append(aggIntColumnNames, a.ctx.query.AggIntColumnNames[aggOpIdx])

		for i, bucket := range buckets {
			idx := i*colCnt + colIdx
			aggOp := intAggResult.m[bucket.hash][aggOpIdx]
			aggIntResult[idx] = aggOp.intVal
			aggIntHasValue[idx] = aggOp.hasValue
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

func (a *basicAggregator) aggregateBlock(r *BlockQueryResult) map[uint64][]aggOp[int64] {
	hasher := buildHasherForBlock(a.ctx, r)
	hashes := hasher.getHashes()

	intAggResult := make(map[uint64][]aggOp[int64], 0)

	for _, hash := range hashes {
		_, ok := intAggResult[hash]
		if ok {
			continue
		}
		// First time seeting this hash in this block, so initialize the aggResult for it.
		intAggResult[hash], _ = getAggOpSlice[int64](a.ctx.op, a.ctx.intColCnt-a.ctx.firstAggIntCol)

		// Also initialize the global aggbucket for it if needed. we do this here instead of
		// when all blocks are aggregated since the hasher knows the row of the hash.
		if _, ok := a.aggBuckets.Load(hash); !ok {
			aggBucket, _ := hasher.getAggBucket(hash)
			a.aggBuckets.Store(hash, aggBucket)
		}
	}

	for colIdx := a.ctx.firstAggIntCol; colIdx < a.ctx.intColCnt; colIdx++ {
		intHasVal := r.IntResult.hasValue[colIdx]
		intVals := r.IntResult.matrix[colIdx]

		for rowIdx, hash := range hashes {
			if !intHasVal[rowIdx] {
				continue
			}
			intAggResult[hash][colIdx-a.ctx.firstAggIntCol].addValue(intVals[rowIdx])
		}
	}

	return intAggResult
}
