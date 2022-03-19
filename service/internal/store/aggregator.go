package store

import (
	"bapi/internal/pb"
	"sort"
	"sync"
)

type aggCtx struct {
	op               pb.AggOp
	groupbyIntColCnt int
	intColCnt        int
	groupbyStrColCnt int
	strColCnt        int
	// for assemble result
	groupbyIntColumnNames []string
	groupbyStrColumnNames []string
	aggIntColumnNames     []string
	strStore              strStore
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

type aggResult[T OrderedNumeric] struct {
	m               map[uint64][]accResult[T]
	intResIdxes     []int
	floatResIdxes   []int
	genericResIdxes []int
}

type accSliceMap[T OrderedNumeric] map[uint64][]accumulator[T]

func (accMap accSliceMap[T]) finalize() (aggResult[T], bool) {
	aggRes := aggResult[T]{
		m:               make(map[uint64][]accResult[T]),
		intResIdxes:     make([]int, 0),
		floatResIdxes:   make([]int, 0),
		genericResIdxes: make([]int, 0),
	}

	for hash, accSlice := range accMap {
		aggRes.m[hash] = make([]accResult[T], len(accSlice))
		for i, accumulator := range accSlice {
			aggRes.m[hash][i] = accumulator.finalize()
			switch aggRes.m[hash][i].valType {
			case accIntRes:
				aggRes.intResIdxes = append(aggRes.intResIdxes, i)
			case accFloatRes:
				aggRes.floatResIdxes = append(aggRes.floatResIdxes, i)
			case accGenericRes:
				aggRes.genericResIdxes = append(aggRes.genericResIdxes, i)
			default:
				// abort: invalid result type
				return aggRes, false
			}
		}
	}

	return aggRes, true
}

func newAggregator(c *aggCtx) *aggregator {
	return &aggregator{
		ctx:        c,
		aggBuckets: sync.Map{},
	}
}

func (a *aggregator) aggregate(filterResults []*BlockQueryResult) (*pb.TableQueryResult, bool) {
	tableIntAccSliceMap := make(accSliceMap[int64])

	for _, result := range filterResults {
		blockIntAccSliceMap := a.aggregateBlock(result)
		for hash, blockAccSlice := range blockIntAccSliceMap {
			tableAccSlice, ok := tableIntAccSliceMap[hash]
			if !ok {
				// if this is the first time seeing this hash, we just initialize the table level
				// accumulator with the block level accumulator
				tableIntAccSliceMap[hash] = blockAccSlice
				continue
			}

			// merge block level accumulators into table level's
			for i, tableAccumulator := range tableAccSlice {
				tableAccumulator.consume(blockAccSlice[i])
			}
		}
	}

	aggResult, ok := tableIntAccSliceMap.finalize()
	if !ok {
		return nil, false
	}
	return a.buildResult(aggResult)
}

func (a *aggregator) buildResult(intAggResult aggResult[int64]) (*pb.TableQueryResult, bool) {
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

func (a *aggregator) toPbTableQueryResult(buckets []*aggBucket, intAggResult aggResult[int64]) (*pb.TableQueryResult, bool) {
	bucketCount := len(buckets)

	intResultLen := bucketCount * a.ctx.groupbyIntColCnt
	intResult := make([]int64, intResultLen)
	intHasValue := make([]bool, intResultLen)
	strIdMap := make(map[uint32]string)
	strResultLen := bucketCount * a.ctx.groupbyStrColCnt
	strResult := make([]uint32, strResultLen)
	strHasValue := make([]bool, strResultLen)

	// fills values for groupby columns
	for i, bucket := range buckets {
		for colIdx := 0; colIdx < a.ctx.groupbyIntColCnt; colIdx++ {
			idx := colIdx*bucketCount + i
			intResult[idx] = bucket.intVals[colIdx]
			intHasValue[idx] = bucket.intHasVal[colIdx]
		}

		for colIdx := 0; colIdx < a.ctx.groupbyStrColCnt; colIdx++ {
			idx := colIdx*bucketCount + i
			strHasValue[idx] = bucket.strHasVal[colIdx]
			if strHasValue[idx] {
				sid := uint32(bucket.strVals[colIdx])
				str, _ := a.ctx.strStore.getStr(strId(sid))
				strResult[idx] = sid
				strIdMap[sid] = str
			}
		}
	}

	// fills values for aggregated columns that have int results
	colCnt := len(intAggResult.intResIdxes) + len(intAggResult.genericResIdxes)
	aggIntColumnNames := make([]string, 0)
	aggIntResultLen := bucketCount * colCnt
	aggIntResult := make([]int64, aggIntResultLen)
	aggIntHasValue := make([]bool, aggIntResultLen)

	colIdxOffset := 0
	for colIdx, accIdx := range intAggResult.intResIdxes {
		aggIntColumnNames = append(aggIntColumnNames, a.ctx.aggIntColumnNames[accIdx])

		for i, bucket := range buckets {
			idx := (colIdxOffset+colIdx)*bucketCount + i
			accRes := intAggResult.m[bucket.hash][accIdx]
			aggIntResult[idx] = accRes.intVal
			aggIntHasValue[idx] = accRes.hasValue
		}
	}

	colIdxOffset += len(intAggResult.intResIdxes)
	for colIdx, accIdx := range intAggResult.genericResIdxes {
		aggIntColumnNames = append(aggIntColumnNames, a.ctx.aggIntColumnNames[accIdx])

		for i, bucket := range buckets {
			idx := (colIdxOffset+colIdx)*bucketCount + i
			accRes := intAggResult.m[bucket.hash][accIdx]
			aggIntResult[idx] = accRes.intVal
			aggIntHasValue[idx] = accRes.hasValue
		}
	}

	// TODO: support float

	return &pb.TableQueryResult{
		Count:          int32(bucketCount),
		IntColumnNames: a.ctx.groupbyIntColumnNames,
		IntResult:      intResult,
		IntHasValue:    intHasValue,
		StrColumnNames: a.ctx.groupbyStrColumnNames,
		StrIdMap:       strIdMap,
		StrResult:      strResult,
		StrHasValue:    strHasValue,

		AggIntColumnNames: aggIntColumnNames,
		AggIntResult:      aggIntResult,
		AggIntHasValue:    aggIntHasValue,
	}, true
}

func (a *aggregator) aggregateBlock(r *BlockQueryResult) accSliceMap[int64] {
	hasher := buildHasherForBlock(a.ctx, r)
	hashes := hasher.getHashes()

	intAccSliceMap := make(accSliceMap[int64], 0)

	for _, hash := range hashes {
		_, ok := intAccSliceMap[hash]
		if ok {
			continue
		}
		// First time seeting this hash in this block, so initialize the aggResult for it.
		intAccSliceMap[hash], _ = getAccumulatorSlice[int64](a.ctx.op, a.ctx.intColCnt-a.ctx.groupbyIntColCnt)

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
			intAccSliceMap[hash][colIdx-a.ctx.groupbyIntColCnt].addValue(intVals[rowIdx])
		}
	}

	return intAccSliceMap
}
