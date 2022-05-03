package store

import (
	"bapi/internal/pb"
	"unsafe"
)

// TimelineQuery supports only count aggregation at this time. This is achived via having
// the `ts` column as the aggIntCol with AggOp_TIMELINE_COUNT.
func (t *Table) TimeilneQuery(query *pb.TimelineQuery) (*pb.TimelineQueryResult, bool) {
	blockResults, hasResult := t.queryBlocks(queryWithFilter{query})
	if !hasResult {
		return nil, false
	}

	aggIntCols := []string{TS_COLUMN_NAME}
	intColCnt := len(query.GroupbyIntColumnNames) + len(aggIntCols)

	aggregator := newAggregator(&aggCtx{
		logger:           t.ctx.Logger,
		op:               pb.AggOp_TIMELINE_COUNT,
		groupbyIntColCnt: len(query.GroupbyIntColumnNames),
		intColCnt:        intColCnt,
		groupbyStrColCnt: len(query.GroupbyStrColumnNames),
		strColCnt:        len(query.GroupbyStrColumnNames),

		groupbyIntColumnNames: query.GroupbyIntColumnNames,
		groupbyStrColumnNames: query.GroupbyStrColumnNames,
		aggIntColumnNames:     aggIntCols,
		strStore:              t.strStore,

		isTimelineQuery: true,
		startTs:         query.MinTs,
		gran:            uint64(query.Gran),
	})

	aggregator.aggregateForTableQuery(blockResults)
	return nil, false
}

func (t *Table) TableQuery(query *pb.TableQuery) (*pb.TableQueryResult, bool) {
	if len(query.AggIntColumnNames) == 0 {
		return nil, false
	}

	blockResults, hasResult := t.queryBlocks(queryWithFilter{query})
	if !hasResult {
		return nil, false
	}

	aggregator := newAggregator(&aggCtx{
		logger:           t.ctx.Logger,
		op:               query.AggOp,
		groupbyIntColCnt: len(query.GroupbyIntColumnNames), // aggIntCols are after groupByIntCols
		intColCnt:        len(query.GroupbyIntColumnNames) + len(query.AggIntColumnNames),
		groupbyStrColCnt: len(query.GroupbyStrColumnNames),
		strColCnt:        len(query.GroupbyStrColumnNames), // aggby str not currently supported

		groupbyIntColumnNames: query.GroupbyIntColumnNames,
		groupbyStrColumnNames: query.GroupbyStrColumnNames,
		aggIntColumnNames:     query.AggIntColumnNames,
		strStore:              t.strStore,
	})
	return aggregator.aggregateForTableQuery(blockResults)
}

func (t *Table) RowsQuery(query *pb.RowsQuery) (*pb.RowsQueryResult, bool) {
	if len(query.IntColumnNames) == 0 && len(query.StrColumnNames) == 0 {
		return nil, false
	}

	blockResults, hasResult := t.queryBlocks(queryWithFilter{query})
	if !hasResult {
		return nil, false
	}

	return t.toPbRowsQueryResult(query, blockResults)
}

func (t *Table) toPbTableQueryResult(query *pb.TableQuery, blockResults []*BlockQueryResult) (*pb.TableQueryResult, bool) {
	if len(blockResults) == 0 {
		return nil, false
	}

	rowCount := 0
	for _, r := range blockResults {
		rowCount += r.Count
	}

	intResult, intHasValue, ok := t.toPbIntColResult(rowCount, query.GroupbyIntColumnNames, blockResults)
	if !ok {
		return nil, false
	}

	strResult, strHasValue, strIdMap, ok := t.toPbStrColResult(rowCount, query.GroupbyStrColumnNames, blockResults)
	if !ok {
		return nil, false
	}

	aggIntResult, aggIntHasValue, ok := t.toPbIntColResult(rowCount, query.AggIntColumnNames, blockResults)
	if !ok {
		return nil, false
	}

	return &pb.TableQueryResult{
		Count: int32(rowCount),

		IntColumnNames: query.GroupbyIntColumnNames,
		IntResult:      intResult,
		IntHasValue:    intHasValue,

		StrColumnNames: query.GroupbyStrColumnNames,
		StrIdMap:       strIdMap,
		StrResult:      strResult,
		StrHasValue:    strHasValue,

		AggIntColumnNames: query.AggIntColumnNames,
		AggIntResult:      aggIntResult,
		AggIntHasValue:    aggIntHasValue,
	}, true
}

func (t *Table) toPbRowsQueryResult(query *pb.RowsQuery, blockResults []*BlockQueryResult) (*pb.RowsQueryResult, bool) {
	if len(blockResults) == 0 {
		return nil, false
	}

	rowCount := 0
	for _, r := range blockResults {
		rowCount += r.Count
	}

	intResult, intHasValue, ok := t.toPbIntColResult(rowCount, query.IntColumnNames, blockResults)
	if !ok {
		return nil, false
	}

	strResult, strHasValue, strIdMap, ok := t.toPbStrColResult(rowCount, query.StrColumnNames, blockResults)
	if !ok {
		return nil, false
	}

	return &pb.RowsQueryResult{
		Count: int32(rowCount),

		IntColumnNames: query.IntColumnNames,
		IntResult:      intResult,
		IntHasValue:    intHasValue,

		StrColumnNames: query.StrColumnNames,
		StrIdMap:       strIdMap,
		StrResult:      strResult,
		StrHasValue:    strHasValue,
	}, true
}

func (t *Table) toPbIntColResult(rowCount int, colNames []string, blockResults []*BlockQueryResult) ([]int64, []bool, bool) {
	intResultLen := rowCount * len(colNames)
	intResult := make([]int64, intResultLen)
	intHasValue := make([]bool, intResultLen)
	for colIdx := range colNames {
		rowStartIdx := colIdx * rowCount
		copied := 0

		for _, result := range blockResults {
			blockStartIdx := rowStartIdx + copied

			count := copy(intResult[blockStartIdx:], result.IntResult.matrix[colIdx])
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, nil, false
			}

			count = copy(intHasValue[blockStartIdx:], result.IntResult.hasValue[colIdx])
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, nil, false
			}
			copied += result.Count
		}
	}

	return intResult, intHasValue, true
}

func (t *Table) toPbStrColResult(rowCount int, colNames []string, blockResults []*BlockQueryResult) ([]uint32, []bool, map[uint32]string, bool) {
	strIdMap := make(map[uint32]string)

	strResultLen := rowCount * len(colNames)
	strResult := make([]uint32, strResultLen)
	strHasValue := make([]bool, strResultLen)
	for colIdx := range colNames {
		rowStartIdx := colIdx * rowCount
		copied := 0

		for _, result := range blockResults {
			for sid := range result.StrResult.strIdSet {
				str, _ := t.strStore.getStr(sid)
				strIdMap[uint32(sid)] = str
			}

			blockStartIdx := rowStartIdx + copied
			// *Note* casting strId to its underline type uint32
			castedStrResult := (*[]uint32)(unsafe.Pointer(&result.StrResult.matrix[colIdx]))
			count := copy(strResult[blockStartIdx:], *castedStrResult)
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, nil, nil, false
			}

			count = copy(strHasValue[blockStartIdx:], result.StrResult.hasValue[colIdx])
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, nil, nil, false
			}
			copied += result.Count
		}
	}

	return strResult, strHasValue, strIdMap, true
}
