package store

import (
	"bapi/internal/pb"
	"unsafe"
)

func (t *Table) TableQuery(query *pb.TableQuery) (*pb.RowsQueryResult, bool) {
	if len(query.AggIntColumnNames) == 0 {
		return nil, false
	}

	blockResults, hasResult := t.queryBlocks(queryWithFilter{query})
	if !hasResult {
		return nil, false
	}

	aggregator := newBasicAggregator(&aggCtx{
		op:             query.AggOp,
		firstAggIntCol: len(query.GroupByIntColumnNames), // aggIntCols are after groupByIntCols
		intColCnt:      len(query.GroupByIntColumnNames) + len(query.AggIntColumnNames),
	})
	aggregator.aggregate(blockResults)
	// TODO: implement
	return nil, false
	// return t.toPbQueryResult(query, blockResults)
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

func (t *Table) toPbRowsQueryResult(query *pb.RowsQuery, blockResults []*BlockQueryResult) (*pb.RowsQueryResult, bool) {
	if len(blockResults) == 0 {
		return nil, false
	}

	rowCount := 0
	for _, r := range blockResults {
		rowCount += r.Count
	}

	intResultLen := rowCount * len(query.IntColumnNames)
	intResult := make([]int64, intResultLen)
	intHasValue := make([]bool, intResultLen)
	for colIdx := range query.IntColumnNames {
		rowStartIdx := colIdx * rowCount
		copied := 0

		for _, result := range blockResults {
			blockStartIdx := rowStartIdx + copied

			count := copy(intResult[blockStartIdx:], result.IntResult.matrix[colIdx])
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, false
			}

			count = copy(intHasValue, result.IntResult.hasValue[colIdx])
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, false
			}
			copied += result.Count
		}
	}

	strIdMap := make(map[uint32]string)

	strResultLen := rowCount * len(query.StrColumnNames)
	strResult := make([]uint32, strResultLen)
	strHasValue := make([]bool, strResultLen)
	for colIdx := range query.StrColumnNames {
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
				return nil, false
			}

			count = copy(strHasValue, result.StrResult.hasValue[colIdx])
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, false
			}
			copied += result.Count
		}
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
