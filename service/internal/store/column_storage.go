package store

import (
	"errors"
)

// --------------------------- intColumnsStorage ----------------------------
type intColumnsStorage struct {
	numericStore[int64]
}

func newIntColumnsStorage(
	partialColumns partialColumns[int64],
	rowCount int,
) (*intColumnsStorage, error) {
	storage, err := fromPartialColumns(partialColumns, rowCount)
	if err != nil {
		return nil, err
	}

	intStorage := &intColumnsStorage{
		numericStore: *storage,
	}

	if rowCount != len(intStorage.matrix[0]) {
		return nil, errors.New("not all rows are being stored to intColStorage, this means that some are missing ts")
	}
	return intStorage, nil
}

func (ics *intColumnsStorage) get(
	ctx *getCtx,
) IntResult {
	storageResult, _ := ics.numericStore.get(ctx, false /*recordValues*/)
	return IntResult{matrix: storageResult.matrix, hasValue: storageResult.hasValue}
}

func (ics *intColumnsStorage) filter(ctx *filterCtx, filters []IntFilter) {
	for _, filter := range filters {
		localColumnId, ok := ics.getLocalColumnId(filter.ColumnInfo)
		if !ok {
			if canContinueElseStopForColNotExist(ctx, filter.FilterOp) {
				continue
			} else {
				return
			}
		}

		ics.filterNumericStore(
			ctx,
			numericFilter[int64]{
				localColId: localColumnId,
				op:         filter.FilterOp,
				value:      filter.Value,
			},
		)
	}
}

// --------------------------- strColumnsStorage ----------------------------
type strColumnsStorage struct {
	numericStore[strId]
	strIdSet    map[strId]bool
	strIdMap    map[strId]string
	strValueMap map[string]strId
}

func newStrColumnsStorage(
	partialColumns partialColumns[strId],
	rowCount int,
	strIdSet map[strId]bool,
	strIdMap map[strId]string,
	strValueMap map[string]strId,
) (*strColumnsStorage, error) {
	storage, err := fromPartialColumns(partialColumns, rowCount)
	if err != nil {
		return nil, err
	}

	return &strColumnsStorage{
		strIdSet:     strIdSet,
		strIdMap:     strIdMap,
		strValueMap:  strValueMap,
		numericStore: *storage,
	}, nil
}

func (scs *strColumnsStorage) get(
	ctx *getCtx,
) StrResult {
	storageResult, strIdInResult := scs.numericStore.get(ctx, true /*recordValues*/)
	strIdMap := make(map[strId]string)
	for strId := range strIdInResult {
		strIdMap[strId] = scs.strIdMap[strId]
	}

	return StrResult{
		strIdMap: strIdMap,
		matrix:   storageResult.matrix,
		hasValue: storageResult.hasValue,
	}
}

func (scs *strColumnsStorage) filter(ctx *filterCtx, filters []StrFilter) {
	for _, filter := range filters {
		localColumnId, ok := scs.getLocalColumnId(filter.ColumnInfo)
		if !ok {
			if canContinueElseStopForColNotExist(ctx, filter.FilterOp) {
				continue
			} else {
				return
			}
		}

		_, containsStr := scs.strIdSet[filter.Value]
		switch filter.FilterOp {
		case FilterNull, FilterNonnull:
			{
				// do nothing, don't care about missing value
			}
		case FilterEq:
			if !containsStr {
				// string does not exist, clear all bits
				ctx.bitmap.Clear()
				return
			}
		case FilterNe:
			if !containsStr {
				// string does not exist, continue to process the next filter
				continue
			}
		default:
			ctx.ctx.Logger.DPanicf("unexpected str filter op: %d", filter.FilterOp)
			continue
		}

		scs.filterNumericStore(
			ctx,
			numericFilter[strId]{
				localColId: localColumnId,
				op:         filter.FilterOp,
				value:      filter.Value,
			},
		)
	}
}
