package store

import (
	"bapi/internal/pb"
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

func (ics *intColumnsStorage) filter(ctx *filterCtx, filters []columnFilter[int64]) {
	for _, filter := range filters {
		localColumnId, ok := ics.getLocalColumnId(filter.col)
		if !ok {
			if canContinueElseStopForColNotExist(ctx, filter.op) {
				continue
			} else {
				return
			}
		}

		ics.filterNumericStore(
			ctx,
			numericFilter[int64]{
				localColId: localColumnId,
				op:         filter.op,
				values:     filter.values,
			},
		)
	}
}

// --------------------------- strColumnsStorage ----------------------------
type strColumnsStorage struct {
	numericStore[strId]
	strIdSet map[strId]bool
}

func newStrColumnsStorage(
	partialColumns partialColumns[strId],
	rowCount int,
	strIdSet map[strId]bool,
) (*strColumnsStorage, error) {
	storage, err := fromPartialColumns(partialColumns, rowCount)
	if err != nil {
		return nil, err
	}

	return &strColumnsStorage{
		strIdSet:     strIdSet,
		numericStore: *storage,
	}, nil
}

func (scs *strColumnsStorage) get(
	ctx *getCtx,
) StrResult {
	storageResult, strIdInResult := scs.numericStore.get(ctx, true /*recordValues*/)

	return StrResult{
		strIdSet: strIdInResult,
		matrix:   storageResult.matrix,
		hasValue: storageResult.hasValue,
	}
}

func (scs *strColumnsStorage) filter(ctx *filterCtx, filters []columnFilter[strId]) {
	for _, filter := range filters {
		localColumnId, ok := scs.getLocalColumnId(filter.col)
		if !ok {
			if canContinueElseStopForColNotExist(ctx, filter.op) {
				continue
			} else {
				return
			}
		}

		containsStrValues := false
		for _, sid := range filter.values {
			_, containsStr := scs.strIdSet[sid]
			containsStrValues = containsStrValues || containsStr
		}

		switch filter.op {
		case pb.FilterOp_NULL, pb.FilterOp_NONNULL:
			{
				// do nothing, don't care about missing value
			}
		case pb.FilterOp_EQ:
			if !containsStrValues {
				// string does not exist, clear all bits
				ctx.bitmap.Clear()
				return
			}
		case pb.FilterOp_NE:
			if !containsStrValues {
				// string does not exist, continue to process the next filter
				continue
			}
		default:
			ctx.ctx.Logger.DPanicf("unexpected str filter op: %d", filter.op)
			continue
		}

		scs.filterNumericStore(
			ctx,
			numericFilter[strId]{
				localColId: localColumnId,
				op:         filter.op,
				values:     filter.values,
			},
		)
	}
}
