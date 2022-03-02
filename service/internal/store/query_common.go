package store

import (
	"bapi/internal/common"
	"bapi/internal/pb"

	"github.com/kelindar/bitmap"
)

type singularFilter[T comparable] struct {
	col   *ColumnInfo
	op    pb.FilterOp
	value T
}

type blockFilter struct {
	minTs      int64
	maxTs      int64
	tsFilters  []singularFilter[int64]
	intFilters []singularFilter[int64]
	strFilters []singularFilter[strId]
}

func newBlockFilter(
	minTs int64,
	maxTs int64,
	tsColInfo *ColumnInfo,
	intFilters []singularFilter[int64],
	strFilters []singularFilter[strId],
) blockFilter {
	return blockFilter{
		minTs: minTs,
		maxTs: maxTs,
		tsFilters: []singularFilter[int64]{
			{
				col:   tsColInfo,
				op:    pb.FilterOp_GE,
				value: minTs,
			},
			{
				col:   tsColInfo,
				op:    pb.FilterOp_LE,
				value: maxTs,
			},
		},
		intFilters: intFilters,
		strFilters: strFilters,
	}
}

type blockQuery struct {
	filter     blockFilter
	intColumns []*ColumnInfo
	strColumns []*ColumnInfo
}

type IntResult struct {
	matrix   [][]int64
	hasValue [][]bool
}

type StrResult struct {
	strIdSet map[strId]bool
	matrix   [][]strId
	hasValue [][]bool
}

type BlockQueryResult struct {
	Count     int
	IntResult IntResult
	StrResult StrResult
}

// --------------------------- internal ----------------------------
type numericFilter[T OrderedNumeric] struct {
	localColId localColumnId
	op         pb.FilterOp
	value      T
}

type filterCtx struct {
	ctx        *common.BapiCtx
	bitmap     *bitmap.Bitmap
	queryMinTs int64
	queryMaxTs int64
}

type getCtx struct {
	ctx     *common.BapiCtx
	bitmap  *bitmap.Bitmap
	columns []*ColumnInfo
}

// --------------------------- util ----------------------------
func filterByNullable[T OrderedNumeric](
	ctx *filterCtx,
	filter *numericFilter[T],
	rows []valueIndex,
) {
	// FilterNonnull: check != nullValueIndex
	predicate := predicateNe[valueIndex]
	if filter.op == pb.FilterOp_NULL {
		// check == nullValueIndex
		predicate = predicateEq[valueIndex]
	}

	for idx, row := range rows {
		if !ctx.bitmap.Contains(uint32(idx)) {
			continue
		}

		if !predicate(row, nullValueIndex) {
			ctx.bitmap.Remove(uint32(idx))
		}
	}
}

func getTargetValueAndPredicate[T OrderedNumeric](
	filter *numericFilter[T]) (T, func(T, T) bool, bool) {
	switch filter.op {
	case pb.FilterOp_EQ:
		return filter.value, predicateEq[T], true
	case pb.FilterOp_NE:
		return filter.value, predicateNe[T], true
	case pb.FilterOp_LT:
		return filter.value, predicateLt[T], true
	case pb.FilterOp_GT:
		return filter.value, predicateGt[T], true
	case pb.FilterOp_LE:
		return filter.value, predicateLe[T], true
	case pb.FilterOp_GE:
		return filter.value, predicateGe[T], true
	default:
		return filter.value, nil, false
	}
}

func predicateEq[T OrderedNumeric](left T, right T) bool {
	return left == right
}

func predicateNe[T OrderedNumeric](left T, right T) bool {
	return left != right
}

func predicateLt[T OrderedNumeric](left T, right T) bool {
	return left < right
}

func predicateGt[T OrderedNumeric](left T, right T) bool {
	return left > right
}

func predicateLe[T OrderedNumeric](left T, right T) bool {
	return left <= right
}

func predicateGe[T OrderedNumeric](left T, right T) bool {
	return left >= right
}
