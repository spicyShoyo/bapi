package store

import (
	"bapi/internal/common"
	"bapi/internal/pb"

	"github.com/kelindar/bitmap"
)

type columnFilter[T comparable] struct {
	col    *ColumnInfo
	op     pb.FilterOp
	values []T
}

type blockFilter struct {
	minTs      int64
	maxTs      int64
	tsFilters  []columnFilter[int64]
	intFilters []columnFilter[int64]
	strFilters []columnFilter[strId]
}

func newBlockFilter(
	minTs int64,
	maxTs int64,
	tsColInfo *ColumnInfo,
	intFilters []columnFilter[int64],
	strFilters []columnFilter[strId],
) blockFilter {
	return blockFilter{
		minTs: minTs,
		maxTs: maxTs,
		tsFilters: []columnFilter[int64]{
			{
				col:    tsColInfo,
				op:     pb.FilterOp_GE,
				values: []int64{minTs},
			},
			{
				col:    tsColInfo,
				op:     pb.FilterOp_LE,
				values: []int64{maxTs},
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
	values     []T
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
	filter *numericFilter[T]) ([]T, func(T, T) bool, bool) {
	switch filter.op {
	case pb.FilterOp_EQ:
		return filter.values, predicateEq[T], true
	case pb.FilterOp_NE:
		return filter.values, predicateNe[T], true
	case pb.FilterOp_LT:
		return filter.values, predicateLt[T], true
	case pb.FilterOp_GT:
		return filter.values, predicateGt[T], true
	case pb.FilterOp_LE:
		return filter.values, predicateLe[T], true
	case pb.FilterOp_GE:
		return filter.values, predicateGe[T], true
	default:
		return filter.values, nil, false
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
