package store

import (
	"bapi/internal/common"
	"bapi/internal/pb"
	"fmt"

	"github.com/kelindar/bitmap"
)

type FilterOp = uint8

const (
	FilterEq      FilterOp = iota
	FilterNe      FilterOp = iota
	FilterLt      FilterOp = iota
	FilterGt      FilterOp = iota
	FilterLe      FilterOp = iota
	FilterGe      FilterOp = iota
	FilterNonnull FilterOp = iota
	FilterNull    FilterOp = iota
)

func fromPbFilter(pbFilterOp pb.FilterOp) FilterOp {
	switch pbFilterOp {
	case pb.FilterOp_EQ:
		return FilterEq
	case pb.FilterOp_NE:
		return FilterNe
	case pb.FilterOp_LT:
		return FilterLt
	case pb.FilterOp_GT:
		return FilterGt
	case pb.FilterOp_LE:
		return FilterLe
	case pb.FilterOp_GE:
		return FilterGe
	case pb.FilterOp_NONNULL:
		return FilterNonnull
	case pb.FilterOp_NULL:
		return FilterNull
	default:
		panic(fmt.Sprintf("unknown filter type: %d", pbFilterOp))
	}
}

type IntFilter struct {
	ColumnInfo *ColumnInfo
	FilterOp   FilterOp
	Value      int64
}

type StrFilter struct {
	ColumnInfo *ColumnInfo
	FilterOp   FilterOp
	Value      string
}

type BlockFilter struct {
	MinTs      int64
	MaxTs      int64
	intFilters []IntFilter
	strFilters []StrFilter
}

type blockQuery struct {
	filter     BlockFilter
	intColumns []*ColumnInfo
	strColumns []*ColumnInfo
}

type IntResult struct {
	matrix   [][]int64
	hasValue [][]bool
}

type StrResult struct {
	strIdMap map[strId]string
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
	op         FilterOp
	value      T
}

type filterCtx struct {
	ctx      *common.BapiCtx
	bitmap   *bitmap.Bitmap
	startIdx uint32
	endIdx   uint32
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
	if filter.op == FilterNull {
		// check == nullValueIndex
		predicate = predicateEq[valueIndex]
	}

	for idx := ctx.startIdx; idx <= ctx.endIdx; idx++ {
		if !ctx.bitmap.Contains(idx) {
			continue
		}

		if !predicate(rows[idx], nullValueIndex) {
			ctx.bitmap.Remove(idx)
		}
	}
}

func getTargetValueAndPredicate[T OrderedNumeric](
	filter *numericFilter[T]) (T, func(T, T) bool, bool) {
	switch filter.op {
	case FilterEq:
		return filter.value, predicateEq[T], true
	case FilterNe:
		return filter.value, predicateNe[T], true
	case FilterLt:
		return filter.value, predicateLt[T], true
	case FilterGt:
		return filter.value, predicateGt[T], true
	case FilterLe:
		return filter.value, predicateLe[T], true
	case FilterGe:
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
