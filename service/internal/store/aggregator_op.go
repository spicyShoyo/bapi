package store

import "bapi/internal/pb"

const (
	aggOpInvalidRes = iota
	aggOpIntRes     = iota
	aggOpFloatRes   = iota
	aggOpGenericRes = iota
)

// Wraps the return value of a aggOp due to the generic appOp interface
type aggOpResult[T OrderedNumeric] struct {
	intVal     int64
	floatVal   float64
	genericVal T
	valType    int
	hasValue   bool
}

func newAggOpIntResult[T OrderedNumeric](intVal int64, hasValue bool) aggOpResult[T] {
	return aggOpResult[T]{intVal: intVal, valType: aggOpIntRes, hasValue: hasValue}
}

func newAggOpFloatResult[T OrderedNumeric](floatVal float64, hasValue bool) aggOpResult[T] {
	return aggOpResult[T]{floatVal: floatVal, valType: aggOpFloatRes, hasValue: hasValue}
}

func newAggOpGenericResult[T OrderedNumeric](genericVal T, hasValue bool) aggOpResult[T] {
	return aggOpResult[T]{genericVal: genericVal, valType: aggOpGenericRes, hasValue: hasValue}
}

type aggOp[T OrderedNumeric] interface {
	addValue(T)
	consume(aggOp[T])
	finalize() aggOpResult[T]
}

func getAggOpSlice[T OrderedNumeric](op pb.AggOp, colCount int) ([]aggOp[T], bool) {
	s := make([]aggOp[T], colCount)
	for i := 0; i < colCount; i++ {
		aggOp, ok := getAggOp[T](op)
		if !ok {
			return nil, false
		}
		s[i] = aggOp
	}
	return s, true
}

func getAggOp[T OrderedNumeric](op pb.AggOp) (aggOp[T], bool) {
	switch op {
	case pb.AggOp_COUNT:
		return newAggOpCount[T](), true
	case pb.AggOp_COUNT_DISTINCT:
		return newAggOpCountDistinct[T](), true
	case pb.AggOp_SUM:
		return newAggOpSum[T](), true
	case pb.AggOp_AVG:
		return newAggOpAvg[T](), true
	default:
		return nil, false
	}
}

func getAggResultType[T OrderedNumeric](op pb.AggOp) (int, bool) {
	switch op {
	case pb.AggOp_COUNT:
		return aggOpIntRes, true
	case pb.AggOp_COUNT_DISTINCT:
		return aggOpIntRes, true
	case pb.AggOp_SUM:
		return aggOpGenericRes, true
	case pb.AggOp_AVG:
		return aggOpFloatRes, true
	default:
		return aggOpInvalidRes, false
	}
}

// --------------------------- aggOpCount ---------------------------
type aggOpCount[T OrderedNumeric] struct {
	count int64
}

func newAggOpCount[T OrderedNumeric]() *aggOpCount[T] {
	return &aggOpCount[T]{count: 0}
}

func (op *aggOpCount[T]) addValue(T) {
	op.count += 1
}

func (op *aggOpCount[T]) consume(other aggOp[T]) {
	op.count += other.(*aggOpCount[T]).count
}

func (op *aggOpCount[T]) finalize() aggOpResult[T] {
	return newAggOpIntResult[T](op.count, true /*hasValue*/)
}

// --------------------------- aggOpCountDistinct ---------------------------
type aggOpCountDistinct[T OrderedNumeric] struct {
	m map[T]bool
}

func newAggOpCountDistinct[T OrderedNumeric]() *aggOpCountDistinct[T] {
	return &aggOpCountDistinct[T]{m: make(map[T]bool)}
}

func (op *aggOpCountDistinct[T]) addValue(v T) {
	op.m[v] = true
}

func (op *aggOpCountDistinct[T]) consume(other aggOp[T]) {
	for v := range other.(*aggOpCountDistinct[T]).m {
		op.m[v] = true
	}
}

func (op *aggOpCountDistinct[T]) finalize() aggOpResult[T] {
	return newAggOpIntResult[T](int64(len(op.m)), true /*hasValue*/)
}

// --------------------------- aggOpSum ---------------------------
type aggOpSum[T OrderedNumeric] struct {
	sum      T
	hasValue bool
}

func newAggOpSum[T OrderedNumeric]() *aggOpSum[T] {
	return &aggOpSum[T]{sum: T(0), hasValue: false}
}

func (op *aggOpSum[T]) addValue(v T) {
	// ! overflow is not handled, but fine for now
	op.sum += v
	op.hasValue = true
}

func (op *aggOpSum[T]) consume(other aggOp[T]) {
	op.sum += other.(*aggOpSum[T]).sum
	op.hasValue = op.hasValue || other.(*aggOpSum[T]).hasValue
}

func (op *aggOpSum[T]) finalize() aggOpResult[T] {
	return newAggOpGenericResult(op.sum, op.hasValue)
}

// --------------------------- aggOpAvg ---------------------------
type aggOpAvg[T OrderedNumeric] struct {
	sum   T
	count int
}

func newAggOpAvg[T OrderedNumeric]() *aggOpAvg[T] {
	return &aggOpAvg[T]{sum: T(0)}
}

func (op *aggOpAvg[T]) addValue(v T) {
	// ! overflow is not handled, but fine for now
	op.sum += v
	op.count += 1
}

func (op *aggOpAvg[T]) consume(other aggOp[T]) {
	op.sum += other.(*aggOpAvg[T]).sum
	op.count += other.(*aggOpAvg[T]).count
}

func (op *aggOpAvg[T]) finalize() aggOpResult[T] {
	if op.count == 0 {
		return newAggOpFloatResult[T](0, false /*hasValue*/)
	}
	return newAggOpFloatResult[T](float64(op.sum)/float64(op.count), true /*hasValue*/)
}
