package store

import "bapi/internal/pb"

const (
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
}

func newAggOpIntResult[T OrderedNumeric](intVal int64) aggOpResult[T] {
	return aggOpResult[T]{intVal: intVal, valType: aggOpIntRes}
}

func newAggOpFloatResult[T OrderedNumeric](floatVal float64) aggOpResult[T] {
	return aggOpResult[T]{floatVal: floatVal, valType: aggOpFloatRes}
}

func newAggOpGenericResult[T OrderedNumeric](genericVal T) aggOpResult[T] {
	return aggOpResult[T]{genericVal: genericVal, valType: aggOpGenericRes}
}

type aggOp[T OrderedNumeric] interface {
	addValue(T)
	consume(aggOp[T])
	finalize() aggOpResult[T]
}

func getAggOp[T OrderedNumeric](op pb.AggOp) (bool, aggOp[T]) {
	switch op {
	case pb.AggOp_COUNT:
		return true, newAggOpCount[T]()
	case pb.AggOp_COUNT_DISTINCT:
		return true, newAggOpCountDistinct[T]()
	case pb.AggOp_SUM:
		return true, newAggOpSum[T]()
	case pb.AggOp_AVG:
		return true, newAggOpAvg[T]()
	default:
		return false, nil
	}
}

// --------------------------- aggOpCount ---------------------------
type aggOpCount[T OrderedNumeric] struct {
	count int64
}

func newAggOpCount[T OrderedNumeric]() *aggOpCount[T] {
	return &aggOpCount[T]{}
}

func (op *aggOpCount[T]) addValue(T) {
	op.count += 1
}

func (op *aggOpCount[T]) consume(other aggOp[T]) {
	op.count += other.(*aggOpCount[T]).count
}

func (op *aggOpCount[T]) finalize() aggOpResult[T] {
	return newAggOpIntResult[T](op.count)
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
	return newAggOpIntResult[T](int64(len(op.m)))
}

// --------------------------- aggOpSum ---------------------------
type aggOpSum[T OrderedNumeric] struct {
	sum T
}

func newAggOpSum[T OrderedNumeric]() *aggOpSum[T] {
	return &aggOpSum[T]{sum: T(0)}
}

func (op *aggOpSum[T]) addValue(v T) {
	// ! overflow is not handled, but fine for now
	op.sum += v
}

func (op *aggOpSum[T]) consume(other aggOp[T]) {
	op.sum += other.(*aggOpSum[T]).sum
}

func (op *aggOpSum[T]) finalize() aggOpResult[T] {
	return newAggOpGenericResult(op.sum)
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
	return newAggOpFloatResult[T](float64(op.sum) / float64(op.count))
}
