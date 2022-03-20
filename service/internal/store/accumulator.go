package store

import (
	"bapi/internal/pb"
)

// Responsible for accumulating values of a given col
type accumulator[T OrderedNumeric] interface {
	addValue(T)
	consume(accumulator[T])
	finalize() accResult[T]
	new() accumulator[T]
}

// The type of the accumulated value for a col.
const (
	accInvalidRes       = iota
	accIntRes           = iota
	accFloatRes         = iota
	accTimelineCountRes = iota
	accGenericRes       = iota
)

// Wraps the return value of a accumulator due to the generic appOp interface
type accResult[T OrderedNumeric] struct {
	intVal           int64
	floatVal         float64
	genericVal       T
	timelintCountVal map[int64]int
	valType          int
	hasValue         bool
}

// Creates a slice of accumulator for the given pb.AggOp and the number of cols
func getAccumulatorSlice[T OrderedNumeric](op pb.AggOp, colCount int) ([]accumulator[T], bool) {
	var templteAccumulator accumulator[T]
	switch op {
	case pb.AggOp_COUNT:
		templteAccumulator = newAccumulatorCount[T]()
	case pb.AggOp_COUNT_DISTINCT:
		templteAccumulator = newAccumulatorCountDistinct[T]()
	case pb.AggOp_SUM:
		templteAccumulator = newAccumulatorSum[T]()
	case pb.AggOp_AVG:
		templteAccumulator = newAccumulatorAvg[T]()
	case pb.AggOp_TIMELINE_COUNT:
		templteAccumulator = newAccumulatorTimelineCount[T]()
	default:
		return nil, false
	}

	s := make([]accumulator[T], colCount)
	for i := 0; i < colCount; i++ {
		s[i] = templteAccumulator.new()
	}
	return s, true
}

// --------------------------- constructors for accResult ---------------------------
func newAccIntResult[T OrderedNumeric](intVal int64, hasValue bool) accResult[T] {
	return accResult[T]{intVal: intVal, valType: accIntRes, hasValue: hasValue}
}

func newAccFloatResult[T OrderedNumeric](floatVal float64, hasValue bool) accResult[T] {
	return accResult[T]{floatVal: floatVal, valType: accFloatRes, hasValue: hasValue}
}

func newAccGenericResult[T OrderedNumeric](genericVal T, hasValue bool) accResult[T] {
	return accResult[T]{genericVal: genericVal, valType: accGenericRes, hasValue: hasValue}
}

func newAccTimelineCountResult[T OrderedNumeric](timelintCountVal map[int64]int, hasValue bool) accResult[T] {
	return accResult[T]{timelintCountVal: timelintCountVal, valType: accTimelineCountRes, hasValue: hasValue}
}

// --------------------------- accumulatorCount ---------------------------
type accumulatorCount[T OrderedNumeric] struct {
	count int64
}

func newAccumulatorCount[T OrderedNumeric]() *accumulatorCount[T] {
	return &accumulatorCount[T]{count: 0}
}

func (op *accumulatorCount[T]) new() accumulator[T] {
	return newAccumulatorCount[T]()
}

func (op *accumulatorCount[T]) addValue(T) {
	op.count += 1
}

func (op *accumulatorCount[T]) consume(other accumulator[T]) {
	op.count += other.(*accumulatorCount[T]).count
}

func (op *accumulatorCount[T]) finalize() accResult[T] {
	return newAccIntResult[T](op.count, true /*hasValue*/)
}

// --------------------------- accumulatorCountDistinct ---------------------------
type accumulatorCountDistinct[T OrderedNumeric] struct {
	m map[T]bool
}

func newAccumulatorCountDistinct[T OrderedNumeric]() *accumulatorCountDistinct[T] {
	return &accumulatorCountDistinct[T]{m: make(map[T]bool)}
}

func (op *accumulatorCountDistinct[T]) new() accumulator[T] {
	return newAccumulatorCountDistinct[T]()
}

func (op *accumulatorCountDistinct[T]) addValue(v T) {
	op.m[v] = true
}

func (op *accumulatorCountDistinct[T]) consume(other accumulator[T]) {
	for v := range other.(*accumulatorCountDistinct[T]).m {
		op.m[v] = true
	}
}

func (op *accumulatorCountDistinct[T]) finalize() accResult[T] {
	return newAccIntResult[T](int64(len(op.m)), true /*hasValue*/)
}

// --------------------------- accumulatorSum ---------------------------
type accumulatorSum[T OrderedNumeric] struct {
	sum      T
	hasValue bool
}

func newAccumulatorSum[T OrderedNumeric]() *accumulatorSum[T] {
	return &accumulatorSum[T]{sum: T(0), hasValue: false}
}

func (op *accumulatorSum[T]) new() accumulator[T] {
	return newAccumulatorSum[T]()
}

func (op *accumulatorSum[T]) addValue(v T) {
	// ! overflow is not handled, but fine for now
	op.sum += v
	op.hasValue = true
}

func (op *accumulatorSum[T]) consume(other accumulator[T]) {
	op.sum += other.(*accumulatorSum[T]).sum
	op.hasValue = op.hasValue || other.(*accumulatorSum[T]).hasValue
}

func (op *accumulatorSum[T]) finalize() accResult[T] {
	return newAccGenericResult(op.sum, op.hasValue)
}

// --------------------------- accumulatorAvg ---------------------------
type accumulatorAvg[T OrderedNumeric] struct {
	sum   T
	count int
}

func newAccumulatorAvg[T OrderedNumeric]() *accumulatorAvg[T] {
	return &accumulatorAvg[T]{sum: T(0)}
}

func (op *accumulatorAvg[T]) new() accumulator[T] {
	return newAccumulatorAvg[T]()
}

func (op *accumulatorAvg[T]) addValue(v T) {
	// ! overflow is not handled, but fine for now
	op.sum += v
	op.count += 1
}

func (op *accumulatorAvg[T]) consume(other accumulator[T]) {
	op.sum += other.(*accumulatorAvg[T]).sum
	op.count += other.(*accumulatorAvg[T]).count
}

func (op *accumulatorAvg[T]) finalize() accResult[T] {
	if op.count == 0 {
		return newAccFloatResult[T](0, false /*hasValue*/)
	}
	return newAccFloatResult[T](float64(op.sum)/float64(op.count), true /*hasValue*/)
}

// --------------------------- accumulatorTimelineCount ---------------------------
type accumulatorTimelineCount[T OrderedNumeric] struct {
	m map[int64]int
}

func newAccumulatorTimelineCount[T OrderedNumeric]() *accumulatorTimelineCount[T] {
	return &accumulatorTimelineCount[T]{m: make(map[int64]int)}
}

func (op *accumulatorTimelineCount[T]) new() accumulator[T] {
	return newAccumulatorAvg[T]()
}

// The caller is responsible to make sure v is the ts bucket
func (op *accumulatorTimelineCount[T]) addValue(v T) {
	// https://github.com/golang/go/issues/49206
	bucket := (interface{})(v).(int64)
	if _, ok := op.m[bucket]; !ok {
		op.m[bucket] = 0
	}
	op.m[bucket] += 1
}

func (op *accumulatorTimelineCount[T]) consume(other accumulator[T]) {
	for bucket, count := range other.(*accumulatorTimelineCount[T]).m {
		if _, ok := op.m[bucket]; !ok {
			op.m[bucket] = 0
		}
		op.m[bucket] += count
	}
}

func (op *accumulatorTimelineCount[T]) finalize() accResult[T] {
	return newAccTimelineCountResult[T](op.m, len(op.m) != 0)
}
