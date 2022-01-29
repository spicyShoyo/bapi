package store

type OrderedNumeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64
}

func max[T OrderedNumeric](left T, right T) T {
	if left > right {
		return left
	}
	return right
}

func min[T OrderedNumeric](left T, right T) T {
	if left < right {
		return left
	}
	return right
}

func every[T any](s []T, f func(T) bool) bool {
	for _, v := range s {
		if !f(v) {
			return false
		}
	}
	return true
}

func some[T any](s []T, f func(T) bool) bool {
	for _, v := range s {
		if f(v) {
			return true
		}
	}
	return false
}

func forEach[T any](s []T, f func(T)) {
	for _, v := range s {
		f(v)
	}
}

func make2dSlice[T any](matrixRowCount int, matrixColCount int) ([][]T, bool) {
	if matrixRowCount < 0 || matrixColCount < 0 {
		return nil, false
	}
	matrix := make([][]T, matrixRowCount)
	for rowIdx := 0; rowIdx < matrixRowCount; rowIdx++ {
		matrix[rowIdx] = make([]T, matrixColCount)
	}
	return matrix, true
}
