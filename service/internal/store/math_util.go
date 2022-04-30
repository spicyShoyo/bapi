package store

import "golang.org/x/exp/constraints"

type numeric interface {
	constraints.Integer | constraints.Float
}

func max[T numeric](left T, right T) T {
	if left > right {
		return left
	}
	return right
}

func min[T numeric](left T, right T) T {
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
