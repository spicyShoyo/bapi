package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaxAndMin(t *testing.T) {
	assert.Equal(t, max(1, 2), 2)
	assert.Equal(t, min(1, 2), 1)
}

func TestEveryAndSome(t *testing.T) {
	assert.True(t, every([]int{}, func(v int) bool { return false }))
	assert.False(t, some([]int{}, func(v int) bool { return true }))

	largerThanZero := func(v int) bool { return v > 0 }
	largerThanOne := func(v int) bool { return v > 1 }
	largerThanTwo := func(v int) bool { return v > 2 }

	assert.True(t, every([]int{1, 2}, largerThanZero))
	assert.False(t, every([]int{1, 2}, largerThanOne))

	assert.True(t, some([]int{1, 2}, largerThanOne))
	assert.False(t, some([]int{1, 2}, largerThanTwo))
}

func TestForEach(t *testing.T) {
	values := make(map[string]bool)
	forEach([]string{"4", "5", "6"}, func(v string) {
		values[v] = true
	})
	assert.EqualValues(t, values, map[string]bool{"4": true, "5": true, "6": true})
}

func TestMake2dSlice(t *testing.T) {
	_, isValid := make2dSlice[int](-1, 2)
	assert.False(t, isValid)
	_, isValid = make2dSlice[int](2, -3)
	assert.False(t, isValid)

	matrix, _ := make2dSlice[int](2, 3)
	assert.Equal(t, len(matrix), 2)
	for _, row := range matrix {
		assert.Equal(t, len(row), 3)
	}
}
