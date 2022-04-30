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
