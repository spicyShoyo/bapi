package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStrStore(t *testing.T) {
	s := newBasicStrStore()
	id, loaded := s.getOrInsertStrId("hi")
	assert.False(t, loaded)
	assert.Equal(t, strId(0), id)
	id, loaded = s.getOrInsertStrId("hello")
	assert.False(t, loaded)
	assert.Equal(t, strId(1), id)
	id, loaded = s.getOrInsertStrId("hello")
	assert.True(t, loaded)
	assert.Equal(t, strId(1), id)

	id, ok := s.getStrId("hello")
	assert.True(t, ok)
	assert.Equal(t, strId(1), id)
	_, ok = s.getStrId("world")
	assert.False(t, ok)

	str, ok := s.getStr(strId(1))
	assert.True(t, ok)
	assert.Equal(t, "hello", str)
	str, ok = s.getStr(strId(0))
	assert.True(t, ok)
	assert.Equal(t, "hi", str)
	_, ok = s.getStr(strId(2))
	assert.False(t, ok)
}
