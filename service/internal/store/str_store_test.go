package store

import (
	"bapi/internal/common"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStrStore(t *testing.T) {
	s := newBasicStrStore(common.NewBapiCtx())
	id, loaded, ok := s.getOrInsertStrId("hi")
	assert.True(t, ok)
	assert.False(t, loaded)
	assert.Equal(t, strId(0), id)
	id, loaded, ok = s.getOrInsertStrId("hello")
	assert.True(t, ok)
	assert.False(t, loaded)
	assert.Equal(t, strId(1), id)
	id, loaded, ok = s.getOrInsertStrId("hello")
	assert.True(t, ok)
	assert.True(t, loaded)
	assert.Equal(t, strId(1), id)

	id, ok = s.getStrId("hello")
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

// 50 pairs of goroutines writing the same string
func TestConcurrentReadWrite(t *testing.T) {
	jobs := 100
	s := newBasicStrStore(common.NewBapiCtx())
	ids := make([]strId, jobs)
	loadeds := make([]bool, jobs)
	oks := make([]bool, jobs)

	var wg sync.WaitGroup
	ready := make(chan bool)
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		idx := i
		go func() {
			<-ready
			defer wg.Done()

			// for each even idx, 2 goroutines try to write
			str := strconv.Itoa(idx)
			if idx%2 == 1 {
				str = strconv.Itoa(idx - 1)
			}

			id, loaded, ok := s.getOrInsertStrId(str)
			ids[idx] = id
			loadeds[idx] = loaded
			oks[idx] = ok
		}()
	}

	close(ready)
	wg.Wait()

	allSuccess := every(oks, func(ok bool) bool { return ok })
	assert.True(t, allSuccess)

	for i := 0; i < jobs; i += 2 {
		// same strId for the same str
		assert.True(t, ids[i] == ids[i+1])
		// single insert
		assert.True(t, loadeds[i] != loadeds[i+1])
	}
}

// 20 coroutines try to write a single string
func TestConcurrentStressWrite(t *testing.T) {
	jobs := 20
	s := newBasicStrStore(common.NewBapiCtx())
	ids := make([]strId, jobs)
	loadeds := make([]bool, jobs)
	oks := make([]bool, jobs)

	var wg sync.WaitGroup
	ready := make(chan bool)
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		idx := i
		go func() {
			<-ready
			defer wg.Done()

			id, loaded, ok := s.getOrInsertStrId("hi")
			ids[idx] = id
			loadeds[idx] = loaded
			oks[idx] = ok
		}()
	}

	close(ready)
	wg.Wait()

	sameId := every(ids, func(id strId) bool { return id == ids[0] })
	allSuccess := every(oks, func(ok bool) bool { return ok })
	insertCount := 0
	for _, loaded := range loadeds {
		if !loaded {
			insertCount++
		}
	}

	assert.True(t, sameId)
	assert.True(t, allSuccess)
	assert.Equal(t, 1, insertCount)
}
