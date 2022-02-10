package store

import (
	"bapi/internal/common"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColInfoStore(t *testing.T) {
	s := newColInfoStore(common.NewBapiCtx())
	_, ok := s.getColumnInfo("col1")
	assert.False(t, ok)

	colId1, err1 := s.getOrRegisterColumnId("col1", StrColumnType)
	colId2, err2 := s.getOrRegisterColumnId("col1", StrColumnType)
	assert.Nil(t, err1)
	assert.Nil(t, err2)
	assert.Equal(t, colId1, colId2)

	colInfo, ok := s.getColumnInfo("col1")
	assert.True(t, ok)
	assert.EqualValues(t, &ColumnInfo{
		id:         colId1,
		Name:       "col1",
		ColumnType: StrColumnType}, colInfo)

	_, err := s.getOrRegisterColumnId("col1", IntColumnType)
	assert.NotNil(t, err)

	colInfo, ok = s.getColumnInfoAndAssertType("col1", StrColumnType)
	assert.True(t, ok)
	assert.Equal(t, colId1, colInfo.id)

	_, ok1 := s.getColumnInfoAndAssertType("col1", IntColumnType)
	_, ok2 := s.getColumnInfoAndAssertType("col2", IntColumnType)
	assert.False(t, ok1)
	assert.False(t, ok2)
}

func TestGetColumnInfoSliceForType(t *testing.T) {
	s := newColInfoStore(common.NewBapiCtx())
	s.getOrRegisterColumnId("col1", StrColumnType)
	s.getOrRegisterColumnId("col2", IntColumnType)
	s.getOrRegisterColumnId("col3", IntColumnType)
	s.getOrRegisterColumnId("col4", StrColumnType)

	columns, allMatch := s.getColumnInfoSliceForType([]string{"col1", "col4"}, StrColumnType)
	assert.True(t, allMatch)
	assert.Equal(t, 2, len(columns))
	assert.Equal(t, "col1", columns[0].Name)
	assert.Equal(t, "col4", columns[1].Name)

	columns, allMatch = s.getColumnInfoSliceForType([]string{"col1", "col2", "col3", "col4", "col5"}, IntColumnType)
	assert.False(t, allMatch)
	assert.Equal(t, 2, len(columns))
	assert.Equal(t, "col2", columns[0].Name)
	assert.Equal(t, "col3", columns[1].Name)
}

// 50 pairs of goroutines writing the same column
func TestColInfoStoreConcurrentReadWrite(t *testing.T) {
	jobs := 100
	s := newColInfoStore(common.NewBapiCtx())
	ids := make([]columnId, jobs)
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
			colName := strconv.Itoa(idx)
			if idx%2 == 1 {
				colName = strconv.Itoa(idx - 1)
			}

			colId, err := s.getOrRegisterColumnId(colName, IntColumnType)

			ids[idx] = colId
			oks[idx] = err == nil
		}()
	}

	close(ready)
	wg.Wait()

	allSuccess := every(oks, func(ok bool) bool { return ok })
	assert.True(t, allSuccess)

	for i := 0; i < jobs; i += 2 {
		// same strId for the same str
		assert.True(t, ids[i] == ids[i+1])
	}

	// no wasted columnId
	assert.Equal(t, columnId(50), s.nextColId)
}

// 20 coroutines try to write a single column
func TestColInfoStoreStressWrite(t *testing.T) {
	jobs := 20
	s := newColInfoStore(common.NewBapiCtx())
	ids := make([]columnId, jobs)
	oks := make([]bool, jobs)

	var wg sync.WaitGroup
	ready := make(chan bool)
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		idx := i
		go func() {
			<-ready
			defer wg.Done()

			id, err := s.getOrRegisterColumnId("hi", StrColumnType)
			ids[idx] = id
			oks[idx] = err == nil
		}()
	}

	close(ready)
	wg.Wait()

	sameId := every(ids, func(id columnId) bool { return id == ids[0] })
	allSuccess := every(oks, func(ok bool) bool { return ok })

	assert.True(t, sameId)
	assert.True(t, allSuccess)
	assert.Equal(t, columnId(1), s.nextColId)
}
