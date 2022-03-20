package store

import (
	"bapi/internal/common"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasher(t *testing.T) {
	hashGroups := debugGetHashGroups([][]interface{}{
		{1, 3, 0, "ok", ""},
		{2, 4, 0, "ads", ""},
		{2, 4, 0, "ads", ""},
	})
	assert.Equal(t, hashGroups[0], []int{0})
	assert.Equal(t, hashGroups[1], []int{1, 2})

	hashGroups = debugGetHashGroups([][]interface{}{
		{2, 0, 9},
		{2, 9, 0},
		{2, 0, 9},
	})
	assert.Equal(t, hashGroups[0], []int{0, 2})
	assert.Equal(t, hashGroups[1], []int{1})

	hashGroups = debugGetHashGroups([][]interface{}{
		{"", ""},
		{"", ""},
		{"", "ok"},
		{"ok", ""},
		{"ok", ""},
	})
	assert.Equal(t, hashGroups[0], []int{0, 1})
	assert.Equal(t, hashGroups[1], []int{2})
	assert.Equal(t, hashGroups[2], []int{3, 4})
}

func debugGetHashGroups(rows [][]interface{}) [][]int {
	blkRes, _ := debugNewBlockQueryResult(rows)
	aggCtx := &aggCtx{
		groupbyIntColCnt: len(blkRes.IntResult.matrix),
		intColCnt:        len(blkRes.IntResult.matrix),
		groupbyStrColCnt: len(blkRes.StrResult.matrix),
		strColCnt:        len(blkRes.StrResult.matrix),
	}
	hashes := buildHasherForBlock(aggCtx, blkRes).getHashes()

	hashMap := make(map[uint64]int)
	hashGroups := make([][]int, 0)
	for rowIdx, hash := range hashes {
		if _, ok := hashMap[hash]; !ok {
			hashMap[hash] = len(hashGroups)
			hashGroups = append(hashGroups, make([]int, 0))
		}
		hashGroups[hashMap[hash]] = append(hashGroups[hashMap[hash]], rowIdx)
	}

	return hashGroups
}

// Creates a BlockQueryResult for testing. Returns nil if invalid.
// int of 0 and string of "" are considered as no value.
func debugNewBlockQueryResult(rows [][]interface{}) (*BlockQueryResult, *basicStrStore) {
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil, nil
	}

	rowCount := len(rows)
	colCount := len(rows[0])
	for _, row := range rows {
		if colCount != len(row) {
			return nil, nil
		}
	}

	intColCnt := 0
	for _, val := range rows[0] {
		if _, ok := val.(int); !ok {
			break
		}
		intColCnt++
	}

	intVals := make([][]int64, intColCnt)
	intHasVals := make([][]bool, intColCnt)
	strVals := make([][]strId, colCount-intColCnt)
	strHasVals := make([][]bool, colCount-intColCnt)
	for i := 0; i < intColCnt; i++ {
		intVals[i] = make([]int64, rowCount)
		intHasVals[i] = make([]bool, rowCount)
	}
	for i := 0; i < colCount-intColCnt; i++ {
		strVals[i] = make([]strId, rowCount)
		strHasVals[i] = make([]bool, rowCount)
	}

	strStore := newBasicStrStore(common.NewTestBapiCtx())
	strIdSet := make(map[strId]bool)

	for rowIdx, row := range rows {
		for valIdx, val := range row {
			if valIdx < intColCnt {
				intVal, ok := val.(int)
				if !ok {
					return nil, nil
				}
				colIdx := valIdx
				intHasVals[colIdx][rowIdx] = intVal != 0
				intVals[colIdx][rowIdx] = int64(intVal)

				continue
			}

			strVal, ok := val.(string)
			if !ok {
				return nil, nil
			}
			colIdx := valIdx - intColCnt
			strId, _, _ := strStore.getOrInsertStrId(strVal)
			strHasVals[colIdx][rowIdx] = strVal != ""
			strVals[colIdx][rowIdx] = strId
			strIdSet[strId] = true
		}
	}

	return &BlockQueryResult{
		Count: len(rows),
		IntResult: IntResult{
			matrix:   intVals,
			hasValue: intHasVals,
		},
		StrResult: StrResult{
			strIdSet: strIdSet,
			matrix:   strVals,
			hasValue: strHasVals,
		},
	}, strStore
}
