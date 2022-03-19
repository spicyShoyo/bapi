package store

import (
	"bapi/internal/common"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasher(t *testing.T) {
	assert.True(t, true)

	blkRes, _ := debugNewBlockQueryResult([][]interface{}{
		{1, 3, 0, "ok"},
		{2, 4, 0, "ads"},
		{2, 4, 0, "ads"}})

	aggCtx := &aggCtx{
		firstAggIntCol: 2,
		intColCnt:      3,
	}
	hashes := buildHasherForBlock(aggCtx, blkRes).getHashes()

	assert.NotEqual(t, hashes[0], hashes[1])
	assert.Equal(t, hashes[1], hashes[2])
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
			strHasVals[colIdx][rowIdx] = strVal == ""
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
