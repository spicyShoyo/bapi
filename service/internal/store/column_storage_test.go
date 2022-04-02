package store

import (
	"bapi/internal/common"
	"bapi/internal/pb"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --------------------------- intColumnsStorage ----------------------------
func TestNewIntColumnsStorage(t *testing.T) {
	rows := debugRows[int64]{
		0: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091786),
			debugNewDebugPair[int64](columnId(22), 15),
			debugNewDebugPair[int64](columnId(23), 16),
		},
		1: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091788),
			debugNewDebugPair[int64](columnId(22), 19),
			debugNewDebugPair[int64](columnId(28), 20),
		},
		2: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091790),
			debugNewDebugPair[int64](columnId(22), 15),
		},
	}

	debugNewIntColumnsStorageFromRows(t, rows)
}

func TestFilterIntColumnsStorage(t *testing.T) {
	rows := debugRows[int64]{
		0: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091786),
			debugNewDebugPair[int64](columnId(22), 15),
			debugNewDebugPair[int64](columnId(23), 16),
		},
		1: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091788),
			debugNewDebugPair[int64](columnId(22), 19),
			debugNewDebugPair[int64](columnId(28), 20),
		},
		2: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091790),
			debugNewDebugPair[int64](columnId(22), 15),
		},
	}
	debugNewIntColumnsStorageFromRows(t, rows)

	// col_28 < 21 && col_22 <= 19
	assertIntFilterHasResult(t, debugFilterTestSetup[int64, int64]{
		rows: rows, rowCount: 3,
		filters: []debugFilter[int64]{
			{
				colId: columnId(28),
				op:    pb.FilterOp_LT,
				value: 21,
			},
			{
				colId: columnId(22),
				op:    pb.FilterOp_LE,
				value: 19,
			},
		},
		expectedRows: []uint32{1},
	})

	// col_22 >= 15
	assertIntFilterHasResult(t, debugFilterTestSetup[int64, int64]{
		rows: rows, rowCount: 3,
		filters: []debugFilter[int64]{{
			colId: columnId(22),
			op:    pb.FilterOp_EQ,
			value: 15,
		}},
		expectedRows: []uint32{0, 2},
	})

	// col_22 == 15 && col_23 != null && col28 == null
	assertIntFilterHasResult(t, debugFilterTestSetup[int64, int64]{
		rows: rows, rowCount: 3,
		filters: []debugFilter[int64]{
			{
				colId: columnId(22),
				op:    pb.FilterOp_EQ,
				value: 15,
			},
			{
				colId: columnId(23),
				op:    pb.FilterOp_NONNULL,
				value: 0,
			},
			{
				colId: columnId(28),
				op:    pb.FilterOp_NULL,
				value: 0,
			},
		},
		expectedRows: []uint32{0},
	})

	// col_22 != 15 && col_22 != null && col_28 != null
	assertIntFilterHasResult(t, debugFilterTestSetup[int64, int64]{
		rows: rows, rowCount: 3,
		filters: []debugFilter[int64]{
			{
				colId: columnId(22),
				op:    pb.FilterOp_NE,
				value: 15,
			},
			{
				colId: columnId(22),
				op:    pb.FilterOp_NONNULL,
				value: 0,
			},
			{
				colId: columnId(28),
				op:    pb.FilterOp_NONNULL,
				value: 0,
			},
		},
		expectedRows: []uint32{1},
	})

	// col_29 == null && col_30 != "test"
	assertIntFilterHasResult(t, debugFilterTestSetup[int64, int64]{
		rows: rows, rowCount: 3,
		filters: []debugFilter[int64]{
			{
				colId: columnId(29),
				op:    pb.FilterOp_NULL,
				value: 0,
			},
			{
				colId: columnId(30),
				op:    pb.FilterOp_NE,
				value: 32,
			},
		},
		expectedRows: []uint32{0, 1, 2},
	})
}

func TestGetIntColumnsStorage(t *testing.T) {
	rows := debugRows[int64]{
		0: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091786),
			debugNewDebugPair[int64](columnId(22), 15),
			debugNewDebugPair[int64](columnId(23), 16),
		},
		1: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091788),
			debugNewDebugPair[int64](columnId(22), 19),
			debugNewDebugPair[int64](columnId(28), 20),
		},
		2: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091790),
			debugNewDebugPair[int64](columnId(22), 15),
		},
		3: {
			debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091796),
			debugNewDebugPair[int64](columnId(30), 21),
		},
	}

	debugNewIntColumnsStorageFromRows(t, rows)

	assertGetResultIntStorage(t, debugGetTestSetup[int64]{
		colType:         IntColumnType,
		rows:            rows,
		colIds:          []columnId{columnId(TS_COLUMN_ID), columnId(22)},
		requestedRowIds: []uint32{0, 1, 3},
		expectedRows: debugRows[int64]{
			0: {
				debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091786),
				debugNewDebugPair[int64](columnId(22), 15),
			},
			1: {
				debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091788),
				debugNewDebugPair[int64](columnId(22), 19),
			},
			3: {
				debugNewDebugPair[int64](columnId(TS_COLUMN_ID), 1643091796),
			},
		},
	})
}

// --------------------------- strColumnsStorage ----------------------------
func TestNewStrColumnsStorage(t *testing.T) {
	rows := debugRows[strId]{
		0: {
			debugNewDebugPair(columnId(22), strId(15)),
			debugNewDebugPair(columnId(23), strId(16)),
		},
		5: {
			debugNewDebugPair(columnId(22), strId(19)),
			debugNewDebugPair(columnId(28), strId(20)),
		},
		6: {
			debugNewDebugPair(columnId(22), strId(15)),
		},
	}
	debugNewStrColumnsStorageFromRows(t, rows, 10 /* totalRowCount */)
}

func TestFilterStrColumnsStorage(t *testing.T) {
	rows := debugRows[strId]{
		0: {
			debugNewDebugPair(columnId(22), strId(15)),
			debugNewDebugPair(columnId(23), strId(16)),
		},
		5: {
			debugNewDebugPair(columnId(22), strId(19)),
			debugNewDebugPair(columnId(28), strId(20)),
		},
		6: {
			debugNewDebugPair(columnId(22), strId(15)),
		},
	}

	// col_22 == "15"
	assertStrFilterHasResult(t, debugFilterTestSetup[strId, string]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[string]{{
			colId: columnId(22),
			op:    pb.FilterOp_EQ,
			value: "15",
		}},
		expectedRows: []uint32{0, 6},
	})

	// col_22 == "15" && col_23 != null && col28 == null
	assertStrFilterHasResult(t, debugFilterTestSetup[strId, string]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[string]{
			{
				colId: columnId(22),
				op:    pb.FilterOp_EQ,
				value: "15",
			},
			{
				colId: columnId(23),
				op:    pb.FilterOp_NONNULL,
				value: "",
			},
			{
				colId: columnId(28),
				op:    pb.FilterOp_NULL,
				value: "",
			},
		},
		expectedRows: []uint32{0},
	})

	// col_22 != "15" && col_22 != null
	assertStrFilterHasResult(t, debugFilterTestSetup[strId, string]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[string]{
			{
				colId: columnId(22),
				op:    pb.FilterOp_NE,
				value: "15",
			},
			{
				colId: columnId(28),
				op:    pb.FilterOp_NONNULL,
				value: "",
			},
		},
		expectedRows: []uint32{5},
	})

	// col_29 == null && col_30 != "test"
	assertStrFilterHasResult(t, debugFilterTestSetup[strId, string]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[string]{
			{
				colId: columnId(29),
				op:    pb.FilterOp_NULL,
				value: "",
			},
			{
				colId: columnId(30),
				op:    pb.FilterOp_NE,
				value: "test",
			},
		},
		expectedRows: []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
	})
}

func TestGetStrColumnsStorage(t *testing.T) {
	rows := debugRows[strId]{
		0: {
			debugNewDebugPair(columnId(22), strId(15)),
			debugNewDebugPair(columnId(23), strId(16)),
		},
		5: {
			debugNewDebugPair(columnId(22), strId(19)),
			debugNewDebugPair(columnId(28), strId(20)),
		},
		6: {
			debugNewDebugPair(columnId(22), strId(15)),
		},
	}

	assertGetResultStrStorage(t, debugGetTestSetup[strId]{
		colType:         StrColumnType,
		rows:            rows,
		colIds:          []columnId{columnId(22)},
		requestedRowIds: []uint32{0, 2, 3, 5, 6},
		expectedRows: debugRows[strId]{
			0: {
				debugNewDebugPair(columnId(22), strId(15)),
			},
			2: {}, // in storage but has no value in col22
			3: {}, // not in storage but requested
			5: {
				debugNewDebugPair(columnId(22), strId(19)),
			},
			6: {
				debugNewDebugPair(columnId(22), strId(15)),
			},
		},
	})
}

// --------------------------- test util ----------------------------
func debugNewIntColumnsStorageFromRows(t *testing.T, rows debugRows[int64]) *intColumnsStorage {
	// since we require every row to have ts, we can assume there is no row not in the intColStorage
	totalRowCount := len(rows)

	intStorage, err := newIntColumnsStorage(
		debugNewPartialColumns(rows),
		totalRowCount,
	)
	assert.Nil(t, err)

	ns := intStorage.numericStore
	assert.Nil(t, ns.debugInvariantCheck(), "storage: %v", ns)
	assertNumericStoreMatchRows(t, rows, &ns, totalRowCount)
	return intStorage
}

func assertIntFilterHasResult(
	t *testing.T,
	s debugFilterTestSetup[int64, int64],
) {
	bitmap := newBitmapWithOnes(s.rowCount)
	ctx := &filterCtx{
		ctx:    common.NewBapiCtx(),
		bitmap: bitmap,
	}

	intStorage := debugNewIntColumnsStorageFromRows(t, s.rows)

	filters := make([]singularFilter[int64], 0)
	for _, df := range s.filters {
		filter := singularFilter[int64]{
			col: &ColumnInfo{
				Name:       strconv.Itoa(int(df.colId)),
				ColumnType: IntColumnType,
				id:         df.colId},
			op:    df.op,
			value: df.value,
		}
		filters = append(filters, filter)
	}
	intStorage.filter(ctx, filters)

	actualRows := make([]uint32, 0)
	ctx.bitmap.Range(func(rowId uint32) { actualRows = append(actualRows, rowId) })

	assert.EqualValues(t, s.expectedRows, actualRows)
}

func assertGetResultIntStorage(
	t *testing.T,
	s debugGetTestSetup[int64],
) {
	storageRowCount := len(s.rows) // intStorage should contain all rows since ts is required

	// set up bitmap to include only rows requested
	bitmap := newBitmapWithOnes(storageRowCount)
	bitmap.Clear()
	for _, rowId := range s.requestedRowIds {
		bitmap.Set(rowId)
	}

	// build getter context
	columns := make([]*ColumnInfo, 0)
	for _, colId := range s.colIds {
		columns = append(columns, &ColumnInfo{
			Name:       strconv.Itoa(int(colId)),
			ColumnType: s.colType,
			id:         colId,
		})
	}
	ctx := &getCtx{
		ctx:     common.NewBapiCtx(),
		bitmap:  bitmap,
		columns: columns,
	}

	// since the colIdx in the result is indexed on `ctx.columns`, we need a lookup from colId to colIdx.
	colIdxLookup := make(map[columnId]int)
	for i, colInfo := range columns {
		colIdxLookup[colInfo.id] = i
	}
	// since the rowIdx in the result is indexed on `ctx.bitmap`'s 1 bits, we need a lookup from rowId to rowIdx.
	rowIdxLookup := make(map[uint32]int)
	idx := 0
	ctx.bitmap.Range(func(rowId uint32) {
		rowIdxLookup[rowId] = idx
		idx++
	})

	intStorage := debugNewIntColumnsStorageFromRows(t, s.rows)
	intResult := intStorage.get(ctx)

	matrix := intResult.matrix
	hasValue := intResult.hasValue

	// assert that the result matches the expected
	assertGetResult(t, s, colIdxLookup, rowIdxLookup,
		matrix, hasValue, make(map[int64]bool), false /* we do not record distinct result value for intStorage */)
}

func debugNewStrColumnsStorageFromRows(t *testing.T, rows debugRows[strId], totalRowCount int) (*strColumnsStorage, readOnlyStrStore) {
	strIdSet := make(map[strId]bool)
	strIdMap := make(map[strId]string)
	strValueMap := make(map[string]strId)

	for _, row := range rows {
		for _, pair := range row {
			if _, ok := strIdMap[pair.value]; !ok {
				strIdMap[pair.value] = strconv.Itoa(int(pair.value))
				strValueMap[strIdMap[pair.value]] = pair.value
				strIdSet[pair.value] = true
			}
		}
	}

	strStorage, _ := newStrColumnsStorage(
		debugNewPartialColumns(rows),
		totalRowCount,
		strIdSet,
	)
	ns := strStorage.numericStore
	assert.Nil(t, ns.debugInvariantCheck(), "storage: %v", ns)
	assertNumericStoreMatchRows(t, rows, &ns, totalRowCount)
	return strStorage, &testReadOnlyStrStore{
		strIdMap,
		strValueMap,
	}
}

func assertStrFilterHasResult(
	t *testing.T,
	s debugFilterTestSetup[strId, string],
) {
	bitmap := newBitmapWithOnes(s.rowCount)
	ctx := &filterCtx{
		ctx:    common.NewBapiCtx(),
		bitmap: bitmap,
	}

	strStorage, tableStrStore := debugNewStrColumnsStorageFromRows(t, s.rows, s.rowCount)

	filters := make([]singularFilter[strId], 0)
	for _, df := range s.filters {
		sid, _ := tableStrStore.getStrId(df.value)
		filter := singularFilter[strId]{
			col: &ColumnInfo{
				Name:       strconv.Itoa(int(df.colId)),
				ColumnType: StrColumnType,
				id:         df.colId},
			op:    df.op,
			value: sid,
		}
		filters = append(filters, filter)
	}
	strStorage.filter(ctx, filters)

	actualRows := make([]uint32, 0)
	ctx.bitmap.Range(func(rowId uint32) { actualRows = append(actualRows, rowId) })

	assert.EqualValues(t, s.expectedRows, actualRows)
}

func assertGetResultStrStorage(
	t *testing.T,
	s debugGetTestSetup[strId],
) {
	storageRowCount := 0
	for rowId := range s.rows {
		// set the rowCount to the max rowId plus one since it's enough for this test.
		storageRowCount = max(storageRowCount, int(rowId)+1)
	}

	// set up bitmap to include only rows requested
	bitmap := newBitmapWithOnes(storageRowCount)
	bitmap.Clear()
	for _, rowId := range s.requestedRowIds {
		bitmap.Set(rowId)
	}

	// build getter context
	columns := make([]*ColumnInfo, 0)
	for _, colId := range s.colIds {
		columns = append(columns, &ColumnInfo{
			Name:       strconv.Itoa(int(colId)),
			ColumnType: s.colType,
			id:         colId,
		})
	}
	ctx := &getCtx{
		ctx:     common.NewBapiCtx(),
		bitmap:  bitmap,
		columns: columns,
	}

	// since the colIdx in the result is indexed on `ctx.columns`, we need a lookup from colId to colIdx.
	colIdxLookup := make(map[columnId]int)
	for i, colInfo := range columns {
		colIdxLookup[colInfo.id] = i
	}
	// since the rowIdx in the result is indexed on `ctx.bitmap`'s 1 bits, we need a lookup from rowId to rowIdx.
	rowIdxLookup := make(map[uint32]int)
	idx := 0
	ctx.bitmap.Range(func(rowId uint32) {
		rowIdxLookup[rowId] = idx
		idx++
	})

	strStorage, tableStrStore := debugNewStrColumnsStorageFromRows(t, s.rows, storageRowCount)
	strResult := strStorage.get(ctx)
	actualResultValues := make(map[strId]bool)

	// assert that the strings are included in the result
	for strId := range strResult.strIdSet {
		str, _ := tableStrStore.getStr(strId)
		assert.Equal(t, strconv.Itoa(int(strId)), str)
		actualResultValues[strId] = true
	}

	matrix := strResult.matrix
	hasValue := strResult.hasValue

	// assert that the result matches the expected
	assertGetResult(t, s, colIdxLookup, rowIdxLookup, matrix, hasValue, actualResultValues, true /* recordValues */)
}

type testReadOnlyStrStore struct {
	strIdMap    map[strId]string
	strValueMap map[string]strId
}

func (s *testReadOnlyStrStore) getStrId(str string) (strId, bool) {
	if id, loaded := s.strValueMap[str]; loaded {
		return id, true
	}
	return nonexistentStr, false
}

func (s *testReadOnlyStrStore) getStr(id strId) (string, bool) {
	if str, loaded := s.strIdMap[id]; loaded {
		return str, true
	}
	return "", false
}

func (s *testReadOnlyStrStore) search(colId columnId, searchStr string) ([]string, bool) {
	return nil, false
}
