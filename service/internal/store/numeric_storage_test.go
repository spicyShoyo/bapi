package store

import (
	"bapi/internal/common"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewNumericStorage(t *testing.T) {
	_, ok := newNumericStorage[int64](2, 0)
	assert.False(t, ok)
	_, ok = newNumericStorage[int64](-1, 3)
	assert.False(t, ok)
	storage, _ := newNumericStorage[int64](2, 3)
	assert.Equal(t, len(storage.matrix), 2)
	assert.Equal(t, len(storage.matrix[0]), 3)
	assert.Equal(t, len(storage.values), 2)
}

func assertNumericStorageMatchRows[V OrderedNumeric](
	t *testing.T,
	rows debugRows[V],
	ns *numericStorage[V],
	rowCount int,
) {
	assert.Equal(t, rowCount, len(ns.matrix[0]))

	rowsHaveValue := make(map[uint32]bool)
	for rowId, pairs := range rows {
		rowsHaveValue[rowId] = true
		colsHaveValue := make(map[localColumnId]bool)

		// assert that each columns of this row is stored
		for _, pair := range pairs {
			localColId, _ := ns.columnIds[pair.colId]
			colsHaveValue[localColId] = true
			valueIdx := ns.matrix[localColId][rowId]
			actualValue := ns.values[localColId][valueIdx]

			assert.Equal(
				t, pair.value, actualValue,
				"value not match for rowId: %d, colId: %d, valueIdx: %d, localColId: %d,\n expected: %d, actual: %d,\n storage: %+v",
				rowId, pair.colId, valueIdx, localColId, pair.value, actualValue, ns,
			)
		}
		// assert that the row has no value in other columns
		for localColId := 0; localColId < len(ns.matrix); localColId++ {
			if _, hasValue := colsHaveValue[localColumnId(localColId)]; hasValue {
				continue
			}
			assert.Equal(t, nullValueIndex, ns.matrix[localColId][rowId])
		}
	}

	// assert that the rest of the rows have no value in any column
	for localColId := 0; localColId < len(ns.matrix); localColId++ {
		valuesForCol := ns.matrix[localColId]
		for rowId := 0; rowId < len(rows); rowId++ {
			if _, hasValue := rowsHaveValue[uint32(rowId)]; hasValue {
				// already checked null cols in the loop above
				continue
			}

			assert.Equal(t, valuesForCol[rowId], nullValueIndex)
		}
	}
}

func TestFromPartialColumns(t *testing.T) {
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
	partialColumns := debugNewPartialColumns(rows)
	ns, _ := fromPartialColumns(partialColumns, 10 /*rowCount*/)
	assert.Nil(t, ns.debugInvariantCheck(), "storage: %v", ns)
	assertNumericStorageMatchRows(t, rows, ns, 10)
}

type debugFilter[T comparable] struct {
	colId columnId
	op    FilterOp
	value T
}
type debugFilterTestSetup[T, U comparable] struct {
	rows         debugRows[T]
	rowCount     int
	filters      []debugFilter[U] // this is a separate type so that strColStorage can reuse some code
	expectedRows []uint32
}

func assertFilterHasResult[V OrderedNumeric](
	t *testing.T,
	s debugFilterTestSetup[V, V],
) {
	bitmap := newBitmapWithOnes(s.rowCount)
	ctx := &filterCtx{
		ctx:    common.NewBapiCtx(),
		bitmap: bitmap,
	}

	partialColumns := debugNewPartialColumns(s.rows)
	ns, _ := fromPartialColumns(partialColumns, s.rowCount)
	assert.Nil(t, ns.debugInvariantCheck(), "storage: %v", ns)

	for _, df := range s.filters {
		filter := numericFilter[V]{
			localColId: ns.columnIds[df.colId],
			op:         df.op,
			value:      df.value,
		}
		ns.filterNumericStorage(ctx, filter)
	}

	actualRows := make([]uint32, 0)
	ctx.bitmap.Range(func(rowId uint32) { actualRows = append(actualRows, rowId) })

	assert.EqualValues(t, s.expectedRows, actualRows)
}

func TestFilterBasic(t *testing.T) {
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
	// col_22 == strId(15)
	assertFilterHasResult(t, debugFilterTestSetup[strId, strId]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[strId]{{
			colId: columnId(22),
			op:    FilterEq,
			value: strId(15),
		}},
		expectedRows: []uint32{0, 6},
	})

	// col_22 != strId(15) && col_22 != null
	assertFilterHasResult(t, debugFilterTestSetup[strId, strId]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[strId]{
			{
				colId: columnId(22),
				op:    FilterNe,
				value: strId(15),
			},
			{
				colId: columnId(22),
				op:    FilterNonnull,
				value: strId(0),
			}},
		expectedRows: []uint32{5},
	})

	// col_22 == null
	assertFilterHasResult(t, debugFilterTestSetup[strId, strId]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[strId]{
			{
				colId: columnId(22),
				op:    FilterNull,
				value: strId(0),
			}},
		expectedRows: []uint32{1, 2, 3, 4, 7, 8, 9},
	})
}

func TestFilterComparator(t *testing.T) {
	rows := debugRows[int]{
		0: {
			debugNewDebugPair(columnId(22), 15),
			debugNewDebugPair(columnId(23), 16),
		},
		5: {
			debugNewDebugPair(columnId(22), 19),
			debugNewDebugPair(columnId(23), 17),
			debugNewDebugPair(columnId(28), 20),
		},
		6: {
			debugNewDebugPair(columnId(22), 15),
			debugNewDebugPair(columnId(23), 17),
		},
	}

	assertFilterHasResult(t, debugFilterTestSetup[int, int]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[int]{
			{
				colId: columnId(28),
				op:    FilterLt,
				value: 21,
			},
		},
		expectedRows: []uint32{5},
	})

	assertFilterHasResult(t, debugFilterTestSetup[int, int]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[int]{
			{
				colId: columnId(28),
				op:    FilterGt,
				value: 19,
			},
		},
		expectedRows: []uint32{5},
	})

	// col_22 < 19 && col_23 > 16
	assertFilterHasResult(t, debugFilterTestSetup[int, int]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[int]{
			{
				colId: columnId(22),
				op:    FilterLt,
				value: 19,
			},
			{colId: columnId(23),
				op:    FilterGt,
				value: 16,
			},
		},
		expectedRows: []uint32{6},
	})

	// col_22 <= 19 && col_23 >= 16 && col_28 != null
	assertFilterHasResult(t, debugFilterTestSetup[int, int]{
		rows: rows, rowCount: 10,
		filters: []debugFilter[int]{
			{
				colId: columnId(22),
				op:    FilterLe,
				value: 19,
			},
			{colId: columnId(23),
				op:    FilterGe,
				value: 16,
			},
			{colId: columnId(28),
				op:    FilterNull,
				value: 0,
			},
		},
		expectedRows: []uint32{0, 6},
	})
}

func TestNewNumericStorageResult(t *testing.T) {
	result := newNumericStorageResult[int](2 /* rowCnt */, 3 /* colCnt */)
	assert.Equal(t, 3, len(result.matrix))
	assert.Equal(t, 3, len(result.hasValue))
	for _, rows := range result.matrix {
		assert.Equal(t, 2, len(rows))
	}
}

type debugGetTestSetup[T OrderedNumeric] struct {
	colType         ColumnType
	rows            debugRows[T]
	colIds          []columnId
	requestedRowIds []uint32
	expectedRows    debugRows[T]
}

func assertGetResultNumericStorage[V OrderedNumeric](
	t *testing.T,
	s debugGetTestSetup[V],
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

	ns, _ := fromPartialColumns(debugNewPartialColumns(s.rows), storageRowCount)
	assert.Nil(t, ns.debugInvariantCheck(), "storage: %v", ns)
	result, actualResultValues := ns.get(ctx, true /* recordValue */)
	matrix, hasValue := result.matrix, result.hasValue

	// assert that the result matches the expected
	assertGetResult(t, s, colIdxLookup, rowIdxLookup, matrix, hasValue, actualResultValues, true /* recordValues */)
}

func assertGetResult[V OrderedNumeric](
	t *testing.T,
	s debugGetTestSetup[V],
	colIdxLookup map[columnId]int,
	rowIdxLookup map[uint32]int,
	matrix [][]V,
	hasValue [][]bool,
	actualResultValues map[V]bool,
	recordValues bool,
) {
	assert.True(t, len(s.colIds) == len(matrix) && len(matrix) == len(hasValue))
	assert.True(t, len(s.expectedRows) == len(matrix[0]) && len(matrix[0]) == len(hasValue[0]))

	expectedResultValues := make(map[V]bool)
	for rowId, row := range s.expectedRows {
		rowIdx := rowIdxLookup[rowId]
		colIdxSeen := make(map[int]bool)

		// assert the row has value in expected columns
		for _, pair := range row {
			colIdx := colIdxLookup[pair.colId]
			colIdxSeen[colIdx] = true

			assert.True(t, hasValue[colIdx][rowIdx])
			assert.Equal(t, pair.value, matrix[colIdx][rowIdx])

			if recordValues {
				expectedResultValues[pair.value] = true
			}
		}

		// assert the row has no value in other columns
		for colIdx := 0; colIdx < len(matrix); colIdx++ {
			if _, seen := colIdxSeen[colIdx]; seen {
				continue
			}
			assert.False(t, hasValue[colIdx][rowIdx])
		}
	}

	// assert that we are recording the distinct values in the result
	assert.EqualValues(t, expectedResultValues, actualResultValues)
}

func TestGet(t *testing.T) {
	rows := debugRows[int]{
		0: {
			debugNewDebugPair(columnId(22), 15),
			debugNewDebugPair(columnId(23), 16),
		},
		2: {
			debugNewDebugPair(columnId(23), 16),
		},
		5: {
			debugNewDebugPair(columnId(22), 19),
			debugNewDebugPair(columnId(23), 17),
			debugNewDebugPair(columnId(28), 20),
		},
		6: {
			debugNewDebugPair(columnId(22), 15),
			debugNewDebugPair(columnId(23), 17),
		},
	}

	assertGetResultNumericStorage(t, debugGetTestSetup[int]{
		colType:         IntColumnType,
		rows:            rows,
		colIds:          []columnId{columnId(22)},
		requestedRowIds: []uint32{0, 2, 3, 5, 6},
		expectedRows: debugRows[int]{
			0: {
				debugNewDebugPair(columnId(22), 15),
			},
			2: {}, // in storage but has no value in col22
			3: {}, // not in storage but requested
			5: {
				debugNewDebugPair(columnId(22), 19),
			},
			6: {
				debugNewDebugPair(columnId(22), 15),
			},
		},
	})
}
