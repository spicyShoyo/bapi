package store

import (
	"bapi/internal/common"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildBlock(t *testing.T) {
	table := NewTable(common.NewBapiCtx(), "asd")
	ingester := table.newIngester()
	_, err := ingester.buildPartialBlock()
	assert.NotNilf(t, err, "should not build block when empty")

	rawRows := []RawJson{
		{
			Int: map[string]int64{"ts": 1643175607},
			Str: map[string]string{"event": "init_app"},
		},
		{
			Int: map[string]int64{"ts": 1643175609, "count": 1},
			Str: map[string]string{"event": "publish"},
		},
		{
			Int: map[string]int64{"ts": 1643175611, "count": 1},
			Str: map[string]string{"event": "create", "source": "modal"},
		},
	}
	for _, rawRow := range rawRows {
		ingester.ingestRawJson(rawRow, false /*useServerTs*/)
	}
	assert.Equal(t, table.colInfoMap.nextColId, columnId(4))

	pb, _ := ingester.buildPartialBlock()
	assert.Equal(t, int64(1643175607), pb.minTs)
	assert.Equal(t, int64(1643175611), pb.maxTs)

	assertIntPartialColData(t, "ts", partialColumnData[int64]{
		1643175607: {0},
		1643175609: {1},
		1643175611: {2},
	}, table, pb.intPartialColumns)

	assertIntPartialColData(t, "count", partialColumnData[int64]{
		1: {1, 2},
	}, table, pb.intPartialColumns)

	assertStrPartialColData(t, "event", partialColumnData[string]{
		"init_app": {0},
		"publish":  {1},
		"create":   {2},
	}, table, ingester, pb.strPartialColumns)

	assertStrPartialColData(t, "source", partialColumnData[string]{
		"modal": {2},
	}, table, ingester, pb.strPartialColumns)

	block, _ := pb.buildBlock()
	assert.Equal(t, int64(1643175607), block.minTs)
	assert.Equal(t, int64(1643175611), block.maxTs)
	assert.Equal(t, 3, block.rowCount)
}

func assertIntPartialColData(
	t *testing.T, colName string, expected partialColumnData[int64], table *Table, intCol partialColumns[int64]) {
	colId, found := table.colInfoMap.getOrRegisterColumnId(colName, IntColumnType)
	assert.Nil(t, found)
	assertPartialColumnDataEqual(t, expected, intCol[colId])
}

func assertStrPartialColData(
	t *testing.T, colName string, expected partialColumnData[string], table *Table, ingester *ingester, strCol partialColumns[strId]) {
	expectedStrIdCol := make(partialColumnData[strId])
	for s, rows := range expected {
		sid, _ := table.strStore.getStrId(s)
		expectedStrIdCol[sid] = rows
	}

	colId, found := table.colInfoMap.getOrRegisterColumnId(colName, StrColumnType)
	assert.Nil(t, found)
	assertPartialColumnDataEqual(t, expectedStrIdCol, strCol[colId])
}

// --------------------------- partialColumn ----------------------------
func TestGetOrCreateColumnData(t *testing.T) {
	columns := newPartialColumns[int64]()

	columns.getOrCreateColumnData(columnId(1))
	assert.Equal(t, len(columns), 1)
	intCol := columns.getOrCreateColumnData(columnId(2))
	assert.Equal(t, len(columns), 2)

	intColAgain := columns.getOrCreateColumnData(columnId(2))
	assert.Equal(t, len(columns), 2)
	assert.Equal(t, intCol, intColAgain)
}

func TestInsertValue(t *testing.T) {
	columns := newPartialColumns[strId]()

	columns.insertValue(columnId(2), 0, strId(5))
	columns.insertValue(columnId(2), 1, strId(5))
	columns.insertValue(columnId(3), 1, strId(6))
	assert.Equal(t, len(columns), 2)

	strCol := columns.getOrCreateColumnData(columnId(3))
	values, ok := strCol[strId(6)]
	assert.True(t, ok)
	assert.ElementsMatch(t, values, []uint32{1})

	strCol = columns.getOrCreateColumnData(columnId(2))
	values, ok = strCol[strId(5)]
	assert.True(t, ok)
	assert.ElementsMatch(t, values, []uint32{0, 1})
}

func TestDebugNewPartialColumns(t *testing.T) {
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
		7: {
			debugNewDebugPair(columnId(22), strId(15)),
		},
		8: {},
	}
	columns := debugNewPartialColumns(rows)

	assert.Equal(t, len(columns), 3)
	assertPartialColumnDataEqual(t, partialColumnData[strId]{
		strId(15): {0, 6, 7},
		strId(19): {5},
	}, columns[columnId(22)])
	assertPartialColumnDataEqual(t, partialColumnData[strId]{
		strId(16): {0},
	}, columns[columnId(23)])
	assertPartialColumnDataEqual(t, partialColumnData[strId]{
		strId(20): {5},
	}, columns[columnId(28)])
}

func assertPartialColumnDataEqual[V comparable](
	t *testing.T,
	expected partialColumnData[V],
	actual partialColumnData[V],
) {
	assert.Equal(t, len(expected), len(actual))
	for val, actualRows := range actual {
		expectedRows := expected[val]
		// order can be different
		assert.ElementsMatch(t, expectedRows, actualRows)
	}
}
