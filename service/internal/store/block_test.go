package store

import (
	"bapi/internal/common"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockQuery(t *testing.T) {
	table, block := debugBuildTableAndBlockFromIngester([]RawJson{
		{
			Int: map[string]int64{"ts": 1643175607, "event_index": 12},
			Str: map[string]string{"event": "init_app"},
		},
		{
			Int: map[string]int64{"ts": 1643175609, "count": 1},
			Str: map[string]string{"event": "publish"},
		},
		{
			Int: map[string]int64{"ts": 1643175611, "count": 2},
			Str: map[string]string{"event": "create", "source": "toolbar"},
		},
		{
			Int: map[string]int64{"ts": 1643175616, "count": 2},
			Str: map[string]string{"event": "discard", "source": "modal"},
		},
	})

	table.colInfoMap.getOrRegisterColumnId("not_exist_int", IntColumnType)
	table.colInfoMap.getOrRegisterColumnId("not_exist_str", StrColumnType)
	assertBlockQuery(t, table, block, 1643175607, 1643175618,
		[]debugBlockFilter[int]{
			debugGt("count", 1),
		},
		[]debugBlockFilter[string]{
			debugNe("event", "discard"),
		},
		[]string{"count", "not_exist_int", "event_index"},
		[]string{"source", "not_exist_str"},
		[]RawJson{
			{
				Int: map[string]int64{"count": 2},
				Str: map[string]string{"source": "toolbar"},
			},
		},
	)

}

func assertBlockQuery(
	t *testing.T, table *Table, block *Block,
	minTs int64, maxTs int64,
	intDebugFilters []debugBlockFilter[int], strDebugFilters []debugBlockFilter[string],
	intCols []string, strCols []string,
	expectedJsons []RawJson,
) {
	ctx := common.NewBapiCtx()
	blockQuery := debugNewQuery(t, table, minTs, maxTs, intDebugFilters, strDebugFilters, intCols, strCols)
	result, hasValue := block.query(ctx, blockQuery)
	if len(expectedJsons) == 0 {
		assert.False(t, hasValue)
		return
	}

	assert.EqualValues(t, debugToRawJson(table, blockQuery, result), expectedJsons)
}

func debugNewQuery(
	t *testing.T, table *Table,
	minTs int64, maxTs int64,
	intDebugFilters []debugBlockFilter[int], strDebugFilters []debugBlockFilter[string],
	intCols []string, strCols []string,
) *blockQuery {
	filter := debugNewBlockFilter(t, table, minTs, maxTs, intDebugFilters, strDebugFilters)
	intColumns := make([]*ColumnInfo, 0)
	strColumns := make([]*ColumnInfo, 0)
	for _, intCol := range intCols {
		colInfo, found := table.colInfoMap.getColumnInfo(intCol)
		assert.True(t, found)
		intColumns = append(intColumns, colInfo)
	}
	for _, strCol := range strCols {
		colInfo, found := table.colInfoMap.getColumnInfo(strCol)
		assert.True(t, found)
		strColumns = append(strColumns, colInfo)
	}

	return &blockQuery{filter, intColumns, strColumns}
}

func debugToRawJson(table *Table, query *blockQuery, result *BlockQueryResult) []RawJson {
	rawJsons := make([]RawJson, result.Count)
	for i := 0; i < result.Count; i++ {
		rawJsons[i] = RawJson{
			Int: make(map[string]int64),
			Str: make(map[string]string),
		}
	}
	for colIdx, colInfo := range query.intColumns {
		for rowIdx := 0; rowIdx < result.Count; rowIdx++ {
			if result.IntResult.hasValue[colIdx][rowIdx] {
				rawJsons[rowIdx].Int[colInfo.Name] = result.IntResult.matrix[colIdx][rowIdx]
			}
		}
	}

	for colIdx, colInfo := range query.strColumns {
		for rowIdx := 0; rowIdx < result.Count; rowIdx++ {
			if result.StrResult.hasValue[colIdx][rowIdx] {
				strId := result.StrResult.matrix[colIdx][rowIdx]
				rawJsons[rowIdx].Str[colInfo.Name] = result.StrResult.strIdMap[strId]
			}
		}
	}

	return rawJsons
}

func TestBlockFilter(t *testing.T) {
	table, block := debugBuildTableAndBlockFromIngester([]RawJson{
		{
			Int: map[string]int64{"ts": 1643175607},
			Str: map[string]string{"event": "init_app"},
		},
		{
			Int: map[string]int64{"ts": 1643175609, "count": 1},
			Str: map[string]string{"event": "publish"},
		},
		{
			Int: map[string]int64{"ts": 1643175611, "count": 2},
			Str: map[string]string{"event": "create", "source": "modal"},
		},
		{
			Int: map[string]int64{"ts": 1643175616, "count": 2},
			Str: map[string]string{"event": "discard", "source": "modal"},
		},
	})

	assertBlockFilter(t, table, block, 1643175600, 1643175606,
		make([]debugBlockFilter[int], 0),
		make([]debugBlockFilter[string], 0),
		make([]int, 0),
	)
	assertBlockFilter(t, table, block, 1643175620, 1643175621,
		make([]debugBlockFilter[int], 0),
		make([]debugBlockFilter[string], 0),
		make([]int, 0),
	)
	assertBlockFilter(t, table, block, 1643175606, 1643175616,
		make([]debugBlockFilter[int], 0),
		make([]debugBlockFilter[string], 0),
		[]int{0, 1, 2, 3},
	)
	assertBlockFilter(t, table, block, 1643175607, 1643175617,
		make([]debugBlockFilter[int], 0),
		make([]debugBlockFilter[string], 0),
		[]int{0, 1, 2, 3},
	)

	assertBlockFilter(t, table, block, 1643175607, 1643175618,
		make([]debugBlockFilter[int], 0),
		[]debugBlockFilter[string]{
			debugEq("event", "init_app"),
		},
		[]int{0},
	)

	assertBlockFilter(t, table, block, 1643175607, 1643175618,
		[]debugBlockFilter[int]{
			debugGt("count", 1),
		},
		[]debugBlockFilter[string]{
			debugNe("event", "discard"),
		},
		[]int{2},
	)

	// can handle columns not in block but in table
	table.colInfoMap.getOrRegisterColumnId("not_exist_int", IntColumnType)
	table.colInfoMap.getOrRegisterColumnId("not_exist_str", StrColumnType)
	assertBlockFilter(t, table, block, 1643175608, 1643175618,
		[]debugBlockFilter[int]{
			debugNull[int]("not_exist_int"),
		},
		[]debugBlockFilter[string]{
			debugNull[string]("not_exist_str"),
		},
		[]int{1, 2, 3},
	)
	assertBlockFilter(t, table, block, 1643175608, 1643175618,
		[]debugBlockFilter[int]{
			debugNonnull[int]("not_exist_int"),
		},
		[]debugBlockFilter[string]{
			debugNull[string]("not_exist_str"),
		},
		[]int{},
	)
}

func debugNewBlockFilter(
	t *testing.T, table *Table,
	minTs int64, maxTs int64,
	intDebugFilters []debugBlockFilter[int], strDebugFilters []debugBlockFilter[string],
) blockFilter {
	intFilters := make([]IntFilter, 0)
	for _, filter := range intDebugFilters {
		colInfo, ok := table.colInfoMap.getColumnInfo(filter.colName)
		assert.True(t, ok)

		intFilters = append(intFilters, IntFilter{
			ColumnInfo: colInfo,
			FilterOp:   filter.op,
			Value:      int64(filter.value),
		})
	}

	strFilters := make([]StrFilter, 0)
	for _, filter := range strDebugFilters {
		colInfo, ok := table.colInfoMap.getColumnInfo(filter.colName)
		assert.True(t, ok)

		sid, _ := table.strStore.getStrId(filter.value)
		strFilters = append(strFilters, StrFilter{
			ColumnInfo: colInfo,
			FilterOp:   filter.op,
			Value:      sid,
		})
	}
	tsColInfo, _ := table.colInfoMap.getColumnInfo(TS_COLUMN_NAME)
	return newBlockFilter(
		minTs,
		maxTs,
		tsColInfo,
		intFilters,
		strFilters,
	)
}

func assertBlockFilter(
	t *testing.T, table *Table, block *Block,
	minTs int64, maxTs int64,
	intDebugFilters []debugBlockFilter[int], strDebugFilters []debugBlockFilter[string],
	expectedRows []int, // assume the ids idexed on rows sorted by ts
) {
	ctx := common.NewBapiCtx()
	blockFilter := debugNewBlockFilter(t, table, minTs, maxTs, intDebugFilters, strDebugFilters)

	storage := block.storage.(*basicBlockStorage)
	bitmap, hasValue := storage.filterBlock(ctx, &blockFilter)

	if len(expectedRows) == 0 {
		assert.False(t, hasValue)
	} else {
		assert.True(t, hasValue)
		assert.Equal(t, len(expectedRows), bitmap.Count())
		for _, rowId := range expectedRows {
			assert.True(t, bitmap.Contains(uint32(rowId)))
		}
	}
}

func debugBuildTableAndBlockFromIngester(rawRows []RawJson) (*Table, *Block) {
	table := NewTable(common.NewBapiCtx(), "asd")
	ingester := table.newIngester()
	for _, rawRow := range rawRows {
		ingester.ingestRawJson(rawRow)
	}

	pb, _ := ingester.buildPartialBlock()
	block, _ := pb.buildBlock()
	return table, block
}

// --------------------------- bitmap ----------------------------
func TestBitmapCreation(t *testing.T) {
	bitmap := newBitmapWithOnes(3)
	assert.True(t, bitmap.Contains(0))
	assert.True(t, bitmap.Contains(1))
	assert.True(t, bitmap.Contains(2))
	assert.Equal(t, bitmap.Count(), 3)
}

func TestBitmapMutation(t *testing.T) {
	bitmap := newBitmapWithOnes(300)
	bitmap.Remove(246)
	bitmap.Remove(287)
	assert.Equal(t, bitmap.Count(), 298)
	maxOne, _ := bitmap.Max()
	assert.Equal(t, maxOne, uint32(299))
	minZero, _ := bitmap.MinZero()
	assert.Equal(t, minZero, uint32(246))

	bitmap.Clear()
	assert.Equal(t, bitmap.Count(), 0)

	bitmap.Set(299)
	assert.True(t, bitmap.Contains(299))
}

// --------------------------- util ----------------------------
type debugBlockFilter[T comparable] struct {
	colName string
	op      FilterOp
	value   T
}

func debugEq[T comparable](colName string, value T) debugBlockFilter[T] {
	return newDebugBlockFilter(colName, FilterEq, value)
}
func debugNe[T comparable](colName string, value T) debugBlockFilter[T] {
	return newDebugBlockFilter(colName, FilterNe, value)
}
func debugLt[T comparable](colName string, value T) debugBlockFilter[T] {
	return newDebugBlockFilter(colName, FilterLt, value)
}
func debugGt[T comparable](colName string, value T) debugBlockFilter[T] {
	return newDebugBlockFilter(colName, FilterGt, value)
}
func debugLe[T comparable](colName string, value T) debugBlockFilter[T] {
	return newDebugBlockFilter(colName, FilterLe, value)
}
func debugGe[T comparable](colName string, value T) debugBlockFilter[T] {
	return newDebugBlockFilter(colName, FilterGe, value)
}
func debugNonnull[T comparable](colName string) debugBlockFilter[T] {
	var zeroed T
	return newDebugBlockFilter(colName, FilterNonnull, zeroed)
}
func debugNull[T comparable](colName string) debugBlockFilter[T] {
	var zeroed T
	return newDebugBlockFilter(colName, FilterNull, zeroed)
}

func newDebugBlockFilter[T comparable](colName string, op FilterOp, value T) debugBlockFilter[T] {
	return debugBlockFilter[T]{
		colName, op, value,
	}
}
