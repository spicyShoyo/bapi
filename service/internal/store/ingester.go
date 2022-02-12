package store

import (
	"errors"
	"fmt"
	"strings"
)

// A data structure for processing a set of rows and building the block for inserting into
// the table.
// stdIdSet: strIds used in the rows
// strIdMap: new strIds to be added to the table's mao
// strValueMap: new strValue to be added to the table's mao
// rows: the rows to be injected
type ingester struct {
	ctx      ingesterCtx
	strIdSet map[strId]bool
	rows     []*row
}

type ingesterCtx interface {
	strStore
	getOrRegisterColumnId(colName string, colType ColumnType) (columnId, error)
}

type tableIngesterCtx struct {
	strStore
	*colInfoStore
}

func (t *Table) newIngester() *ingester {
	return &ingester{
		ctx: &tableIngesterCtx{
			t.strStore,
			t.colInfoMap,
		},
		strIdSet: make(map[strId]bool),
		rows:     make([]*row, 0),
	}
}

// Zeros out so can be reused.
// *Always* call this before using the ingester.
func (ingester *ingester) zeroOut() {
	ingester.strIdSet = make(map[strId]bool)
	ingester.rows = ingester.rows[:0]
}

// Returns intPartialColumns, strPartialColumns, minTs, maxTs
// should only be called by buildBlock or unit tests
func (ingester *ingester) buildPartialBlock() (*partialBlock, error) {
	if len(ingester.rows) == 0 {
		return nil, errors.New("ingester is emptry")
	}

	intPartialColumns := newPartialColumns[int64]()
	strPartialColumns := newPartialColumns[strId]()
	maxTs := int64(0)
	minTs := int64(0xFFFFFFFF)

	for idx, row := range ingester.rows {
		rowIdx := uint32(idx)
		rowTs := row.getTs()
		maxTs = max(maxTs, rowTs)
		minTs = min(minTs, rowTs)

		for idx := 0; idx < len(row.intValues); idx++ {
			intPartialColumns.insertValue(row.intColumnId[idx], rowIdx, row.intValues[idx])
		}
		for idx := 0; idx < len(row.strValues); idx++ {
			strPartialColumns.insertValue(row.strColumnId[idx], rowIdx, row.strValues[idx])
		}
	}

	return &partialBlock{
		intPartialColumns: intPartialColumns,
		strPartialColumns: strPartialColumns,

		strIdSet: ingester.strIdSet,

		rowCount: len(ingester.rows),
		minTs:    minTs,
		maxTs:    maxTs,
	}, nil
}

func (ingester *ingester) ingestRawJson(rawJson RawJson) error {
	row := newRow()

	ts, hasTsCol := rawJson.Int[TS_COLUMN_NAME]
	if !hasTsCol || ts <= 0 {
		return fmt.Errorf("Missing or invalid ts: %d", ts)
	}
	row.addInt(columnId(TS_COLUMN_ID), ts) // making sure the first value is ts

	for columnName, value := range rawJson.Int {
		colId, err := ingester.ctx.getOrRegisterColumnId(columnName, IntColumnType)
		if err != nil {
			return err
		}
		if colId == columnId(TS_COLUMN_ID) {
			continue // already inserted
		}

		row.addInt(colId, value)
	}

	for columnName, value := range rawJson.Str {
		colId, err := ingester.ctx.getOrRegisterColumnId(columnName, StrColumnType)
		if err != nil {
			return err
		}

		strId, err := ingester.getOrInsertStrId(strings.TrimSpace(value))
		if err != nil {
			return err
		}
		row.addStr(colId, strId)
	}

	ingester.rows = append(ingester.rows, row)
	return nil
}

// Inserts or gets the id for the string value.
// Note: the size of the string value store is unbounded.
func (ingester *ingester) getOrInsertStrId(strValue string) (strId, error) {
	strId, _, ok := ingester.ctx.getOrInsertStrId(strValue)
	if !ok {
		return nonexistentStr, fmt.Errorf("reached max str count: %d", strId)
	}

	ingester.strIdSet[strId] = true
	return strId, nil
}

// --------------------------- row ----------------------------
/**
 * A bookkeeping data structure for processing a raw row received from an external client (e.g. website logger)
 * The first element (TS_COLUMN_ID) of intValues/intColumnId is the ts.
 * Invariant: len(intColumnId) == len(intValues) && len(strColumnId) == len(strValues)
 *
 * e.g. given a raw row looks like {"ts": 1642906206, "count": 12, "event": "init", },
 * 	and "event" has colId of 2, "count" has colId of 3, "init" has strId of 9,the row would look like:
 * 	{
 * 		intColumnId: [0, 3],
 * 		intValues: [1642906206, 12],
 * 		strColumnId: [2],
 *		strValues: [9],
 *  }
 */
type row struct {
	intColumnId []columnId
	intValues   []int64
	strColumnId []columnId
	strValues   []strId
}

func newRow() *row {
	return &row{
		intColumnId: make([]columnId, 0),
		intValues:   make([]int64, 0),
		strColumnId: make([]columnId, 0),
		strValues:   make([]strId, 0),
	}
}

func (row *row) addInt(colId columnId, value int64) {
	row.intColumnId = append(row.intColumnId, colId)
	row.intValues = append(row.intValues, value)
}

func (row *row) addStr(colId columnId, value strId) {
	row.strColumnId = append(row.strColumnId, colId)
	row.strValues = append(row.strValues, value)
}

func (row *row) getTs() int64 {
	return row.intValues[TS_COLUMN_ID]
}

// --------------------------- partialColumn ----------------------------
/**
 * A bookkeeping data structure created after all raw rows have been proocessed and used
 * 	for creating a new partialBlock, which is then used to create a block to be inserted into the table.
 * partialColumns is a map from a column id to a partialColumnData, which is
 * 	a map from a value to a slice of rows that have this value.
 * e.g. given a row with rowId of 123 has value "init" in the column with columnId of 2:
 * 	partialColumns looks like {2: {"init": [123]}}
 */
type partialColumnData[T comparable] map[T][]uint32
type partialColumns[T comparable] map[columnId]partialColumnData[T]

// A bookkeeping data structure that has everything needed to create a block
type partialBlock struct {
	intPartialColumns partialColumns[int64]
	strPartialColumns partialColumns[strId]

	strIdSet map[strId]bool

	rowCount int
	minTs    int64
	maxTs    int64
}

func (pb *partialBlock) buildBlock() (*Block, error) {
	blockStorage, err := newBasicBlockStorage(pb)
	if err != nil {
		return nil, err
	}

	return &Block{
		minTs:    pb.minTs,
		maxTs:    pb.maxTs,
		rowCount: pb.rowCount,

		storage: blockStorage,
	}, nil
}

func newPartialColumns[T comparable]() partialColumns[T] {
	return make(map[columnId]partialColumnData[T])
}

func (partialColumns partialColumns[T]) getOrCreateColumnData(colId columnId) partialColumnData[T] {
	if column, ok := partialColumns[colId]; ok {
		return column
	}
	partialColumns[colId] = make(partialColumnData[T])
	return partialColumns[colId]
}

/**
 * Inserts the value of the row in the column to the partialColumns
 * A partialColumnData will be created if not already in the map.
 * e.g. given a partialColumns {2: {"init": [2]}}, "init" has strId of 11, "ok" has strId of 15
 * 			and we are inserting a row of rowId 5: {2: "init", 4: "ok"},
 * 		first call `insertValue(4, 5, getStrId("ok"))`
 * 			partialColumns looks like:  {2: {11: [2]}, 4: {15: [5]}}
 * 		then call `insertValue(2, 5, getStrId("init"))`:
 * 			partialColumns looks like:  {2: {11: [2, 5]}, 4: {15: [5]}}
 */
func (partialColumns partialColumns[T]) insertValue(colId columnId, rowId uint32, value T) {
	columnData := partialColumns.getOrCreateColumnData(colId)

	if _, ok := columnData[value]; !ok {
		columnData[value] = make([]uint32, 0)
	}
	columnData[value] = append(columnData[value], rowId)
}

// --------------------------- test util ----------------------------
type debugPair[T comparable] struct {
	colId columnId
	value T
}
type debugRows[T comparable] map[uint32][]debugPair[T]

func debugNewDebugPair[T comparable](colId columnId, value T) debugPair[T] {
	return debugPair[T]{colId, value}
}

func debugNewPartialColumns[T comparable](rows debugRows[T]) partialColumns[T] {
	partialColums := newPartialColumns[T]()

	for rowId, pairs := range rows {
		for _, pair := range pairs {
			partialColums.insertValue(pair.colId, rowId, pair.value)
		}
	}

	return partialColums
}
