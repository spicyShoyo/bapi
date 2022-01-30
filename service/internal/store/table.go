package store

import (
	"bapi/internal/common"
	"bapi/internal/pb"
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
	"unsafe"

	"github.com/davecgh/go-spew/spew"
)

/**
 * Represents a table
 * Name: the name of the table
 * Blocks: data structure that stores the data of this table
 * ColumnNameMap: mapping from column names to column info
 *   A column exists in this table iff the column name is present in this map.
 *   A column is created when the table ingests the first row that has value in this column,
 *   except `ts`, which is created upon the creation of the table.
 */
type Table struct {
	Name          string
	blocks        []*Block
	ColumnNameMap map[string]*ColumnInfo

	rowCount int
	minTs    int64
	maxTs    int64
	ctx      *common.BapiCtx
	ingester *ingester
}

func (t *Table) RowsQuery(query *pb.RowsQuery) (*pb.RowsQueryResult, bool) {
	if len(query.IntColumnNames) == 0 && len(query.StrColumnNames) == 0 {
		return nil, false
	}

	rowsQuery, ok := t.newRowsQuery(query)
	if !ok {
		t.ctx.Logger.Warn("failed to build query")
	}

	// TODO: handle multiple blocks
	result, ok := t.blocks[len(t.blocks)-1].query(t.ctx, rowsQuery)
	if !ok {
		return nil, false
	}
	return t.toPbQueryResult(query, result)
}

// TODO: handle multiple blocks
func (t *Table) toPbQueryResult(query *pb.RowsQuery, result *BlockQueryResult) (*pb.RowsQueryResult, bool) {
	intResultLen := result.Count * len(query.IntColumnNames)
	intResult := make([]int64, intResultLen)
	intHasValue := make([]bool, intResultLen)
	for idx := range query.IntColumnNames {
		count := copy(intResult, result.IntResult.matrix[idx])
		if count != result.Count {
			t.ctx.Logger.DPanic("invalid result")
			return nil, false
		}
		count = copy(intHasValue, result.IntResult.hasValue[idx])
		if count != result.Count {
			t.ctx.Logger.DPanic("invalid result")
			return nil, false
		}
	}

	strResultLen := result.Count * len(query.StrColumnNames)
	strResult := make([]uint32, strResultLen)
	strHasValue := make([]bool, strResultLen)
	for idx := range query.StrColumnNames {
		// *Note* casting strId to its underline type uint32
		castedStrResult := (*[]uint32)(unsafe.Pointer(&result.StrResult.matrix[idx]))
		count := copy(strResult, *castedStrResult)
		if count != result.Count {
			t.ctx.Logger.DPanic("invalid result")
			return nil, false
		}
		count = copy(strHasValue, result.StrResult.hasValue[idx])
		if count != result.Count {
			t.ctx.Logger.DPanic("invalid result")
			return nil, false
		}
	}

	strIdMap := make(map[uint32]string)
	for strId, str := range result.StrResult.strIdMap {
		strIdMap[uint32(strId)] = str
	}

	return &pb.RowsQueryResult{
		Count: int32(result.Count),

		IntColumnNames: query.IntColumnNames,
		IntResult:      intResult,
		IntHasValue:    intHasValue,

		StrColumnNames: query.StrColumnNames,
		StrIdMap:       strIdMap,
		StrResult:      strResult,
		StrHasValue:    strHasValue,
	}, true
}

func (t *Table) verifyRowsQuery(query *pb.RowsQuery) bool {
	// has value and t.minTs <= query.minTs <= query.MaxTs < t.maxTs
	if t.rowCount == 0 || t.maxTs < query.MinTs {
		return false
	}

	if query.MaxTs != nil {
		maxTs := query.GetMaxTs()
		if t.minTs > maxTs || query.MinTs > maxTs {
			return false
		}
	}

	return true
}

func (t *Table) newRowsQuery(query *pb.RowsQuery) (*blockQuery, bool) {
	if ok := t.verifyRowsQuery(query); !ok {
		return nil, false
	}

	blockFilter, ok := t.newBlockfilter(query)
	if !ok {
		return nil, false
	}

	intColumns := make([]*ColumnInfo, 0)
	for _, intColName := range query.IntColumnNames {
		colInfo, ok := t.getColumnInfoAndAssertType(intColName, IntColumnType)
		if !ok {
			return nil, false
		}
		intColumns = append(intColumns, colInfo)
	}

	strColumns := make([]*ColumnInfo, 0)
	for _, strColName := range query.StrColumnNames {
		colInfo, ok := t.getColumnInfoAndAssertType(strColName, StrColumnType)
		if !ok {
			return nil, false
		}
		strColumns = append(strColumns, colInfo)
	}

	return &blockQuery{
		filter:     blockFilter,
		intColumns: intColumns,
		strColumns: strColumns,
	}, true
}

func (t *Table) newBlockfilter(query *pb.RowsQuery) (BlockFilter, bool) {
	intFilters := make([]IntFilter, 0)
	for _, intFilter := range query.IntFilters {
		colInfo, ok := t.getColumnInfoAndAssertType(intFilter.ColumnName, IntColumnType)
		if !ok {
			return BlockFilter{}, false
		}

		intVal, ok := intFilter.GetValue().(*pb.Filter_IntVal)
		if !ok {
			t.ctx.Logger.Warnf("fail to build filter. int value missing for int filter: %s", intFilter.ColumnName)
			return BlockFilter{}, false
		}

		intFilters = append(intFilters, IntFilter{
			ColumnInfo: colInfo,
			FilterOp:   fromPbFilter(intFilter.FilterOp),
			Value:      intVal.IntVal,
		})
	}

	strFilters := make([]StrFilter, 0)
	for _, strFilter := range query.StrFilters {
		colInfo, ok := t.getColumnInfoAndAssertType(strFilter.ColumnName, StrColumnType)
		if !ok {
			return BlockFilter{}, false
		}

		strVal, ok := strFilter.GetValue().(*pb.Filter_StrVal)
		if !ok {
			t.ctx.Logger.Warnf("fail to build filter. str value missing for int filter: %s", strFilter.ColumnName)
			return BlockFilter{}, false
		}

		strFilters = append(strFilters, StrFilter{
			ColumnInfo: colInfo,
			FilterOp:   fromPbFilter(strFilter.FilterOp),
			Value:      strVal.StrVal,
		})
	}

	maxTs := time.Now().Unix()
	if query.MaxTs != nil {
		maxTs = query.GetMaxTs()
	}

	return BlockFilter{
		MinTs:      query.MinTs,
		MaxTs:      maxTs,
		intFilters: intFilters,
		strFilters: strFilters,
	}, true
}

// Creates a new table
func NewTable(ctx *common.BapiCtx, name string) *Table {
	table := &Table{
		Name:          name,
		blocks:        make([]*Block, 0),
		ColumnNameMap: make(map[string]*ColumnInfo),
		rowCount:      0,
		minTs:         int64(0xFFFFFFFF),
		maxTs:         0,
		ctx:           ctx,
		ingester:      newIngester(),
	}

	table.getOrRegisterColumnId(TS_COLUMN_NAME, IntColumnType)
	_, inColNameMap := table.ColumnNameMap[TS_COLUMN_NAME]
	if !inColNameMap {
		ctx.Logger.Panic("missing ts column")
	}

	return table
}

/**
 * Reads the given file and ingests rows to the table
 * The file should be newline separated jsons:
 * ```
 * {"int":{"ts":1641742859,"count":906},"str":{"event":"init_app"}}
 * {"int":{"ts":1641763082},"str":{"event":"edit"}}
 * ```
 */
func (table *Table) IngestFile(fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		table.ctx.Logger.Errorf("failed to open file for ingestion: %s, %v", fileName, err)
	}
	defer file.Close()

	table.IngestBuf(bufio.NewScanner(file))
}

// Reads the given buffer and ingests rows to the table
func (table *Table) IngestBuf(scanner *bufio.Scanner) {
	cnt_success := 0
	cnt_all := 0

	for scanner.Scan() {
		batch_cnt_success, batch_cnt_all := table.ingestBufOneBlock(scanner)
		cnt_success += batch_cnt_success
		cnt_all += batch_cnt_all
	}

	table.ctx.Logger.Infof("injested: %d, total: %d", cnt_success, cnt_all)
}

// TODO sort blocks
func (table *Table) addBlock(block *Block) bool {
	if block.rowCount == 0 {
		table.ctx.Logger.Error("refuse to add an empty block")
		return false
	}

	table.minTs = min(table.minTs, block.minTs)
	table.maxTs = max(table.maxTs, block.maxTs)
	table.rowCount += block.rowCount
	table.blocks = append(table.blocks, block)
	return true
}

// Reads the given buffer and process the rows until either the buffer is empty
// or reached max rows per block, then add a new block to the table.
// *Note* this assumes that Scan was just called on the scanner.
func (table *Table) ingestBufOneBlock(scanner *bufio.Scanner) (int, int) {
	table.ingester.zeroOut()
	cnt_success := 0
	cnt_all := 0

	// assumes Scan was called and has outstanding unprocessed bytes
	for {
		cnt_all += 1
		var rawJson RawJson
		if err := json.Unmarshal(scanner.Bytes(), &rawJson); err != nil {
			table.ctx.Logger.Errorf("failed to parse json: %v", err)
			continue
		}
		if err := table.ingester.ingestRawJson(table, rawJson); err == nil {
			cnt_success += 1
		} else {
			table.ctx.Logger.Errorf("failed to ingest json: %v", err)
		}

		if cnt_all == table.ctx.GetMaxRowsPerBlock() || !scanner.Scan() {
			break
		}
	}

	block, err := table.ingester.buildBlock()
	if err != nil {
		table.ctx.Logger.Error("fail to build block: %v", err)
		return 0, cnt_all
	}

	ok := table.addBlock(block)
	if !ok {
		return 0, cnt_all
	}
	table.ctx.Logger.Infof("batch injested: %d, total: %d", cnt_success, cnt_all)
	return cnt_success, cnt_all
}

// Reads the given buffer and process the rows until either the buffer is empty
// @param useServerTs if true, this overrides the `ts` column with time.Now().Unix()
// 	This should be set to true for production logging cases and set to false for data backfill.
func (table *Table) IngestJsonRows(rows []*pb.RawRow, useServerTs bool) int {
	cnt_success := 0
	serverReceviedTs := time.Now().Unix()

	i := 0
	for i < len(rows) {
		table.ingester.zeroOut()
		cur_block_cnt := 0

		// process until end of rows or reached max rows per block
		for i < len(rows) {
			row := rows[i]
			i++
			cur_block_cnt++

			if useServerTs {
				row.Int[TS_COLUMN_NAME] = serverReceviedTs
			}

			if err := table.ingester.ingestRawJson(table, RawJson{
				Int: row.Int,
				Str: row.Str,
			}); err != nil {
				table.ctx.Logger.Errorf("failed to ingest row: %v", err)
			}

			if cur_block_cnt == table.ctx.GetMaxRowsPerBlock() {
				break
			}
		}

		block, err := table.ingester.buildBlock()
		if err != nil {
			table.ctx.Logger.Error("fail to build block: %v", err)
			continue
		}

		if ok := table.addBlock(block); ok {
			cnt_success += cur_block_cnt
		}
	}

	table.ctx.Logger.Infof("injested: %d, total: %d", cnt_success, len(rows))
	return cnt_success
}

// Creates a new column
func (table *Table) registerNewColumn(colName string, colType ColumnType) (columnId, error) {
	if _, alreadyExists := table.ColumnNameMap[colName]; alreadyExists {
		return 0, fmt.Errorf("column with name already exists: %s", colName)
	}
	if len(table.ColumnNameMap) == table.ctx.GetMaxColumn() {
		return 0, fmt.Errorf("too many columns, max: %d", table.ctx.GetMaxColumn())
	}

	columnId := columnId(len(table.ColumnNameMap))

	columnInfo := &ColumnInfo{
		id:         columnId,
		Name:       colName,
		ColumnType: colType,
	}

	table.ColumnNameMap[colName] = columnInfo

	return columnId, nil
}

// Gets or creates a column of the given name and type
func (table *Table) getOrRegisterColumnId(colName string, colType ColumnType) (columnId, error) {
	if columnInfo, ok := table.ColumnNameMap[colName]; ok {
		if columnInfo.ColumnType != colType {
			return 0, fmt.Errorf(
				"column type mismatch for %s, expected: %d, got: %d", columnInfo.Name, columnInfo.ColumnType, colType)
		}
		return columnInfo.id, nil
	}

	return table.registerNewColumn(colName, colType)
}

func (table *Table) getColumnInfo(colName string) (*ColumnInfo, bool) {
	if colInfo, ok := table.ColumnNameMap[colName]; ok {
		return colInfo, true
	}
	return nil, false
}

func (table *Table) getColumnInfoAndAssertType(colName string, colType ColumnType) (*ColumnInfo, bool) {
	colInfo, ok := table.getColumnInfo(colName)
	if !ok {
		table.ctx.Logger.Warnf("unknown column: %s", colName)
		return nil, false
	}

	if colInfo.ColumnType != colType {
		table.ctx.Logger.Warnf("unexpected type for column: %s, expected: %d, got: %d", colName, colInfo.ColumnType, colType)
		return nil, false
	}

	return colInfo, true
}

// --------------------------- debug helpers ----------------------------
func (table *Table) DebugDump() {
	fmt.Printf(spew.Sdump(table))
}
