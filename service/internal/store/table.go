package store

import (
	"bapi/internal/common"
	"bapi/internal/pb"
	"bufio"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"
	"unsafe"

	"go.uber.org/atomic"
)

/**
 * Represents a table
 * blocks: data structure that stores the data of this table
 * colInfoMap: mapping from column names to column info
 *   A column exists in this table iff the column name is present in this map.
 *   A column is created when the table ingests the first row that has value in this column,
 *   except `ts`, which is created upon the creation of the table.
 */
type Table struct {
	ctx        *common.BapiCtx
	tableInfo  tableInfo
	colInfoMap *colInfoStore

	ingesterPool *sync.Pool
	pbChan       chan pbMessage
	pbQueue      []*partialBlock

	strStore strStore

	blocksLock *sync.RWMutex
	blocks     []*Block
}

type tableInfo struct {
	name     string
	rowCount *atomic.Uint32
	minTs    *atomic.Int64
	maxTs    *atomic.Int64
}

func (t *Table) TableQuery(query *pb.TableQuery) (*pb.RowsQueryResult, bool) {
	if len(query.AggIntColumnNames) == 0 {
		return nil, false
	}

	blocksQuery, ok := t.newBlockQuery(queryWithFilter{query})
	if !ok {
		t.ctx.Logger.Warn("failed to build query")
		return nil, false
	}

	blocksToQuery, ok := t.getBlocksToQuery(queryWithFilter{query})
	if !ok {
		return nil, false
	}

	blockResults := make([]*BlockQueryResult, 0)
	for _, block := range blocksToQuery {
		result, ok := block.query(t.ctx, blocksQuery)
		if !ok {
			continue
		}
		blockResults = append(blockResults, result)
	}

	aggregator := newBasicAggregator(&aggCtx{
		op:             query.AggOp,
		firstAggIntCol: len(query.GroupByIntColumnNames), // aggIntCols are after groupByIntCols
		intColCnt:      len(query.GroupByIntColumnNames) + len(query.AggIntColumnNames),
	})
	aggregator.aggregate(blockResults)
	// TODO: implement
	return nil, false
	// return t.toPbQueryResult(query, blockResults)
}

func (t *Table) RowsQuery(query *pb.RowsQuery) (*pb.RowsQueryResult, bool) {
	if len(query.IntColumnNames) == 0 && len(query.StrColumnNames) == 0 {
		return nil, false
	}

	blocksQuery, ok := t.newBlockQuery(queryWithFilter{query})
	if !ok {
		t.ctx.Logger.Warn("failed to build query")
		return nil, false
	}

	blocksToQuery, ok := t.getBlocksToQuery(queryWithFilter{query})
	if !ok {
		return nil, false
	}

	blockResults := make([]*BlockQueryResult, 0)
	for _, block := range blocksToQuery {
		result, ok := block.query(t.ctx, blocksQuery)
		if !ok {
			continue
		}
		blockResults = append(blockResults, result)
	}

	return t.toPbQueryResult(query, blockResults)
}

func (t *Table) getBlocksToQuery(query queryWithFilter) ([]*Block, bool) {
	t.blocksLock.RLock()
	defer func() {
		t.blocksLock.RUnlock()
	}()
	// first block whose minTs >= query.minTs
	startBlock := sort.Search(len(t.blocks),
		func(i int) bool {
			return t.blocks[i].minTs >= query.getMinTs()
		})

	endBlock := len(t.blocks) - 1
	if queryMaxTs, queryHasMaxTs := query.getMaxTs(); queryHasMaxTs {
		// first block whose minTs > query.maxTs
		firstLarger := sort.Search(len(t.blocks),
			func(i int) bool {
				return t.blocks[i].minTs > queryMaxTs
			})
		endBlock = firstLarger - 1
	}

	if startBlock == len(t.blocks) {
		return nil, false
	}

	if startBlock > endBlock {
		t.ctx.Logger.DPanic("table.blocks is not sorted")
		return nil, false
	}

	blocksToQuery := make([]*Block, endBlock-startBlock+1)
	copy(blocksToQuery, t.blocks[startBlock:endBlock+1])
	return blocksToQuery, true
}

func (t *Table) toPbQueryResult(query *pb.RowsQuery, blockResults []*BlockQueryResult) (*pb.RowsQueryResult, bool) {
	if len(blockResults) == 0 {
		return nil, false
	}

	rowCount := 0
	for _, r := range blockResults {
		rowCount += r.Count
	}

	intResultLen := rowCount * len(query.IntColumnNames)
	intResult := make([]int64, intResultLen)
	intHasValue := make([]bool, intResultLen)
	for colIdx := range query.IntColumnNames {
		rowStartIdx := colIdx * rowCount
		copied := 0

		for _, result := range blockResults {
			blockStartIdx := rowStartIdx + copied

			count := copy(intResult[blockStartIdx:], result.IntResult.matrix[colIdx])
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, false
			}

			count = copy(intHasValue, result.IntResult.hasValue[colIdx])
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, false
			}
			copied += result.Count
		}
	}

	strIdMap := make(map[uint32]string)

	strResultLen := rowCount * len(query.StrColumnNames)
	strResult := make([]uint32, strResultLen)
	strHasValue := make([]bool, strResultLen)
	for colIdx := range query.StrColumnNames {
		rowStartIdx := colIdx * rowCount
		copied := 0

		for _, result := range blockResults {
			for sid := range result.StrResult.strIdSet {
				str, _ := t.strStore.getStr(sid)
				strIdMap[uint32(sid)] = str
			}

			blockStartIdx := rowStartIdx + copied
			// *Note* casting strId to its underline type uint32
			castedStrResult := (*[]uint32)(unsafe.Pointer(&result.StrResult.matrix[colIdx]))
			count := copy(strResult[blockStartIdx:], *castedStrResult)
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, false
			}

			count = copy(strHasValue, result.StrResult.hasValue[colIdx])
			if count != result.Count {
				t.ctx.Logger.DPanic("invalid result")
				return nil, false
			}
			copied += result.Count
		}
	}

	return &pb.RowsQueryResult{
		Count: int32(rowCount),

		IntColumnNames: query.IntColumnNames,
		IntResult:      intResult,
		IntHasValue:    intHasValue,

		StrColumnNames: query.StrColumnNames,
		StrIdMap:       strIdMap,
		StrResult:      strResult,
		StrHasValue:    strHasValue,
	}, true
}

// A wrapper around pb querys providing getters for filtering related fields
type queryWithFilter struct {
	q interface{} // *pb.RowsQuery | *pb.TableQuery
}

func (q *queryWithFilter) getMinTs() int64 {
	if query := q.q.(*pb.RowsQuery); query != nil {
		return query.MinTs
	}
	if query := q.q.(*pb.TableQuery); query != nil {
		return query.MinTs
	}
	return 0
}

func (q *queryWithFilter) getMaxTs() (int64, bool) {
	if query := q.q.(*pb.RowsQuery); query != nil {
		return query.GetMaxTs(), query != nil
	}
	if query := q.q.(*pb.TableQuery); query != nil {
		return query.GetMaxTs(), query != nil
	}
	return 0, false
}

func (q *queryWithFilter) getIntFilters() []*pb.Filter {
	if query := q.q.(*pb.RowsQuery); query != nil {
		return query.IntFilters
	}
	if query := q.q.(*pb.TableQuery); query != nil {
		return query.IntFilters
	}
	return make([]*pb.Filter, 0)
}

func (q *queryWithFilter) getStrFilters() []*pb.Filter {
	if query := q.q.(*pb.RowsQuery); query != nil {
		return query.StrFilters
	}
	if query := q.q.(*pb.TableQuery); query != nil {
		return query.StrFilters
	}
	return make([]*pb.Filter, 0)
}

func (q *queryWithFilter) getIntColNames() []string {
	if query := q.q.(*pb.RowsQuery); query != nil {
		return query.IntColumnNames
	}
	if query := q.q.(*pb.TableQuery); query != nil {
		return append(query.GroupByIntColumnNames, query.AggIntColumnNames...)
	}
	return make([]string, 0)
}

func (q *queryWithFilter) getStrColNames() []string {
	if query := q.q.(*pb.RowsQuery); query != nil {
		return query.StrColumnNames
	}
	if query := q.q.(*pb.TableQuery); query != nil {
		return query.GroupByStrColumnNames
	}
	return make([]string, 0)
}

func (t *Table) verifyQueryFilter(query queryWithFilter) bool {
	// has value and t.minTs <= query.minTs <= query.MaxTs < t.maxTs
	if t.tableInfo.rowCount.Load() == 0 || t.tableInfo.maxTs.Load() < query.getMinTs() {
		return false
	}

	maxTs, hasMaxTs := query.getMaxTs()
	if hasMaxTs {
		maxTs := maxTs
		if t.tableInfo.minTs.Load() > maxTs || query.getMinTs() > maxTs {
			return false
		}
	}

	return true
}

func (t *Table) newBlockQuery(query queryWithFilter) (*blockQuery, bool) {
	if ok := t.verifyQueryFilter(query); !ok {
		return nil, false
	}

	blockFilter, ok := t.newBlockfilter(query)
	if !ok {
		return nil, false
	}

	intColumns, ok := t.colInfoMap.getColumnInfoSliceForType(query.getIntColNames(), IntColumnType)
	if !ok {
		return nil, false
	}

	strColumns, ok := t.colInfoMap.getColumnInfoSliceForType(query.getStrColNames(), StrColumnType)
	if !ok {
		return nil, false
	}

	return &blockQuery{
		filter:     blockFilter,
		intColumns: intColumns,
		strColumns: strColumns,
	}, true
}

func (t *Table) newBlockfilter(query queryWithFilter) (blockFilter, bool) {
	intFilters := make([]singularFilter[int64], 0)
	for _, intFilter := range query.getIntFilters() {
		colInfo, ok := t.colInfoMap.getColumnInfoAndAssertType(intFilter.ColumnName, IntColumnType)
		if !ok {
			return blockFilter{}, false
		}

		intVal, ok := intFilter.GetValue().(*pb.Filter_IntVal)
		if !ok {
			t.ctx.Logger.Warnf("fail to build filter. int value missing for int filter: %s", intFilter.ColumnName)
			return blockFilter{}, false
		}

		intFilters = append(intFilters, singularFilter[int64]{
			col:   colInfo,
			op:    intFilter.FilterOp,
			value: intVal.IntVal,
		})
	}

	strFilters := make([]singularFilter[strId], 0)
	for _, strFilter := range query.getStrFilters() {
		colInfo, ok := t.colInfoMap.getColumnInfoAndAssertType(strFilter.ColumnName, StrColumnType)
		if !ok {
			return blockFilter{}, false
		}

		strVal, ok := strFilter.GetValue().(*pb.Filter_StrVal)
		if !ok {
			t.ctx.Logger.Warnf("fail to build filter. str value missing for str filter: %s", strFilter.ColumnName)
			return blockFilter{}, false
		}
		// If the string does not exist in the store, sid will be `nonexistentStr`. The strColStore
		// is responsible to handle this.
		sid, _ := t.strStore.getStrId(strVal.StrVal)

		strFilters = append(strFilters, singularFilter[strId]{
			col:   colInfo,
			op:    strFilter.FilterOp,
			value: sid,
		})
	}

	maxTs := time.Now().Unix()
	if queryMaxTs, queryHasMaxTs := query.getMaxTs(); queryHasMaxTs {
		maxTs = queryMaxTs
	}

	tsColInfo, ok := t.colInfoMap.getColumnInfoAndAssertType(TS_COLUMN_NAME, IntColumnType)
	if !ok {
		t.ctx.Logger.DPanic("fail to get tsColInfo ")
		return blockFilter{}, false
	}

	return newBlockFilter(
		query.getMinTs(),
		maxTs,
		tsColInfo,
		intFilters,
		strFilters,
	), true
}

type pbMessage struct {
	pb       *partialBlock
	syncChan chan bool
}

// Creates a new table
func NewTable(ctx *common.BapiCtx, name string) *Table {
	table := &Table{
		ctx:        ctx,
		colInfoMap: newColInfoStore(ctx),
		tableInfo: tableInfo{
			name:     name,
			rowCount: atomic.NewUint32(0),
			minTs:    atomic.NewInt64(0xFFFFFFFF),
			maxTs:    atomic.NewInt64(0),
		},

		strStore: newBasicStrStore(ctx),

		blocksLock: &sync.RWMutex{},
		blocks:     make([]*Block, 0),
		pbChan:     make(chan pbMessage, ctx.GetMaxPartialBlocks()),
		pbQueue:    make([]*partialBlock, 0),
	}

	table.ingesterPool = &sync.Pool{New: func() interface{} { return table.newIngester() }}

	table.colInfoMap.getOrRegisterColumnId(TS_COLUMN_NAME, IntColumnType)
	_, inColNameMap := table.colInfoMap.getColumnInfo(TS_COLUMN_NAME)
	if !inColNameMap {
		ctx.Logger.Panic("missing ts column")
	}

	go func() {
		ticker := time.NewTicker(table.ctx.GetPartialBlockFlushInterval())
		for {
			select {
			case pbMsg := <-table.pbChan:
				table.pbQueue = append(table.pbQueue, pbMsg.pb)

				if len(table.pbQueue) == table.ctx.GetMaxPartialBlocks() || pbMsg.syncChan != nil {
					success := table.processPbQueue()
					if pbMsg.syncChan != nil {
						pbMsg.syncChan <- success
					}
				}

			case <-ticker.C:
				if success := table.processPbQueue(); success {
					table.ctx.Logger.Info("added blocks from peroidic task")
				}
			}
		}
	}()

	return table
}

func (t *Table) processPbQueue() bool {
	if len(t.pbQueue) == 0 {
		return false
	}

	success := true
	for _, pb := range t.pbQueue {
		block, err := pb.buildBlock()
		if err != nil {
			t.ctx.Logger.Error("fail to build block: %v", err)
			success = false
		} else {
			t.addBlock(block)
		}
	}
	t.pbQueue = t.pbQueue[:0]
	return success
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
	ingester := table.ingesterPool.Get().(*ingester)
	cnt_success := 0
	cnt_all := 0

	for scanner.Scan() {
		batch_cnt_success, batch_cnt_all := table.ingestBufOneBlock(ingester, scanner)
		cnt_success += batch_cnt_success
		cnt_all += batch_cnt_all
	}

	table.ctx.Logger.Infof("injested: %d, total: %d", cnt_success, cnt_all)
	table.ingesterPool.Put(ingester)
}

func (t *Table) addPartialBlock(pb *partialBlock, flushImmediatly bool) bool {
	if pb.rowCount == 0 {
		t.ctx.Logger.Error("refuse to add an empty block")
		return false
	}

	var syncChan chan bool
	if flushImmediatly {
		syncChan = make(chan bool)
	}

	t.pbChan <- pbMessage{pb, syncChan}

	if flushImmediatly {
		return <-syncChan
	}

	return true
}

func (table *Table) addBlock(block *Block) bool {
	if block.rowCount == 0 {
		table.ctx.Logger.Error("refuse to add an empty block")
		return false
	}

	// we probably should update metadata and the blocks atomically, but this is good for now.
	for {
		oldMinTs := table.tableInfo.minTs.Load()
		if swapped := table.tableInfo.minTs.CAS(oldMinTs, min(oldMinTs, block.minTs)); swapped {
			break
		}
	}

	for {
		oldMaxTs := table.tableInfo.maxTs.Load()
		if swapped := table.tableInfo.maxTs.CAS(oldMaxTs, max(oldMaxTs, block.maxTs)); swapped {
			break
		}
	}

	table.tableInfo.rowCount.Add(uint32(block.rowCount))

	table.blocksLock.Lock()
	defer func() {
		table.blocksLock.Unlock()
	}()

	table.blocks = append(table.blocks, block)
	sort.Slice(table.blocks, func(i, j int) bool {
		left, right := table.blocks[i], table.blocks[j]
		return left.minTs < right.minTs || (left.minTs == right.minTs && left.maxTs < right.maxTs)
	})
	return true
}

// Reads the given buffer and process the rows until either the buffer is empty
// or reached max rows per block, then add a new block to the table.
// *Note* this assumes that Scan was just called on the scanner.
func (table *Table) ingestBufOneBlock(ingester *ingester, scanner *bufio.Scanner) (int, int) {
	ingester.zeroOut()
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
		if err := ingester.ingestRawJson(rawJson); err == nil {
			cnt_success += 1
		} else {
			table.ctx.Logger.Errorf("failed to ingest json: %v", err)
		}

		if cnt_all == table.ctx.GetMaxRowsPerBlock() || !scanner.Scan() {
			break
		}
	}

	pb, err := ingester.buildPartialBlock()
	if err != nil {
		table.ctx.Logger.Error("fail to build partialBlock: %v", err)
		return 0, cnt_all
	}

	ok := table.addPartialBlock(pb, true /* flushImmediatly */)
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
	ingester := table.ingesterPool.Get().(*ingester)
	cnt_success := 0
	serverReceviedTs := time.Now().Unix()

	i := 0
	for i < len(rows) {
		ingester.zeroOut()
		cur_block_cnt := 0

		// process until end of rows or reached max rows per block
		for i < len(rows) {
			row := rows[i]
			i++
			cur_block_cnt++

			if useServerTs {
				row.Int[TS_COLUMN_NAME] = serverReceviedTs
			}

			if err := ingester.ingestRawJson(RawJson{
				Int: row.Int,
				Str: row.Str,
			}); err != nil {
				table.ctx.Logger.Errorf("failed to ingest row: %v", err)
			}

			if cur_block_cnt == table.ctx.GetMaxRowsPerBlock() {
				break
			}
		}

		pb, err := ingester.buildPartialBlock()
		if err != nil {
			table.ctx.Logger.Error("fail to build partialBlock: %v", err)
			continue
		}

		if ok := table.addPartialBlock(pb, false /* flushImmediatly */); ok {
			cnt_success += cur_block_cnt
		}
	}

	table.ctx.Logger.Infof("injested: %d, total: %d", cnt_success, len(rows))
	table.ingesterPool.Put(ingester)
	return cnt_success
}
