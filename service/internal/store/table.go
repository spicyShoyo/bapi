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

func (t *Table) GetTableInfo() *pb.TableInfo {
	intColumns, strColumns := t.colInfoMap.getColumns()

	pbIntColumns := make([]*pb.ColumnInfo, 0)
	pbStrColumns := make([]*pb.ColumnInfo, 0)

	for _, colInfo := range intColumns {
		pbIntColumns = append(pbIntColumns, &pb.ColumnInfo{
			ColumnName: colInfo.Name,
			ColumnType: pb.ColumnType_INT,
		})
	}

	for _, colInfo := range strColumns {
		pbStrColumns = append(pbStrColumns, &pb.ColumnInfo{
			ColumnName: colInfo.Name,
			ColumnType: pb.ColumnType_STR,
		})
	}

	return &pb.TableInfo{
		TableName:  t.tableInfo.name,
		RowCount:   int64(t.tableInfo.rowCount.Load()),
		MinTs:      int64(t.tableInfo.minTs.Load()),
		MaxTs:      int64(t.tableInfo.maxTs.Load()),
		IntColumns: pbIntColumns,
		StrColumns: pbStrColumns,
	}
}
