package store

import (
	"fmt"
	"sync"

	"go.uber.org/atomic"
)

type columnInfoMap struct {
	colMap   *sync.Map
	colCount *atomic.Uint32
}

func newColumnInfoMap() columnInfoMap {
	return columnInfoMap{
		colMap:   &sync.Map{},
		colCount: atomic.NewUint32(0),
	}
}

func (t *Table) getColumnInfo(colName string) (*ColumnInfo, bool) {
	if colInfo, ok := t.colInfoMap.colMap.Load(colName); ok {
		return colInfo.(*ColumnInfo), true
	}
	return nil, false
}

// Creates a new column
func (t *Table) registerNewColumn(colName string, colType ColumnType) (columnId, error) {
	var colCount uint32
	for {
		colCount = t.colInfoMap.colCount.Load()
		if colCount == uint32(t.ctx.GetMaxColumn()) {
			return 0, fmt.Errorf("too many columns, max: %d", t.ctx.GetMaxColumn())
		}
		// atomically increase the columnCount and make sure we are the only one reserved the
		// colCount, which will be used as the columnId
		if swapped := t.colInfoMap.colCount.CAS(colCount, colCount+1); swapped {
			break
		}
	}

	colId := columnId(colCount)
	columnInfo := &ColumnInfo{
		id:         colId,
		Name:       colName,
		ColumnType: colType,
	}

	if _, loaded := t.colInfoMap.colMap.LoadOrStore(colName, columnInfo); loaded {
		return 0, fmt.Errorf("column already exists")
	}

	return colId, nil
}

// Gets or creates a column of the given name and type
func (table *Table) getOrRegisterColumnId(colName string, colType ColumnType) (columnId, error) {
	if columnInfo, ok := table.getColumnInfo(colName); ok {
		if columnInfo.ColumnType != colType {
			return 0, fmt.Errorf(
				"column type mismatch for %s, expected: %d, got: %d", columnInfo.Name, columnInfo.ColumnType, colType)
		}
		return columnInfo.id, nil
	}

	return table.registerNewColumn(colName, colType)
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
