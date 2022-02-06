package store

import (
	"bapi/internal/common"
	"fmt"
	"sync"
)

type columnInfoMap struct {
	ctx       *common.BapiCtx
	colMap    sync.Map
	m         sync.Mutex
	nextColId columnId
}

func newColumnInfoMap(ctx *common.BapiCtx) *columnInfoMap {
	return &columnInfoMap{
		ctx:       ctx,
		colMap:    sync.Map{},
		m:         sync.Mutex{},
		nextColId: columnId(0),
	}
}

func (m *columnInfoMap) getColumnInfo(colName string) (*ColumnInfo, bool) {
	if colInfo, ok := m.colMap.Load(colName); ok {
		return colInfo.(*ColumnInfo), true
	}
	return nil, false
}

// Gets or creates a column of the given name and type
func (m *columnInfoMap) getOrRegisterColumnId(colName string, colType ColumnType) (columnId, error) {
	if columnInfo, ok := m.getColumnInfo(colName); ok {
		if columnInfo.ColumnType != colType {
			return 0, fmt.Errorf(
				"column type mismatch for %s, expected: %d, got: %d", columnInfo.Name, columnInfo.ColumnType, colType)
		}
		return columnInfo.id, nil
	}

	return m.registerNewColumn(colName, colType)
}

func (m *columnInfoMap) getColumnInfoSliceForType(colNames []string, colType ColumnType) ([]*ColumnInfo, bool) {
	columns := make([]*ColumnInfo, 0)

	for _, colName := range colNames {
		colInfo, ok := m.getColumnInfoAndAssertType(colName, colType)
		if !ok {
			continue
		}
		columns = append(columns, colInfo)
	}

	allMatch := len(columns) == len(colNames)
	return columns, allMatch
}

func (m *columnInfoMap) getColumnInfoAndAssertType(colName string, colType ColumnType) (*ColumnInfo, bool) {
	colInfo, ok := m.getColumnInfo(colName)
	if !ok {
		m.ctx.Logger.Warnf("unknown column: %s", colName)
		return nil, false
	}

	if colInfo.ColumnType != colType {
		m.ctx.Logger.Warnf("unexpected type for column: %s, expected: %d, got: %d", colName, colInfo.ColumnType, colType)
		return nil, false
	}

	return colInfo, true
}

// --------------------------- internal ----------------------------
func (m *columnInfoMap) registerNewColumn(colName string, colType ColumnType) (columnId, error) {
	if uint16(m.nextColId) == m.ctx.GetMaxColumn() {
		return 0, fmt.Errorf("too many columns, max: %d", m.ctx.GetMaxColumn())
	}
	// we need this lock to make sure colId is not wasted or reused and no double insertion
	// of the same column.
	m.m.Lock()
	defer func() {
		m.m.Unlock()
	}()

	// need to check again since another thread may just inserted a column
	if uint16(m.nextColId) == m.ctx.GetMaxColumn() {
		return 0, fmt.Errorf("too many columns, max: %d", m.ctx.GetMaxColumn())
	}

	colInfo, loaded := m.colMap.LoadOrStore(colName, &ColumnInfo{
		id:         m.nextColId,
		Name:       colName,
		ColumnType: colType,
	})

	// another thread tried to insert the same colName and won the race.
	if loaded {
		return 0, fmt.Errorf("column already exists")
	}

	m.nextColId++
	return colInfo.(*ColumnInfo).id, nil
}
