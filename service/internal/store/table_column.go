package store

import (
	"bapi/internal/common"
	"fmt"
	"sync"

	"go.uber.org/atomic"
)

type columnInfoMap struct {
	ctx      *common.BapiCtx
	colMap   *sync.Map
	colCount *atomic.Uint32
}

func newColumnInfoMap(ctx *common.BapiCtx) columnInfoMap {
	return columnInfoMap{
		ctx:      ctx,
		colMap:   &sync.Map{},
		colCount: atomic.NewUint32(0),
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
	var colCount uint32
	for {
		colCount = m.colCount.Load()
		if colCount == uint32(m.ctx.GetMaxColumn()) {
			return 0, fmt.Errorf("too many columns, max: %d", m.ctx.GetMaxColumn())
		}
		// atomically increase the columnCount and make sure we are the only one reserved the
		// colCount, which will be used as the columnId. The colId still can be wasted if the
		// later insertion fails.
		if swapped := m.colCount.CAS(colCount, colCount+1); swapped {
			break
		}
	}

	colId := columnId(colCount)
	columnInfo := &ColumnInfo{
		id:         colId,
		Name:       colName,
		ColumnType: colType,
	}

	// another thread tried to insert the same colName and won the race.
	if _, loaded := m.colMap.LoadOrStore(colName, columnInfo); loaded {
		return 0, fmt.Errorf("column already exists")
	}

	return colId, nil
}
