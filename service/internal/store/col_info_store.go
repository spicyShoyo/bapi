package store

import (
	"bapi/internal/common"
	"fmt"
	"sync"
)

type colInfoStore struct {
	ctx       *common.BapiCtx
	colMap    sync.Map
	m         sync.Mutex
	nextColId columnId
}

func newColInfoStore(ctx *common.BapiCtx) *colInfoStore {
	return &colInfoStore{
		ctx:       ctx,
		colMap:    sync.Map{},
		m:         sync.Mutex{},
		nextColId: columnId(0),
	}
}

func (s *colInfoStore) getColumnInfo(colName string) (*ColumnInfo, bool) {
	if colInfo, ok := s.colMap.Load(colName); ok {
		return colInfo.(*ColumnInfo), true
	}
	return nil, false
}

// Gets or creates a column of the given name and type
func (s *colInfoStore) getOrRegisterColumnId(colName string, colType ColumnType) (columnId, error) {
	if columnInfo, ok := s.getColumnInfo(colName); ok {
		if columnInfo.ColumnType != colType {
			return 0, fmt.Errorf(
				"column type mismatch for %s, expected: %d, got: %d", columnInfo.Name, columnInfo.ColumnType, colType)
		}
		return columnInfo.id, nil
	}

	colId, _, err := s.registerNewColumn(colName, colType)
	if err != nil {
		return 0, err
	}

	return colId, nil
}

func (s *colInfoStore) getColumnInfoSliceForType(colNames []string, colType ColumnType) ([]*ColumnInfo, bool) {
	columns := make([]*ColumnInfo, 0)

	for _, colName := range colNames {
		colInfo, ok := s.getColumnInfoAndAssertType(colName, colType)
		if !ok {
			continue
		}
		columns = append(columns, colInfo)
	}

	allMatch := len(columns) == len(colNames)
	return columns, allMatch
}

func (s *colInfoStore) getColumnInfoAndAssertType(colName string, colType ColumnType) (*ColumnInfo, bool) {
	colInfo, ok := s.getColumnInfo(colName)
	if !ok {
		s.ctx.Logger.Warnf("unknown column: %s", colName)
		return nil, false
	}

	if colInfo.ColumnType != colType {
		s.ctx.Logger.Warnf("unexpected type for column: %s, expected: %d, got: %d", colName, colInfo.ColumnType, colType)
		return nil, false
	}

	return colInfo, true
}

// --------------------------- internal ----------------------------
func (s *colInfoStore) registerNewColumn(colName string, colType ColumnType) (columnId, bool, error) {
	if uint16(s.nextColId) == s.ctx.GetMaxColumn() {
		return 0, false, fmt.Errorf("too many columns, max: %d", s.ctx.GetMaxColumn())
	}
	// we need this lock to make sure colId is not wasted or reused and no double insertion
	// of the same column.
	s.m.Lock()
	defer func() {
		s.m.Unlock()
	}()

	// need to check again since another thread may just inserted a column
	if uint16(s.nextColId) == s.ctx.GetMaxColumn() {
		return 0, false, fmt.Errorf("too many columns, max: %d", s.ctx.GetMaxColumn())
	}

	colInfo, loaded := s.colMap.LoadOrStore(colName, &ColumnInfo{
		id:         s.nextColId,
		Name:       colName,
		ColumnType: colType,
	})

	// another thread tried to insert the same colName and won the race.
	if loaded {
		return colInfo.(*ColumnInfo).id, false, nil
	}

	s.nextColId++
	return colInfo.(*ColumnInfo).id, true, nil
}
