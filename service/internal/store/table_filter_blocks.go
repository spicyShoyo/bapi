package store

import (
	"bapi/internal/pb"
	"sort"
	"time"
)

// Filters all the blocks of the table to return rows and cols needed for the query result.
func (t *Table) queryBlocks(query queryWithFilter) ([]*BlockQueryResult, bool) {
	blocksQuery, ok := t.newBlockQuery(query)
	if !ok {
		t.ctx.Logger.Warn("failed to build query")
		return nil, false
	}

	blocksToQuery, ok := t.getBlocksToQuery(query)
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

	if len(blockResults) == 0 {
		return nil, false
	}
	return blockResults, true
}

// --------------------------- internals ----------------------------
// A wrapper around pb querys providing getters for filtering related fields
type queryWithFilter struct {
	q interface{} // *pb.RowsQuery | *pb.TableQuery
}

func (q *queryWithFilter) getMinTs() int64 {
	if query, ok := q.q.(*pb.RowsQuery); ok {
		return query.MinTs
	}
	if query, ok := q.q.(*pb.TableQuery); ok {
		return query.MinTs
	}
	return 0
}

func (q *queryWithFilter) getMaxTs() (int64, bool) {
	if query, ok := q.q.(*pb.RowsQuery); ok {
		return query.GetMaxTs(), query.MaxTs != nil
	}
	if query, ok := q.q.(*pb.TableQuery); ok {
		return query.GetMaxTs(), query.MaxTs != nil
	}
	return 0, false
}

func (q *queryWithFilter) getIntFilters() []*pb.Filter {
	if query, ok := q.q.(*pb.RowsQuery); ok {
		return query.IntFilters
	}
	if query, ok := q.q.(*pb.TableQuery); ok {
		return query.IntFilters
	}
	return make([]*pb.Filter, 0)
}

func (q *queryWithFilter) getStrFilters() []*pb.Filter {
	if query, ok := q.q.(*pb.RowsQuery); ok {
		return query.StrFilters
	}
	if query, ok := q.q.(*pb.TableQuery); ok {
		return query.StrFilters
	}
	return make([]*pb.Filter, 0)
}

func (q *queryWithFilter) getIntColNames() []string {
	if query, ok := q.q.(*pb.RowsQuery); ok {
		return query.IntColumnNames
	}
	if query, ok := q.q.(*pb.TableQuery); ok {
		return append(query.GroupbyIntColumnNames, query.AggIntColumnNames...)
	}
	return make([]string, 0)
}

func (q *queryWithFilter) getStrColNames() []string {
	if query, ok := q.q.(*pb.RowsQuery); ok {
		return query.StrColumnNames
	}
	if query, ok := q.q.(*pb.TableQuery); ok {
		return query.GroupbyStrColumnNames
	}
	return make([]string, 0)
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

func (t *Table) verifyQueryFilter(query queryWithFilter) bool {
	t.ctx.Logger.Info(t.tableInfo.rowCount.Load(), t.tableInfo.maxTs.Load(), query.getMinTs())
	// has value and t.minTs <= query.minTs <= query.MaxTs < t.maxTs
	if t.tableInfo.rowCount.Load() == 0 || t.tableInfo.maxTs.Load() < query.getMinTs() {
		t.ctx.Logger.Info(query)
		return false
	}

	maxTs, hasMaxTs := query.getMaxTs()
	if hasMaxTs {
		maxTs := maxTs
		if t.tableInfo.minTs.Load() > maxTs || query.getMinTs() > maxTs {
			t.ctx.Logger.Info(query)
			return false
		}
	}

	return true
}

func (t *Table) newBlockQuery(query queryWithFilter) (*blockQuery, bool) {
	if ok := t.verifyQueryFilter(query); !ok {
		t.ctx.Logger.Info(query)
		return nil, false
	}

	blockFilter, ok := t.newBlockfilter(query)
	if !ok {
		t.ctx.Logger.Info(query)
		return nil, false
	}

	intColumns, ok := t.colInfoMap.getColumnInfoSliceForType(query.getIntColNames(), IntColumnType)
	if !ok {
		t.ctx.Logger.Info(query)
		return nil, false
	}

	strColumns, ok := t.colInfoMap.getColumnInfoSliceForType(query.getStrColNames(), StrColumnType)
	if !ok {
		t.ctx.Logger.Info(query)
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
