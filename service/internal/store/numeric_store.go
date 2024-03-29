package store

import (
	"bapi/internal/pb"
	"errors"

	"github.com/kelindar/bitmap"
)

type localColumnId = uint16
type valueIndex = uint16

const nullValueIndex = valueIndex(0)

/**
 * An immutable data structure for storing numerical values of the same data type
 *
 * Concepts:
 * 	- "row" refers to a record being stored in Bapi (same as the rest of the codebase),
 * 		not a row in a matrix, unless specified.
 * 	- Under the context of numericStore, an "id" is used as index into some slice.
 * 	- Given a row looks like {"count": 3}, we say this row has value 3 in the column "count".
 *
 * matrix:
 * 	- A 2d matrix of size (colCount, rowCount).
 * 	- `matrix[localColId][rowIdx]` is the `valueIdx` for getting the value of the row in this column.
 * 	- Null value is represented by having `valueIdx` of `nullValueIndex``.
 * values:
 * 	- A 2d slice of size (colCount, valueCount).
 * 	- `values[localColId][valueIdx]` is a value in this column.
 *  - `values[localColId]` is allowed to have duplicated values.
 * 			This is to support merging numeric storage without having to de-duplicate the values.
 * columnIds:
 * 	- For mapping a table-level column id to the local column id.
 *
 * Invariants (of initialized numericStore):
 * 	1. A `columnId` is present in `columnIds` iff this storage has at least one row that has value in the column.
 * 		i.e. `some(matrix[localColId], valueIdx => valueIdx != nullValueIndex)` for all columnId
 * 		since we do not store a column if no row has value in it.
 * 	2. A value is in `values[localColId]` iff there is at least one row has this value in the column.
 * 	3. `every(matrix[localColId], valueIdx => valueIdx < len(values[localColId]))` since they are used as index.
 * 	4. `every(columnIds[colId], localColId => localColId < len(values) && localColId < len(matrix))` since they are used as index.
 * 	5. Null value is represented by having `valueIdx` of nullValueIndex;
 * 		`values[localColId][nullValueIndex]` is a zero initialized placeholder, representing null,
 * 		thus `len(values[localColId]) > 0` for all localColId.
 * 	6. All matrix[localColId] have the same length and len(matrix[localColId]) > 0
 *  7. len(matrix) > 0 and len(matrix) == len(values) == len(columnIds)
 *
 * Example: to get the value in column with `columnId` for the row of `rowIdx`:
 * 	1. localColId := columnIds[columnId]
 * 	2. valueIdx := matrix[localColId][rowIdx]
 * 	3. value := values[localColId][valueIdx]
 */
type numericStore[T numeric] struct {
	matrix [][]valueIndex
	values [][]T

	columnIds map[columnId]localColumnId
}

// Creates an uninitialized storage.
// The created storage violates the invariants because the values are not populated and
// the caller is responsible to initialized the storage correctly.
func newNumericStore[T numeric](colCount int, rowCount int) (*numericStore[T], bool) {
	if colCount <= 0 || rowCount <= 0 {
		return nil, false
	}

	matrix := make([][]valueIndex, colCount)
	for rowIdx := 0; rowIdx < colCount; rowIdx++ {
		matrix[rowIdx] = make([]valueIndex, rowCount)
	}

	return &numericStore[T]{
		matrix:    matrix,
		values:    make([][]T, colCount),
		columnIds: make(map[columnId]localColumnId),
	}, true
}

// Creates a numericStore from a partialColumns
// The caller is responsible to make sure that colId and rowId are valid.
func fromPartialColumns[T numeric](partialColumns partialColumns[T], rowCount int) (*numericStore[T], error) {
	colCount := len(partialColumns)
	storage, ok := newNumericStore[T](colCount, rowCount)
	if !ok {
		return nil, errors.New("failed to create numeric storage")
	}

	localColId := localColumnId(0)
	processCol := func(colId columnId) {
		storage.columnIds[colId] = localColId
		columnData := partialColumns[colId]

		// `Values` is a slice whose first element is a zero initialized T
		// indicating null and the rest are the values seen in the rows.
		storage.values[localColId] = make([]T, len(columnData)+1)
		valueIdx := valueIndex(1)
		for value := range columnData {
			storage.values[localColId][valueIdx] = value
			for _, rowId := range columnData[value] {
				storage.matrix[localColId][rowId] = valueIdx
			}

			valueIdx++
		}

		localColId++
	}

	// we want to make sure ts col is always the first being processed
	// partialColumns is a map thus we can't rely on the iteration and need to handle it separately
	if _, hasTs := partialColumns[columnId(TS_COLUMN_ID)]; hasTs {
		processCol(columnId(TS_COLUMN_ID))
	}
	for colId := range partialColumns {
		if colId == columnId(TS_COLUMN_ID) {
			continue
		}
		processCol(colId)
	}

	return storage, nil
}

// Gets the local column id
// The column exist in the storage iff there is at least one row has value in this column.
func (ns *numericStore[T]) getLocalColumnId(colInfo *ColumnInfo) (localColumnId, bool) {
	localColId, exists := ns.columnIds[colInfo.id]
	if !exists {
		return 0, false
	}

	return localColId, true
}

// Performs the filtering and updates the bitmap in filterCtx in place.
func (ns *numericStore[T]) filterNumericStore(
	ctx *filterCtx,
	filter numericFilter[T],
) {
	rows := ns.matrix[filter.localColId]
	if filter.op == pb.FilterOp_NONNULL || filter.op == pb.FilterOp_NULL {
		filterByNullable(ctx, &filter, rows)
		return
	}

	targetValues, predicate, ok := getTargetValueAndPredicate(&filter)
	if !ok {
		ctx.ctx.Logger.DPanicf("unexpected filter op: %d", filter.op)
		return
	}

	values := ns.values[filter.localColId]
	for rowIdx, valueIdx := range rows {
		if !ctx.bitmap.Contains(uint32(rowIdx)) {
			continue
		}
		if valueIdx == nullValueIndex {
			switch filter.op {
			case pb.FilterOp_NULL, pb.FilterOp_NE:
				continue
			default:
				ctx.bitmap.Remove(uint32(rowIdx))
				continue
			}
		}

		keep := false
		for _, targetValue := range targetValues {
			keep = keep || predicate(values[valueIdx], targetValue)
		}
		if !keep {
			ctx.bitmap.Remove(uint32(rowIdx))
		}
	}
}

// Called when the column for filtering does not exist
// If filtering for "is null" or "!= value", all rows must be true, thus can
// continue process the next filter. Otherwise, clears the bits and returns false.
func canContinueElseStopForColNotExist(ctx *filterCtx, op pb.FilterOp) bool {
	switch op {
	case pb.FilterOp_NULL, pb.FilterOp_NE:
		return true
	default:
		ctx.bitmap.Clear()
		return false
	}
}

/**
 * Represents the result of a query
 *
 * Only rows/cols requested in the getCtx are included, so the colIdx and rowIdx are
 * different from rowId, colId in other parts of the codebase.
 *  - `colIdx` is indexed on `getCtx.columns`.
 *  - `rowIdx` is indexed on `getCtx.bitmap`'s 1 bits.
 *
 * hasValue[colIdx].Contains(rowIdx) is true iff the row has value in the column
 * matrix[colIdx][rowIdx] is the value of the row in the column
 *
 * Invariants:
 * 	len(matrix) == len(hasValue)
 * 	max(hasValue[colIdx]) < len(matrix[colIdx]) for all colIdx
 */
type numericStoreResult[T numeric] struct {
	matrix   [][]T
	hasValue [][]bool
}

func newNumericStoreResult[T numeric](rowCount int, colCount int) *numericStoreResult[T] {
	matrix := make([][]T, colCount)
	hasValue := make([][]bool, colCount)
	for colId := 0; colId < colCount; colId++ {
		matrix[colId] = make([]T, rowCount)
		hasValue[colId] = make([]bool, rowCount)
	}

	return &numericStoreResult[T]{
		matrix,
		hasValue,
	}
}

/**
 * Gets the columns for the rows specified in the given ctx
 * The order of the rows is retained; it's the same as the order in the storage.
 * The order of the cols is also retained; it's the same as the order in the resultCtx.
 */
func (ns *numericStore[T]) get(
	ctx *getCtx,
	recordValue bool,
) (*numericStoreResult[T], map[T]bool) {
	result := newNumericStoreResult[T](ctx.bitmap.Count(), len(ctx.columns))
	resultValues := make(map[T]bool)

	resultColIdx := 0
	for _, colInfo := range ctx.columns {
		localColumnId, ok := ns.getLocalColumnId(colInfo)
		if !ok {
			continue
		}

		values := ns.values[localColumnId]
		rows := ns.matrix[localColumnId]
		resultMatrix := result.matrix[resultColIdx]
		resultHasValue := result.hasValue[resultColIdx]

		resultRowIdx := uint32(0)
		ctx.bitmap.Range(func(rowIdx uint32) {
			if int(rowIdx) >= len(rows) {
				ctx.ctx.Logger.DPanic("perform get on invalid storage or with invalid ctx")
				return
			}

			valueIdx := rows[rowIdx]
			if valueIdx != nullValueIndex {
				resultHasValue[resultRowIdx] = true
				resultMatrix[resultRowIdx] = values[valueIdx]
				if recordValue {
					resultValues[values[valueIdx]] = true
				}
			}

			resultRowIdx++
		})

		resultColIdx++
	}

	return result, resultValues
}

// Returns true if the invariants hold
// @see numericStore struct comment for details
func (ns *numericStore[T]) debugInvariantCheck() error {
	if len(ns.columnIds) <= 0 || len(ns.columnIds) != len(ns.matrix) || len(ns.matrix) != len(ns.values) {
		// invariants #7
		return errors.New("columnIds, matrix, and values do not have the same positive length")
	}

	rowCount := len(ns.matrix[0])
	isValidMatrix := every(ns.matrix, func(matrixRow []uint16) bool {
		return len(matrixRow) == rowCount
	})
	if rowCount == 0 || !isValidMatrix {
		// invariant #6
		return errors.New("matrix rows have zero or different length")
	}

	for columnId := range ns.columnIds {
		localColId := ns.columnIds[columnId]
		if int(localColId) >= len(ns.values) || int(localColId) >= len(ns.matrix) {
			// invariant #4
			return errors.New("invalid localColId")
		}

		if len(ns.values[localColId]) == 0 {
			// invariant #5
			return errors.New("values[localColId] has no null element")
		}

		someRowHasValueInCol := some(
			ns.matrix[localColId],
			func(idx valueIndex) bool { return idx != nullValueIndex })
		if !someRowHasValueInCol {
			// invariant #1
			return errors.New("have an emptry column")
		}

		valueIsUsedBySomeRow := bitmap.Bitmap{}
		valueIsUsedBySomeRow.Set(uint32(nullValueIndex))
		badValueIdx := false

		for _, valueIdx := range ns.matrix[localColId] {
			if int(valueIdx) >= len(ns.values[localColId]) {
				badValueIdx = true
				continue
			}

			valueIsUsedBySomeRow.Set(uint32(valueIdx))
		}

		if badValueIdx {
			// invariant #3
			return errors.New("invalid valueIdx")
		}

		lastOne, hasLastOne := valueIsUsedBySomeRow.Max()
		// invariant #2
		if valueIsUsedBySomeRow.Count() != len(ns.values[localColId]) ||
			!hasLastOne ||
			int(lastOne) != len(ns.values[localColId])-1 {
			return errors.New("there is a value not used by any row")
		}
	}

	return nil
}
