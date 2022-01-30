package store

import (
	"bapi/internal/common"

	"github.com/kelindar/bitmap"
)

type strId uint32

type Block struct {
	minTs    int64
	maxTs    int64
	rowCount int

	storage blockStorage
}

func (b *Block) query(ctx *common.BapiCtx, query *blockQuery) (*BlockQueryResult, bool) {
	return b.storage.query(ctx, query)
}

type blockStorage interface {
	query(*common.BapiCtx, *blockQuery) (*BlockQueryResult, bool)
}

// --------------------------- basicBlockStorage ----------------------------
type basicBlockStorage struct {
	minTs    int64
	maxTs    int64
	rowCount int

	intColsStorage *intColumnsStorage
	strColsStorage *strColumnsStorage
}

func newBasicBlockStorage(
	minTs int64,
	maxTs int64,
	rowCount int,
	strIdMap map[strId]string,
	strValueMap map[string]strId,
	intPartialColumns partialColumns[int64],
	strPartialColumns partialColumns[strId],
) (*basicBlockStorage, error) {
	intColStorage, err := newIntColumnsStorage(intPartialColumns, rowCount)
	if err != nil {
		return nil, err
	}

	strColStorage, err := newStrColumnsStorage(strPartialColumns, rowCount, strIdMap, strValueMap)
	if err != nil {
		return nil, err
	}

	return &basicBlockStorage{
		minTs:    minTs,
		maxTs:    maxTs,
		rowCount: rowCount,

		intColsStorage: intColStorage,
		strColsStorage: strColStorage,
	}, nil
}

func (bbs *basicBlockStorage) query(ctx *common.BapiCtx, query *blockQuery) (*BlockQueryResult, bool) {
	bbs.filterBlock(ctx, &query.filter)
	bitmap, ok := bbs.filterBlock(ctx, &query.filter)
	if !ok {
		return nil, false
	}

	return bbs.buildResult(ctx, query, bitmap)
}

// --------------------------- build result ----------------------------
func (bbs *basicBlockStorage) buildResult(ctx *common.BapiCtx, query *blockQuery, bitmap *bitmap.Bitmap) (*BlockQueryResult, bool) {
	intResult := bbs.intColsStorage.get(&getCtx{
		ctx:     ctx,
		bitmap:  bitmap,
		columns: query.intColumns,
	})
	strResult := bbs.strColsStorage.get(&getCtx{
		ctx:     ctx,
		bitmap:  bitmap,
		columns: query.strColumns,
	})

	return &BlockQueryResult{
		Count:     bitmap.Count(),
		IntResult: intResult,
		StrResult: strResult,
	}, true
}

// --------------------------- filter ----------------------------
func (bbs *basicBlockStorage) filterBlock(
	ctx *common.BapiCtx,
	filter *BlockFilter,
) (*bitmap.Bitmap, bool) {
	filterCtx, hasRows := bbs.getBlockFilterCtx(ctx, filter.MinTs, filter.MaxTs)
	if !hasRows {
		return nil, false
	}

	ctx.Logger.Info("filtering block")

	bbs.intColsStorage.filter(filterCtx, filter.intFilters)
	bbs.strColsStorage.filter(filterCtx, filter.strFilters)

	if _, hasRows = filterCtx.bitmap.Min(); !hasRows {
		return nil, false
	}

	return filterCtx.bitmap, true
}

func (bbs *basicBlockStorage) getBlockFilterCtx(ctx *common.BapiCtx,
	queryMinTs int64,
	queryMaxTs int64) (*filterCtx, bool) {
	startIdx, endIdx, ok := bbs.getStartIdxAndEndIdx(ctx, queryMinTs, queryMaxTs)
	if !ok {
		return nil, false
	}

	bitmap, ok := newBitmapWithOnesRange(bbs.rowCount, startIdx, endIdx)
	if !ok {
		ctx.Logger.DPanicf(
			"unable to create bitmap with size: %d, startIdx: %d, endIdx: %d", bbs.rowCount, startIdx, endIdx)
		return nil, false
	}
	return &filterCtx{
		ctx,
		bitmap,
		startIdx,
		endIdx,
	}, true
}

func (bbs *basicBlockStorage) getStartIdxAndEndIdx(
	ctx *common.BapiCtx,
	queryMinTs int64,
	queryMaxTs int64) (uint32, uint32, bool) {
	if bbs.maxTs < queryMinTs || bbs.minTs > queryMaxTs {
		ctx.Logger.Info("skip block for not in range")
		return 0, 0, false
	}

	startIdx, endIdx, ok := bbs.intColsStorage.getStartIdxAndEndIdx(queryMinTs, queryMaxTs)
	if !ok {
		ctx.Logger.DPanicf("storage is empty or not correctly sorted by timestamp")
	}

	return uint32(startIdx), uint32(endIdx), ok
}

// Creates a bitmap of the given set and sets the bits [startIdx, endIdx] to 1.
// Invariant: startIdx <= endIdx < size. The bitmap starts from idx 0.
func newBitmapWithOnesRange(size int, startIdx uint32, endIdx uint32) (*bitmap.Bitmap, bool) {
	if !(startIdx <= endIdx && endIdx < uint32(size)) {
		return nil, false
	}
	bitmap := &bitmap.Bitmap{}
	bitmap.Grow(uint32(size))

	bitmap.Ones()
	bitmap.Filter(func(idx uint32) bool {
		return idx >= startIdx && idx <= endIdx
	})

	return bitmap, true
}
