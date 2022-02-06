package store

import (
	"bapi/internal/common"

	"github.com/kelindar/bitmap"
)

type strId uint32

// An immutable data structure storing some rows in the sorted by ts order
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

func newBasicBlockStorage(pb *partialBlock) (*basicBlockStorage, error) {
	intColStorage, err := newIntColumnsStorage(pb.intPartialColumns, pb.rowCount)
	if err != nil {
		return nil, err
	}

	strColStorage, err := newStrColumnsStorage(pb.strPartialColumns, pb.rowCount, pb.strIdSet)
	if err != nil {
		return nil, err
	}

	return &basicBlockStorage{
		minTs:    pb.minTs,
		maxTs:    pb.maxTs,
		rowCount: pb.rowCount,

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
	filter *blockFilter,
) (*bitmap.Bitmap, bool) {
	filterCtx, hasRows := bbs.getBlockFilterCtx(ctx, filter.minTs, filter.maxTs)
	if !hasRows {
		return nil, false
	}

	ctx.Logger.Info("filtering block")

	bbs.intColsStorage.filter(filterCtx, filter.tsFilters)
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
	if bbs.maxTs < queryMinTs || bbs.minTs > queryMaxTs {
		ctx.Logger.Info("skip block for not in range")
		return nil, false
	}

	bitmap := newBitmapWithOnes(bbs.rowCount)

	return &filterCtx{
		ctx,
		bitmap,
		queryMinTs,
		queryMaxTs,
	}, true
}

// Creates a bitmap of the given size and set all bits to 1
func newBitmapWithOnes(size int) *bitmap.Bitmap {
	bitmap := &bitmap.Bitmap{}
	bitmap.Grow(uint32(size))

	bitmap.Ones()
	bitmap.Filter(func(idx uint32) bool {
		return idx < uint32(size)
	})

	return bitmap
}
