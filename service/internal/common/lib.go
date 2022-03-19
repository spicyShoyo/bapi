package common

import (
	"time"

	"go.uber.org/zap"
)

type BapiCfg struct {
	// The upper limit for the number of columns in a table
	maxColumn                  uint16
	maxStrCount                uint32
	maxRowsPerBlock            uint16
	maxParitialBlocks          uint16
	partialBlocksFlushInterval time.Duration
}

func NewDefaultCfg() *BapiCfg {
	return &BapiCfg{
		maxColumn:                  512,     // should be enough for common product use cases
		maxStrCount:                0xFFFFF, //1048575
		maxRowsPerBlock:            0xFFF,   // an arbitrary number...
		maxParitialBlocks:          0xF,     // max number of partial blocks in partialBlockQueue
		partialBlocksFlushInterval: 5 * time.Second,
	}
}

type BapiCtx struct {
	Logger *zap.SugaredLogger

	cfg *BapiCfg
}

func NewBapiCtx() *BapiCtx {
	logger, _ := zap.NewDevelopment()
	return &BapiCtx{
		Logger: logger.Sugar(),

		cfg: NewDefaultCfg(),
	}
}

func NewTestBapiCtx() *BapiCtx {
	return NewBapiCtx()
}

func (ctx *BapiCtx) GetMaxColumn() uint16 {
	return ctx.cfg.maxColumn
}

func (ctx *BapiCtx) GetMaxStrCount() uint32 {
	return ctx.cfg.maxStrCount
}

func (ctx *BapiCtx) GetMaxRowsPerBlock() int {
	return int(ctx.cfg.maxRowsPerBlock)
}

func (ctx *BapiCtx) GetMaxPartialBlocks() int {
	return int(ctx.cfg.maxParitialBlocks)
}

func (ctx *BapiCtx) GetPartialBlockFlushInterval() time.Duration {
	return ctx.cfg.partialBlocksFlushInterval
}
