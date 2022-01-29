package common

import "go.uber.org/zap"

type BapiCfg struct {
	// The upper limit for the number of columns in a table
	maxColumn       uint16
	maxRowsPerBlock uint16
}

func NewDefaultCfg() *BapiCfg {
	return &BapiCfg{
		maxColumn:       512,   // should be enough for common product use cases
		maxRowsPerBlock: 0xFFF, // an arbitrary number...
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

func (ctx *BapiCtx) GetMaxColumn() int {
	return int(ctx.cfg.maxColumn)
}

func (ctx *BapiCtx) GetMaxRowsPerBlock() int {
	return int(ctx.cfg.maxRowsPerBlock)
}
