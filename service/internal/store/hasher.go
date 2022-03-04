package store

type aggBucket struct {
	hash      uint64
	intVals   []int64
	intHasVal []bool
	strVals   []strId
	strHasVal []bool
	tsBucket  int
}

type hasher struct {
	c            *aggCtx
	r            *BlockQueryResult
	hashes       []uint64
	hashToRowIdx map[uint64]int
}

func buildHasherForBlock(ctx *aggCtx, blockResult *BlockQueryResult) *hasher {
	h := &hasher{
		c:            ctx,
		r:            blockResult,
		hashes:       make([]uint64, blockResult.Count),
		hashToRowIdx: make(map[uint64]int),
	}

	h.maybeProcessTsBucket()
	for colIdx := 0; colIdx < h.c.firstAggIntCol; colIdx++ {
		h.processIntCol(h.r.IntResult, colIdx)
	}
	// aggregation on str not supported so can assume all str cols are for group by
	for colIdx := 0; colIdx < len(h.r.StrResult.matrix); colIdx++ {
		h.processStrCol(h.r.StrResult, colIdx)
	}

	for rowIdx, hash := range h.hashes {
		h.hashToRowIdx[hash] = rowIdx
	}

	return h
}

func (h *hasher) getHashes() []uint64 {
	return h.hashes
}

func (h *hasher) getAggBucket(hash uint64) (*aggBucket, bool) {
	rowIdx, ok := h.hashToRowIdx[hash]
	if !ok {
		return nil, false
	}

	bucket := &aggBucket{
		hash:      uint64(hash),
		intVals:   make([]int64, h.c.firstAggIntCol),
		intHasVal: make([]bool, h.c.firstAggIntCol),
		strVals:   make([]strId, len(h.r.StrResult.matrix)),
		strHasVal: make([]bool, len(h.r.StrResult.matrix)),
		tsBucket:  0,
	}

	if h.c.isTimelineQuery {
		rowTs := h.r.IntResult.matrix[h.c.tsCol][rowIdx]
		bucket.tsBucket = int(rowTs-h.c.startTs) / int(h.c.gran)
	}

	for colIdx := 0; colIdx < h.c.firstAggIntCol; colIdx++ {
		bucket.intVals[colIdx] = h.r.IntResult.matrix[colIdx][rowIdx]
		bucket.intHasVal[colIdx] = h.r.IntResult.hasValue[colIdx][rowIdx]
	}

	for colIdx := 0; colIdx < len(h.r.StrResult.matrix); colIdx++ {
		bucket.strVals[colIdx] = h.r.StrResult.matrix[colIdx][rowIdx]
		bucket.strHasVal[colIdx] = h.r.StrResult.hasValue[colIdx][rowIdx]
	}

	return bucket, true
}

// --------------------------- internal ----------------------------
func (h *hasher) processIntCol(r IntResult, colIdx int) {
	values := r.matrix[colIdx]
	hasValue := r.hasValue[colIdx]
	for i, v := range values {
		h.hashes[i] = hash128To64(h.hashes[i], uint64(v))
		if !hasValue[i] {
			h.hashes[i] = hash128To64(h.hashes[i], uint64(v))
		}
	}
}

func (h *hasher) processStrCol(r StrResult, colIdx int) {
	values := r.matrix[colIdx]
	hasValue := r.hasValue[colIdx]
	for i, v := range values {
		h.hashes[i] = hash128To64(h.hashes[i], uint64(v))
		if !hasValue[i] {
			h.hashes[i] = hash128To64(h.hashes[i], uint64(v))
		}
	}
}

func (h *hasher) getTsBucket(rowIdx int) int {
	rowTs := h.r.IntResult.matrix[h.c.tsCol][rowIdx]
	return int(rowTs-h.c.startTs) / int(h.c.gran)
}

func (h *hasher) maybeProcessTsBucket() {
	if !h.c.isTimelineQuery {
		return
	}
	for rowIdx := 0; rowIdx < h.r.Count; rowIdx++ {
		bucket := uint64(h.getTsBucket(rowIdx))
		h.hashes[rowIdx] = hash128To64(h.hashes[rowIdx], bucket)
	}
}

// https://github.com/facebook/folly/blob/main/folly/hash/Hash.h#L65
func hash128To64(upper uint64, lower uint64) uint64 {
	kMul := uint64(0x9ddfea08eb382d69)
	a := (lower ^ upper) * kMul
	a ^= (a >> 47)
	b := (upper ^ a) * kMul
	b ^= (b >> 47)
	b *= kMul
	return b
}
