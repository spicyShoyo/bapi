package store

import "bapi/internal/pb"

type aggBucket struct {
	ints     []int64
	strIds   []strId
	tsBucket int
	aggInts  []int64
}

type aggregationResult struct {
	ints     [][]int64
	hasValue [][]bool
}

func aggregateForTableQuery(r *BlockQueryResult, firstAggIntCol int) {
	hasher := newNumericHasher(r.Count)
	for colIdx := 0; colIdx < firstAggIntCol; colIdx++ {
		hasher.processIntCol(r.IntResult, colIdx)
	}
	// we do not support aggregation on str so all str cols are for group by
	for colIdx := 0; colIdx < len(r.StrResult.matrix); colIdx++ {
		hasher.processStrCol(r.StrResult, colIdx)
	}

	hashes := hasher.finalize()
	uniqueHashes := make(map[uint64]bool, 0)
	for _, hash := range hashes {
		uniqueHashes[hash] = true
	}

}

type hasher interface {
	processIntCol(r IntResult, colIdx int)
	processStrCol(r StrResult, colIdx int)
	processTsBucket(values []int64, startTs int64, gran pb.TimeGran)
	finalized() []uint64
}

type numericHasher struct {
	count  int
	hashes []uint64
}

func newNumericHasher(count int) *numericHasher {
	return &numericHasher{
		count:  count,
		hashes: make([]uint64, count),
	}
}

func (h *numericHasher) processIntCol(r IntResult, colIdx int) {
	values := r.matrix[colIdx]
	hasValue := r.hasValue[colIdx]
	for i, v := range values {
		h.hashes[i] = hash128To64(h.hashes[i], uint64(v))
		if !hasValue[i] {
			h.hashes[i] = hash128To64(h.hashes[i], uint64(v))
		}
	}
}

func (h *numericHasher) processStrCol(r StrResult, colIdx int) {
	values := r.matrix[colIdx]
	hasValue := r.hasValue[colIdx]
	for i, v := range values {
		h.hashes[i] = hash128To64(h.hashes[i], uint64(v))
		if !hasValue[i] {
			h.hashes[i] = hash128To64(h.hashes[i], uint64(v))
		}
	}
}

func (h *numericHasher) processTsBucket(values []int64, startTs int64, gran uint64) {
	for i, v := range values {
		bucket := uint64(v-startTs) / gran
		h.hashes[i] = hash128To64(h.hashes[i], bucket)
	}
}

func (h *numericHasher) finalize() []uint64 {
	return h.hashes
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
