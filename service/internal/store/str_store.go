package store

import (
	"bapi/internal/common"
	"sync"

	"go.uber.org/atomic"
)

const nonexistentStr = strId(0xFFFFFFFF)

type readOnlyStrStore interface {
	getStrId(str string) (strId, bool)
	getStr(id strId) (string, bool)
}

type strStore interface {
	readOnlyStrStore
	getOrInsertStrId(str string) (strId, bool, bool)
}

type basicStrStore struct {
	ctx         *common.BapiCtx
	strIdMap    sync.Map // map[strId]string
	strValueMap sync.Map // map[string]strId
	strCount    *atomic.Uint32
}

func newBasicStrStore(ctx *common.BapiCtx) *basicStrStore {
	return &basicStrStore{
		ctx:         ctx,
		strIdMap:    sync.Map{},
		strValueMap: sync.Map{},
		strCount:    atomic.NewUint32(0),
	}
}

func (s *basicStrStore) getOrInsertStrId(str string) (strId, bool, bool) {
	if id, loaded := s.strValueMap.Load(str); loaded {
		// happy path: string is already in the store
		return id.(strId), true, true
	}

	var strCount uint32
	for {
		strCount = s.strCount.Load()
		if strCount == s.ctx.GetMaxStrCount() {
			return nonexistentStr, false, false
		}

		// atomically increase the strCount and make sure we are the only one reserved the
		// strCount, which will be used as the strId. The strId still can be wasted if the
		// later insertion fails, which is fine.
		if swapped := s.strCount.CAS(strCount, strCount+1); swapped {
			break
		}
	}

	reservedStrId := strId(strCount)
	// we still need to load in case it's stored before we get the lock
	id, loaded := s.strValueMap.LoadOrStore(str, reservedStrId)
	if !loaded {
		// stored: also need to insert to strIdMap and update nextStrId
		s.strIdMap.Store(id, str)
	}

	return id.(strId), loaded, true
}

func (s *basicStrStore) getStrId(str string) (strId, bool) {
	if id, loaded := s.strValueMap.Load(str); loaded {
		return id.(strId), true
	}

	return nonexistentStr, false
}

func (s *basicStrStore) getStr(id strId) (string, bool) {
	if str, loaded := s.strIdMap.Load(id); loaded {
		return str.(string), true
	}

	return "", false
}
