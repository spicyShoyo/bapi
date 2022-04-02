package store

import (
	"bapi/internal/common"
	"strings"
	"sync"

	"go.uber.org/atomic"
)

const nonexistentStr = strId(0xFFFFFFFF)

type readOnlyStrStore interface {
	search(colId columnId, searchStr string) ([]string, bool)
	getStrId(str string) (strId, bool)
	getStr(id strId) (string, bool)
}

type strStore interface {
	readOnlyStrStore
	getOrInsertStrId(str string, colId columnId) (strId, bool, bool)
}
type colStrLookup struct {
	m sync.Map // map[colId]*map[strId]bool
}

func (lookup *colStrLookup) add(sid strId, cid columnId) {
	strIds, ok := lookup.m.Load(cid)
	if !ok {
		strIds, _ = lookup.m.LoadOrStore(cid, &sync.Map{})
	}

	strIds.(*sync.Map).Store(sid, true)
}

func (lookup *colStrLookup) search(cid columnId, store readOnlyStrStore, searchStr string) ([]string, bool) {
	strIds, ok := lookup.m.Load(cid)
	if !ok {
		return nil, false
	}

	matched := make([]string, 0)
	strIds.(*sync.Map).Range(
		func(sid, _ interface{}) bool {
			str, ok := store.getStr(sid.(strId))
			if !ok {
				// This shouldn't happen
				return false
			}

			if strings.Contains(str, searchStr) {
				matched = append(matched, str)
			}

			return true
		},
	)

	return matched, len(matched) != 0
}

type basicStrStore struct {
	ctx         *common.BapiCtx
	strIdMap    sync.Map // map[strId]string
	strValueMap sync.Map // map[string]strId
	strCount    *atomic.Uint32
	lookup      colStrLookup
}

func newBasicStrStore(ctx *common.BapiCtx) *basicStrStore {
	return &basicStrStore{
		ctx:         ctx,
		strIdMap:    sync.Map{},
		strValueMap: sync.Map{},
		strCount:    atomic.NewUint32(0),
		lookup: colStrLookup{
			m: sync.Map{},
		},
	}
}

// Gets the id from the store if exists otherwise inserts it.
// The colId is used to build a map from col to its vals to support typeahead UI.
// Returns strId, loaded, ok.
func (s *basicStrStore) getOrInsertStrId(str string, colId columnId) (strId, bool, bool) {
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
		s.lookup.add(id.(strId), colId)
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

func (s *basicStrStore) search(colId columnId, searchStr string) ([]string, bool) {
	return s.lookup.search(colId, s, searchStr)
}
