package store

import (
	"sync"
)

type readOnlyStrStore interface {
	getStrId(str string) (strId, bool)
	getStr(id strId) (string, bool)
}

type strStore interface {
	readOnlyStrStore
	getOrInsertStrId(str string) (strId, bool)
}

type basicStrStore struct {
	strIdMap    sync.Map // map[strId]string
	strValueMap sync.Map // map[string]strId
	m           sync.Mutex
	nextStrId   strId
}

func newBasicStrStore() *basicStrStore {
	return &basicStrStore{
		strIdMap:    sync.Map{},
		strValueMap: sync.Map{},
		m:           sync.Mutex{},
		nextStrId:   strId(0),
	}
}

func (s *basicStrStore) getOrInsertStrId(str string) (strId, bool) {
	if id, loaded := s.strValueMap.Load(str); loaded {
		// happy path: string is already in the store
		return id.(strId), true
	}

	// While Map is threadsafe, we need this lock to make sure strId is not reused and no double
	// insertion of the same string.
	s.m.Lock()
	defer func() {
		s.m.Unlock()
	}()

	// we still need to load in case it's stored before we get the lock
	id, loaded := s.strValueMap.LoadOrStore(str, s.nextStrId)
	if !loaded {
		// stored: also need to insert to strIdMap and update nextStrId
		s.strIdMap.Store(id, str)
		s.nextStrId++
	}

	return id.(strId), loaded
}

func (s *basicStrStore) getStrId(str string) (strId, bool) {
	if id, loaded := s.strValueMap.Load(str); loaded {
		return id.(strId), true
	}

	return strId(0), false
}

func (s *basicStrStore) getStr(id strId) (string, bool) {
	if str, loaded := s.strIdMap.Load(id); loaded {
		return str.(string), true
	}

	return "", false
}
