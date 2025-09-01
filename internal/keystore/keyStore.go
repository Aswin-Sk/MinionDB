package keystore

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type MiniKV struct {
	mu    sync.RWMutex
	index map[string][]byte
	wb    *WriteBatcher
}

func open(path string) (*MiniKV, error) {
	wb, err := NewWriteBatcher(path+".wal", 128, 5*time.Millisecond)
	if err != nil {
		return nil, err
	}

	return &MiniKV{
		index: make(map[string][]byte),
		wb:    wb,
	}, nil
}

func (db *MiniKV) apply(op opType, key string, val []byte) {
	db.mu.Lock()
	switch op {
	case opSet:
		db.index[key] = append([]byte(nil), val...)
	case opDel:
		delete(db.index, key)
	}
	db.mu.Unlock()
}

func (db *MiniKV) Set(key string, val []byte) error {
	db.mu.Lock()
	db.index[key] = append([]byte(nil), val...)
	db.mu.Unlock()
	return db.wb.EnqueueSet(key, val)
}

func (db *MiniKV) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	v, ok := db.index[key]
	db.mu.RUnlock()
	return v, ok
}

func (db *MiniKV) Delete(key string) error {
	db.mu.Lock()
	delete(db.index, key)
	db.mu.Unlock()
	return db.wb.EnqueueDel(key)
}

func (db *MiniKV) Close() error {
	return db.wb.Close()
}

func (db *MiniKV) swapWriteBatcher(newBatcher *WriteBatcher) *WriteBatcher {
	old := db.wb
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&db.wb)), unsafe.Pointer(newBatcher))
	return old
}
