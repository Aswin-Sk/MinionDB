package keystore

import (
	"MinionDB/internal/SSTables"
	"MinionDB/internal/logger"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const Tombstone = "__deleted__"

const maxInMemoryEntries = 1000

type MiniKV struct {
	mu       sync.RWMutex
	index    map[string][]byte
	wb       *WriteBatcher
	sstables []SSTables.SSTable
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

func (db *MiniKV) Set(key string, val []byte) error {
	db.mu.Lock()
	db.index[key] = append([]byte(nil), val...)
	db.mu.Unlock()
	err := db.CheckIfFlushNeeded()
	if err != nil {
		return err
	}
	return db.wb.EnqueueSet(key, val)
}

func (db *MiniKV) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	v, ok := db.index[key]
	db.mu.RUnlock()
	if ok {
		return v, true
	}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		v, ok, err := SSTables.ReadSSTable(db.sstables[i].Path, key)
		if err != nil {
			logger.Logger.Error("Error reading SSTable:", "error", err)
			continue
		}
		if ok {
			return v, true
		}
	}
	return nil, false
}

func (db *MiniKV) Delete(key string) error {
	db.mu.Lock()
	db.index[key] = []byte(Tombstone)
	db.mu.Unlock()
	return db.wb.EnqueueDel(key)
}

func (db *MiniKV) Close() error {
	err := db.flushToSSTable("data-" + time.Now().Format("20060102150405") + ".sst")
	if err != nil {
		return err
	}
	return db.wb.Close()
}

func (db *MiniKV) swapWriteBatcher(newBatcher *WriteBatcher) *WriteBatcher {
	old := db.wb
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&db.wb)), unsafe.Pointer(newBatcher))
	return old
}

func (db *MiniKV) flushToSSTable(path string) error {
	db.mu.RLock()
	snapshot := make(map[string][]byte, len(db.index))
	for k, v := range db.index {
		snapshot[k] = append([]byte(nil), v...)
	}
	db.mu.RUnlock()

	if len(snapshot) == 0 {
		return nil
	}

	err := SSTables.WriteSSTable(path, snapshot)
	if err != nil {
		return err
	}
	db.mu.Lock()
	db.sstables = append(db.sstables, SSTables.SSTable{Path: path})
	db.mu.Unlock()
	return nil
}

func (db *MiniKV) CheckIfFlushNeeded() error {
	if len(db.index) >= maxInMemoryEntries {
		return db.flushToSSTable("data-" + time.Now().Format("20060102150405") + ".sst")
	}
	return nil
}
