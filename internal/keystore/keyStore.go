package keystore

import (
	"os"
	"sync"
	"time"
)

type MiniKV struct {
	wb    *WriteBatcher
	index map[string][]byte
	file  *os.File
	mu    sync.RWMutex
	path  string
}

func Open(path string) (*MiniKV, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	kv := &MiniKV{
		index: make(map[string][]byte),
		file:  f,
		path:  path,
	}

	kv.wb = NewWriteBatcher(f, 512, 50*time.Millisecond, kv.apply)
	kv.wb.Start()

	return kv, nil
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

func (db *MiniKV) Set(key string, value []byte) error {
	valCopy := append([]byte(nil), value...)

	db.mu.Lock()
	db.index[key] = valCopy
	db.mu.Unlock()

	db.wb.EnqueueSet(key, valCopy)
	return nil
}

func (db *MiniKV) Delete(key string) error {
	err := db.wb.EnqueueDel(key)
	if err != nil {
		return err
	}

	db.mu.Lock()
	delete(db.index, key)
	db.mu.Unlock()
	return nil
}

func (db *MiniKV) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	v, ok := db.index[key]
	db.mu.RUnlock()
	return v, ok
}

func (db *MiniKV) Close() error {
	db.wb.PauseForMaintenance()
	db.wb.WaitForDrained()
	close(db.wb.reqCh)
	if db.wb.doneCh != nil {
		<-db.wb.doneCh
	}
	_ = db.file.Sync()
	return db.file.Close()
}
