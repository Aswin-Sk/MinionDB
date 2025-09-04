package keystore

import (
	"MinionDB/internal/SSTables"
	"MinionDB/internal/logger"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const Tombstone = "__deleted__"

const maxInMemoryEntries = 1000

type MiniKV struct {
	mu            sync.RWMutex
	index         map[string][]byte
	wb            *WriteBatcher
	sstables      []SSTables.SSTable
	baseDirectory string
}

func open(path string) (*MiniKV, error) {
	CreateDirs(path)
	index := make(map[string][]byte)

	walPath, err := getWALPath(path)
	if err != nil {
		if err != WALExistsError {
			return nil, err
		}
		index, err = ReplayWAL(walPath)
		if err != nil {
			return nil, err
		}
	}
	wb, err := NewWriteBatcher(walPath, 128, 5*time.Millisecond)
	if err != nil {
		return nil, err
	}

	return &MiniKV{
		index:         index,
		wb:            wb,
		baseDirectory: path,
	}, nil
}

func CreateDirs(base string) error {
	sstDir := filepath.Join(base, "sstables")
	walDir := filepath.Join(base, "wal")
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return err
	}
	return nil
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
	err := db.flushToSSTable()
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

func (db *MiniKV) flushToSSTable() error {
	db.mu.RLock()
	snapshot := make(map[string][]byte, len(db.index))
	for k, v := range db.index {
		snapshot[k] = append([]byte(nil), v...)
	}
	db.mu.RUnlock()

	if len(snapshot) == 0 {
		return nil
	}

	ssTablePath := db.nextSSTablePath()
	err := SSTables.WriteSSTable(ssTablePath, snapshot)
	if err != nil {
		return err
	}
	db.mu.Lock()
	db.sstables = append(db.sstables, SSTables.SSTable{Path: ssTablePath})
	db.mu.Unlock()
	return nil
}

func (db *MiniKV) CheckIfFlushNeeded() error {
	if len(db.index) >= maxInMemoryEntries {
		return db.flushToSSTable()
	}
	return nil
}

func (db *MiniKV) nextSSTablePath() string {
	sstDir := filepath.Join(db.baseDirectory, "sstables")
	id := len(db.sstables) + 1
	return filepath.Join(sstDir, fmt.Sprintf("sst-%05d.sst", id))
}

func ReplayWAL(path string) (map[string][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	index := make(map[string][]byte)
	var op [1]byte
	var klenBuf, vlenBuf [4]byte

	for {
		// read op
		_, err := f.Read(op[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		// read key length
		if _, err := f.Read(klenBuf[:]); err != nil {
			return nil, err
		}
		klen := binary.LittleEndian.Uint32(klenBuf[:])

		var vlen uint32
		if op[0] == byte(opSet) {
			if _, err := f.Read(vlenBuf[:]); err != nil {
				return nil, err
			}
			vlen = binary.LittleEndian.Uint32(vlenBuf[:])
		}

		// read key
		key := make([]byte, klen)
		if _, err := io.ReadFull(f, key); err != nil {
			return nil, err
		}

		if op[0] == byte(opSet) {
			// read value
			val := make([]byte, vlen)
			if _, err := io.ReadFull(f, val); err != nil {
				return nil, err
			}
			index[string(key)] = val
		} else if op[0] == byte(opDel) {
			delete(index, string(key))
		}
	}

	return index, nil
}
