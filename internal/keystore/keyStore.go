package keystore

import (
	"MinionDB/internal/wal"
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const tombstone = "__deleted__"

type MiniKV struct {
	mu       sync.RWMutex
	dataFile *os.File
	walFile  *wal.WAL
	index    map[string]string
	path     string
}

// Open opens or creates a MiniKV instance with WAL support
func Open(path string) (*MiniKV, error) {
	dataPath := path + ".data"
	walPath := path + ".wal"

	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	walFile, err := wal.Open(walPath, 2*time.Second)
	if err != nil {
		dataFile.Close()
		return nil, err
	}

	db := &MiniKV{
		dataFile: dataFile,
		walFile:  walFile,
		index:    make(map[string]string),
		path:     path,
	}

	if err := db.loadDataFile(); err != nil {
		db.Close()
		return nil, err
	}

	if err := db.replayWAL(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// loadDataFile rebuilds the index from the data snapshot
func (db *MiniKV) loadDataFile() error {
	if _, err := db.dataFile.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(db.dataFile)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key, val := parts[0], parts[1]
		if val != tombstone {
			db.index[key] = val
		}
	}
	return scanner.Err()
}

// replayWAL applies pending operations from WAL to the index
func (db *MiniKV) replayWAL() error {
	if _, err := db.walFile.File.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(db.walFile.File)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key, val := parts[0], parts[1]
		if val == tombstone {
			delete(db.index, key)
		} else {
			db.index[key] = val
		}
	}

	return scanner.Err()
}

// Set inserts or updates a key-value pair
func (db *MiniKV) Set(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	line := fmt.Sprintf("%s:%s\n", key, value)
	err := db.walFile.Append([]byte(line))
	if err != nil {
		return err
	}
	db.index[key] = value
	return nil
}

// Get retrieves a value by key
func (db *MiniKV) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, ok := db.index[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}
	return val, nil
}

// Delete removes a key (using tombstone marker)
func (db *MiniKV) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.index[key]; !ok {
		return fmt.Errorf("key not found")
	}
	line := fmt.Sprintf("%s:%s\n", key, tombstone)
	err := db.walFile.Append([]byte(line))
	if err != nil {
		return err
	}
	delete(db.index, key)
	return nil
}

// Close closes all files
func (db *MiniKV) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	err := db.dataFile.Close()
	if err != nil {
		return err
	}
	err = db.walFile.Close()
	if err != nil {
		return err
	}
	return nil
}
