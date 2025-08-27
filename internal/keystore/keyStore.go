package keystore

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

const tombstone = "__deleted__"

type record struct {
	key   string
	value string
}

type MiniKV struct {
	mu    sync.RWMutex
	file  *os.File
	index map[string]string
	path  string
}

// Open opens the DB or creates a new one
func Open(path string) (*MiniKV, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	db := &MiniKV{
		file:  file,
		index: make(map[string]string),
		path:  path,
	}

	if err := db.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}
	return db, nil
}

// loadIndex rebuilds the in-memory index from the file
func (db *MiniKV) loadIndex() error {
	db.index = make(map[string]string)
	if _, err := db.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(db.file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		val := parts[1]

		if val == tombstone {
			delete(db.index, key)
		} else {
			db.index[key] = val
		}
	}
	return scanner.Err()
}

// Set stores a key-value pair
func (db *MiniKV) Set(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	line := fmt.Sprintf("%s:%s\n", key, value)
	if _, err := db.file.WriteString(line); err != nil {
		return err
	}
	db.index[key] = value
	return db.file.Sync()
}

// Get retrieves a value
func (db *MiniKV) Get(key string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	val, ok := db.index[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}
	return val, nil
}

// Delete marks a key as deleted
func (db *MiniKV) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.index[key]; !ok {
		return fmt.Errorf("key not found")
	}

	line := fmt.Sprintf("%s:%s\n", key, tombstone)
	if _, err := db.file.WriteString(line); err != nil {
		return err
	}
	delete(db.index, key)
	return db.file.Sync()
}

// Compact writes only live keys to a new file and atomically swaps
func (db *MiniKV) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tempPath := db.path + ".tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	// Write all live keys
	for key, val := range db.index {
		line := fmt.Sprintf("%s:%s\n", key, val)
		if _, err := tempFile.WriteString(line); err != nil {
			tempFile.Close()
			return err
		}
	}
	tempFile.Sync()
	tempFile.Close()

	db.file.Close()
	if err := os.Rename(tempPath, db.path); err != nil {
		return err
	}

	db.file, err = os.OpenFile(db.path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the DB
func (db *MiniKV) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.file.Close()
}
