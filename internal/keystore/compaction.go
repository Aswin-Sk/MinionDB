package keystore

import (
	"MinionDB/internal/SSTables"
	"MinionDB/internal/logger"
	"errors"
	"os"
	"path/filepath"
)

var WALExistsError = errors.New("WAL already exists")

func (db *MiniKV) Compact(basePath string) error {

	newPath, err := nextWALPath(basePath)
	if err != nil {
		return err
	}
	newBatcher, err := NewWriteBatcher(newPath, db.wb.batchSz, db.wb.interval)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	oldBatcher := db.swapWriteBatcher(newBatcher)

	go func(old *WriteBatcher) {
		// Snapshot current memtable
		db.mu.RLock()
		snapshot := make(map[string][]byte, len(db.index))
		for k, v := range db.index {
			snapshot[k] = append([]byte(nil), v...)
		}
		db.mu.RUnlock()

		// Flush memtable snapshot to a new SSTable
		sstPath := db.nextSSTablePath()
		if err := SSTables.WriteSSTable(sstPath, snapshot); err != nil {
			logger.Logger.Error("Error writing SSTable:", "error", err)
			return
		}

		// Update db.sstables and clear memtable
		db.mu.Lock()
		db.sstables = append(db.sstables, SSTables.SSTable{Path: sstPath})
		db.index = make(map[string][]byte)
		db.mu.Unlock()

		// Compact existing SSTables
		if err := db.CompactSSTables(); err != nil {
			logger.Logger.Error("Error compacting SSTables:", "error", err)
		}

		old.Close()
		os.Remove(old.file.Name())

	}(oldBatcher)

	return nil
}

func (db *MiniKV) CompactSSTables() error {
	db.mu.Lock()
	if len(db.sstables) < 2 {
		db.mu.Unlock()
		return nil
	}

	sst1 := db.sstables[0]
	sst2 := db.sstables[1]
	db.mu.Unlock()

	mergedPath := db.nextSSTablePath()
	if err := SSTables.MergeSSTables(mergedPath, sst1.Path, sst2.Path); err != nil {
		return err
	}

	db.mu.Lock()
	db.sstables = append(db.sstables[2:], SSTables.SSTable{Path: mergedPath})
	db.mu.Unlock()

	os.Remove(sst1.Path)
	os.Remove(sst2.Path)

	return nil
}

func nextWALPath(basePath string) (string, error) {
	walDir := filepath.Join(basePath, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return "", err
	}

	newPath := filepath.Join(walDir, "new.wal")
	return newPath, nil
}

func getWALPath(basePath string) (string, error) {
	walDir := filepath.Join(basePath, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return "", err
	}

	newPath := filepath.Join(walDir, "active.wal")
	_, err := os.Stat(filepath.Join(walDir, "active.wal"))
	if err == nil {
		return newPath, WALExistsError
	}

	return newPath, nil
}
