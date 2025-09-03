package keystore

import (
	"MinionDB/internal/SSTables"
	"MinionDB/internal/logger"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (db *MiniKV) Compact(basePath string) error {
	// Swap the WAL/write batcher to a new file
	newPath := filepath.Join(filepath.Dir(basePath), fmt.Sprintf("active-%d.wal", time.Now().UnixNano()))
	newBatcher, err := NewWriteBatcher(newPath, db.wb.batchSz, db.wb.interval)
	if err != nil {
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
		sstPath := filepath.Join(filepath.Dir(basePath), fmt.Sprintf("sst-%d.dat", time.Now().UnixNano()))
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
			fmt.Println("Error compacting SSTables:", err)
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

	mergedPath := fmt.Sprintf("sst-merged-%d.dat", time.Now().UnixNano())
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
