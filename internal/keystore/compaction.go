package keystore

import (
	"MinionDB/internal/logger"
	"MinionDB/internal/wal"
	"fmt"
	"os"
	"time"
)

// Compact merges index into data file and clears WAL
func (db *MiniKV) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	tempPath := db.path + ".data.tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	for key, val := range db.index {
		line := fmt.Sprintf("%s:%s\n", key, val)
		if _, err := tempFile.WriteString(line); err != nil {
			tempFile.Close()
			return err
		}
	}
	tempFile.Sync()
	tempFile.Close()

	db.dataFile.Close()
	err = os.Rename(tempPath, db.path+".data")
	if err != nil {
		return err
	}
	db.dataFile, err = os.OpenFile(db.path+".data", os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	db.walFile.Close()
	db.walFile, err = wal.Open(db.path+".wal", 2*time.Second)
	if err != nil {
		return err
	}
	return err
}

func (db *MiniKV) RunBackgroundCompaction(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := db.Compact(); err != nil {
				logger.Logger.Error("Compaction error", "error", err)
			}
			logger.Logger.Info("Compaction completed")
		case <-stopCh:
			return
		}
	}
}
