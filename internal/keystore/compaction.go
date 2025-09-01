package keystore

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (db *MiniKV) Compact(basePath string) error {
	newPath := filepath.Join(filepath.Dir(basePath), fmt.Sprintf("active-%d.wal", time.Now().UnixNano()))
	newBatcher, err := NewWriteBatcher(newPath, db.wb.batchSz, db.wb.interval)
	if err != nil {
		return err
	}

	oldBatcher := db.swapWriteBatcher(newBatcher)

	go func(old *WriteBatcher, compactPath string) {
		db.mu.RLock()
		snapshot := make(map[string][]byte, len(db.index))
		for k, v := range db.index {
			snapshot[k] = append([]byte(nil), v...)
		}
		db.mu.RUnlock()

		tmpPath := compactPath + ".compacting"
		f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Println("compaction error:", err)
			return
		}
		defer f.Close()

		for k, v := range snapshot {
			f.Write([]byte{byte(opSet)})
			f.Write([]byte(k))
			f.Write([]byte{0})
			f.Write(v)
			f.Write([]byte{'\n'})
		}
		f.Sync()

		os.Rename(tmpPath, compactPath)

		old.Close()
		os.Remove(old.file.Name())
	}(oldBatcher, filepath.Join(filepath.Dir(basePath), "compacted.wal"))

	return nil
}
