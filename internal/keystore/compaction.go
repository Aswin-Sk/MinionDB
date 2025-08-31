package keystore

import (
	"bufio"
	"encoding/binary"
	"os"
	"time"
)

func (db *MiniKV) Compact() error {
	// Pause batching during compaction
	db.wb.PauseForMaintenance()
	defer db.wb.ResumeAfterMaintenance() // always called

	tmpPath := db.path + ".compact"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	buf := bufio.NewWriterSize(tmpFile, 1<<20)

	// write only the latest state from index
	for k, v := range db.index {
		buf.WriteByte(byte(opSet))
		var klen, vlen [4]byte
		binary.LittleEndian.PutUint32(klen[:], uint32(len(k)))
		binary.LittleEndian.PutUint32(vlen[:], uint32(len(v)))
		buf.Write([]byte(k))
		buf.Write(v)
	}

	if err := buf.Flush(); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return err
	}
	tmpFile.Close()

	// safely replace old WAL file
	db.file.Close()
	if err := os.Rename(tmpPath, db.path); err != nil {
		return err
	}

	// reopen the WAL
	newF, err := os.OpenFile(db.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	db.file = newF

	// restart the batcher with the new file
	db.wb.Stop()
	db.wb = NewWriteBatcher(newF, 128, 10*time.Millisecond, db.apply)
	db.wb.Start()

	return nil
}

func (db *MiniKV) RunBackgroundCompaction(interval time.Duration, stopCh chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = db.Compact()
		case <-stopCh:
			return
		}

	}
}
