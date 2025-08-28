package wal

import (
	"bufio"
	"os"
	"sync"
	"time"
)

type WAL struct {
	File       *os.File
	writer     *bufio.Writer
	mu         sync.Mutex
	flushEvery time.Duration
	lastFlush  time.Time
	closeCh    chan struct{}
	wg         sync.WaitGroup
}

func Open(path string, flushEvery time.Duration) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		File:       file,
		writer:     bufio.NewWriter(file),
		flushEvery: flushEvery,
		lastFlush:  time.Now(),
		closeCh:    make(chan struct{}),
	}

	// Start background flusher
	w.wg.Add(1)
	go w.backgroundFlush()

	return w, nil
}

func (w *WAL) Append(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write length + data (optional, for parsing)
	_, err := w.writer.Write(data)
	if err != nil {
		return err
	}

	// Flush if time elapsed
	if time.Since(w.lastFlush) >= w.flushEvery {
		if err := w.flush(); err != nil {
			return err
		}
	}
	return nil
}

// Flush forces buffered writes to disk
func (w *WAL) flush() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.File.Sync(); err != nil {
		return err
	}
	w.lastFlush = time.Now()
	return nil
}

// Background flush loop (for safety)
func (w *WAL) backgroundFlush() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			w.flush()
			w.mu.Unlock()
		case <-w.closeCh:
			return
		}
	}
}

// Close WAL safely
func (w *WAL) Close() error {
	close(w.closeCh)
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.flush(); err != nil {
		return err
	}
	return w.File.Close()
}
