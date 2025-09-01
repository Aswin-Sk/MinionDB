package keystore

import (
	"os"
	"sync"
	"time"
)

type opType byte

const (
	opSet opType = iota
	opDel
)

type writeReq struct {
	t    opType
	key  string
	val  []byte
	done chan error
}

type WriteBatcher struct {
	mu       sync.Mutex
	reqCh    chan writeReq
	file     *os.File
	batchSz  int
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewWriteBatcher(path string, batchSz int, interval time.Duration) (*WriteBatcher, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wb := &WriteBatcher{
		reqCh:    make(chan writeReq, 4096),
		file:     f,
		batchSz:  batchSz,
		interval: interval,
		stopCh:   make(chan struct{}),
	}

	wb.wg.Add(1)
	go wb.loop()
	return wb, nil
}

func (wb *WriteBatcher) loop() {
	defer wb.wg.Done()

	ticker := time.NewTicker(wb.interval)
	defer ticker.Stop()

	var batch []writeReq
	for {
		select {
		case req := <-wb.reqCh:
			batch = append(batch, req)
			if len(batch) >= wb.batchSz {
				wb.flush(batch)
				batch = nil
			}
		case <-ticker.C:
			if len(batch) > 0 {
				wb.flush(batch)
				batch = nil
			}
		case <-wb.stopCh:
			if len(batch) > 0 {
				wb.flush(batch)
			}
			return
		}
	}
}

func (wb *WriteBatcher) flush(batch []writeReq) {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	for _, r := range batch {
		switch r.t {
		case opSet:
			wb.file.Write([]byte{byte(opSet)})
			wb.file.Write([]byte(r.key))
			wb.file.Write([]byte{0})
			wb.file.Write(r.val)
			wb.file.Write([]byte{'\n'})
		case opDel:
			wb.file.Write([]byte{byte(opDel)})
			wb.file.Write([]byte(r.key))
			wb.file.Write([]byte{'\n'})
		}
	}

	wb.file.Sync()

	// Acknowledge all requests
	for _, r := range batch {
		r.done <- nil
		close(r.done)
	}
}

func (wb *WriteBatcher) EnqueueSet(key string, val []byte) error {
	req := writeReq{
		t:    opSet,
		key:  key,
		val:  append([]byte(nil), val...), // copy
		done: make(chan error, 1),
	}
	wb.reqCh <- req
	return <-req.done
}

func (wb *WriteBatcher) EnqueueDel(key string) error {
	req := writeReq{
		t:    opDel,
		key:  key,
		done: make(chan error, 1),
	}
	wb.reqCh <- req
	return <-req.done
}

func (wb *WriteBatcher) Close() error {
	close(wb.stopCh)
	wb.wg.Wait()
	return wb.file.Close()
}
