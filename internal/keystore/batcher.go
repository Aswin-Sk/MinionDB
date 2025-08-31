package keystore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type opType byte

const (
	opSet opType = 1
	opDel opType = 2
)

type writeReq struct {
	t    opType
	key  string
	val  []byte
	resp chan error
}

type WriteBatcher struct {
	// mutable runtime fields
	reqCh   chan writeReq
	doneCh  chan struct{}
	walFile *os.File
	walBuf  *bufio.Writer

	batchSize  int
	flushEvery time.Duration

	// synchronization
	inFlight int64
	stopped  uint32
	sendMu   sync.Mutex

	applyFunc func(op opType, key string, val []byte)
}

func NewWriteBatcher(f *os.File, batchSize int, flushEvery time.Duration, apply func(op opType, key string, val []byte)) *WriteBatcher {
	return &WriteBatcher{
		reqCh:      make(chan writeReq, 8192),
		doneCh:     nil,
		walFile:    f,
		walBuf:     bufio.NewWriterSize(f, 1<<20),
		batchSize:  batchSize,
		flushEvery: flushEvery,
		applyFunc:  apply,
	}
}

func (wb *WriteBatcher) Start() {
	wb.doneCh = make(chan struct{})
	go wb.loop()
}

func (wb *WriteBatcher) loop() {
	defer close(wb.doneCh)
	tick := time.NewTicker(wb.flushEvery)
	defer tick.Stop()

	var batch []writeReq

	for {
		select {
		case r, ok := <-wb.reqCh:
			if !ok {
				wb.flush(batch)
				return
			}
			batch = append(batch, r)
			if len(batch) >= wb.batchSize {
				wb.flush(batch)
				batch = batch[:0]
			}
		case <-tick.C:
			if len(batch) > 0 {
				wb.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (wb *WriteBatcher) Stop() {
	wb.sendMu.Lock()
	if atomic.LoadUint32(&wb.stopped) == 1 {
		if wb.doneCh != nil {
			<-wb.doneCh
		}
		wb.sendMu.Unlock()
		return
	}
	atomic.StoreUint32(&wb.stopped, 1)
	close(wb.reqCh)
	wb.sendMu.Unlock()

	if wb.doneCh != nil {
		<-wb.doneCh
	}
}

func (wb *WriteBatcher) EnqueueSet(key string, val []byte) error {
	wb.applyFunc(opSet, key, append([]byte(nil), val...))

	ch := make(chan error, 1)
	req := writeReq{t: opSet, key: key, val: append([]byte(nil), val...), resp: ch}

	wb.sendMu.Lock()
	if atomic.LoadUint32(&wb.stopped) == 1 {
		wb.sendMu.Unlock()
		return errors.New("write batcher stopped")
	}
	atomic.AddInt64(&wb.inFlight, 1)
	wb.reqCh <- req
	wb.sendMu.Unlock()

	return <-ch
}

func (wb *WriteBatcher) EnqueueDel(key string) error {
	wb.applyFunc(opDel, key, nil)

	ch := make(chan error, 1)
	req := writeReq{t: opDel, key: key, val: nil, resp: ch}

	wb.sendMu.Lock()
	if atomic.LoadUint32(&wb.stopped) == 1 {
		wb.sendMu.Unlock()
		return errors.New("write batcher stopped")
	}
	atomic.AddInt64(&wb.inFlight, 1)
	wb.reqCh <- req
	wb.sendMu.Unlock()

	return <-ch
}

// flush writes batch to WAL, fsyncs once, then ack callers and decrement inFlight.
func (wb *WriteBatcher) flush(batch []writeReq) {
	if len(batch) == 0 {
		return
	}
	// WAL: [op(1)] [klen(u32)] [vlen(u32)] [key] [value]
	for _, r := range batch {
		_ = wb.walBuf.WriteByte(byte(r.t))
		kb := []byte(r.key)
		var klen, vlen [4]byte
		binary.LittleEndian.PutUint32(klen[:], uint32(len(kb)))
		binary.LittleEndian.PutUint32(vlen[:], uint32(len(r.val)))
		_, _ = wb.walBuf.Write(klen[:])
		_, _ = wb.walBuf.Write(vlen[:])
		_, _ = wb.walBuf.Write(kb)
		if r.t == opSet && len(r.val) > 0 {
			_, _ = wb.walBuf.Write(r.val)
		}
	}

	_ = wb.walBuf.Flush()
	_ = wb.walFile.Sync()

	for _, r := range batch {
		r.resp <- nil
		atomic.AddInt64(&wb.inFlight, -1)
	}
}

// WaitForDrained waits until all currently enqueued requests have been flushed to WAL.
func (wb *WriteBatcher) WaitForDrained() {
	for {
		inflight := atomic.LoadInt64(&wb.inFlight)
		queued := 0
		if wb.reqCh != nil {
			queued = len(wb.reqCh)
		}
		if inflight == 0 && queued == 0 {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// PauseForMaintenance acquires sendMu and sets stopped flag; caller holds sendMu and must call ResumeAfterMaintenance.
func (wb *WriteBatcher) PauseForMaintenance() {
	wb.sendMu.Lock()
	atomic.StoreUint32(&wb.stopped, 1)
}

// ResumeAfterMaintenance releases stopped flag and unlocks sendMu.
func (wb *WriteBatcher) ResumeAfterMaintenance() {
	atomic.StoreUint32(&wb.stopped, 0)
	if wb.sendMu.TryLock() {
		wb.sendMu.Unlock()
	}
}
