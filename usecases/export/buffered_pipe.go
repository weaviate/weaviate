//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package export

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

const defaultPipeBufferSize = 16 * 1024 * 1024 // 16 MB per range pipeline

// bufferedPipe is a bounded, in-memory pipe that decouples a writer (scan
// side) from a reader (upload side). Unlike io.Pipe, writes do not block
// until the reader consumes them — they block only when the internal buffer
// reaches its capacity. This prevents slow uploads from holding LSM cursors
// open.
//
// The buffer is a FIFO queue of byte-slice chunks. Each Write call appends
// one chunk; each Read call dequeues from the head. Total buffered bytes
// are bounded by maxSize.
//
// Thread safety: all methods are safe for concurrent use by one writer
// goroutine and one reader goroutine. Using multiple concurrent writers or
// multiple concurrent readers is not supported and will panic.
type bufferedPipe struct {
	mu   sync.Mutex
	cond *sync.Cond

	chunks   [][]byte // FIFO queue of byte chunks
	buffered int      // total bytes currently in chunks
	maxSize  int      // capacity in bytes

	writerClosed bool  // writer has called CloseWriter
	writerErr    error // error passed to CloseWriter

	readerClosed bool  // reader has called Close or CloseWithError
	readerErr    error // error passed to CloseWithError (reader side)
}

// bufferedPipeWriter is the write half of a bufferedPipe. It implements
// io.WriteCloser.
type bufferedPipeWriter struct {
	p      *bufferedPipe
	active atomic.Int32 // guards against concurrent writers
}

// bufferedPipeReader is the read half of a bufferedPipe. It implements
// io.ReadCloser and backup.ReadCloserWithError.
type bufferedPipeReader struct {
	p      *bufferedPipe
	active atomic.Int32 // guards against concurrent readers

	// partial holds leftover bytes from a chunk that was larger than the
	// caller's Read buffer. Only accessed by the single reader goroutine.
	partial []byte
}

func newBufferedPipe(maxSize int) (*bufferedPipeReader, *bufferedPipeWriter) {
	p := &bufferedPipe{maxSize: maxSize}
	p.cond = sync.NewCond(&p.mu)
	return &bufferedPipeReader{p: p}, &bufferedPipeWriter{p: p}
}

// Write appends a copy of b to the buffer. It blocks when the buffer is
// full, and returns an error if the reader has closed with an error.
// Write must not be called after Close or CloseWithError.
func (w *bufferedPipeWriter) Write(b []byte) (int, error) {
	if v := w.active.Add(1); v != 1 {
		w.active.Add(-1)
		return 0, fmt.Errorf("bufferedPipeWriter: concurrent Write calls are not supported")
	}
	defer w.active.Add(-1)

	if len(b) == 0 {
		return 0, nil
	}

	p := w.p
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.writerClosed {
		return 0, io.ErrClosedPipe
	}

	for {
		if p.readerClosed {
			if p.readerErr != nil {
				return 0, p.readerErr
			}
			return 0, io.ErrClosedPipe
		}

		if p.buffered < p.maxSize || p.maxSize <= 0 {
			break
		}

		// Buffer full — wait for reader to drain.
		p.cond.Wait()
	}

	chunk := make([]byte, len(b))
	copy(chunk, b)
	p.chunks = append(p.chunks, chunk)
	p.buffered += len(chunk)

	// Wake reader if it was waiting for data.
	p.cond.Signal()
	return len(b), nil
}

// Close signals that no more data will be written (EOF on reader side).
func (w *bufferedPipeWriter) Close() error {
	return w.CloseWithError(nil)
}

// CloseWithError signals that no more data will be written. If err is
// non-nil, subsequent reads will return err after the buffer is drained.
// If err is nil, reads return io.EOF after the buffer is drained.
func (w *bufferedPipeWriter) CloseWithError(err error) error {
	p := w.p
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.writerClosed {
		p.writerClosed = true
		p.writerErr = err
		p.cond.Broadcast()
	}
	return nil
}

// Read dequeues data from the buffer. It blocks when the buffer is empty
// and the writer has not yet closed.
func (r *bufferedPipeReader) Read(b []byte) (int, error) {
	if v := r.active.Add(1); v != 1 {
		r.active.Add(-1)
		return 0, fmt.Errorf("bufferedPipeReader: concurrent Read calls are not supported")
	}
	defer r.active.Add(-1)

	if len(b) == 0 {
		return 0, nil
	}

	// Serve leftover bytes from a previous partial read first.
	if len(r.partial) > 0 {
		n := copy(b, r.partial)
		r.partial = r.partial[n:]
		if len(r.partial) == 0 {
			r.partial = nil
		}
		return n, nil
	}

	p := r.p
	p.mu.Lock()
	defer p.mu.Unlock()

	for len(p.chunks) == 0 {
		if p.writerClosed {
			if p.writerErr != nil {
				return 0, p.writerErr
			}
			return 0, io.EOF
		}
		// Buffer empty — wait for writer.
		p.cond.Wait()
	}

	// Dequeue the head chunk.
	chunk := p.chunks[0]
	p.chunks[0] = nil // allow GC
	p.chunks = p.chunks[1:]
	p.buffered -= len(chunk)

	// Wake writer if it was waiting for space.
	p.cond.Signal()

	n := copy(b, chunk)
	if n < len(chunk) {
		r.partial = chunk[n:]
	}
	return n, nil
}

// Close closes the reader. Pending and subsequent writes return
// io.ErrClosedPipe.
func (r *bufferedPipeReader) Close() error {
	return r.CloseWithError(nil)
}

// CloseWithError closes the reader with an error. Pending and subsequent
// writes return err (or io.ErrClosedPipe if err is nil).
func (r *bufferedPipeReader) CloseWithError(err error) error {
	p := r.p
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.readerClosed {
		p.readerClosed = true
		p.readerErr = err
		p.cond.Broadcast()
	}
	return nil
}
