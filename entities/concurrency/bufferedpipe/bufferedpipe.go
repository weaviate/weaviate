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

package bufferedpipe

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// pipe is a bounded, in-memory pipe that decouples a writer from a reader.
// Writes block only once the internal buffer reaches its capacity, not until
// the reader consumes them, so a slow reader does not stall the writer while
// there is still buffer space.
//
// The buffer is a FIFO queue of byte-slice chunks. Each Write call appends
// one chunk; each Read call dequeues from the head. A pipe holds at most
// maxSize bytes, so a caller running N pipes concurrently must budget
// N * maxSize of memory.
//
// maxSize is a soft limit. Write only blocks once buffered >= maxSize, so a
// single Write larger than maxSize is accepted and pushes buffered past the
// limit by one chunk. This is intentional: rejecting or splitting such a
// write would either deadlock (no reader can drain a chunk that was never
// enqueued) or require the writer to hand back partial progress, which
// io.Writer semantics do not express cleanly.
//
// A writer error only surfaces to the reader once the buffered bytes have
// been drained, so already-buffered data reaches the reader before the error.
//
// Thread safety: all methods are safe for concurrent use as long as at most
// one Write and one Read are in flight at a time. A second concurrent Write
// or Read returns an error rather than corrupting the queue. Close and
// CloseWithError may be called from any goroutine, including while a Write
// or Read is blocked.
type pipe struct {
	mu   sync.Mutex
	cond *sync.Cond

	chunks   [][]byte // FIFO queue of byte chunks
	buffered int      // total bytes currently in chunks
	maxSize  int      // capacity in bytes

	writerClosed bool  // writer has called Close or CloseWithError
	writerErr    error // error passed to CloseWithError (writer side)

	readerClosed bool  // reader has called Close or CloseWithError
	readerErr    error // error passed to CloseWithError (reader side)
}

// Writer is the write half of a pipe. It implements io.WriteCloser.
type Writer struct {
	p      *pipe
	active atomic.Int32 // guards against concurrent writers
}

// Reader is the read half of a pipe. It implements io.ReadCloser and adds
// CloseWithError.
type Reader struct {
	p      *pipe
	active atomic.Int32 // guards against concurrent readers

	// partial holds leftover bytes from a chunk that was larger than the
	// caller's Read buffer. Only accessed by the single reader goroutine.
	partial []byte
}

// New returns the two halves of a pipe buffering up to maxSize bytes. A
// maxSize of zero or less leaves the buffer unbounded.
func New(maxSize int) (*Reader, *Writer) {
	p := &pipe{maxSize: maxSize}
	p.cond = sync.NewCond(&p.mu)
	return &Reader{p: p}, &Writer{p: p}
}

// Write appends a copy of b to the buffer. It blocks when the buffer is
// full, and returns an error if the reader has closed with an error.
// Write must not be called after Close or CloseWithError.
func (w *Writer) Write(b []byte) (int, error) {
	if v := w.active.Add(1); v != 1 {
		w.active.Add(-1)
		return 0, fmt.Errorf("bufferedpipe: concurrent Write calls are not supported")
	}
	defer w.active.Add(-1)

	if len(b) == 0 {
		return 0, nil
	}

	p := w.p
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		// Checked inside the loop so closing either half releases a Write
		// that is already blocked on a full buffer.
		if p.writerClosed {
			return 0, io.ErrClosedPipe
		}

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
func (w *Writer) Close() error {
	return w.CloseWithError(nil)
}

// CloseWithError signals that no more data will be written. If err is
// non-nil, subsequent reads will return err after the buffer is drained.
// If err is nil, reads return io.EOF after the buffer is drained.
func (w *Writer) CloseWithError(err error) error {
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
// and the writer has not yet closed. After the reader has been closed via
// Close or CloseWithError, Read returns io.ErrClosedPipe (matching
// io.PipeReader behavior).
func (r *Reader) Read(b []byte) (int, error) {
	if v := r.active.Add(1); v != 1 {
		r.active.Add(-1)
		return 0, fmt.Errorf("bufferedpipe: concurrent Read calls are not supported")
	}
	defer r.active.Add(-1)

	if len(b) == 0 {
		return 0, nil
	}

	p := r.p
	p.mu.Lock()

	// A closed reader discards buffered data and aborts in-progress reads,
	// matching io.PipeReader.
	if p.readerClosed {
		p.mu.Unlock()
		r.partial = nil
		return 0, io.ErrClosedPipe
	}

	p.mu.Unlock()

	// Serve leftover bytes from a previous partial read first.
	if len(r.partial) > 0 {
		n := copy(b, r.partial)
		r.partial = r.partial[n:]
		if len(r.partial) == 0 {
			r.partial = nil
		}
		return n, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for len(p.chunks) == 0 {
		if p.readerClosed {
			return 0, io.ErrClosedPipe
		}
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
func (r *Reader) Close() error {
	return r.CloseWithError(nil)
}

// CloseWithError closes the reader with an error. Pending and subsequent
// writes return err (or io.ErrClosedPipe if err is nil).
func (r *Reader) CloseWithError(err error) error {
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
