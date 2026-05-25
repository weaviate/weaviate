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

package changelog

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// ChangeLog is a single-producer append-only log of Entry records, one file
// per in-flight movement op. It is safe for concurrent AppendPut/AppendDelete
// calls; appends serialize through an internal mutex and the LSN counter is
// assigned atomically with the file write.
//
// Tailers read from an independent *os.File handle and are woken via a
// close-and-replace notify channel — no internal goroutine, and ctx
// cancellation is handled in the tailer's own select.
//
// Crash survival is out of scope for V1 (per plans/plan.md); the log is
// bufio-buffered and flushes each frame, but does not fsync on every append.
type ChangeLog struct {
	path   string
	file   *os.File
	w      *bufio.Writer
	logger logrus.FieldLogger

	mu          sync.Mutex
	notify      chan struct{}
	lsn         uint64
	finalized   bool
	finalLSN    uint64
	deactivated bool
	offsetIndex []int64 // offsetIndex[i] = byte offset of LSN i+1's frame in file
	flushedSize int64
	scratch     []byte
}

// Open creates a new change-capture log file at path. Fails if the file
// already exists — each op-id's log is expected to be fresh.
func Open(path string, logger logrus.FieldLogger) (*ChangeLog, error) {
	if logger == nil {
		logger = logrus.New()
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open changelog %q: %w", path, err)
	}
	return &ChangeLog{
		path:   path,
		file:   f,
		w:      bufio.NewWriter(f),
		logger: logger,
		notify: make(chan struct{}),
	}, nil
}

// AppendPut appends a PUT entry and returns the assigned LSN.
func (l *ChangeLog) AppendPut(uuid [16]byte, updateTimeMillis int64, payload []byte) (uint64, error) {
	return l.append(&Entry{
		IsDelete:         false,
		UpdateTimeMillis: updateTimeMillis,
		UUID:             uuid,
		Payload:          payload,
	})
}

// AppendDelete appends a DELETE entry (no payload) and returns the assigned LSN.
func (l *ChangeLog) AppendDelete(uuid [16]byte, updateTimeMillis int64) (uint64, error) {
	return l.append(&Entry{
		IsDelete:         true,
		UpdateTimeMillis: updateTimeMillis,
		UUID:             uuid,
	})
}

// append assigns the next LSN, writes the frame, and flushes. On any I/O
// error, LSN and flushedSize are left untouched (unchanged state), and
// bufio.Writer's own sticky-error behaviour guarantees every subsequent
// Write/Flush on this ChangeLog returns that error — so callers that
// blunder on after an error will hit the same error again, not corruption.
// Caller contract after an error: treat the log as dead and Deactivate.
func (l *ChangeLog) append(e *Entry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.deactivated {
		return 0, ErrLogDeactivated
	}
	if l.finalized {
		return 0, ErrLogFinalized
	}

	nextLSN := l.lsn + 1
	e.LSN = nextLSN
	frame, err := Encode(l.scratch[:0], e)
	if err != nil {
		return 0, fmt.Errorf("changelog: encode frame: %w", err)
	}
	l.scratch = frame

	offset := l.flushedSize
	if _, err := l.w.Write(frame); err != nil {
		return 0, fmt.Errorf("changelog: write frame: %w", err)
	}
	if err := l.w.Flush(); err != nil {
		return 0, fmt.Errorf("changelog: flush frame: %w", err)
	}

	l.lsn = nextLSN
	l.flushedSize += int64(len(frame))
	l.offsetIndex = append(l.offsetIndex, offset)
	l.wakeTailersLocked()
	return nextLSN, nil
}

// Finalize seals the log at its current LSN. Subsequent Append calls return
// ErrLogFinalized. Tailers blocked at the tail are woken and will drain the
// remaining entries before returning io.EOF.
func (l *ChangeLog) Finalize() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.deactivated {
		return 0, ErrLogDeactivated
	}
	if l.finalized {
		return l.finalLSN, nil
	}
	if err := l.w.Flush(); err != nil {
		return 0, fmt.Errorf("changelog: flush on finalize: %w", err)
	}
	l.finalized = true
	l.finalLSN = l.lsn
	l.wakeTailersLocked()
	return l.finalLSN, nil
}

// Deactivate closes the log's file handle, removes the file from disk, and
// wakes any blocked tailers (which will return ErrLogDeactivated). Safe to
// call multiple times. Tailers that already hold their own *os.File handle
// may continue to read on POSIX systems until they call Close themselves.
func (l *ChangeLog) Deactivate() error {
	if err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()

		if l.deactivated {
			return nil
		}
		l.deactivated = true
		l.wakeTailersLocked()

		// Best-effort flush+close; errors are logged but don't prevent cleanup.
		if err := l.w.Flush(); err != nil {
			l.logger.Errorf("changelog: flush during deactivate: %v", err)
		}
		if err := l.file.Close(); err != nil {
			l.logger.Errorf("changelog: close during deactivate: %v", err)
		}
		return nil
	}(); err != nil {
		return err
	}
	if err := os.Remove(l.path); err != nil && !os.IsNotExist(err) {
		l.logger.Errorf("changelog: remove during deactivate: %v", err)
		return fmt.Errorf("changelog: remove %q: %w", l.path, err)
	}
	return nil
}

// LSN returns the highest assigned LSN (0 if no appends yet).
func (l *ChangeLog) LSN() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lsn
}

// Path returns the on-disk path of the log file.
func (l *ChangeLog) Path() string { return l.path }

// wakeTailersLocked closes the current notify channel and installs a fresh
// one. Must be called with l.mu held.
func (l *ChangeLog) wakeTailersLocked() {
	close(l.notify)
	l.notify = make(chan struct{})
}
