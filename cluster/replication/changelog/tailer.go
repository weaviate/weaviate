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
	"context"
	"fmt"
	"io"
	"math"
	"os"
)

// Tailer is a pull-based reader over a ChangeLog. Call Next to read the next
// entry; Next blocks until one is available, the log finalizes (io.EOF), the
// log is deactivated (ErrLogDeactivated), the tailer's LSN cap is reached
// (io.EOF), or ctx is cancelled.
//
// Tailers hold their own file handle independent of the producer; multiple
// tailers per ChangeLog are supported.
type Tailer struct {
	log      *ChangeLog
	file     *os.File
	lastLSN  uint64
	untilLSN uint64 // Inclusive upper bound; math.MaxUint64 = effectively unbounded.
}

// NewTailer returns a tailer that emits entries with LSN > fromLSN, draining
// until the log is finalized.
//
// If fromLSN is ahead of the log's current LSN, Next blocks until the log
// grows past fromLSN (returning the entry at fromLSN+1 first), or until the
// log is finalized. If fromLSN is at or beyond the finalLSN of an
// already-finalized log, Next returns io.EOF on the first call.
func (l *ChangeLog) NewTailer(fromLSN uint64) (*Tailer, error) {
	return l.NewTailerWithCap(fromLSN, math.MaxUint64)
}

// NewTailerWithCap is like NewTailer but the tailer EOFs once it has emitted
// every entry through LSN==untilLSN, without sealing the log. untilLSN=0
// emits no entries (the snapshot-on-empty-log case must not hang).
//
// The cap lets the consumer drain a phase boundary while the log keeps
// accepting writes — Finalize is one-shot and would block further appends.
// Capped EOF and finalized EOF are independent; whichever fires first wins.
func (l *ChangeLog) NewTailerWithCap(fromLSN, untilLSN uint64) (*Tailer, error) {
	f, err := os.Open(l.path)
	if err != nil {
		return nil, fmt.Errorf("changelog: open tailer for %q: %w", l.path, err)
	}
	return &Tailer{log: l, file: f, lastLSN: fromLSN, untilLSN: untilLSN}, nil
}

// Next returns the next entry, or an error describing why no entry is
// available:
//   - io.EOF: log finalized and drained, or untilLSN cap reached.
//   - ErrLogDeactivated: the producer called Deactivate.
//   - ctx.Err(): the provided context was cancelled while waiting.
func (t *Tailer) Next(ctx context.Context) (*Entry, error) {
	l := t.log
	for {
		l.mu.Lock()
		if l.deactivated {
			l.mu.Unlock()
			return nil, ErrLogDeactivated
		}
		// Cap check sits before the emit branch so a tailer constructed at or
		// past the cap (including untilLSN=0) EOFs on the first Next call
		// rather than emitting one entry past the boundary.
		if t.lastLSN >= t.untilLSN {
			l.mu.Unlock()
			return nil, io.EOF
		}
		if t.lastLSN < l.lsn {
			offset := l.offsetIndex[t.lastLSN]
			l.mu.Unlock()
			if _, err := t.file.Seek(offset, io.SeekStart); err != nil {
				return nil, fmt.Errorf("changelog: tailer seek: %w", err)
			}
			entry, err := DecodeFrame(t.file)
			if err != nil {
				return nil, err
			}
			t.lastLSN = entry.LSN
			return entry, nil
		}
		if l.finalized && t.lastLSN >= l.finalLSN {
			l.mu.Unlock()
			return nil, io.EOF
		}
		wait := l.notify
		l.mu.Unlock()

		select {
		case <-wait:
			// state changed; loop and re-check.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close releases the tailer's file handle.
func (t *Tailer) Close() error {
	if t.file == nil {
		return nil
	}
	err := t.file.Close()
	t.file = nil
	return err
}
