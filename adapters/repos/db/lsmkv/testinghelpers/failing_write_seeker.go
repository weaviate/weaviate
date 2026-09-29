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

// Package testinghelpers holds test doubles shared by the lsmkv packages. A
// _test.go type cannot be imported across packages, and roaringset cannot
// import lsmkv, so a double both need lives here.
package testinghelpers

import (
	"errors"
	"fmt"
	"io"
)

// ErrDiskFull is what a FailingWriteSeeker returns by default. Tables asserting
// on it with errors.Is need one value, and both segment writers want the same
// text, so it lives here rather than once per package.
var ErrDiskFull = errors.New("no space left on device")

// FailingWriteSeeker fails the FailOnWrite'th Write or the Write carrying the
// FailAtByte'th byte, both counting from 1, or every Seek when FailSeek is set.
// It stands in for the ENOSPC or EIO a real segment file returns, which nothing
// else can produce in a unit test.
//
// It tracks a size the way a file does, so Seek reports a position a caller can
// assert on: SeekEnd answers from the furthest byte written, not from wherever
// the last Seek left the cursor.
type FailingWriteSeeker struct {
	// Err is what the failing call returns. Zero means ErrDiskFull: a double
	// that returned a nil error on a short write would violate io.Writer, and a
	// bufio.Writer in front of it would read that as success.
	Err error
	// FailOnWrite fails no write when zero.
	FailOnWrite int
	// FailAtByte counts bytes accepted through Write, so it names a position in
	// the stream rather than a call. A caller sweeping it does not have to know
	// how the writer under test splits its output.
	FailAtByte int
	FailSeek   bool

	writes  int
	written int
	offset  int64
	size    int64
}

func (w *FailingWriteSeeker) err() error {
	if w.Err != nil {
		return w.Err
	}
	return ErrDiskFull
}

func (w *FailingWriteSeeker) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == w.FailOnWrite {
		return 0, w.err()
	}
	if w.FailAtByte > 0 && w.written+len(p) >= w.FailAtByte {
		accepted := w.FailAtByte - 1 - w.written
		w.accept(p[:accepted])
		return accepted, w.err()
	}
	w.accept(p)
	return len(p), nil
}

func (w *FailingWriteSeeker) accept(p []byte) {
	w.written += len(p)
	w.offset += int64(len(p))
	w.size = max(w.size, w.offset)
}

func (w *FailingWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	if w.FailSeek {
		return 0, w.err()
	}
	switch whence {
	case io.SeekStart:
		w.offset = offset
	case io.SeekCurrent:
		w.offset += offset
	case io.SeekEnd:
		w.offset = w.size + offset
	default:
		return 0, fmt.Errorf("unknown whence %d", whence)
	}
	if w.offset < 0 {
		return 0, fmt.Errorf("seek to %d, before the start of the file", w.offset)
	}
	// A seek past the end does not extend a file; only a write there does.
	return w.offset, nil
}
