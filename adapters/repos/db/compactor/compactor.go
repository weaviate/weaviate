//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package compactor

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// SegmentWriterBufferSize controls the buffer size of the segment writer. But
// in addition it also acts as the threshold to switch between the "regular"
// write path and the "fully in memory" path which was added in v1.27.23. See
// [NewCompactor] for more details about the decision logic and motivation
// behind it..
const SegmentWriterBufferSize = 256 * 1024

type Writer interface {
	segmentindex.SegmentWriter
	Reset(io.Writer)
}

type MemoryWriter struct {
	buffer []byte
	pos    int
	maxPos int
	writer io.WriteSeeker
}

// newMemoryWriterWrapper creates a new MemoryWriter with initialized pointers
func newMemoryWriterWrapper(initialCapacity int64, writer io.WriteSeeker) *MemoryWriter {
	return &MemoryWriter{
		buffer: make([]byte, initialCapacity),
		pos:    0,
		maxPos: 0,
		writer: writer,
	}
}

func (mw *MemoryWriter) Write(p []byte) (n int, err error) {
	lenCopyBytes := len(p)

	requiredSize := mw.pos + lenCopyBytes
	if requiredSize > len(mw.buffer) {
		mw.buffer = append(mw.buffer, make([]byte, requiredSize)...)
	}

	numCopiedBytes := copy(mw.buffer[mw.pos:], p)

	mw.pos += numCopiedBytes
	if mw.pos >= mw.maxPos {
		mw.maxPos = mw.pos
	}

	if numCopiedBytes != lenCopyBytes {
		return numCopiedBytes, errors.New("could not copy all data into buffer")
	}

	return numCopiedBytes, nil
}

func (mw *MemoryWriter) Flush() error {
	buf := mw.buffer
	_, err := mw.writer.Write(buf[:mw.maxPos])
	if err != nil {
		return err
	}

	return nil
}

// Reset needs to be present to fulfill interface
func (mw *MemoryWriter) Reset(writer io.Writer) {}

func (mw *MemoryWriter) ResetWritePositionToZero() {
	mw.pos = 0
}

func (mw *MemoryWriter) ResetWritePositionToMax() {
	mw.pos = mw.maxPos
}

func NewWriter(w io.WriteSeeker, maxNewFileSize int64) (Writer, *MemoryWriter) {
	var writer Writer
	var mw *MemoryWriter
	if maxNewFileSize < SegmentWriterBufferSize {
		mw = newMemoryWriterWrapper(maxNewFileSize, w)
		writer = mw
	} else {
		writer = bufio.NewWriterSize(w, SegmentWriterBufferSize)
	}

	return writer, mw
}

func WriteHeader(mw *MemoryWriter, w io.WriteSeeker, bufw Writer, f *segmentindex.SegmentFile,
	level, version, secondaryIndices uint16, startOfIndex uint64, strategy segmentindex.Strategy,
) error {
	h := &segmentindex.Header{
		Level:            level,
		Version:          version,
		SecondaryIndices: secondaryIndices,
		Strategy:         strategy,
		IndexStart:       startOfIndex,
	}

	if mw == nil {
		if _, err := w.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek to beginning to write header: %w", err)
		}

		// We have to write directly to compactor writer,
		// since it has seeked back to start. The following
		// call to f.WriteHeader will not write again.
		if _, err := h.WriteTo(w); err != nil {
			return err
		}
		if _, err := f.WriteHeader(h); err != nil {
			return err
		}
	} else {
		mw.ResetWritePositionToZero()
		if _, err := h.WriteTo(bufw); err != nil {
			return err
		}
		f.SetHeader(h)
	}

	// We need to seek back to the end so we can write a checksum
	if mw == nil {
		if _, err := w.Seek(0, io.SeekEnd); err != nil {
			return fmt.Errorf("seek to end after writing header: %w", err)
		}
	} else {
		mw.ResetWritePositionToMax()
	}

	bufw.Reset(w)

	return nil
}
