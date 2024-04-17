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

package contentReader

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
)

const (
	Uint64Len = 8
	Uint32Len = 4
)

type ContentReader interface {
	ReadValue(offset uint64) (byte, uint64)
	ReadRange(offset uint64, length uint64) ([]byte, uint64)
	CopyRange(offset uint64, length uint64) ([]byte, uint64)
	ReadUint64(offset uint64) (uint64, uint64)
	ReadUint32(offset uint64) (uint32, uint64)
	Length() uint64
	Close() error
	NewWithOffsetStart(start uint64) (ContentReader, error)
	NewWithOffsetStartEnd(start uint64, end uint64) (ContentReader, error)
	ReaderFromOffset(start uint64, end uint64) io.Reader
}

type MMap struct {
	contents []byte
}

func (c MMap) ReadValue(offset uint64) (byte, uint64) {
	return c.contents[offset], offset + 1
}

func (c MMap) ReadRange(offset uint64, length uint64) ([]byte, uint64) {
	return c.contents[offset : offset+length], offset + length
}

func (c MMap) CopyRange(offset uint64, length uint64) ([]byte, uint64) {
	b := make([]byte, length)
	copy(b, c.contents[offset:offset+length])
	return b, offset + length
}

func (c MMap) ReadUint64(offset uint64) (uint64, uint64) {
	return binary.LittleEndian.Uint64(c.contents[offset : offset+Uint64Len]), offset + Uint64Len
}

func (c MMap) ReadUint32(offset uint64) (uint32, uint64) {
	return binary.LittleEndian.Uint32(c.contents[offset : offset+Uint32Len]), offset + Uint32Len
}

func (c MMap) Length() uint64 {
	return uint64(len(c.contents))
}

func (c MMap) Close() error {
	m := mmap.MMap(c.contents)
	if err := m.Unmap(); err != nil {
		return fmt.Errorf("close segment: munmap: %w", err)
	}
	return nil
}

func (c MMap) NewWithOffsetStart(start uint64) (ContentReader, error) {
	if start > uint64(len(c.contents)) {
		return nil, fmt.Errorf("start offset %d is greater than the length of the file", start)
	}
	return c.NewWithOffsetStartEnd(start, uint64(len(c.contents)))
}

func (c MMap) NewWithOffsetStartEnd(start uint64, end uint64) (ContentReader, error) {
	if end > uint64(len(c.contents)) {
		return nil, fmt.Errorf("end offset %d is greater than the length of the contents %d", end, len(c.contents))
	}
	return MMap{contents: c.contents[start:end]}, nil
}

func (c MMap) ReaderFromOffset(start uint64, end uint64) io.Reader {
	if end == 0 {
		return bytes.NewReader(c.contents[start:])
	}
	return bytes.NewReader(c.contents[start:end])
}

func NewMMap(contents []byte) ContentReader {
	return MMap{contents: contents}
}

func NewPread(contentFile *os.File, size uint64) ContentReader {
	return Pread{contentFile: contentFile, size: size, startOffset: 0, endOffset: size}
}

type Pread struct {
	contentFile *os.File
	size        uint64
	startOffset uint64
	endOffset   uint64
}

func (c Pread) ReadValue(offset uint64) (byte, uint64) {
	b := make([]byte, 1)
	c.contentFile.ReadAt(b, int64(c.startOffset+offset))
	return b[0], offset + 1
}

func (c Pread) ReadRange(offset uint64, length uint64) ([]byte, uint64) {
	b := make([]byte, length)
	c.contentFile.ReadAt(b, int64(c.startOffset+offset))
	return b, offset + length
}

func (c Pread) CopyRange(offset uint64, length uint64) ([]byte, uint64) {
	return c.ReadRange(offset, length)
}

func (c Pread) ReadUint64(offset uint64) (uint64, uint64) {
	val, _ := c.ReadRange(offset, Uint64Len)
	return binary.LittleEndian.Uint64(val), offset + Uint64Len
}

func (c Pread) ReadUint32(offset uint64) (uint32, uint64) {
	val, _ := c.ReadRange(offset, Uint32Len)
	return binary.LittleEndian.Uint32(val), offset + Uint32Len
}

func (c Pread) Length() uint64 {
	return c.size
}

func (c Pread) Close() error {
	if err := c.contentFile.Close(); err != nil {
		return fmt.Errorf("close contents file: %w", err)
	}
	return nil
}

func (c Pread) NewWithOffsetStart(start uint64) (ContentReader, error) {
	if c.startOffset+start > c.endOffset {
		return nil, fmt.Errorf("start offset %d is greater than the length of the file", c.startOffset+start)
	}
	return c.NewWithOffsetStartEnd(start, c.endOffset)
}

func (c Pread) NewWithOffsetStartEnd(start uint64, end uint64) (ContentReader, error) {
	if end > c.endOffset {
		return nil, fmt.Errorf("end offset %d is greater than the length of the file %d", c.endOffset+end, c.size)
	}
	return Pread{contentFile: c.contentFile, size: c.size, startOffset: c.startOffset + start, endOffset: c.startOffset + end}, nil
}

func (c Pread) ReaderFromOffset(start uint64, end uint64) io.Reader {
	if end == 0 {
		end = c.size
	}
	return bufio.NewReader(io.NewSectionReader(c.contentFile, int64(start), int64(end-start)))
}
