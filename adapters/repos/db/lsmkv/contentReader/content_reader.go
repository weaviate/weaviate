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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"syscall"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/edsrzf/mmap-go"
)

const (
	uint64Len = 8
	uint32Len = 4
)

type ContentReader interface {
	ReadValue(offset uint64) (byte, uint64)
	ReadRange(offset uint64, length uint64, outBuf []byte) ([]byte, uint64)
	ReadUint64(offset uint64, tmpBuf []byte) (uint64, uint64)
	ReadUint32(offset uint64, tmpBuf []byte) (uint32, uint64)
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

func (c MMap) ReadRange(offset uint64, length uint64, outBuf []byte) ([]byte, uint64) {
	if outBuf == nil {
		return c.contents[offset : offset+length], offset + length
	}
	copy(outBuf, c.contents[offset:offset+length])
	return outBuf, offset + length
}

func (c MMap) ReadUint64(offset uint64, tmpBuf []byte) (uint64, uint64) {
	return binary.LittleEndian.Uint64(c.contents[offset : offset+uint64Len]), offset + uint64Len
}

func (c MMap) ReadUint32(offset uint64, tmpBuf []byte) (uint32, uint64) {
	return binary.LittleEndian.Uint32(c.contents[offset : offset+uint32Len]), offset + uint32Len
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
	pageSize := syscall.Getpagesize()
	l, _ := lru.New[int, []byte](128)

	return Pread{contentFile: contentFile, size: size, startOffset: 0, endOffset: size, cache: l, pageSize: pageSize}
}

type Pread struct {
	contentFile *os.File
	size        uint64
	startOffset uint64
	endOffset   uint64
	cache       *lru.Cache[int, []byte]
	pageSize    int
}

func (c Pread) ReadValue(offset uint64) (byte, uint64) {
	b := make([]byte, 1)
	c.contentFile.ReadAt(b, int64(c.startOffset+offset))
	return b[0], offset + 1
}

func (c Pread) ReadRange(offset uint64, length uint64, outBuf []byte) ([]byte, uint64) {
	fullOffset := c.startOffset + offset
	offsetKey := int(fullOffset / uint64(c.pageSize))

	pagesAffected := 1 + int((fullOffset+length-1)/uint64(c.pageSize)) - int(fullOffset/uint64(c.pageSize))
	offsetInFirstPage := fullOffset % uint64(c.pageSize)
	if pagesAffected == 1 {
		memory := c.readFromCache(offsetKey)

		// if the memory is in a single page we can just return the slice
		if outBuf == nil {
			return memory[offsetInFirstPage : offsetInFirstPage+length], offset + length
		}
		copy(outBuf, memory[offsetInFirstPage:offsetInFirstPage+length])
		return outBuf, offset + length
	}

	if outBuf == nil {
		outBuf = make([]byte, length)
	}

	// (partial) first page
	memory := c.readFromCache(offsetKey)
	copy(outBuf[:uint64(c.pageSize)-offsetInFirstPage], memory[offsetInFirstPage:])

	// (partial) last page if needed
	if lastPageOffsetEnd := int(fullOffset+length) % c.pageSize; lastPageOffsetEnd > 0 {
		memory = c.readFromCache(int((fullOffset + length) / uint64(c.pageSize)))
		copy(outBuf[len(outBuf)-lastPageOffsetEnd:], memory[:lastPageOffsetEnd])
	}

	// full pages in-between, last (potentially partial page) is handled above
	for i := 1; i <= pagesAffected; i++ {
		if uint64(c.pageSize*(i+1)) > length {
			break
		}
		offsetKey = int((fullOffset + uint64(c.pageSize*i)) / uint64(c.pageSize))
		memory = c.readFromCache(offsetKey)
		copy(outBuf[uint64(c.pageSize*i)-offsetInFirstPage:uint64(c.pageSize*(i+1))-offsetInFirstPage], memory)
	}

	return outBuf, offset + length
}

func (c Pread) readFromCache(offsetKey int) []byte {
	memory, ok := c.cache.Get(offsetKey)
	if !ok {
		memory = make([]byte, c.pageSize)
		c.contentFile.ReadAt(memory, int64(c.pageSize*offsetKey))
		c.cache.Add(offsetKey, memory)
	}
	return memory
}

func (c Pread) ReadUint64(offset uint64, tmpBuf []byte) (uint64, uint64) {
	val, _ := c.ReadRange(offset, uint64Len, tmpBuf)
	return binary.LittleEndian.Uint64(val), offset + uint64Len
}

func (c Pread) ReadUint32(offset uint64, tmpBuf []byte) (uint32, uint64) {
	val, _ := c.ReadRange(offset, uint32Len, tmpBuf)
	return binary.LittleEndian.Uint32(val), offset + uint32Len
}

func (c Pread) Length() uint64 {
	return c.endOffset - c.startOffset
}

func (c Pread) Close() error {
	if err := c.contentFile.Close(); err != nil {
		return fmt.Errorf("close contents file: %w", err)
	}
	c.cache.Purge()
	return nil
}

func (c Pread) NewWithOffsetStart(start uint64) (ContentReader, error) {
	if c.startOffset+start > c.endOffset {
		return nil, fmt.Errorf("start offset %d is greater than the length of the file", c.startOffset+start)
	}
	return c.NewWithOffsetStartEnd(start, c.Length())
}

func (c Pread) NewWithOffsetStartEnd(start uint64, end uint64) (ContentReader, error) {
	if end > c.endOffset {
		return nil, fmt.Errorf("end offset %d is greater than the length of the file %d", c.endOffset+end, c.size)
	}
	return Pread{contentFile: c.contentFile, size: c.size, startOffset: c.startOffset + start, endOffset: c.startOffset + end, cache: c.cache, pageSize: c.pageSize}, nil
}

func (c Pread) ReaderFromOffset(start uint64, end uint64) io.Reader {
	if end == 0 {
		end = c.size
	} else {
		end = c.startOffset + end
	}
	startVal := c.startOffset + start
	return PReader{contentReader: c, position: &startVal, end: end}
}

type PReader struct {
	contentReader ContentReader
	position      *uint64 // cant use a pointer receiver for Read because of the io.Reader interface
	end           uint64
}

func (c PReader) Read(b []byte) (int, error) {
	lenB := uint64(len(b))
	position := *c.position
	if position >= c.end {
		return 0, io.EOF
	} else if position+lenB >= c.end {
		lenB = c.end - position
	}
	_, _ = c.contentReader.ReadRange(position, lenB, b[:lenB])
	*c.position += lenB
	return len(b), nil
}
