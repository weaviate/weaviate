//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

// bufferedKeyAndTombstoneExtractor is a tool to build up the count stats for
// disk segments (i.e. all the keys in this segment as well as whether they
// contain a tombstone or not). It tries to be relatively memory-efficient
// while doing a whole-segment disk scan. It uses a primitive []byte buffer
// for its output which needs to be allocated just once. It can only read until
// the buffer is full, then it needs to call a callback fn which can do
// something with the data. After the callback function has been called on each
// key, the output buffer is reset. If the input segment it not at EOF yet,
// this cycle repeats
type bufferedKeyAndTombstoneExtractor struct {
	rawSegment          []byte
	outputBuffer        []byte
	outputBufferOffset  uint64
	offset              uint64
	end                 uint64
	segmentFile         io.ReaderAt
	segmentSize         int64
	secondaryIndexCount uint16
	callback            keyAndTombstoneCallbackFn
	callbackCycle       int
	mmapedContents      bool
}

type keyAndTombstoneCallbackFn func(key []byte, tombstone bool)

func newBufferedKeyAndTombstoneExtractor(rawSegment []byte, initialOffset uint64,
	end uint64, outputBufferSize uint64, secondaryIndexCount uint16,
	callback keyAndTombstoneCallbackFn, seg *os.File, segSize int64,
	mmappedContents bool,
) *bufferedKeyAndTombstoneExtractor {
	return &bufferedKeyAndTombstoneExtractor{
		rawSegment:          rawSegment,
		segmentFile:         seg,
		segmentSize:         segSize,
		offset:              initialOffset,
		end:                 end,
		outputBuffer:        make([]byte, outputBufferSize),
		outputBufferOffset:  0,
		secondaryIndexCount: secondaryIndexCount,
		callback:            callback,
		mmapedContents:      mmappedContents,
	}
}

func (e *bufferedKeyAndTombstoneExtractor) do() {
	for {
		if e.offset >= e.end {
			break
		}

		// returns false if the output buffer ran full
		ok := e.readSingleEntry()
		if !ok {
			e.flushAndCallback()
		}
	}

	// one final callback
	e.flushAndCallback()
}

// returns true if the cycle completed, returns false if the cycle did not
// complete because the output buffer was full. In that case, the offsets have
// been reset to the values they had at the beginning of the cycle
func (e *bufferedKeyAndTombstoneExtractor) readSingleEntry() bool {
	// if we discover during an iteration that the next entry can't fit in the
	// buffer anymore, we must return to the start of this iteration, so that
	// this work can be picked up here once the buffer has been flushed
	offsetAtLoopStart := e.offset
	outputOffsetAtLoopStart := e.outputBufferOffset

	// the first output size check is static, as we will always read 5 bytes,
	// no matter what. If they can't even fit, we can abort right away
	if !e.outputBufferCanFit(5) {
		e.offset = offsetAtLoopStart
		e.outputBufferOffset = outputOffsetAtLoopStart
		return false
	}

	var r tombstoneReader
	if e.mmapedContents {
		r = newMmapReader(e.rawSegment, e.offset)
	} else {
		r = newPReader(e.segmentFile, e.offset, e.segmentSize)
	}

	buf, err := r.Read(1 + 8)
	if err != nil {
		log.Printf("failed to read 9 bytes: %v", err)
		return false
	}
	e.outputBuffer[e.outputBufferOffset] = buf[0]
	e.offset++
	e.outputBufferOffset++

	valueLen := binary.LittleEndian.Uint64(buf[1:9])
	e.offset += 8

	// we're not actually interested in the value, so we can skip it entirely
	_, err = r.Read(valueLen)
	if err != nil {
		log.Printf("failed to read %d bytes: %v", valueLen, err)
		return false
	}
	e.offset += valueLen

	buf, err = r.Read(4)
	if err != nil {
		log.Printf("failed to read 4 bytes: %v", err)
		return false
	}
	primaryKeyLen := binary.LittleEndian.Uint32(buf[:4])
	if !e.outputBufferCanFit(uint64(primaryKeyLen) + 4) {
		e.offset = offsetAtLoopStart
		e.outputBufferOffset = outputOffsetAtLoopStart
		return false
	}

	copy(e.outputBuffer[e.outputBufferOffset:e.outputBufferOffset+4], buf[:4])
	e.offset += 4
	e.outputBufferOffset += 4

	// then copy the key itself
	buf, err = r.Read(uint64(primaryKeyLen))
	if err != nil {
		log.Printf("failed to read %d bytes: %v", primaryKeyLen, err)
		return false
	}
	copy(e.outputBuffer[e.outputBufferOffset:e.outputBufferOffset+uint64(primaryKeyLen)], buf[:primaryKeyLen])
	e.offset += uint64(primaryKeyLen)
	e.outputBufferOffset += uint64(primaryKeyLen)

	for i := uint16(0); i < e.secondaryIndexCount; i++ {
		buf, err = r.Read(4)
		if err != nil {
			log.Printf("failed to read 4 bytes: %v", err)
			return false
		}

		secKeyLen := binary.LittleEndian.Uint32(buf[:4])
		e.offset += 4

		_, err = r.Read(uint64(secKeyLen))
		if err != nil {
			log.Printf("failed to read %d bytes: %v", secKeyLen, err)
			return false
		}
		e.offset += uint64(secKeyLen)
	}

	return true
}

func (e *bufferedKeyAndTombstoneExtractor) outputBufferCanFit(size uint64) bool {
	return (uint64(len(e.outputBuffer)) - e.outputBufferOffset) >= size
}

// flushAndCallback calls the callback fn for each key/tombstone pair in the
// buffer, then resets the buffer offset, making it ready to be overwritten in
// the next cycle
func (e *bufferedKeyAndTombstoneExtractor) flushAndCallback() {
	end := e.outputBufferOffset
	e.outputBufferOffset = 0
	for e.outputBufferOffset < end {
		var tombstone bool
		if e.outputBuffer[e.outputBufferOffset] == 0x01 {
			tombstone = true
		}

		e.outputBufferOffset++

		primaryKeyLen := binary.LittleEndian.Uint32(e.outputBuffer[e.outputBufferOffset : e.outputBufferOffset+4])

		e.outputBufferOffset += 4

		e.callback(e.outputBuffer[e.outputBufferOffset:e.outputBufferOffset+uint64(primaryKeyLen)],
			tombstone)
		e.outputBufferOffset += uint64(primaryKeyLen)
	}

	// reset outputBufferOffset for next batch
	e.outputBufferOffset = 0

	e.callbackCycle++
}

type tombstoneReader interface {
	Read(uint64) ([]byte, error)
}

type pReader struct {
	buf    []byte
	r      *bufio.Reader
	offset uint64
}

func newPReader(r io.ReaderAt, offset uint64, n int64) *pReader {
	return &pReader{
		r:      bufio.NewReader(io.NewSectionReader(r, int64(offset), n)),
		offset: offset,
	}
}

func (r *pReader) Read(n uint64) ([]byte, error) {
	if uint64(len(r.buf)) <= r.offset+n {
		r.grow(n)
	}
	_, err := r.r.Read(r.buf[r.offset : r.offset+n])
	if err != nil {
		return nil, fmt.Errorf("pReader: read %d bytes: %w", n, err)
	}

	defer func() { r.offset += n }()
	return r.buf[r.offset : r.offset+n], nil
}

func (r *pReader) grow(n uint64) {
	extend := make([]byte, n)
	r.buf = append(r.buf, extend...)
}

type mmapReader struct {
	contents []byte
	offset   uint64
}

func newMmapReader(contents []byte, offset uint64) *mmapReader {
	return &mmapReader{contents: contents, offset: offset}
}

func (r *mmapReader) Read(n uint64) ([]byte, error) {
	defer func() { r.offset += n }()
	return r.contents[r.offset : r.offset+n], nil
}
