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

package lsmkv

import (
	"encoding/binary"
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
	outputBuffer        []byte
	outputBufferOffset  uint64
	offset              uint64
	end                 uint64
	rawSegment          []byte
	secondaryIndexCount uint16
	callback            keyAndTombstoneCallbackFn
	callbackCycle       int
}

type keyAndTombstoneCallbackFn func(key []byte, tombstone bool)

func newBufferedKeyAndTombstoneExtractor(rawSegment []byte, initialOffset uint64,
	end uint64, outputBufferSize uint64, secondaryIndexCount uint16,
	callback keyAndTombstoneCallbackFn,
) *bufferedKeyAndTombstoneExtractor {
	return &bufferedKeyAndTombstoneExtractor{
		rawSegment:          rawSegment,
		offset:              initialOffset,
		end:                 end,
		outputBuffer:        make([]byte, outputBufferSize),
		outputBufferOffset:  0,
		secondaryIndexCount: secondaryIndexCount,
		callback:            callback,
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
	// the this work can be picked up here once the buffer has been flushed
	offsetAtLoopStart := e.offset
	outputOffsetAtLoopStart := e.outputBufferOffset

	// the first output size check is static, as we will always read 5 bytes,
	// no matter what. If they can't even fit, we can abort right away
	if !e.outputBufferCanFit(5) {
		e.offset = offsetAtLoopStart
		e.outputBufferOffset = outputOffsetAtLoopStart
		return false
	}

	// copy tombstone value into output buffer
	e.outputBuffer[e.outputBufferOffset] = e.rawSegment[e.offset]
	e.offset++
	e.outputBufferOffset++

	valueLen := binary.LittleEndian.Uint64(e.rawSegment[e.offset : e.offset+8])
	e.offset += 8

	// we're not actually interested in the value, so we can skip it entirely
	e.offset += valueLen

	primaryKeyLen := binary.LittleEndian.Uint32(e.rawSegment[e.offset : e.offset+4])
	if !e.outputBufferCanFit(uint64(primaryKeyLen) + 4) {
		e.offset = offsetAtLoopStart
		e.outputBufferOffset = outputOffsetAtLoopStart
		return false
	}

	// copy the primary key len indicator into the output buffer
	copy(e.outputBuffer[e.outputBufferOffset:e.outputBufferOffset+4],
		e.rawSegment[e.offset:e.offset+4])
	e.offset += 4
	e.outputBufferOffset += 4

	// then copy the key itself
	copy(e.outputBuffer[e.outputBufferOffset:e.outputBufferOffset+uint64(primaryKeyLen)], e.rawSegment[e.offset:e.offset+uint64(primaryKeyLen)])
	e.offset += uint64(primaryKeyLen)
	e.outputBufferOffset += uint64(primaryKeyLen)

	for i := uint16(0); i < e.secondaryIndexCount; i++ {
		secKeyLen := binary.LittleEndian.Uint32(e.rawSegment[e.offset : e.offset+4])
		e.offset += 4
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
