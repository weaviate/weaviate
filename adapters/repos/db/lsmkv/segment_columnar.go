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

package lsmkv

import (
	"encoding/binary"
	"math"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// columnarSegmentData holds pre-computed offsets into the mmap'd contents
// of a columnar segment, enabling O(log N) lookups by docID and O(1)
// subsequent column reads.
type columnarSegmentData struct {
	numRows    uint64
	schema     columnar.Schema
	headerSize int // byte size of the columnar header

	// Byte offsets into segment.contents (relative to file start).
	// All offsets are past the standard LSM header.
	docIDOffset uint64 // start of docID array
	validOffset uint64 // start of valid array
	colOffsets  []uint64
	colWidths   []uint8
}

func loadColumnarSegmentData(contents []byte) (*columnarSegmentData, error) {
	// The columnar header starts right after the standard LSM header.
	buf := contents[segmentindex.HeaderSize:]
	ch, headerSize, err := columnar.UnmarshalColumnarHeader(buf)
	if err != nil {
		return nil, err
	}

	d := &columnarSegmentData{
		numRows:    ch.NumRows,
		schema:     ch.Schema,
		headerSize: headerSize,
	}

	base := uint64(segmentindex.HeaderSize) + uint64(headerSize)

	// docID array: numRows × 8 bytes
	d.docIDOffset = base
	base += d.numRows * 8

	// valid array: numRows × 1 byte
	d.validOffset = base
	base += d.numRows

	// value columns
	d.colOffsets = make([]uint64, len(ch.Schema.Columns))
	d.colWidths = make([]uint8, len(ch.Schema.Columns))
	for i, col := range ch.Schema.Columns {
		d.colOffsets[i] = base
		d.colWidths[i] = col.Type.Width()
		base += d.numRows * uint64(d.colWidths[i])
	}

	return d, nil
}

// bsearchDocID performs a binary search on the sorted docID array within
// the mmap'd contents buffer. Returns the row index, or -1 if not found.
func (d *columnarSegmentData) bsearchDocID(contents []byte, docID uint64) int {
	lo, hi := 0, int(d.numRows)
	docIDs := contents[d.docIDOffset:]
	for lo < hi {
		mid := lo + (hi-lo)/2
		midVal := binary.LittleEndian.Uint64(docIDs[mid*8:])
		if midVal < docID {
			lo = mid + 1
		} else if midVal > docID {
			hi = mid
		} else {
			return mid
		}
	}
	return -1
}

// isValid returns whether the row at the given index is live (not a tombstone).
func (d *columnarSegmentData) isValid(contents []byte, rowIdx int) bool {
	return contents[d.validOffset+uint64(rowIdx)] != 0
}

// lookupFloat32 retrieves a float32 value for the given docID and column.
// Returns (value, found, isTombstone).
func (d *columnarSegmentData) lookupFloat32(contents []byte, docID uint64, colIdx int) (float32, bool, bool) {
	rowIdx := d.bsearchDocID(contents, docID)
	if rowIdx < 0 {
		return 0, false, false
	}
	if !d.isValid(contents, rowIdx) {
		return 0, true, true
	}
	off := d.colOffsets[colIdx] + uint64(rowIdx)*4
	bits := binary.LittleEndian.Uint32(contents[off:])
	return math.Float32frombits(bits), true, false
}

// lookupInt64 retrieves an int64 value for the given docID and column.
// Returns (value, found, isTombstone).
func (d *columnarSegmentData) lookupInt64(contents []byte, docID uint64, colIdx int) (int64, bool, bool) {
	rowIdx := d.bsearchDocID(contents, docID)
	if rowIdx < 0 {
		return 0, false, false
	}
	if !d.isValid(contents, rowIdx) {
		return 0, true, true
	}
	off := d.colOffsets[colIdx] + uint64(rowIdx)*8
	val := binary.LittleEndian.Uint64(contents[off:])
	return int64(val), true, false
}

// columnarRowAt extracts the full row at the given index as a
// columnarMemtableRow (used during compaction).
func (d *columnarSegmentData) columnarRowAt(contents []byte, rowIdx int) *columnarMemtableRow {
	docID := binary.LittleEndian.Uint64(contents[d.docIDOffset+uint64(rowIdx)*8:])
	valid := d.isValid(contents, rowIdx)

	values := make([]byte, d.schema.RowWidth())
	for i := range d.colOffsets {
		w := uint64(d.colWidths[i])
		off := d.colOffsets[i] + uint64(rowIdx)*w
		copy(values[d.schema.ColOffset(i):], contents[off:off+w])
	}

	return &columnarMemtableRow{
		docID:  docID,
		valid:  valid,
		values: values,
	}
}

// docIDAt returns the docID at the given row index.
func (d *columnarSegmentData) docIDAt(contents []byte, rowIdx int) uint64 {
	return binary.LittleEndian.Uint64(contents[d.docIDOffset+uint64(rowIdx)*8:])
}
