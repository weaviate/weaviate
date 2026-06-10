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
	"fmt"
	"sort"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// columnarSegmentData holds the parsed columnar header and block directory
// of one segment. Data pages stay in the segment's (mmap'd) contents and are
// only touched when a block survives pruning.
type columnarSegmentData struct {
	schema  columnar.Schema
	entries []columnar.DirectoryEntry
}

func loadColumnarSegmentData(contents []byte, indexStart uint64, version uint16) (*columnarSegmentData, error) {
	h, _, formatVersion, err := columnar.UnmarshalHeader(contents[segmentindex.HeaderSize:])
	if err != nil {
		return nil, fmt.Errorf("parse columnar header: %w", err)
	}

	dirEnd := uint64(len(contents))
	if version >= segmentindex.SegmentV1 {
		// V1 segments carry a trailing checksum after the directory.
		dirEnd -= segmentindex.ChecksumSize
	}
	if indexStart > dirEnd {
		return nil, fmt.Errorf("columnar directory start %d beyond end %d", indexStart, dirEnd)
	}

	entries, err := columnar.UnmarshalDirectory(contents[indexStart:dirEnd], &h.Schema, formatVersion)
	if err != nil {
		return nil, fmt.Errorf("parse columnar directory: %w", err)
	}

	return &columnarSegmentData{schema: h.Schema, entries: entries}, nil
}

// findBlock returns the index of the directory entry whose docID range
// contains docID, or -1.
func (d *columnarSegmentData) findBlock(docID uint64) int {
	idx := sort.Search(len(d.entries), func(i int) bool {
		return d.entries[i].EndDocID >= docID
	})
	if idx == len(d.entries) || d.entries[idx].StartDocID > docID {
		return -1
	}
	return idx
}

// lookup returns (rawBits, found, isTombstone) for docID in column colIdx.
func (d *columnarSegmentData) lookup(contents []byte, docID uint64, colIdx int) (uint64, bool, bool) {
	blockIdx := d.findBlock(docID)
	if blockIdx < 0 {
		return 0, false, false
	}
	br, err := columnar.NewBlockReader(&d.schema, &d.entries[blockIdx], contents)
	if err != nil {
		// A block that fails to parse after a successful segment load means
		// corruption past the checksum gate; treat as not-found rather than
		// failing the entire read path.
		return 0, false, false
	}
	row := br.FindRow(docID)
	if row < 0 {
		return 0, false, false
	}
	if !br.IsLive(row) {
		return 0, true, true
	}
	return br.ValueBitsAt(colIdx, row), true, false
}

// lookupPayload appends the row's payload bytes (vector or multi-vector)
// to dst. Must be called while the segment's contents are pinned; the
// returned slice extends dst and does not alias the segment buffer.
func (d *columnarSegmentData) lookupPayload(contents []byte, docID uint64, dst []byte) ([]byte, bool, bool) {
	ref, found, tomb := d.lookupPayloadRef(contents, docID)
	if !found || tomb {
		return dst, found, tomb
	}
	return append(dst, ref...), true, false
}

// lookupPayloadRef returns the row's payload bytes ALIASING the segment
// buffer. Callers must copy or decode the result before releasing the
// segment pin.
//
// This is the rescore hot path: it operates on the block bytes directly
// via the directory's page table instead of constructing a BlockReader,
// keeping it allocation-free.
func (d *columnarSegmentData) lookupPayloadRef(contents []byte, docID uint64) ([]byte, bool, bool) {
	blockIdx := d.findBlock(docID)
	if blockIdx < 0 {
		return nil, false, false
	}
	e := &d.entries[blockIdx]
	rows := int(e.RowCount)
	base := int(e.Offset)
	if base+rows*9 > len(contents) || len(e.Pages) != 1 {
		return nil, false, false
	}
	docIDs := contents[base : base+rows*8]

	// in-place binary search on the sorted docID array
	lo, hi := 0, rows
	for lo < hi {
		mid := lo + (hi-lo)/2
		v := binary.LittleEndian.Uint64(docIDs[mid*8:])
		switch {
		case v < docID:
			lo = mid + 1
		case v > docID:
			hi = mid
		default:
			lo = mid
			hi = -1
		}
	}
	if hi != -1 {
		return nil, false, false
	}
	row := lo
	if contents[base+rows*8+row] == 0 {
		return nil, true, true // tombstone
	}

	pageStart := base + int(e.Pages[0].Offset)
	pageEnd := pageStart + int(e.Pages[0].Size)
	if pageEnd > len(contents) {
		return nil, false, false
	}
	page := contents[pageStart:pageEnd]

	col := &d.schema.Columns[0]
	if col.IsVariable() {
		// [offsets (rows+1)×u32][pad][payload]
		headerSize := columnar.VariablePageHeaderSize(rows)
		if len(page) < headerSize {
			return nil, false, false
		}
		start := binary.LittleEndian.Uint32(page[4*row:])
		end := binary.LittleEndian.Uint32(page[4*(row+1):])
		payload := page[headerSize:]
		if int(end) > len(payload) || start > end {
			return nil, false, false
		}
		return payload[start:end], true, false
	}

	w := col.Width()
	if (row+1)*w > len(page) {
		return nil, false, false
	}
	return page[row*w : (row+1)*w], true, false
}

// scanBlocks calls visit for each block in docID order. visit returns false
// to stop the scan early.
func (d *columnarSegmentData) scanBlocks(contents []byte,
	visit func(entry *columnar.DirectoryEntry, br *columnar.BlockReader) (bool, error),
) error {
	for i := range d.entries {
		br, err := columnar.NewBlockReader(&d.schema, &d.entries[i], contents)
		if err != nil {
			return err
		}
		cont, err := visit(&d.entries[i], br)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}
