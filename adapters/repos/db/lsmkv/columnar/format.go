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

// Package columnar implements the on-disk format for the columnar LSM
// strategy. Segment layout:
//
//	[standard LSM segment header]
//	[columnar header: format version + column schema]
//	[block 0]
//	[block 1]
//	...
//	[directory]            <- standard header's IndexStart points here
//	[checksum]             (written by the shared SegmentFile machinery)
//
// Rows are sorted by docID and partitioned into blocks of up to BlockSize
// rows. Each block stores, column-major:
//
//	[docIDs:  8B × rows]
//	[live:    1B × rows]   (0 = tombstone)
//	[column 0 page (encoded)]
//	[column 1 page (encoded)]
//	...
//
// The directory is an array of fixed-width entries (one per block) holding
// the block's docID range, file offset, row/live counts, and per-column
// min/max over live rows. Readers binary-search the directory by docID and
// prune blocks by docID range or value range without touching data pages.
package columnar

import (
	"encoding/binary"
	"fmt"
)

// BlockSize is the maximum number of rows per block. 2048 rows keeps the
// per-block directory overhead below 0.4% for single-column buckets while
// amortizing per-block fixed costs in scans.
const BlockSize = 2048

const formatVersion uint8 = 1

// Header is the per-segment columnar header written immediately after the
// standard LSM segment header.
//
// Wire format:
//
//	version    uint8
//	numCols    uint16
//	for each column:
//	  nameLen  uint16
//	  name     []byte
//	  colType  uint8
//	  encoding uint8
//
// Row counts are intentionally not part of the header: they derive from the
// directory, which is written last. This keeps the header size independent
// of the data, so streaming writers (compaction) can write it up front.
type Header struct {
	Schema Schema
}

func (h *Header) MarshalBinary() []byte {
	size := 1 + 2
	for _, c := range h.Schema.Columns {
		size += 2 + len(c.Name) + 1 + 1
	}
	buf := make([]byte, size)
	off := 0

	buf[off] = formatVersion
	off++
	binary.LittleEndian.PutUint16(buf[off:], uint16(len(h.Schema.Columns)))
	off += 2
	for _, c := range h.Schema.Columns {
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(c.Name)))
		off += 2
		copy(buf[off:], c.Name)
		off += len(c.Name)
		buf[off] = byte(c.Type)
		off++
		buf[off] = byte(c.Encoding)
		off++
	}
	return buf
}

// UnmarshalHeader reads a columnar header from buf, returning the header and
// the number of bytes consumed.
func UnmarshalHeader(buf []byte) (*Header, int, error) {
	if len(buf) < 3 {
		return nil, 0, fmt.Errorf("columnar header too short")
	}
	off := 0
	version := buf[off]
	off++
	if version != formatVersion {
		return nil, 0, fmt.Errorf("unsupported columnar format version %d", version)
	}
	numCols := int(binary.LittleEndian.Uint16(buf[off:]))
	off += 2

	h := &Header{}
	h.Schema.Columns = make([]Column, numCols)
	for i := 0; i < numCols; i++ {
		if off+2 > len(buf) {
			return nil, 0, fmt.Errorf("columnar header truncated at column %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		if off+nameLen+2 > len(buf) {
			return nil, 0, fmt.Errorf("columnar header truncated at column %d name", i)
		}
		h.Schema.Columns[i].Name = string(buf[off : off+nameLen])
		off += nameLen
		h.Schema.Columns[i].Type = ColumnType(buf[off])
		off++
		h.Schema.Columns[i].Encoding = EncodingID(buf[off])
		off++
	}
	if err := h.Schema.validate(); err != nil {
		return nil, 0, err
	}
	return h, off, nil
}

// ColStats holds the min/max of a column within one block, computed over
// live rows only. Values are the raw 8-byte little-endian payloads,
// interpreted per the column's type.
type ColStats struct {
	Min uint64
	Max uint64
}

// DirectoryEntry describes one block.
type DirectoryEntry struct {
	StartDocID uint64
	EndDocID   uint64
	Offset     uint64 // absolute file offset of the block
	RowCount   uint32
	LiveCount  uint32 // rows with live=1; 0 means stats are meaningless
	Stats      []ColStats
}

func directoryEntrySize(numCols int) int {
	return 8 + 8 + 8 + 4 + 4 + numCols*16
}

func (e *DirectoryEntry) marshalTo(buf []byte) {
	off := 0
	binary.LittleEndian.PutUint64(buf[off:], e.StartDocID)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], e.EndDocID)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], e.Offset)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], e.RowCount)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], e.LiveCount)
	off += 4
	for _, s := range e.Stats {
		binary.LittleEndian.PutUint64(buf[off:], s.Min)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], s.Max)
		off += 8
	}
}

func unmarshalDirectoryEntry(buf []byte, numCols int) DirectoryEntry {
	off := 0
	e := DirectoryEntry{}
	e.StartDocID = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	e.EndDocID = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	e.Offset = binary.LittleEndian.Uint64(buf[off:])
	off += 8
	e.RowCount = binary.LittleEndian.Uint32(buf[off:])
	off += 4
	e.LiveCount = binary.LittleEndian.Uint32(buf[off:])
	off += 4
	e.Stats = make([]ColStats, numCols)
	for i := range e.Stats {
		e.Stats[i].Min = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		e.Stats[i].Max = binary.LittleEndian.Uint64(buf[off:])
		off += 8
	}
	return e
}

// MarshalDirectory serialises all directory entries.
func MarshalDirectory(entries []DirectoryEntry, numCols int) []byte {
	entrySize := directoryEntrySize(numCols)
	buf := make([]byte, entrySize*len(entries))
	for i := range entries {
		entries[i].marshalTo(buf[i*entrySize:])
	}
	return buf
}

// UnmarshalDirectory parses the directory section. Its length must be an
// exact multiple of the entry size.
func UnmarshalDirectory(buf []byte, numCols int) ([]DirectoryEntry, error) {
	entrySize := directoryEntrySize(numCols)
	if len(buf)%entrySize != 0 {
		return nil, fmt.Errorf("directory size %d not a multiple of entry size %d",
			len(buf), entrySize)
	}
	entries := make([]DirectoryEntry, len(buf)/entrySize)
	for i := range entries {
		entries[i] = unmarshalDirectoryEntry(buf[i*entrySize:], numCols)
	}
	return entries, nil
}

// BlockWriter accumulates rows (sorted by docID, deduplicated) and emits
// encoded blocks plus their directory entries. It holds at most one block's
// rows in memory, which is what makes streaming compaction possible.
type BlockWriter struct {
	schema *Schema

	docIDs []uint64
	live   []byte
	pages  [][]byte // per column, raw fixed-width values

	emit func(block []byte, entry DirectoryEntry) error

	// offset tracks the absolute file position of the next block.
	offset uint64

	lastDocID    uint64
	rowsAppended bool
}

// NewBlockWriter creates a writer that calls emit for every completed block.
// startOffset is the absolute file offset at which the first block will be
// written.
func NewBlockWriter(schema *Schema, startOffset uint64,
	emit func(block []byte, entry DirectoryEntry) error,
) *BlockWriter {
	pages := make([][]byte, len(schema.Columns))
	for i, c := range schema.Columns {
		pages[i] = make([]byte, 0, BlockSize*c.Type.Width())
	}
	return &BlockWriter{
		schema: schema,
		docIDs: make([]uint64, 0, BlockSize),
		live:   make([]byte, 0, BlockSize),
		pages:  pages,
		emit:   emit,
		offset: startOffset,
	}
}

// Append adds one row. Rows must arrive in strictly increasing docID order.
// values is the packed row payload (RowWidth bytes).
func (w *BlockWriter) Append(docID uint64, isLive bool, values []byte) error {
	if w.rowsAppended && docID <= w.lastDocID {
		return fmt.Errorf("columnar block writer: docID %d out of order (last %d)",
			docID, w.lastDocID)
	}
	w.lastDocID = docID
	w.rowsAppended = true

	w.docIDs = append(w.docIDs, docID)
	liveByte := byte(0)
	if isLive {
		liveByte = 1
	}
	w.live = append(w.live, liveByte)
	for i := range w.schema.Columns {
		width := w.schema.Columns[i].Type.Width()
		colOff := w.schema.ColOffset(i)
		w.pages[i] = append(w.pages[i], values[colOff:colOff+width]...)
	}

	if len(w.docIDs) == BlockSize {
		return w.flushBlock()
	}
	return nil
}

// Flush emits the final partial block, if any.
func (w *BlockWriter) Flush() error {
	if len(w.docIDs) == 0 {
		return nil
	}
	return w.flushBlock()
}

// Offset returns the absolute file offset just past the last emitted block,
// i.e. where the directory begins. Only meaningful after Flush.
func (w *BlockWriter) Offset() uint64 { return w.offset }

func (w *BlockWriter) flushBlock() error {
	rows := len(w.docIDs)

	entry := DirectoryEntry{
		StartDocID: w.docIDs[0],
		EndDocID:   w.docIDs[rows-1],
		Offset:     w.offset,
		RowCount:   uint32(rows),
		Stats:      make([]ColStats, len(w.schema.Columns)),
	}

	// min/max over live rows only
	for c := range w.schema.Columns {
		ct := w.schema.Columns[c].Type
		first := true
		for r := 0; r < rows; r++ {
			if w.live[r] == 0 {
				continue
			}
			v := binary.LittleEndian.Uint64(w.pages[c][r*8:])
			if first {
				entry.Stats[c] = ColStats{Min: v, Max: v}
				entry.LiveCount++
				first = false
				continue
			}
			entry.LiveCount++
			if ct.less(v, entry.Stats[c].Min) {
				entry.Stats[c].Min = v
			}
			if ct.less(entry.Stats[c].Max, v) {
				entry.Stats[c].Max = v
			}
		}
	}

	size := rows*8 + rows
	encs := make([]Encoding, len(w.schema.Columns))
	for i, c := range w.schema.Columns {
		enc, err := EncodingByID(c.Encoding)
		if err != nil {
			return err
		}
		encs[i] = enc
		size += enc.EncodedSize(c.Type.Width(), rows)
	}

	block := make([]byte, 0, size)
	for _, id := range w.docIDs {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], id)
		block = append(block, b[:]...)
	}
	block = append(block, w.live...)
	for i, c := range w.schema.Columns {
		block = encs[i].Encode(block, w.pages[i], c.Type.Width(), rows)
	}

	if err := w.emit(block, entry); err != nil {
		return err
	}

	w.offset += uint64(len(block))
	w.docIDs = w.docIDs[:0]
	w.live = w.live[:0]
	for i := range w.pages {
		w.pages[i] = w.pages[i][:0]
	}
	return nil
}

// BlockReader provides access to one block's contents within an mmap'd or
// in-memory segment buffer.
type BlockReader struct {
	schema *Schema
	entry  *DirectoryEntry
	docIDs []byte // 8B × rows
	live   []byte // 1B × rows
	pages  [][]byte
}

// NewBlockReader interprets the block described by entry within contents
// (the full segment buffer).
func NewBlockReader(schema *Schema, entry *DirectoryEntry, contents []byte) (*BlockReader, error) {
	rows := int(entry.RowCount)
	off := int(entry.Offset)

	r := &BlockReader{schema: schema, entry: entry}
	if off+rows*9 > len(contents) {
		return nil, fmt.Errorf("columnar block at %d out of bounds", entry.Offset)
	}
	r.docIDs = contents[off : off+rows*8]
	off += rows * 8
	r.live = contents[off : off+rows]
	off += rows

	r.pages = make([][]byte, len(schema.Columns))
	for i, c := range schema.Columns {
		enc, err := EncodingByID(c.Encoding)
		if err != nil {
			return nil, err
		}
		encSize := enc.EncodedSize(c.Type.Width(), rows)
		if off+encSize > len(contents) {
			return nil, fmt.Errorf("columnar block column %d out of bounds", i)
		}
		page, err := enc.Decode(contents[off:off+encSize], c.Type.Width(), rows)
		if err != nil {
			return nil, err
		}
		r.pages[i] = page
		off += encSize
	}
	return r, nil
}

func (r *BlockReader) Rows() int { return int(r.entry.RowCount) }

func (r *BlockReader) DocIDAt(row int) uint64 {
	return binary.LittleEndian.Uint64(r.docIDs[row*8:])
}

func (r *BlockReader) IsLive(row int) bool { return r.live[row] != 0 }

// ValueBitsAt returns the raw 8-byte payload of column col at row as uint64
// bits. Interpret via the column's type (int64 cast or
// math.Float64frombits).
func (r *BlockReader) ValueBitsAt(col, row int) uint64 {
	return binary.LittleEndian.Uint64(r.pages[col][row*8:])
}

// FindRow binary-searches the block for docID, returning the row index or
// -1.
func (r *BlockReader) FindRow(docID uint64) int {
	lo, hi := 0, r.Rows()
	for lo < hi {
		mid := lo + (hi-lo)/2
		v := r.DocIDAt(mid)
		switch {
		case v < docID:
			lo = mid + 1
		case v > docID:
			hi = mid
		default:
			return mid
		}
	}
	return -1
}

// RowValues extracts the packed row payload (RowWidth bytes) at row,
// appending to dst. Used by compaction to re-emit rows.
func (r *BlockReader) RowValues(dst []byte, row int) []byte {
	for c := range r.schema.Columns {
		width := r.schema.Columns[c].Type.Width()
		dst = append(dst, r.pages[c][row*width:(row+1)*width]...)
	}
	return dst
}
