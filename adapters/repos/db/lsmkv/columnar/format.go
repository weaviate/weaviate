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
// rows (VectorBlockSize for vector schemas). Each block stores,
// column-major:
//
//	[docIDs:  8B × rows]
//	[live:    1B × rows]   (0 = tombstone)
//	[column 0 page]        (vector pages padded to payloadAlign)
//	[column 1 page]
//	...
//
// The directory is an array of fixed-width entries (one per block) holding
// the block's docID range, file offset, row/live counts, and per column the
// page offset/size within the block plus min/max stats over live rows
// (scalar columns only; zeroed for vectors). Readers binary-search the
// directory by docID and prune blocks by docID range or value range without
// touching data pages.
//
// Format versions: v1 (scalar-only, no per-column dims or page table) is
// read-supported; all new segments are written as v2.
package columnar

import (
	"encoding/binary"
	"fmt"
	"math"
)

// BlockSize is the maximum number of rows per block for scalar schemas.
// 2048 rows keeps the per-block directory overhead below 0.4% for
// single-column buckets while amortizing per-block fixed costs in scans.
const BlockSize = 2048

// VectorBlockSize is the maximum number of rows per block for vector and
// multi-vector schemas. Vector rows are KB-scale (e.g. 1536 dims = 6 KB;
// multi-vector token matrices reach hundreds of KB), so 2048-row blocks
// would defeat block-granular access. 256 rows keeps blocks in the
// 1.5–8 MB range for common single-vector dims.
const VectorBlockSize = 256

const (
	formatVersionV1 uint8 = 1
	formatVersion   uint8 = 2
)

// BlockSizeFor returns the rows-per-block limit for the given schema.
func BlockSizeFor(s *Schema) int {
	if s.IsVector() {
		return VectorBlockSize
	}
	return BlockSize
}

// Header is the per-segment columnar header written immediately after the
// standard LSM segment header.
//
// Wire format v2:
//
//	version    uint8
//	numCols    uint16
//	for each column:
//	  nameLen  uint16
//	  name     []byte
//	  colType  uint8
//	  encoding uint8
//	  dims     uint32
//
// v1 lacks the dims field. Row counts are intentionally not part of the
// header: they derive from the directory, which is written last. This keeps
// the header size independent of the data, so streaming writers
// (compaction) can write it up front.
type Header struct {
	Schema Schema
}

func (h *Header) MarshalBinary() []byte {
	size := 1 + 2
	for _, c := range h.Schema.Columns {
		size += 2 + len(c.Name) + 1 + 1 + 4
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
		binary.LittleEndian.PutUint32(buf[off:], c.Dims)
		off += 4
	}
	return buf
}

// UnmarshalHeader reads a columnar header from buf, returning the header,
// the number of bytes consumed, and the format version.
func UnmarshalHeader(buf []byte) (*Header, int, uint8, error) {
	if len(buf) < 3 {
		return nil, 0, 0, fmt.Errorf("columnar header too short")
	}
	off := 0
	version := buf[off]
	off++
	if version != formatVersionV1 && version != formatVersion {
		return nil, 0, 0, fmt.Errorf("unsupported columnar format version %d", version)
	}
	numCols := int(binary.LittleEndian.Uint16(buf[off:]))
	off += 2

	h := &Header{}
	h.Schema.Columns = make([]Column, numCols)
	for i := 0; i < numCols; i++ {
		if off+2 > len(buf) {
			return nil, 0, 0, fmt.Errorf("columnar header truncated at column %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		tail := 2 // colType + encoding
		if version >= formatVersion {
			tail += 4 // dims
		}
		if off+nameLen+tail > len(buf) {
			return nil, 0, 0, fmt.Errorf("columnar header truncated at column %d name", i)
		}
		h.Schema.Columns[i].Name = string(buf[off : off+nameLen])
		off += nameLen
		h.Schema.Columns[i].Type = ColumnType(buf[off])
		off++
		h.Schema.Columns[i].Encoding = EncodingID(buf[off])
		off++
		if version >= formatVersion {
			h.Schema.Columns[i].Dims = binary.LittleEndian.Uint32(buf[off:])
			off += 4
		}
	}
	if err := h.Schema.validate(); err != nil {
		return nil, 0, 0, err
	}
	return h, off, version, nil
}

// ColStats holds the min/max of a scalar column within one block, computed
// over live rows only. Values are the raw 8-byte little-endian payloads,
// interpreted per the column's type. Zeroed for vector columns. For float64
// columns, NaN values are stored as row data but excluded from min/max; a
// block whose live values are all NaN keeps zeroed stats (like a block with
// LiveCount 0).
type ColStats struct {
	Min uint64
	Max uint64
}

// ColPage locates one column's encoded page within its block.
type ColPage struct {
	// Offset is the page's byte offset relative to the block start.
	Offset uint32
	// Size is the page's byte length.
	Size uint32
}

// DirectoryEntry describes one block.
type DirectoryEntry struct {
	StartDocID uint64
	EndDocID   uint64
	Offset     uint64 // absolute file offset of the block
	RowCount   uint32
	LiveCount  uint32 // rows with live=1; 0 means stats are meaningless
	Stats      []ColStats
	Pages      []ColPage // v2 only; derived for v1 segments at load time
}

func directoryEntrySize(numCols int, version uint8) int {
	per := 16 // stats
	if version >= formatVersion {
		per += 8 // page offset+size
	}
	return 8 + 8 + 8 + 4 + 4 + numCols*per
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
	for i := range e.Stats {
		binary.LittleEndian.PutUint64(buf[off:], e.Stats[i].Min)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], e.Stats[i].Max)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:], e.Pages[i].Offset)
		off += 4
		binary.LittleEndian.PutUint32(buf[off:], e.Pages[i].Size)
		off += 4
	}
}

func unmarshalDirectoryEntry(buf []byte, numCols int, version uint8) DirectoryEntry {
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
	e.Pages = make([]ColPage, numCols)
	for i := 0; i < numCols; i++ {
		e.Stats[i].Min = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		e.Stats[i].Max = binary.LittleEndian.Uint64(buf[off:])
		off += 8
		if version >= formatVersion {
			e.Pages[i].Offset = binary.LittleEndian.Uint32(buf[off:])
			off += 4
			e.Pages[i].Size = binary.LittleEndian.Uint32(buf[off:])
			off += 4
		}
	}
	return e
}

// derivePagesV1 reconstructs the page table for a v1 entry, where all
// columns are 8-byte scalars laid out contiguously after docIDs+live with
// no padding.
func derivePagesV1(e *DirectoryEntry, schema *Schema) {
	rows := int(e.RowCount)
	off := rows*8 + rows
	for i := range schema.Columns {
		size := schema.Columns[i].Width() * rows
		e.Pages[i] = ColPage{Offset: uint32(off), Size: uint32(size)}
		off += size
	}
}

// MarshalDirectory serialises all directory entries (always v2).
func MarshalDirectory(entries []DirectoryEntry, numCols int) []byte {
	entrySize := directoryEntrySize(numCols, formatVersion)
	buf := make([]byte, entrySize*len(entries))
	for i := range entries {
		entries[i].marshalTo(buf[i*entrySize:])
	}
	return buf
}

// UnmarshalDirectory parses the directory section. Its length must be an
// exact multiple of the entry size for the segment's format version.
func UnmarshalDirectory(buf []byte, schema *Schema, version uint8) ([]DirectoryEntry, error) {
	numCols := len(schema.Columns)
	entrySize := directoryEntrySize(numCols, version)
	if len(buf)%entrySize != 0 {
		return nil, fmt.Errorf("directory size %d not a multiple of entry size %d",
			len(buf), entrySize)
	}
	entries := make([]DirectoryEntry, len(buf)/entrySize)
	for i := range entries {
		entries[i] = unmarshalDirectoryEntry(buf[i*entrySize:], numCols, version)
		if version < formatVersion {
			derivePagesV1(&entries[i], schema)
		}
	}
	return entries, nil
}

// BlockWriter accumulates rows (sorted by docID, deduplicated) and emits
// encoded blocks plus their directory entries. It holds at most one block's
// rows in memory, which is what makes streaming compaction possible.
type BlockWriter struct {
	schema    *Schema
	blockSize int

	docIDs []uint64
	live   []byte
	pages  [][]byte // per column: raw fixed-width values, or concatenated variable payloads

	// rowEnds tracks per-row payload end offsets for the variable column
	// (multi-vector schemas have exactly one column).
	rowEnds []uint32

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
	return &BlockWriter{
		schema:    schema,
		blockSize: BlockSizeFor(schema),
		docIDs:    make([]uint64, 0, BlockSizeFor(schema)),
		live:      make([]byte, 0, BlockSizeFor(schema)),
		pages:     pages,
		emit:      emit,
		offset:    startOffset,
	}
}

// Append adds one row. Rows must arrive in strictly increasing docID order.
// values is the packed row payload: fixed-width columns concatenated in
// schema order, or the raw variable payload for variable schemas.
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

	if w.schema.Columns[0].IsVariable() {
		w.pages[0] = append(w.pages[0], values...)
		w.rowEnds = append(w.rowEnds, uint32(len(w.pages[0])))
	} else {
		for i := range w.schema.Columns {
			width := w.schema.Columns[i].Width()
			colOff := w.schema.ColOffset(i)
			w.pages[i] = append(w.pages[i], values[colOff:colOff+width]...)
		}
	}

	if len(w.docIDs) == w.blockSize {
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

	// Vector schemas align block starts absolutely (the in-block page
	// offsets are payloadAlign multiples, so an aligned block start makes
	// every vector payload payloadAlign-aligned in the file and therefore
	// in the mmap). The pad bytes are emitted as a prefix; entry.Offset
	// points past them.
	pad := 0
	if w.schema.IsVector() {
		pad = alignUp(int(w.offset), payloadAlign) - int(w.offset)
	}

	entry := DirectoryEntry{
		StartDocID: w.docIDs[0],
		EndDocID:   w.docIDs[rows-1],
		Offset:     w.offset + uint64(pad),
		RowCount:   uint32(rows),
		Stats:      make([]ColStats, len(w.schema.Columns)),
		Pages:      make([]ColPage, len(w.schema.Columns)),
	}

	for r := 0; r < rows; r++ {
		if w.live[r] != 0 {
			entry.LiveCount++
		}
	}

	// min/max over live rows, scalar columns only — meaningless for vectors.
	// NaN float64 values are stored as data but excluded from the stats: NaN
	// compares false against everything, so a NaN that seeds Min/Max would
	// freeze them (less(v, NaN) and less(NaN, v) are both always false) and
	// poison block pruning. A block whose live values are all NaN keeps
	// zeroed stats, same as a block with no live rows.
	for c := range w.schema.Columns {
		ct := w.schema.Columns[c].Type
		if ct.isVector() {
			continue
		}
		first := true
		for r := 0; r < rows; r++ {
			if w.live[r] == 0 {
				continue
			}
			v := binary.LittleEndian.Uint64(w.pages[c][r*8:])
			if ct == ColumnTypeFloat64 && math.IsNaN(math.Float64frombits(v)) {
				continue
			}
			if first {
				entry.Stats[c] = ColStats{Min: v, Max: v}
				first = false
				continue
			}
			if ct.less(v, entry.Stats[c].Min) {
				entry.Stats[c].Min = v
			}
			if ct.less(entry.Stats[c].Max, v) {
				entry.Stats[c].Max = v
			}
		}
	}

	// layout: docIDs, live, then per-column pages (vector payloads aligned)
	blockSize := rows*8 + rows
	for c := range w.schema.Columns {
		col := &w.schema.Columns[c]
		pageOff := blockSize
		var pageSize int
		if col.IsVariable() {
			// [offsets (rows+1)×u32][pad to payloadAlign][payload] — the
			// offsets header is padded to a payloadAlign multiple, so an
			// aligned page start keeps the payload aligned too
			pageOff = alignUp(pageOff, payloadAlign)
			headerSize := VariablePageHeaderSize(rows)
			pageSize = headerSize + len(w.pages[c])
		} else {
			if col.Type.isVector() {
				pageOff = alignUp(pageOff, payloadAlign)
			}
			pageSize = len(w.pages[c])
		}
		entry.Pages[c] = ColPage{Offset: uint32(pageOff), Size: uint32(pageSize)}
		blockSize = pageOff + pageSize
	}

	buf := make([]byte, pad+blockSize)
	block := buf[pad:]
	off := 0
	for _, id := range w.docIDs {
		binary.LittleEndian.PutUint64(block[off:], id)
		off += 8
	}
	copy(block[off:], w.live)

	for c := range w.schema.Columns {
		col := &w.schema.Columns[c]
		pageStart := int(entry.Pages[c].Offset)
		if col.IsVariable() {
			// offset table: row i spans payload[start(i):end(i)]
			for r := 0; r <= rows; r++ {
				var v uint32
				if r > 0 {
					v = w.rowEnds[r-1]
				}
				binary.LittleEndian.PutUint32(block[pageStart+4*r:], v)
			}
			payloadStart := pageStart + VariablePageHeaderSize(rows)
			copy(block[payloadStart:], w.pages[c])
		} else {
			copy(block[pageStart:], w.pages[c])
		}
	}

	if err := w.emit(buf, entry); err != nil {
		return err
	}

	w.offset += uint64(len(buf))
	w.docIDs = w.docIDs[:0]
	w.live = w.live[:0]
	w.rowEnds = w.rowEnds[:0]
	for i := range w.pages {
		w.pages[i] = w.pages[i][:0]
	}
	return nil
}

// VariablePageHeaderSize returns the byte size of a variable page's
// row-offset header (offset table padded to the payload alignment) for a
// block of the given row count. Exposed for direct page access on the
// point-lookup hot path.
func VariablePageHeaderSize(rows int) int {
	return alignUp(4*(rows+1), payloadAlign)
}

// BlockReader provides access to one block's contents within an mmap'd or
// in-memory segment buffer.
type BlockReader struct {
	schema *Schema
	entry  *DirectoryEntry
	docIDs []byte // 8B × rows
	live   []byte // 1B × rows
	pages  [][]byte

	// varOffsets/varPayload are set for variable-width (multi-vector)
	// schemas: the row-offset table and the aligned payload slab.
	varOffsets []byte
	varPayload []byte
}

// NewBlockReader interprets the block described by entry within contents
// (the full segment buffer).
func NewBlockReader(schema *Schema, entry *DirectoryEntry, contents []byte) (*BlockReader, error) {
	rows := int(entry.RowCount)
	base := int(entry.Offset)

	r := &BlockReader{schema: schema, entry: entry}
	if base+rows*9 > len(contents) {
		return nil, fmt.Errorf("columnar block at %d out of bounds", entry.Offset)
	}
	r.docIDs = contents[base : base+rows*8]
	r.live = contents[base+rows*8 : base+rows*9]

	r.pages = make([][]byte, len(schema.Columns))
	for i := range schema.Columns {
		col := &schema.Columns[i]
		pageStart := base + int(entry.Pages[i].Offset)
		pageEnd := pageStart + int(entry.Pages[i].Size)
		if pageEnd > len(contents) || pageStart > pageEnd {
			return nil, fmt.Errorf("columnar block column %d page out of bounds", i)
		}
		page := contents[pageStart:pageEnd]

		if col.IsVariable() {
			headerSize := VariablePageHeaderSize(rows)
			if len(page) < headerSize {
				return nil, fmt.Errorf("columnar block column %d variable page too short", i)
			}
			r.varOffsets = page[:4*(rows+1)]
			r.varPayload = page[headerSize:]
			r.pages[i] = page
			continue
		}

		enc, err := EncodingByID(col.Encoding)
		if err != nil {
			return nil, err
		}
		decoded, err := enc.Decode(page, col.Width(), rows)
		if err != nil {
			return nil, err
		}
		r.pages[i] = decoded
	}
	return r, nil
}

func (r *BlockReader) Rows() int { return int(r.entry.RowCount) }

func (r *BlockReader) DocIDAt(row int) uint64 {
	return binary.LittleEndian.Uint64(r.docIDs[row*8:])
}

func (r *BlockReader) IsLive(row int) bool { return r.live[row] != 0 }

// ValueBitsAt returns the raw 8-byte payload of scalar column col at row as
// uint64 bits. Interpret via the column's type (int64 cast or
// math.Float64frombits).
func (r *BlockReader) ValueBitsAt(col, row int) uint64 {
	return binary.LittleEndian.Uint64(r.pages[col][row*8:])
}

// FixedValueAt returns the raw bytes of fixed-width column col at row
// (vector payloads included). The returned slice aliases the segment
// buffer and must not be retained past the segment's lifetime.
func (r *BlockReader) FixedValueAt(col, row int) []byte {
	w := r.schema.Columns[col].Width()
	return r.pages[col][row*w : (row+1)*w]
}

// VariableValueAt returns the raw payload bytes of the variable column at
// row. The returned slice aliases the segment buffer and must not be
// retained past the segment's lifetime.
func (r *BlockReader) VariableValueAt(row int) []byte {
	start := binary.LittleEndian.Uint32(r.varOffsets[4*row:])
	end := binary.LittleEndian.Uint32(r.varOffsets[4*(row+1):])
	return r.varPayload[start:end]
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

// RowValues extracts the packed row payload at row, appending to dst. Used
// by compaction to re-emit rows. For variable schemas this is the row's raw
// payload; for fixed schemas the concatenated column values.
func (r *BlockReader) RowValues(dst []byte, row int) []byte {
	if r.schema.Columns[0].IsVariable() {
		return append(dst, r.VariableValueAt(row)...)
	}
	for c := range r.schema.Columns {
		dst = append(dst, r.FixedValueAt(c, row)...)
	}
	return dst
}
