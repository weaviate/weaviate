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
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
)

// columnarMemtableRow holds one document's columnar values in memory.
type columnarMemtableRow struct {
	docID  uint64
	live   bool   // false = tombstone
	values []byte // packed column values, schema layout
}

func (m *Memtable) initColumnar(schema *columnar.Schema) {
	m.columnarSchema = schema
	m.columnarRows = make(map[uint64]*columnarMemtableRow)
	if w := schema.RowWidth(); w > 0 {
		m.columnarWALBuf = make([]byte, 8+1+w) // reusable WAL buffer
	}
	// variable-width (multi-vector) schemas allocate per WAL write
}

func (m *Memtable) columnarPutFloat64(docID uint64, colIdx int, value float64) error {
	if m.columnarSchema == nil {
		return fmt.Errorf("columnar schema not set")
	}
	m.Lock()
	defer m.Unlock()

	row, isNew := m.getOrCreateColumnarRowLocked(docID)
	columnar.EncodeFloat64(row.values, m.columnarSchema.ColOffset(colIdx), value)
	row.live = true

	m.size += m.columnarRowCost(isNew, len(row.values))
	m.updateDirtyAt()
	return m.columnarWriteWALLocked(docID, row)
}

func (m *Memtable) columnarPutInt64(docID uint64, colIdx int, value int64) error {
	if m.columnarSchema == nil {
		return fmt.Errorf("columnar schema not set")
	}
	m.Lock()
	defer m.Unlock()

	row, isNew := m.getOrCreateColumnarRowLocked(docID)
	columnar.EncodeInt64(row.values, m.columnarSchema.ColOffset(colIdx), value)
	row.live = true

	m.size += m.columnarRowCost(isNew, len(row.values))
	m.updateDirtyAt()
	return m.columnarWriteWALLocked(docID, row)
}

func (m *Memtable) columnarDelete(docID uint64) error {
	if m.columnarSchema == nil {
		return fmt.Errorf("columnar schema not set")
	}
	m.Lock()
	defer m.Unlock()

	row, isNew := m.getOrCreateColumnarRowLocked(docID)
	row.live = false

	m.size += m.columnarRowCost(isNew, len(row.values))
	m.updateDirtyAt()
	return m.columnarWriteWALLocked(docID, row)
}

// columnarLookup returns (rawBits, found, isTombstone). found=true means the
// docID exists in this memtable; isTombstone=true means it was deleted.
func (m *Memtable) columnarLookup(docID uint64, colIdx int) (uint64, bool, bool) {
	m.RLock()
	defer m.RUnlock()

	row, ok := m.columnarRows[docID]
	if !ok {
		return 0, false, false
	}
	if !row.live {
		return 0, true, true
	}
	off := m.columnarSchema.ColOffset(colIdx)
	return binary.LittleEndian.Uint64(row.values[off:]), true, false
}

// columnarRowOverhead approximates the heap cost of one new memtable row
// beyond its value bytes: row struct (docID 8 + live 1 + padding ≈ 16),
// slice header (24), and the map entry (~16). Size-based flush thresholds
// depend on this not undercounting.
const columnarRowOverhead = 56

func (m *Memtable) getOrCreateColumnarRowLocked(docID uint64) (*columnarMemtableRow, bool) {
	row, exists := m.columnarRows[docID]
	if !exists {
		width := m.columnarSchema.RowWidth()
		if width < 0 {
			width = 0 // variable-width rows are sized on put
		}
		row = &columnarMemtableRow{
			docID:  docID,
			values: make([]byte, width),
			live:   true,
		}
		m.columnarRows[docID] = row
	}
	return row, !exists
}

// columnarPutPayload replaces the entire row payload — the write shape for
// vector and multi-vector schemas, where a put always carries the full
// value.
func (m *Memtable) columnarPutPayload(docID uint64, payload []byte) error {
	if m.columnarSchema == nil {
		return fmt.Errorf("columnar schema not set")
	}
	m.Lock()
	defer m.Unlock()

	row, isNew := m.getOrCreateColumnarRowLocked(docID)
	grew := len(payload) > cap(row.values)
	if !grew {
		row.values = row.values[:len(payload)]
	} else {
		row.values = make([]byte, len(payload))
	}
	copy(row.values, payload)
	row.live = true

	// Size accounting mirrors columnarRowCost: new rows charge their full
	// heap footprint; in-place updates charge the small delta unless the
	// payload outgrew the existing allocation (variable-width multi-vector
	// schemas), in which case the realloc is charged.
	switch {
	case isNew:
		m.size += columnarRowOverhead + uint64(len(payload))
	case grew:
		m.size += uint64(len(payload))
	default:
		m.size += 16
	}
	m.updateDirtyAt()
	return m.columnarWriteWALLocked(docID, row)
}

// columnarLookupPayload appends the row payload to dst. Returns
// (payload, found, isTombstone); the returned slice extends dst and is
// owned by the caller.
func (m *Memtable) columnarLookupPayload(docID uint64, dst []byte) ([]byte, bool, bool) {
	m.RLock()
	defer m.RUnlock()

	row, ok := m.columnarRows[docID]
	if !ok {
		return dst, false, false
	}
	if !row.live {
		return dst, true, true
	}
	return append(dst, row.values...), true, false
}

// columnarSortedRows returns a snapshot of all rows sorted by docID
// ascending. Callers must hold at least a read lock.
func (m *Memtable) columnarSortedRows() []*columnarMemtableRow {
	rows := make([]*columnarMemtableRow, 0, len(m.columnarRows))
	for _, r := range m.columnarRows {
		rows = append(rows, r)
	}
	slices.SortFunc(rows, func(a, b *columnarMemtableRow) int {
		if a.docID < b.docID {
			return -1
		}
		if a.docID > b.docID {
			return 1
		}
		return 0
	})
	return rows
}

// columnarScanRows visits every row (live and tombstone) in docID order.
// fn returning false stops the scan.
//
// Row state (live flag, value bytes) is materialized into copies UNDER the
// read lock: writers mutate row.values and row.live in place under the
// write lock, so the pointer-snapshot shortcut other strategies use is
// invalid here. The lock is released before the callbacks run — fn chains
// into caller-supplied aggregation code.
func (m *Memtable) columnarScanRows(colIdx int, fn func(docID uint64, live bool, bits uint64) bool) {
	type rowCopy struct {
		docID uint64
		bits  uint64
		live  bool
	}

	m.RLock()
	rows := m.columnarSortedRows()
	colOff := m.columnarSchema.ColOffset(colIdx)
	copies := make([]rowCopy, len(rows))
	for i, row := range rows {
		copies[i] = rowCopy{
			docID: row.docID,
			bits:  binary.LittleEndian.Uint64(row.values[colOff:]),
			live:  row.live,
		}
	}
	m.RUnlock()

	for i := range copies {
		if !fn(copies[i].docID, copies[i].live, copies[i].bits) {
			return
		}
	}
}

// columnarLookupFloats decodes the row payload into dst. Returns
// (floats, found, isTombstone).
func (m *Memtable) columnarLookupFloats(docID uint64, dst []float32) ([]float32, bool, bool) {
	m.RLock()
	defer m.RUnlock()

	row, ok := m.columnarRows[docID]
	if !ok {
		return dst, false, false
	}
	if !row.live {
		return dst, true, true
	}
	return BytesToFloat32s(row.values, dst), true, false
}

// columnarPutRow is used by WAL recovery to bulk-insert a row.
func (m *Memtable) columnarPutRow(docID uint64, live bool, values []byte) {
	row, isNew := m.getOrCreateColumnarRowLocked(docID)
	row.live = live
	if len(row.values) != len(values) {
		row.values = make([]byte, len(values))
	}
	copy(row.values, values)
	m.size += m.columnarRowCost(isNew, len(values))
}

// columnarRowCost returns the size-accounting charge for a write: new rows
// charge their full heap footprint (struct + slice header + map entry +
// value bytes) so the size-based flush threshold trips at realistic memory
// use; in-place updates charge only the small delta. rowLen is the row's
// value-byte length — passed in rather than derived from the schema because
// variable-width (multi-vector) schemas have no fixed RowWidth.
func (m *Memtable) columnarRowCost(isNew bool, rowLen int) uint64 {
	if isNew {
		return columnarRowOverhead + uint64(rowLen)
	}
	return 16
}

// columnarWriteWALLocked writes the current row state to the WAL. Must be
// called with the memtable lock held. Reuses m.columnarWALBuf where the
// schema has a fixed row width.
func (m *Memtable) columnarWriteWALLocked(docID uint64, row *columnarMemtableRow) error {
	buf := m.columnarWALBuf
	if buf == nil || len(buf) != 8+1+len(row.values) {
		buf = make([]byte, 8+1+len(row.values))
	}
	binary.LittleEndian.PutUint64(buf[0:], docID)
	if row.live {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	copy(buf[9:], row.values)
	return m.commitlog.writeEntry(CommitTypeColumnar, buf)
}
