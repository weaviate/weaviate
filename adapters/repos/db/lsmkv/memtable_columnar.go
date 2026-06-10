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
	m.columnarWALBuf = make([]byte, 8+1+schema.RowWidth()) // reusable WAL buffer
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

	m.size += m.columnarRowCost(isNew)
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

	m.size += m.columnarRowCost(isNew)
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

	m.size += m.columnarRowCost(isNew)
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
		row = &columnarMemtableRow{
			docID:  docID,
			values: make([]byte, m.columnarSchema.RowWidth()),
			live:   true,
		}
		m.columnarRows[docID] = row
	}
	return row, !exists
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

// columnarPutRow is used by WAL recovery to bulk-insert a row.
func (m *Memtable) columnarPutRow(docID uint64, live bool, values []byte) {
	row, isNew := m.getOrCreateColumnarRowLocked(docID)
	row.live = live
	copy(row.values, values)
	m.size += m.columnarRowCost(isNew)
}

// columnarRowCost returns the size-accounting charge for a write: new rows
// charge their full heap footprint (struct + slice header + map entry +
// value bytes) so the size-based flush threshold trips at realistic memory
// use; in-place updates charge only the small delta.
func (m *Memtable) columnarRowCost(isNew bool) uint64 {
	if isNew {
		return columnarRowOverhead + uint64(m.columnarSchema.RowWidth())
	}
	return 16
}

// columnarWriteWALLocked writes the current row state to the WAL. Must be
// called with the memtable lock held. Reuses m.columnarWALBuf to avoid
// per-call allocation.
func (m *Memtable) columnarWriteWALLocked(docID uint64, row *columnarMemtableRow) error {
	buf := m.columnarWALBuf
	binary.LittleEndian.PutUint64(buf[0:], docID)
	if row.live {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	copy(buf[9:], row.values)
	return m.commitlog.writeEntry(CommitTypeColumnar, buf)
}
