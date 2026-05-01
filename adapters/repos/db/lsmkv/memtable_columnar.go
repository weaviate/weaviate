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
	"math"
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
)

// columnarMemtableRow holds one document's columnar values in memory.
type columnarMemtableRow struct {
	docID  uint64
	valid  bool   // false = tombstone
	values []byte // packed column values, same layout as schema
}

func (m *Memtable) initColumnar(schema *columnar.Schema) {
	m.columnarSchema = schema
	m.columnarRows = make(map[uint64]*columnarMemtableRow)
	m.columnarWALBuf = make([]byte, 8+1+schema.RowWidth()) // reusable WAL buffer
}

func (m *Memtable) columnarPutFloat32(docID uint64, colIdx int, value float32) error {
	if m.columnarSchema == nil {
		return fmt.Errorf("columnar schema not set")
	}
	m.Lock()
	defer m.Unlock()

	row := m.getOrCreateRow(docID)
	off := m.columnarSchema.ColOffset(colIdx)
	columnar.EncodeFloat32(row.values, off, value)
	row.valid = true

	m.size += 12
	m.updateDirtyAt()
	return m.columnarWriteWALLocked(docID, row)
}

func (m *Memtable) columnarPutInt64(docID uint64, colIdx int, value int64) error {
	if m.columnarSchema == nil {
		return fmt.Errorf("columnar schema not set")
	}
	m.Lock()
	defer m.Unlock()

	row := m.getOrCreateRow(docID)
	off := m.columnarSchema.ColOffset(colIdx)
	columnar.EncodeInt64(row.values, off, value)
	row.valid = true

	m.size += 16
	m.updateDirtyAt()
	return m.columnarWriteWALLocked(docID, row)
}

// columnarPutRowValues writes an entire row of pre-encoded values in a single
// lock acquisition and single WAL entry.
func (m *Memtable) columnarPutRowValues(docID uint64, values []byte) error {
	if m.columnarSchema == nil {
		return fmt.Errorf("columnar schema not set")
	}
	m.Lock()
	defer m.Unlock()

	row := m.getOrCreateRow(docID)
	copy(row.values, values)
	row.valid = true

	m.size += uint64(8 + len(values))
	m.updateDirtyAt()
	return m.columnarWriteWALLocked(docID, row)
}

func (m *Memtable) columnarDelete(docID uint64) error {
	m.Lock()
	defer m.Unlock()

	row := m.getOrCreateRow(docID)
	row.valid = false

	m.size += 9
	m.updateDirtyAt()
	return m.columnarWriteWALLocked(docID, row)
}

// columnarLookupFloat32 returns (value, found, isTombstone).
// found=true means the docID exists in this memtable.
// isTombstone=true means it was deleted.
func (m *Memtable) columnarLookupFloat32(docID uint64, colIdx int) (float32, bool, bool) {
	m.RLock()
	defer m.RUnlock()

	row, ok := m.columnarRows[docID]
	if !ok {
		return 0, false, false
	}
	if !row.valid {
		return 0, true, true
	}
	off := m.columnarSchema.ColOffset(colIdx)
	return columnar.DecodeFloat32(row.values, off), true, false
}

// columnarLookupInt64 returns (value, found, isTombstone).
func (m *Memtable) columnarLookupInt64(docID uint64, colIdx int) (int64, bool, bool) {
	m.RLock()
	defer m.RUnlock()

	row, ok := m.columnarRows[docID]
	if !ok {
		return 0, false, false
	}
	if !row.valid {
		return 0, true, true
	}
	off := m.columnarSchema.ColOffset(colIdx)
	return columnar.DecodeInt64(row.values, off), true, false
}

func (m *Memtable) getOrCreateRow(docID uint64) *columnarMemtableRow {
	row, exists := m.columnarRows[docID]
	if !exists {
		row = &columnarMemtableRow{
			docID:  docID,
			values: make([]byte, m.columnarSchema.RowWidth()),
			valid:  true,
		}
		m.columnarRows[docID] = row
	}
	return row
}

// columnarSortedRows returns rows sorted by docID ascending.
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

// columnarPutRow is used by WAL recovery to bulk-insert a row.
func (m *Memtable) columnarPutRow(docID uint64, valid bool, values []byte) {
	row := m.getOrCreateRow(docID)
	row.valid = valid
	copy(row.values, values)
}

// columnarWriteWALLocked writes the current row state to the WAL.
// Must be called with the memtable lock held.
// Reuses m.columnarWALBuf to avoid per-call allocation.
func (m *Memtable) columnarWriteWALLocked(docID uint64, row *columnarMemtableRow) error {
	buf := m.columnarWALBuf
	binary.LittleEndian.PutUint64(buf[0:], docID)
	if row.valid {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	copy(buf[9:], row.values)
	return m.commitlog.writeEntry(CommitTypeColumnar, buf)
}

// NaN sentinel for float32 columns that haven't been set.
var _ = math.Float32frombits(0x7FC00001)
