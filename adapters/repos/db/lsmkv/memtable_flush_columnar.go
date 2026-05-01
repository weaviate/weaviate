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

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// flushDataColumnar writes the columnar memtable data as a contiguous
// column-major segment. The on-disk layout is:
//
//	[LSM Header]
//	[ColumnarHeader]
//	[docID array   — uint64 × numRows, sorted ascending]
//	[valid array   — uint8  × numRows]
//	[column 0 data — width  × numRows]
//	[column 1 data — width  × numRows]
//	...
//
// No traditional key index is written — the sorted docID array serves as
// the searchable index.
func (m *Memtable) flushDataColumnar(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	m.RLock()
	rows := m.columnarSortedRows()
	schema := m.columnarSchema
	m.RUnlock()

	numRows := uint64(len(rows))
	rowWidth := schema.RowWidth()

	// Build the columnar header
	ch := &columnar.ColumnarHeader{
		NumRows: numRows,
		Schema:  *schema,
	}
	chBytes := ch.MarshalBinary()

	// Total data size: columnarHeader + docIDs + valid + columns
	dataSize := len(chBytes) +
		int(numRows)*8 + // docID array
		int(numRows)*1 + // valid array
		int(numRows)*rowWidth // all value columns

	header := &segmentindex.Header{
		IndexStart:       uint64(dataSize + segmentindex.HeaderSize),
		Level:            0,
		Version:          segmentindex.ChooseHeaderVersion(m.enableChecksumValidation),
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyColumnar,
	}

	if _, err := f.WriteHeader(header); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	w := f.BodyWriter()

	// Write columnar header
	if _, err := w.Write(chBytes); err != nil {
		return nil, fmt.Errorf("write columnar header: %w", err)
	}

	// Write docID array (column-major: all docIDs contiguously)
	buf8 := make([]byte, 8)
	for _, row := range rows {
		binary.LittleEndian.PutUint64(buf8, row.docID)
		if _, err := w.Write(buf8); err != nil {
			return nil, fmt.Errorf("write docID: %w", err)
		}
	}

	// Write valid array
	for _, row := range rows {
		v := byte(0)
		if row.valid {
			v = 1
		}
		if _, err := w.Write([]byte{v}); err != nil {
			return nil, fmt.Errorf("write valid: %w", err)
		}
	}

	// Write each value column
	for colIdx, col := range schema.Columns {
		colOff := schema.ColOffset(colIdx)
		colWidth := int(col.Type.Width())
		for _, row := range rows {
			if _, err := w.Write(row.values[colOff : colOff+colWidth]); err != nil {
				return nil, fmt.Errorf("write column %d: %w", colIdx, err)
			}
		}
	}

	return make([]segmentindex.Key, 0), nil
}
