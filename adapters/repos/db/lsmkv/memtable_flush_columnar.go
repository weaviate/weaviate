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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// flushDataColumnar writes the columnar memtable as a block-structured
// segment (see the columnar package doc for the layout). The directory is
// written at IndexStart, mirroring how other strategies place their key
// index, so the shared segment machinery (checksums, previews) works
// unchanged.
func (m *Memtable) flushDataColumnar(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	m.RLock()
	rows := m.columnarSortedRows()
	schema := m.columnarSchema
	m.RUnlock()

	header := &segmentindex.Header{
		Level:            0,
		Version:          segmentindex.ChooseHeaderVersion(m.enableChecksumValidation),
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyColumnar,
	}

	ch := &columnar.Header{Schema: *schema}
	chBytes := ch.MarshalBinary()

	// All sizes are computable up front: blocks hold fixed-width data.
	dataSize := len(chBytes)
	for blockStart := 0; blockStart < len(rows); blockStart += columnar.BlockSize {
		n := min(len(rows)-blockStart, columnar.BlockSize)
		dataSize += n * (8 + 1 + schema.RowWidth())
	}
	header.IndexStart = uint64(segmentindex.HeaderSize + dataSize)

	if _, err := f.WriteHeader(header); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	w := f.BodyWriter()
	if _, err := w.Write(chBytes); err != nil {
		return nil, fmt.Errorf("write columnar header: %w", err)
	}

	var entries []columnar.DirectoryEntry
	bw := columnar.NewBlockWriter(schema, uint64(segmentindex.HeaderSize+len(chBytes)),
		func(block []byte, entry columnar.DirectoryEntry) error {
			if _, err := w.Write(block); err != nil {
				return err
			}
			entries = append(entries, entry)
			return nil
		})

	for _, row := range rows {
		if err := bw.Append(row.docID, row.live, row.values); err != nil {
			return nil, fmt.Errorf("append row: %w", err)
		}
	}
	if err := bw.Flush(); err != nil {
		return nil, fmt.Errorf("flush block: %w", err)
	}

	// The directory is written through the body writer; header.IndexStart
	// marks where blocks end and the directory begins, which is all the
	// reader needs.
	dir := columnar.MarshalDirectory(entries, len(schema.Columns))
	if _, err := w.Write(dir); err != nil {
		return nil, fmt.Errorf("write directory: %w", err)
	}

	return make([]segmentindex.Key, 0), nil
}
