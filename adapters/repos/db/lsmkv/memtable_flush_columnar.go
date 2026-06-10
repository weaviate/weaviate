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
// segment (see the columnar package doc for the layout). Blocks are built
// first (their sizes depend on variable-width payloads and alignment
// padding), then the header with the final IndexStart, then blocks and
// directory through the body writer.
func (m *Memtable) flushDataColumnar(f *segmentindex.SegmentFile) ([]segmentindex.Key, error) {
	m.RLock()
	rows := m.columnarSortedRows()
	schema := m.columnarSchema
	m.RUnlock()

	ch := &columnar.Header{Schema: *schema}
	chBytes := ch.MarshalBinary()
	blocksStart := uint64(segmentindex.HeaderSize + len(chBytes))

	// Blocks are built before the header: their byte sizes are the block
	// writer's to decide (per-column encodings, future padding), so the
	// header's IndexStart derives from what was actually emitted instead
	// of duplicating the layout arithmetic here. Precomputing it from
	// RowWidth silently breaks on the first non-raw encoding.
	var blocks [][]byte
	var entries []columnar.DirectoryEntry
	blocksSize := uint64(0)
	bw := columnar.NewBlockWriter(schema, blocksStart,
		func(block []byte, entry columnar.DirectoryEntry) error {
			blocks = append(blocks, block)
			entries = append(entries, entry)
			blocksSize += uint64(len(block))
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

	if got := bw.Offset(); got != blocksStart+blocksSize {
		return nil, fmt.Errorf("columnar flush: block writer offset %d != emitted size %d",
			got, blocksStart+blocksSize)
	}

	header := &segmentindex.Header{
		Level:            0,
		Version:          segmentindex.ChooseHeaderVersion(m.enableChecksumValidation),
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyColumnar,
		IndexStart:       blocksStart + blocksSize,
	}
	if _, err := f.WriteHeader(header); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	w := f.BodyWriter()
	if _, err := w.Write(chBytes); err != nil {
		return nil, fmt.Errorf("write columnar header: %w", err)
	}
	for _, block := range blocks {
		if _, err := w.Write(block); err != nil {
			return nil, fmt.Errorf("write block: %w", err)
		}
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
