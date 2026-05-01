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
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// compactorColumnar merges two columnar segments using a merge-join on
// sorted docIDs. The right (newer) segment wins on conflict.
type compactorColumnar struct {
	left  *segment
	right *segment

	currentLevel      uint16
	cleanupTombstones bool

	enableChecksumValidation bool

	w    io.WriteSeeker
	bufw *bufio.Writer
}

func newCompactorColumnar(w io.WriteSeeker, left, right *segment,
	level uint16, cleanupTombstones bool, enableChecksumValidation bool,
) *compactorColumnar {
	return &compactorColumnar{
		left:                     left,
		right:                    right,
		currentLevel:             level,
		cleanupTombstones:        cleanupTombstones,
		enableChecksumValidation: enableChecksumValidation,
		w:                        w,
		bufw:                     bufio.NewWriter(w),
	}
}

func (c *compactorColumnar) do() error {
	ld := c.left.columnarData
	rd := c.right.columnarData

	if ld == nil || rd == nil {
		return fmt.Errorf("compactorColumnar: segments missing columnar data")
	}

	lContents := c.left.contents
	rContents := c.right.contents

	// Merge-join: both sides are sorted by docID ascending.
	merged := make([]*columnarMemtableRow, 0, ld.numRows+rd.numRows)
	li, ri := 0, 0
	lMax, rMax := int(ld.numRows), int(rd.numRows)

	for li < lMax && ri < rMax {
		lDocID := ld.docIDAt(lContents, li)
		rDocID := rd.docIDAt(rContents, ri)

		switch {
		case lDocID < rDocID:
			row := ld.columnarRowAt(lContents, li)
			if !c.cleanupTombstones || row.valid {
				merged = append(merged, row)
			}
			li++
		case lDocID > rDocID:
			row := rd.columnarRowAt(rContents, ri)
			if !c.cleanupTombstones || row.valid {
				merged = append(merged, row)
			}
			ri++
		default: // equal — right (newer) wins
			row := rd.columnarRowAt(rContents, ri)
			if !c.cleanupTombstones || row.valid {
				merged = append(merged, row)
			}
			li++
			ri++
		}
	}

	for ; li < lMax; li++ {
		row := ld.columnarRowAt(lContents, li)
		if !c.cleanupTombstones || row.valid {
			merged = append(merged, row)
		}
	}
	for ; ri < rMax; ri++ {
		row := rd.columnarRowAt(rContents, ri)
		if !c.cleanupTombstones || row.valid {
			merged = append(merged, row)
		}
	}

	return c.writeSegment(merged, &ld.schema)
}

func (c *compactorColumnar) writeSegment(rows []*columnarMemtableRow, schema *columnar.Schema) error {
	numRows := uint64(len(rows))
	rowWidth := schema.RowWidth()

	ch := &columnar.ColumnarHeader{
		NumRows: numRows,
		Schema:  *schema,
	}
	chBytes := ch.MarshalBinary()

	dataSize := len(chBytes) +
		int(numRows)*8 +
		int(numRows)*1 +
		int(numRows)*rowWidth

	header := &segmentindex.Header{
		IndexStart:       uint64(dataSize + segmentindex.HeaderSize),
		Level:            c.currentLevel,
		Version:          segmentindex.ChooseHeaderVersion(c.enableChecksumValidation),
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyColumnar,
	}

	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(c.bufw),
		segmentindex.WithChecksumsDisabled(!c.enableChecksumValidation),
	)

	if _, err := segmentFile.WriteHeader(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	w := segmentFile.BodyWriter()

	// Columnar header
	if _, err := w.Write(chBytes); err != nil {
		return fmt.Errorf("write columnar header: %w", err)
	}

	// DocID array
	buf8 := make([]byte, 8)
	for _, row := range rows {
		binary.LittleEndian.PutUint64(buf8, row.docID)
		if _, err := w.Write(buf8); err != nil {
			return fmt.Errorf("write docID: %w", err)
		}
	}

	// Valid array
	for _, row := range rows {
		v := byte(0)
		if row.valid {
			v = 1
		}
		if _, err := w.Write([]byte{v}); err != nil {
			return fmt.Errorf("write valid: %w", err)
		}
	}

	// Value columns
	for colIdx, col := range schema.Columns {
		colOff := schema.ColOffset(colIdx)
		colWidth := int(col.Type.Width())
		for _, row := range rows {
			if _, err := w.Write(row.values[colOff : colOff+colWidth]); err != nil {
				return fmt.Errorf("write column %d: %w", colIdx, err)
			}
		}
	}

	// Flush buffered data before writing checksum
	if err := c.bufw.Flush(); err != nil {
		return fmt.Errorf("flush buffered: %w", err)
	}

	if _, err := segmentFile.WriteChecksum(); err != nil {
		return fmt.Errorf("write checksum: %w", err)
	}

	return nil
}
