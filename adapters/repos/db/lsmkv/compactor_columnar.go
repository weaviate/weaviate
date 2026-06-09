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
	"context"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/compactor"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// compactorColumnar merges two columnar segments via a streaming merge-join
// on sorted docIDs: the newer (right) segment wins on conflict. At most one
// input block per side plus one output block are held in memory, so memory
// use is independent of segment size.
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

// columnarRowCursor iterates one segment's live+tombstone rows in docID
// order, loading one block at a time.
type columnarRowCursor struct {
	data     *columnarSegmentData
	contents []byte

	blockIdx int
	rowIdx   int
	br       *columnar.BlockReader

	valid bool
	docID uint64
}

func newColumnarRowCursor(data *columnarSegmentData, contents []byte) (*columnarRowCursor, error) {
	c := &columnarRowCursor{data: data, contents: contents, blockIdx: -1}
	return c, c.advanceBlockIfNeeded()
}

func (c *columnarRowCursor) advanceBlockIfNeeded() error {
	for c.br == nil || c.rowIdx >= c.br.Rows() {
		c.blockIdx++
		if c.blockIdx >= len(c.data.entries) {
			c.valid = false
			return nil
		}
		br, err := columnar.NewBlockReader(&c.data.schema, &c.data.entries[c.blockIdx], c.contents)
		if err != nil {
			return err
		}
		c.br = br
		c.rowIdx = 0
	}
	c.valid = true
	c.docID = c.br.DocIDAt(c.rowIdx)
	return nil
}

func (c *columnarRowCursor) next() error {
	c.rowIdx++
	return c.advanceBlockIfNeeded()
}

func (c *columnarRowCursor) isLive() bool { return c.br.IsLive(c.rowIdx) }

func (c *columnarRowCursor) rowValues(dst []byte) []byte {
	return c.br.RowValues(dst, c.rowIdx)
}

func (c *compactorColumnar) do(ctx context.Context) error {
	ld := c.left.columnarData
	rd := c.right.columnarData
	if ld == nil || rd == nil {
		return fmt.Errorf("compactorColumnar: segments missing columnar data")
	}

	// Dummy header now, real header via seek-back at the end — row counts
	// are unknown until the merge completes.
	if _, err := c.bufw.Write(make([]byte, segmentindex.HeaderSize)); err != nil {
		return fmt.Errorf("write empty header: %w", err)
	}

	segmentFile := segmentindex.NewSegmentFile(
		segmentindex.WithBufferedWriter(c.bufw),
		segmentindex.WithChecksumsDisabled(!c.enableChecksumValidation),
	)
	w := segmentFile.BodyWriter()

	ch := &columnar.Header{Schema: ld.schema}
	chBytes := ch.MarshalBinary()
	if _, err := w.Write(chBytes); err != nil {
		return fmt.Errorf("write columnar header: %w", err)
	}

	var entries []columnar.DirectoryEntry
	bw := columnar.NewBlockWriter(&ld.schema, uint64(segmentindex.HeaderSize+len(chBytes)),
		func(block []byte, entry columnar.DirectoryEntry) error {
			if _, err := w.Write(block); err != nil {
				return err
			}
			entries = append(entries, entry)
			return nil
		})

	if err := c.merge(ctx, ld, rd, bw); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flush final block: %w", err)
	}

	dataEnd := bw.Offset()

	dir := columnar.MarshalDirectory(entries, len(ld.schema.Columns))
	if _, err := w.Write(dir); err != nil {
		return fmt.Errorf("write directory: %w", err)
	}

	// flush buffered, so we can safely seek on the underlying writer
	if err := c.bufw.Flush(); err != nil {
		return fmt.Errorf("flush buffered: %w", err)
	}

	version := segmentindex.ChooseHeaderVersion(c.enableChecksumValidation)
	if err := compactor.WriteHeader(nil, c.w, c.bufw, segmentFile, c.currentLevel,
		version, 0, dataEnd, segmentindex.StrategyColumnar); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if _, err := segmentFile.WriteChecksum(); err != nil {
		return fmt.Errorf("write checksum: %w", err)
	}

	return nil
}

func (c *compactorColumnar) merge(ctx context.Context,
	ld, rd *columnarSegmentData, bw *columnar.BlockWriter,
) error {
	lc, err := newColumnarRowCursor(ld, c.left.contents)
	if err != nil {
		return fmt.Errorf("left cursor: %w", err)
	}
	rc, err := newColumnarRowCursor(rd, c.right.contents)
	if err != nil {
		return fmt.Errorf("right cursor: %w", err)
	}

	rowBuf := make([]byte, 0, ld.schema.RowWidth())
	rowsProcessed := 0

	emit := func(cur *columnarRowCursor) error {
		rowsProcessed++
		if rowsProcessed%compactor.AbortCheckEveryN == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		live := cur.isLive()
		if c.cleanupTombstones && !live {
			return nil
		}
		rowBuf = cur.rowValues(rowBuf[:0])
		return bw.Append(cur.docID, live, rowBuf)
	}

	for lc.valid && rc.valid {
		switch {
		case lc.docID < rc.docID:
			if err := emit(lc); err != nil {
				return err
			}
			if err := lc.next(); err != nil {
				return err
			}
		case lc.docID > rc.docID:
			if err := emit(rc); err != nil {
				return err
			}
			if err := rc.next(); err != nil {
				return err
			}
		default: // equal — right (newer) wins
			if err := emit(rc); err != nil {
				return err
			}
			if err := lc.next(); err != nil {
				return err
			}
			if err := rc.next(); err != nil {
				return err
			}
		}
	}
	for lc.valid {
		if err := emit(lc); err != nil {
			return err
		}
		if err := lc.next(); err != nil {
			return err
		}
	}
	for rc.valid {
		if err := emit(rc); err != nil {
			return err
		}
		if err := rc.next(); err != nil {
			return err
		}
	}
	return nil
}
