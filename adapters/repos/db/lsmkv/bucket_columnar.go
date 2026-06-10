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
	"math"
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// WithColumnarSchema sets the column schema for a columnar bucket.
func WithColumnarSchema(schema *columnar.Schema) BucketOption {
	return func(b *Bucket) error {
		b.columnarSchema = schema
		return nil
	}
}

// ColumnarSchema returns the column schema of a columnar bucket, or nil for
// buckets without one (non-columnar strategies, or a columnar bucket created
// without WithColumnarSchema). The schema is fixed at bucket creation, so the
// returned pointer is safe to read without holding any bucket locks.
func (b *Bucket) ColumnarSchema() *columnar.Schema {
	return b.columnarSchema
}

func (b *Bucket) columnarWriteOp(op string, write func(memtable) error) (err error) {
	start := time.Now()
	b.metrics.IncBucketWriteOpCount(op)
	b.metrics.IncBucketWriteOpOngoing(op)
	defer func() {
		b.metrics.DecBucketWriteOpOngoing(op)
		if err != nil {
			b.metrics.IncBucketWriteOpFailureCount(op)
			return
		}
		b.metrics.ObserveBucketWriteOpDuration(op, time.Since(start))
	}()

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	if err := CheckExpectedStrategy(b.strategy, StrategyColumnar); err != nil {
		return err
	}

	return write(active)
}

// ColumnarPutFloat64 writes a float64 value for a given docID and column.
func (b *Bucket) ColumnarPutFloat64(docID uint64, colIdx int, value float64) error {
	return b.columnarWriteOp("columnar_put", func(m memtable) error {
		return m.columnarPutFloat64(docID, colIdx, value)
	})
}

// ColumnarPutInt64 writes an int64 value for a given docID and column.
func (b *Bucket) ColumnarPutInt64(docID uint64, colIdx int, value int64) error {
	return b.columnarWriteOp("columnar_put", func(m memtable) error {
		return m.columnarPutInt64(docID, colIdx, value)
	})
}

// ColumnarDelete marks a docID as deleted.
func (b *Bucket) ColumnarDelete(docID uint64) error {
	return b.columnarWriteOp("columnar_delete", func(m memtable) error {
		return m.columnarDelete(docID)
	})
}

// ColumnarLookupFloat64 reads a float64 value by docID, traversing memtables
// and segments newest-first.
func (b *Bucket) ColumnarLookupFloat64(docID uint64, colIdx int) (float64, bool, error) {
	bits, ok, err := b.ColumnarLookupBits(docID, colIdx)
	return math.Float64frombits(bits), ok, err
}

// ColumnarLookupInt64 reads an int64 value by docID, traversing memtables
// and segments newest-first.
func (b *Bucket) ColumnarLookupInt64(docID uint64, colIdx int) (int64, bool, error) {
	bits, ok, err := b.ColumnarLookupBits(docID, colIdx)
	return int64(bits), ok, err
}

// ColumnarLookupBits is the type-agnostic lookup; interpret the result per
// the column's type. The read runs on a refcounted consistent view of the
// bucket, so it never blocks flush publishing or compaction segment-swaps.
// An error means the segment list contained a non-columnar segment — an
// invariant violation, not a "not found".
func (b *Bucket) ColumnarLookupBits(docID uint64, colIdx int) (uint64, bool, error) {
	if err := CheckExpectedStrategy(b.strategy, StrategyColumnar); err != nil {
		return 0, false, err
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	if bits, found, tomb := view.Active.columnarLookup(docID, colIdx); found {
		return bits, !tomb, nil
	}
	if view.Flushing != nil {
		if bits, found, tomb := view.Flushing.columnarLookup(docID, colIdx); found {
			return bits, !tomb, nil
		}
	}
	bits, found, tomb, err := columnarLookupSegments(view.Disk, docID, colIdx)
	if err != nil {
		return 0, false, err
	}
	return bits, found && !tomb, nil
}

// asColumnarSegment asserts that a segment-group element is a fully loaded
// columnar *segment. Columnar buckets reject lazy segment loading at
// construction (see NewBucket), so any other element type is an invariant
// violation that must surface as an error rather than a silent "not found".
func asColumnarSegment(s Segment, pos int) (*segment, error) {
	seg, ok := s.(*segment)
	if !ok {
		return nil, fmt.Errorf("unexpected segment type %T at position %d "+
			"(columnar buckets require fully loaded segments)", s, pos)
	}
	if seg.strategy != segmentindex.StrategyColumnar || seg.columnarData == nil {
		return nil, fmt.Errorf("non-columnar segment at position %d", pos)
	}
	return seg, nil
}

// columnarLookupSegments iterates a consistent-view segment list
// newest→oldest. Returns (bits, found, isTombstone, err).
func columnarLookupSegments(segments []Segment, docID uint64, colIdx int) (uint64, bool, bool, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		seg, err := asColumnarSegment(segments[i], i)
		if err != nil {
			return 0, false, false, fmt.Errorf("columnar lookup: %w", err)
		}
		bits, found, tomb := seg.columnarData.lookup(seg.contents, docID, colIdx)
		if found {
			return bits, true, tomb, nil
		}
	}
	return 0, false, false, nil
}

// ColumnarScan visits the newest live value of every docID in the bucket for
// the given column, in no particular global order. If allow is non-nil, only
// docIDs contained in it are visited. visit receives the raw 8-byte payload
// as uint64 bits; interpret per the column type (int64 cast or
// math.Float64frombits). Returning false stops the scan.
//
// Newest-wins semantics match point lookups: a docID present in a newer
// source (active memtable, then flushing memtable, then segments newest to
// oldest) shadows all older occurrences, and tombstones hide older values.
//
// The scan runs on a refcounted consistent view of the bucket: memtable and
// segment references are taken once up front and released when the scan
// finishes, so a slow visit callback never blocks flush publishing or
// compaction segment-swaps. Writes that land after the view was taken are
// not observed.
func (b *Bucket) ColumnarScan(colIdx int, allow *sroar.Bitmap,
	visit func(docID uint64, bits uint64) bool,
) error {
	if err := CheckExpectedStrategy(b.strategy, StrategyColumnar); err != nil {
		return err
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	seen := sroar.NewBitmap()
	cont := true

	scanMemtable := func(m memtable) {
		m.columnarScanRows(colIdx, func(docID uint64, live bool, bits uint64) bool {
			if seen.Contains(docID) {
				return true
			}
			seen.Set(docID)
			if !live {
				return true
			}
			if allow != nil && !allow.Contains(docID) {
				return true
			}
			cont = visit(docID, bits)
			return cont
		})
	}

	scanMemtable(view.Active)
	if !cont {
		return nil
	}
	if view.Flushing != nil {
		scanMemtable(view.Flushing)
		if !cont {
			return nil
		}
	}

	return columnarScanSegments(view.Disk, colIdx, allow, seen, &cont, visit)
}

// columnarScanSegments iterates a consistent-view segment list newest→oldest.
func columnarScanSegments(segments []Segment, colIdx int, allow, seen *sroar.Bitmap,
	cont *bool, visit func(docID uint64, bits uint64) bool,
) error {
	for i := len(segments) - 1; i >= 0 && *cont; i-- {
		seg, err := asColumnarSegment(segments[i], i)
		if err != nil {
			return fmt.Errorf("columnar scan: %w", err)
		}

		err = seg.columnarData.scanBlocks(seg.contents,
			func(entry *columnar.DirectoryEntry, br *columnar.BlockReader) (bool, error) {
				if allow != nil &&
					(allow.Maximum() < entry.StartDocID || allow.Minimum() > entry.EndDocID) {
					return true, nil
				}
				for row := 0; row < br.Rows(); row++ {
					docID := br.DocIDAt(row)
					if seen.Contains(docID) {
						continue
					}
					seen.Set(docID)
					if !br.IsLive(row) {
						continue
					}
					if allow != nil && !allow.Contains(docID) {
						continue
					}
					if !visit(docID, br.ValueBitsAt(colIdx, row)) {
						*cont = false
						return false, nil
					}
				}
				return true, nil
			})
		if err != nil {
			return err
		}
	}
	return nil
}
