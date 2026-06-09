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
func (b *Bucket) ColumnarLookupFloat64(docID uint64, colIdx int) (float64, bool) {
	bits, ok := b.ColumnarLookupBits(docID, colIdx)
	return math.Float64frombits(bits), ok
}

// ColumnarLookupInt64 reads an int64 value by docID, traversing memtables
// and segments newest-first.
func (b *Bucket) ColumnarLookupInt64(docID uint64, colIdx int) (int64, bool) {
	bits, ok := b.ColumnarLookupBits(docID, colIdx)
	return int64(bits), ok
}

// ColumnarLookupBits is the type-agnostic lookup; interpret the result per
// the column's type.
func (b *Bucket) ColumnarLookupBits(docID uint64, colIdx int) (uint64, bool) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	if bits, found, tomb := b.active.columnarLookup(docID, colIdx); found {
		return bits, !tomb
	}
	if b.flushing != nil {
		if bits, found, tomb := b.flushing.columnarLookup(docID, colIdx); found {
			return bits, !tomb
		}
	}
	bits, found, tomb := b.disk.columnarLookup(docID, colIdx)
	return bits, found && !tomb
}

// columnarLookup on SegmentGroup iterates segments newest→oldest.
func (sg *SegmentGroup) columnarLookup(docID uint64, colIdx int) (uint64, bool, bool) {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	for i := len(sg.segments) - 1; i >= 0; i-- {
		seg, ok := sg.segments[i].(*segment)
		if !ok || seg.strategy != segmentindex.StrategyColumnar || seg.columnarData == nil {
			// Columnar buckets never enable lazy segment loading, so every
			// element must be a fully loaded columnar *segment.
			continue
		}
		bits, found, tomb := seg.columnarData.lookup(seg.contents, docID, colIdx)
		if found {
			return bits, true, tomb
		}
	}
	return 0, false, false
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
func (b *Bucket) ColumnarScan(colIdx int, allow *sroar.Bitmap,
	visit func(docID uint64, bits uint64) bool,
) error {
	if err := CheckExpectedStrategy(b.strategy, StrategyColumnar); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

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

	scanMemtable(b.active)
	if !cont {
		return nil
	}
	if b.flushing != nil {
		scanMemtable(b.flushing)
		if !cont {
			return nil
		}
	}

	return b.disk.columnarScan(colIdx, allow, seen, &cont, visit)
}

func (sg *SegmentGroup) columnarScan(colIdx int, allow, seen *sroar.Bitmap,
	cont *bool, visit func(docID uint64, bits uint64) bool,
) error {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	for i := len(sg.segments) - 1; i >= 0 && *cont; i-- {
		seg, ok := sg.segments[i].(*segment)
		if !ok {
			return fmt.Errorf("columnar scan: unexpected segment type at position %d", i)
		}
		if seg.strategy != segmentindex.StrategyColumnar || seg.columnarData == nil {
			return fmt.Errorf("columnar scan: non-columnar segment at position %d", i)
		}

		err := seg.columnarData.scanBlocks(seg.contents,
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
