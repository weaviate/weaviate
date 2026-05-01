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
	"time"

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

// ColumnarPutFloat32 writes a float32 value for a given docID and column.
func (b *Bucket) ColumnarPutFloat32(docID uint64, colIdx int, value float32) (err error) {
	start := time.Now()
	b.metrics.IncBucketWriteOpCount("columnar_put")
	b.metrics.IncBucketWriteOpOngoing("columnar_put")
	defer func() {
		b.metrics.DecBucketWriteOpOngoing("columnar_put")
		if err != nil {
			b.metrics.IncBucketWriteOpFailureCount("columnar_put")
			return
		}
		b.metrics.ObserveBucketWriteOpDuration("columnar_put", time.Since(start))
	}()

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	if err := CheckExpectedStrategy(b.strategy, StrategyColumnar); err != nil {
		return err
	}

	return active.columnarPutFloat32(docID, colIdx, value)
}

// ColumnarPutInt64 writes an int64 value for a given docID and column.
func (b *Bucket) ColumnarPutInt64(docID uint64, colIdx int, value int64) (err error) {
	start := time.Now()
	b.metrics.IncBucketWriteOpCount("columnar_put")
	b.metrics.IncBucketWriteOpOngoing("columnar_put")
	defer func() {
		b.metrics.DecBucketWriteOpOngoing("columnar_put")
		if err != nil {
			b.metrics.IncBucketWriteOpFailureCount("columnar_put")
			return
		}
		b.metrics.ObserveBucketWriteOpDuration("columnar_put", time.Since(start))
	}()

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	if err := CheckExpectedStrategy(b.strategy, StrategyColumnar); err != nil {
		return err
	}

	return active.columnarPutInt64(docID, colIdx, value)
}

// ColumnarDelete marks a docID as deleted.
func (b *Bucket) ColumnarDelete(docID uint64) (err error) {
	start := time.Now()
	b.metrics.IncBucketWriteOpCount("columnar_delete")
	b.metrics.IncBucketWriteOpOngoing("columnar_delete")
	defer func() {
		b.metrics.DecBucketWriteOpOngoing("columnar_delete")
		if err != nil {
			b.metrics.IncBucketWriteOpFailureCount("columnar_delete")
			return
		}
		b.metrics.ObserveBucketWriteOpDuration("columnar_delete", time.Since(start))
	}()

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	if err := CheckExpectedStrategy(b.strategy, StrategyColumnar); err != nil {
		return err
	}

	return active.columnarDelete(docID)
}

// ColumnarPutRow writes an entire pre-encoded row of values in a single
// operation — one lock acquisition, one WAL entry.
func (b *Bucket) ColumnarPutRow(docID uint64, values []byte) (err error) {
	start := time.Now()
	b.metrics.IncBucketWriteOpCount("columnar_put_row")
	b.metrics.IncBucketWriteOpOngoing("columnar_put_row")
	defer func() {
		b.metrics.DecBucketWriteOpOngoing("columnar_put_row")
		if err != nil {
			b.metrics.IncBucketWriteOpFailureCount("columnar_put_row")
			return
		}
		b.metrics.ObserveBucketWriteOpDuration("columnar_put_row", time.Since(start))
	}()

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	if err := CheckExpectedStrategy(b.strategy, StrategyColumnar); err != nil {
		return err
	}

	return active.columnarPutRowValues(docID, values)
}

// ColumnarLookupFloat32 reads a float32 value by docID, traversing
// memtables and segments newest-first.
func (b *Bucket) ColumnarLookupFloat32(docID uint64, colIdx int) (float32, bool) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	// 1. Active memtable
	if val, found, tomb := b.active.columnarLookupFloat32(docID, colIdx); found {
		if tomb {
			return 0, false
		}
		return val, true
	}

	// 2. Flushing memtable
	if b.flushing != nil {
		if val, found, tomb := b.flushing.columnarLookupFloat32(docID, colIdx); found {
			if tomb {
				return 0, false
			}
			return val, true
		}
	}

	// 3. Disk segments, newest first
	return b.disk.columnarLookupFloat32(docID, colIdx)
}

// ColumnarLookupInt64 reads an int64 value by docID.
func (b *Bucket) ColumnarLookupInt64(docID uint64, colIdx int) (int64, bool) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	if val, found, tomb := b.active.columnarLookupInt64(docID, colIdx); found {
		if tomb {
			return 0, false
		}
		return val, true
	}

	if b.flushing != nil {
		if val, found, tomb := b.flushing.columnarLookupInt64(docID, colIdx); found {
			if tomb {
				return 0, false
			}
			return val, true
		}
	}

	return b.disk.columnarLookupInt64(docID, colIdx)
}

// columnarLookupFloat32 on SegmentGroup iterates segments newest→oldest.
func (sg *SegmentGroup) columnarLookupFloat32(docID uint64, colIdx int) (float32, bool) {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	for i := len(sg.segments) - 1; i >= 0; i-- {
		seg, ok := sg.segments[i].(*segment)
		if !ok {
			continue
		}
		if seg.strategy != segmentindex.StrategyColumnar || seg.columnarData == nil {
			continue
		}
		val, found, tomb := seg.columnarData.lookupFloat32(seg.contents, docID, colIdx)
		if found {
			if tomb {
				return 0, false
			}
			return val, true
		}
	}
	return 0, false
}

// columnarLookupInt64 on SegmentGroup iterates segments newest→oldest.
func (sg *SegmentGroup) columnarLookupInt64(docID uint64, colIdx int) (int64, bool) {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	for i := len(sg.segments) - 1; i >= 0; i-- {
		seg, ok := sg.segments[i].(*segment)
		if !ok {
			continue
		}
		if seg.strategy != segmentindex.StrategyColumnar || seg.columnarData == nil {
			continue
		}
		val, found, tomb := seg.columnarData.lookupInt64(seg.contents, docID, colIdx)
		if found {
			if tomb {
				return 0, false
			}
			return val, true
		}
	}
	return 0, false
}
