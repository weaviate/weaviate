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
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

func (b *Bucket) RoaringSetRangeAdd(key uint64, values ...uint64) error {
	if err := CheckStrategyRoaringSetRange(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetRangeAdd(key, values...)
}

func (b *Bucket) RoaringSetRangeRemove(key uint64, values ...uint64) error {
	if err := CheckStrategyRoaringSetRange(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetRangeRemove(key, values...)
}

type ReaderRoaringSetRange interface {
	Read(ctx context.Context, value uint64, operator filters.Operator) (result *sroar.Bitmap, release func(), err error)
	Close()
}

func (b *Bucket) ReaderRoaringSetRange() ReaderRoaringSetRange {
	MustBeExpectedStrategy(b.strategy, StrategyRoaringSetRange)

	if b.keepSegmentsInMemory {
		// (c) GH#12199: if the in-memory rep is unpopulated but disk segments
		// exist, the rep is out of sync with disk (a rep-population gap such as a
		// mass-delete, or a future segment-splice bug). Serve from the
		// always-correct disk reader and WARN once per bucket-open. Check rep
		// emptiness FIRST and short-circuit, so the populated hot path pays only
		// one IsEmpty() under the rep's own RLock and never touches
		// maintenanceLock. The two locks are never held simultaneously.
		if b.disk.roaringSetRangeSegmentInMemory.IsUnpopulated() {
			if n := b.disk.roaringSetRangeDiskSegmentCount(); n > 0 {
				b.rangeableFallbackWarnOnce.Do(func() {
					b.logger.WithField("bucket", b.dir).Warnf(
						"rangeable in-memory representation is empty but %d disk segment(s) "+
							"exist; falling back to the disk range reader "+
							"(INV-RANGEABLE-REP-EQUALS-DISK). Results remain correct. This may "+
							"indicate a mass deletion of the indexed property or a rep-population "+
							"gap; restart the node or reload the shard to rebuild the in-memory "+
							"representation (GH#12199).", n)
				})
				// The TOCTOU between this emptiness check and the reader build
				// below is benign: the rep only empties via mass-delete and the
				// disk path is a correct superset either way.
				return b.readerRoaringSetRangeFromSegments()
			}
		}
		return b.readerRoaringSetRangeFromSegmentInMemo()
	}
	return b.readerRoaringSetRangeFromSegments()
}

func (b *Bucket) readerRoaringSetRangeFromSegments() ReaderRoaringSetRange {
	// GH#12199: a bucket marked deferred was opened with keepSegmentsInMemory
	// forced off while the global knob is on (the reindex ingest path). Emit a
	// durable, query-time signal once per bucket-open at the first range read so
	// an operator investigating slow range filters can find the reason and the
	// remedy. The marker gates only this log line; it never selects a read path.
	if b.rangeableInMemoryDeferred {
		b.rangeableDeferredLogOnce.Do(func() {
			b.logger.WithField("bucket", b.dir).Info(
				"rangeable property serving from disk; in-memory knob deferred until next " +
					"reload (GH#12199). Restart the node or reload the shard to rebuild the " +
					"in-memory representation and resume in-memory acceleration.")
		})
	}

	view := b.GetConsistentView()

	readers := make([]roaringsetrange.InnerReader, len(view.Disk))
	for i, segment := range view.Disk {
		readers[i] = segment.newRoaringSetRangeReader()
	}
	if view.Flushing != nil {
		readers = append(readers, view.Flushing.newRoaringSetRangeReader())
	}
	readers = append(readers, view.Active.newRoaringSetRangeReader())

	return roaringsetrange.NewCombinedReader(readers, view.ReleaseView, concurrency.SROAR_MERGE, b.logger)
}

func (b *Bucket) readerRoaringSetRangeFromSegmentInMemo() ReaderRoaringSetRange {
	var active, flushing memtable
	var readers []roaringsetrange.InnerReader
	var release func()

	func() {
		beforeFlushLock := time.Now()

		b.flushLock.RLock()
		defer b.flushLock.RUnlock()

		if took := time.Since(beforeFlushLock); took > 100*time.Millisecond {
			b.logger.WithFields(logrus.Fields{
				"duration": took,
				"action":   "lsm_bucket_get_acquire_flush_lock",
			}).Debugf("Waited more than 100ms to obtain a flush lock during get")
		}

		active, flushing = b.active, b.flushing
		readers, release = b.disk.roaringSetRangeSegmentInMemory.Readers(b.bitmapBufPool)
	}()

	if flushing != nil {
		readers = append(readers, flushing.newRoaringSetRangeReader())
	}
	readers = append(readers, active.newRoaringSetRangeReader())

	return roaringsetrange.NewCombinedReader(readers, release, concurrency.SROAR_MERGE, b.logger)
}
