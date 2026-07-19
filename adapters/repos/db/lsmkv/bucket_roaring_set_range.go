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
	"fmt"
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

// rangeableServesFromMemory reports whether range reads serve from the
// in-memory rep. Both operands are safe to read unlocked: keepSegmentsInMemory
// is immutable after open, rangeableRepRebuilt is atomic.
func (b *Bucket) rangeableServesFromMemory() bool {
	return b.keepSegmentsInMemory || b.rangeableRepRebuilt.Load()
}

func (b *Bucket) ReaderRoaringSetRange() ReaderRoaringSetRange {
	MustBeExpectedStrategy(b.strategy, StrategyRoaringSetRange)

	if b.rangeableServesFromMemory() {
		// Invariant: the rep must mirror disk. If unpopulated while disk
		// segments exist (mass-delete, or a population gap), fall back to disk
		// and warn once. Check emptiness first so the hot path pays one
		// IsEmpty() under the rep's RLock and never holds maintenanceLock too.
		if b.disk.roaringSetRangeSegmentInMemory.IsUnpopulated() {
			if n := b.disk.roaringSetRangeDiskSegmentCount(); n > 0 {
				b.rangeableFallbackWarnOnce.Do(func() {
					b.logger.WithField("bucket", b.dir).Warnf(
						"rangeable in-memory index is empty but %d disk segment(s) exist; "+
							"falling back to reading directly from disk. Results may be "+
							"incorrect if the disk segment(s) are corrupt or unreadable - "+
							"restarting the node will NOT repair this (it re-reads the same "+
							"segments). Re-run the reindex with rebuild:true "+
							`(PUT /v1/schema/<collection>/indexes/<property> `+
							`{"rangeable":{"rebuild":true}}) to rebuild the index.`, n,
					)
				})
				// Benign TOCTOU: the rep only empties via mass-delete, and the
				// disk path is a correct superset either way.
				return b.readerRoaringSetRangeFromSegments()
			}
		}
		return b.readerRoaringSetRangeFromSegmentInMemo()
	}
	return b.readerRoaringSetRangeFromSegments()
}

func (b *Bucket) readerRoaringSetRangeFromSegments() ReaderRoaringSetRange {
	// Deferred marker: emit the disk-serving diagnostic once per bucket-open;
	// never affects read-path selection.
	if b.rangeableInMemoryDeferred {
		b.rangeableDeferredLogOnce.Do(func() {
			b.logger.WithField("bucket", b.dir).Info(
				"rangeable property serving from disk; in-memory acceleration " +
					"deferred until the shard is reloaded. Reload the shard or " +
					"restart the node to rebuild the in-memory index.",
			)
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

// RebuildRangeableSegmentInMemory builds the in-memory rep from disk segments
// and switches range reads to in-memory serving, without a bucket reopen.
// No-op if already serving from memory. Not safe for concurrent
// self-invocation; the single finalize call site guarantees this.
//
// The rep only needs disk-segment coverage: readers compose it with the
// flushing/active memtables, and later flushes merge into it once the
// rebuilt flag is set (atomicallyAddDiskSegmentAndRemoveFlushing).
//
// Lock choreography, in order:
//  1. Compaction is paused, so the segment set can only grow, by flush
//     appends at the tail. A concurrent compaction could otherwise replace
//     mid-list segments and re-order add/delete layers relative to newer
//     flushes, corrupting the merge.
//  2. The bulk merge runs WITHOUT flushLock so writes and flushes stay
//     unblocked for its whole (potentially long) duration.
//  3. Under flushLock: segments flushed during the bulk merge are caught up
//     from the tail, the rep is published, and the serving flag is set. A
//     flush whose segment-add had not yet entered the lock proceeds after
//     us, sees the flag set, and merges its memtable into the rep, so no
//     write is lost in either interleaving.
func (b *Bucket) RebuildRangeableSegmentInMemory(ctx context.Context) error {
	if err := CheckStrategyRoaringSetRange(b.strategy); err != nil {
		return err
	}
	if b.rangeableServesFromMemory() {
		return nil
	}

	if err := b.disk.pauseCompaction(ctx); err != nil {
		return fmt.Errorf("pause compaction for rangeable in-memory rebuild: %w", err)
	}
	defer b.disk.resumeCompaction(context.Background())

	rep, bulkMerged, err := b.disk.buildRoaringSetRangeRep(ctx)
	if err != nil {
		return fmt.Errorf("build rangeable in-memory rep: %w", err)
	}

	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if err := b.disk.installRoaringSetRangeRep(rep, bulkMerged); err != nil {
		return fmt.Errorf("install rangeable in-memory rep: %w", err)
	}
	b.rangeableRepRebuilt.Store(true)
	return nil
}
