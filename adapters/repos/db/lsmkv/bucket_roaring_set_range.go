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
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

// ErrRangeableRepUnpopulatedAfterRebuild is returned by
// RebuildRangeableSegmentInMemory when the rebuilt rep is unpopulated while
// disk segments exist. Publication is refused (rangeableRepRebuilt stays
// false) and the bucket keeps serving from disk; the caller degrades to
// WARN-and-continue instead of failing the migration.
var ErrRangeableRepUnpopulatedAfterRebuild = errors.New(
	"rangeable in-memory rebuild produced an empty representation while disk segments " +
		"exist; leaving disk-serving in place",
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

// RangeableServesFromMemory is the exported form of rangeableServesFromMemory,
// for callers outside this package (e.g. the reindex finalize log) that need
// to log truthfully about which path is actually serving.
func (b *Bucket) RangeableServesFromMemory() bool {
	return b.rangeableServesFromMemory()
}

func (b *Bucket) ReaderRoaringSetRange() ReaderRoaringSetRange {
	MustBeExpectedStrategy(b.strategy, StrategyRoaringSetRange)

	// D2 (weaviate/weaviate#12215): a rep published via
	// RebuildRangeableSegmentInMemory is validated once, at install time,
	// against this exact discriminant before the flag is set - a reader
	// observing it can trust the rep unconditionally, no per-read check.
	if b.rangeableRepRebuilt.Load() {
		return b.readerRoaringSetRangeFromSegmentInMemo()
	}

	if b.keepSegmentsInMemory {
		// Boot-time population (segment_group.go's newSegmentGroup) has no
		// equivalent install-time gate, so this path keeps the original
		// per-read discriminant and disk fallback. Check emptiness first
		// so the hot path pays one IsUnpopulated() call under the rep's
		// RLock and never holds maintenanceLock too.
		if b.disk.roaringSetRangeSegmentInMemory.IsUnpopulated() {
			if n := b.disk.roaringSetRangeDiskSegmentCount(); n > 0 {
				b.rangeableFallbackWarnOnce.Do(func() {
					b.logger.WithField("bucket", b.dir).Warnf(
						"rangeable in-memory index is empty but %d disk segment(s) exist; "+
							"serving from disk instead. Results may be incorrect if those "+
							"segments are corrupt or unreadable; rebuild the index to "+
							"repair it.", n,
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
				"rangeable property serving range queries from disk; in-memory " +
					"serving is restored automatically when the migration finalizes. " +
					"If it persists after the migration has finished, the finalize " +
					"rebuild failed; rebuild the index or reload the shard to " +
					"repair it.",
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

// RebuildRangeableSegmentInMemory builds the in-memory rep from disk
// segments and switches range reads to in-memory serving, without a bucket
// reopen. No-op if already serving from memory. Not safe for concurrent
// self-invocation; the single finalize call site guarantees this.
//
// Lock choreography, in order:
//  1. Compaction is paused so the segment set can only grow via tail flush
//     appends; a concurrent compaction could reorder add/delete layers and
//     corrupt the merge.
//  2. The bulk merge runs without flushLock so writes/flushes stay unblocked.
//  3. Under flushLock, segments flushed during the merge are caught up, the
//     rep is published, and the serving flag is set - so a racing flush
//     either merges before us or sees the flag after, never losing a write.
func (b *Bucket) RebuildRangeableSegmentInMemory(ctx context.Context) error {
	if err := CheckStrategyRoaringSetRange(b.strategy); err != nil {
		return err
	}
	if b.rangeableServesFromMemory() {
		return nil
	}

	// Ref-counted at the bucket level so a concurrent snapshot, digest
	// scan, or another rebuild shares one pause instead of one resuming
	// under the other (bucket.go:481-516; weaviate/0-weaviate-issues#251 +
	// weaviate/weaviate#11486). A failed pause leaves the count exactly
	// where it started, so compaction is never left disabled.
	if err := b.pauseCompaction(ctx); err != nil {
		return fmt.Errorf("pause compaction for rangeable in-memory rebuild: %w", err)
	}
	defer func() {
		if resumeErr := b.resumeCompaction(context.Background()); resumeErr != nil {
			b.logger.Errorf("resume compaction after rangeable in-memory rebuild: %v", resumeErr)
		}
	}()

	rep, alreadyMerged, releaseBuilt, err := b.disk.buildRoaringSetRangeRep(ctx)
	if err != nil {
		return fmt.Errorf("build rangeable in-memory rep: %w", err)
	}

	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if err := b.disk.installRoaringSetRangeRep(rep, alreadyMerged, releaseBuilt); err != nil {
		return fmt.Errorf("install rangeable in-memory rep: %w", err)
	}

	// Finding 8 / D2: gate publication on the same discriminant the
	// per-read boot-time path uses, evaluated once here instead of on
	// every read. A rep that folds empty while disk segments exist is not
	// trustworthy for any reason (corrupt segment, a bug in this rebuild);
	// refuse to publish and leave disk-serving in place rather than
	// advertise memory-serving for a rep that does not mirror disk.
	if rep.IsUnpopulated() {
		if n := b.disk.roaringSetRangeDiskSegmentCount(); n > 0 {
			b.logger.WithField("bucket", b.dir).Warnf(
				"rangeable in-memory rebuild produced an empty representation while "+
					"%d disk segment(s) exist; leaving disk-serving in place instead of "+
					"activating in-memory acceleration. May indicate mass deletion or a "+
					"rep-population gap.", n,
			)
			return fmt.Errorf("%w (bucket=%s)", ErrRangeableRepUnpopulatedAfterRebuild, filepath.Base(b.dir))
		}
	}

	b.rangeableRepRebuilt.Store(true)
	return nil
}
