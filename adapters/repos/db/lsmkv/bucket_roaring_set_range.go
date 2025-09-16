//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

func (b *Bucket) RoaringSetRangeAdd(key uint64, values ...uint64) error {
	if err := CheckStrategyRoaringSetRange(b.strategy); err != nil {
		return err
	}

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.roaringSetRangeAdd(key, values...)
}

func (b *Bucket) RoaringSetRangeRemove(key uint64, values ...uint64) error {
	if err := CheckStrategyRoaringSetRange(b.strategy); err != nil {
		return err
	}

	active, release := b.getActiveMemtableForWrite()
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
		return b.readerRoaringSetRangeFromSegmentInMemo()
	}
	return b.readerRoaringSetRangeFromSegments()
}

func (b *Bucket) readerRoaringSetRangeFromSegments() ReaderRoaringSetRange {
	view := b.getConsistentView()

	readers := make([]roaringsetrange.InnerReader, len(view.Disk))
	for i, segment := range view.Disk {
		readers[i] = segment.newRoaringSetRangeReader()
	}
	if view.Flushing != nil {
		readers = append(readers, view.Flushing.newRoaringSetRangeReader())
	}
	readers = append(readers, view.Active.newRoaringSetRangeReader())

	return roaringsetrange.NewCombinedReader(readers, view.Release, concurrency.SROAR_MERGE, b.logger)
}

func (b *Bucket) readerRoaringSetRangeFromSegmentInMemo() ReaderRoaringSetRange {
	var active, flushing memtable
	var readerInMemo roaringsetrange.InnerReader
	var releaseInMemo func()

	func() {
		beforeFlushLock := time.Now()

		b.flushLock.RLock()
		defer b.flushLock.RUnlock()

		if took := time.Since(beforeFlushLock); took > 100*time.Millisecond {
			b.logger.WithField("duration", took).
				WithField("action", "lsm_bucket_get_acquire_flush_lock").
				Debugf("Waited more than 100ms to obtain a flush lock during get")
		}

		readerInMemo, releaseInMemo = roaringsetrange.NewSegmentInMemoryReader(b.disk.roaringSetRangeSegmentInMemory, b.bitmapBufPool)
		active, flushing = b.active, b.flushing
	}()

	readers := []roaringsetrange.InnerReader{readerInMemo}
	if flushing != nil {
		readers = append(readers, flushing.newRoaringSetRangeReader())
	}
	readers = append(readers, active.newRoaringSetRangeReader())

	return roaringsetrange.NewCombinedReader(readers, releaseInMemo, concurrency.SROAR_MERGE, b.logger)
}
