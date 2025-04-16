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

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

func (b *Bucket) RoaringSetRangeAdd(key uint64, values ...uint64) error {
	if err := CheckStrategyRoaringSetRange(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetRangeAdd(key, values...)
}

func (b *Bucket) RoaringSetRangeRemove(key uint64, values ...uint64) error {
	if err := CheckStrategyRoaringSetRange(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetRangeRemove(key, values...)
}

type ReaderRoaringSetRange interface {
	Read(ctx context.Context, value uint64, operator filters.Operator) (result *sroar.Bitmap, release func(), err error)
	Close()
}

func (b *Bucket) ReaderRoaringSetRange() ReaderRoaringSetRange {
	MustBeExpectedStrategy(b.strategy, StrategyRoaringSetRange)

	b.flushLock.RLock()

	var release func()
	var readers []roaringsetrange.InnerReader
	if b.keepSegmentsInMemory {
		reader, releaseInt := roaringsetrange.NewSegmentInMemoryReader(b.disk.roaringSetRangeSegmentInMemory, b.bitmapBufPool)
		readers, release = []roaringsetrange.InnerReader{reader}, releaseInt
	} else {
		readers, release = b.disk.newRoaringSetRangeReaders()
	}

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		readers = append(readers, b.flushing.newRoaringSetRangeReader())
	}
	readers = append(readers, b.active.newRoaringSetRangeReader())

	return roaringsetrange.NewCombinedReader(readers, func() {
		release()
		b.flushLock.RUnlock()
	}, concurrency.SROAR_MERGE, b.logger)
}
