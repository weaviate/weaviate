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

	return nil
}

func (b *Bucket) RoaringSetRangeRemove(key uint64, values ...uint64) error {
	if err := CheckStrategyRoaringSetRange(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()



	return nil
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


	return roaringsetrange.NewCombinedReader(readers, func() {
		release()
		b.flushLock.RUnlock()
	}, concurrency.SROAR_MERGE, b.logger)
}
