//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"fmt"

	"github.com/dgraph-io/sroar"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/ent"
)

func (b *Bucket) RoaringSetAddOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetAddOne(key, value)
}

func (b *Bucket) RoaringSetRemoveOne(key []byte, value uint64) error {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetRemoveOne(key, value)
}

func (b *Bucket) RoaringSetAddList(key []byte, values []uint64) error {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetAddList(key, values)
}

func (b *Bucket) RoaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetAddBitmap(key, bm)
}

func (b *Bucket) RoaringSetGet(key []byte) (*sroar.Bitmap, error) {
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		return nil, err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	segments, err := b.disk.roaringSetGet(key)
	if err != nil {
		return nil, err
	}

	if b.flushing != nil {
		flushing, err := b.flushing.roaringSetGet(key)
		if err != nil {
			if err != ent.NotFound {
				return nil, err
			}
		} else {
			segments = append(segments, flushing)
		}
	}

	memtable, err := b.active.roaringSetGet(key)
	if err != nil {
		if err != ent.NotFound {
			return nil, err
		}
	} else {
		segments = append(segments, memtable)
	}

	return segments.Flatten(), nil
}

func checkStrategyRoaringSet(bucketStrat string) error {
	if bucketStrat == StrategyRoaringSet {
		return nil
	}

	return fmt.Errorf("this method requires a roaring set strategy, got: %s",
		bucketStrat)
}
