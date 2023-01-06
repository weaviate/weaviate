package lsmkv

import (
	"fmt"

	"github.com/dgraph-io/sroar"
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

	var segments []*roaringSet

	// TODO: check segments other than memtable

	memtable, err := b.active.roaringSetGet(key)
	if err != nil {
		if err != NotFound {
			return nil, err
		}
	} else {
		segments = append(segments, memtable)
	}

	return flattenRoaringSegments(segments)
}

func flattenRoaringSegments(segments []*roaringSet) (*sroar.Bitmap, error) {
	if len(segments) == 0 {
		return sroar.NewBitmap(), nil
	}

	if len(segments) > 1 {
		return nil, fmt.Errorf("merging multiple segments not implemented yet")
	}

	cur := segments[0]
	merged := cur.additions.Clone()

	return merged, nil
}

func checkStrategyRoaringSet(bucketStrat string) error {
	if bucketStrat == StrategyRoaringSet {
		return nil
	}

	return fmt.Errorf("this method requires a roaring set strategy, got: %s",
		bucketStrat)
}
