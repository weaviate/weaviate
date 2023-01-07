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

	segments, err := b.disk.roaringSetGet(key)
	if err != nil {
		return nil, err
	}

	if b.flushing != nil {
		flushing, err := b.flushing.roaringSetGet(key)
		if err != nil {
			if err != NotFound {
				return nil, err
			}
		} else {
			segments = append(segments, flushing)
		}
	}

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

func flattenRoaringSegments(segments []roaringSet) (*sroar.Bitmap, error) {
	if len(segments) == 0 {
		return sroar.NewBitmap(), nil
	}

	cur := segments[0]
	// TODO: is this copy really needed? aren't we already operating on copied
	// bms?
	merged := cur.additions.Clone()

	for i := 1; i < len(segments); i++ {
		merged.AndNot(segments[i].deletions)
		merged.Or(segments[i].additions)
	}

	return merged, nil
}

func checkStrategyRoaringSet(bucketStrat string) error {
	if bucketStrat == StrategyRoaringSet {
		return nil
	}

	return fmt.Errorf("this method requires a roaring set strategy, got: %s",
		bucketStrat)
}
