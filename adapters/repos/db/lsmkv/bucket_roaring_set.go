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
	"errors"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (b *Bucket) RoaringSetAddOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.roaringSetAddOne(key, value)
}

func (b *Bucket) RoaringSetRemoveOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.roaringSetRemoveOne(key, value)
}

func (b *Bucket) RoaringSetAddList(key []byte, values []uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.roaringSetAddList(key, values)
}

// RoaringSetBatchEntry is a key-values pair for use with RoaringSetAddBatch.
type RoaringSetBatchEntry struct {
	Key    []byte
	Values []uint64
}

// RoaringSetAddBatch writes multiple key-values pairs to the bucket under
// a single flushLock acquisition and a single memtable lock acquisition,
// reducing lock overhead compared to calling RoaringSetAddList in a loop.
func (b *Bucket) RoaringSetAddBatch(entries []RoaringSetBatchEntry) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.roaringSetAddBatch(entries)
}

// RoaringSetRemoveBatch removes multiple key-values pairs from the bucket under
// a single flushLock acquisition and a single memtable lock acquisition,
// reducing lock overhead compared to calling RoaringSetRemoveOne in a loop.
func (b *Bucket) RoaringSetRemoveBatch(entries []RoaringSetBatchEntry) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.roaringSetRemoveBatch(entries)
}

func (b *Bucket) RoaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.roaringSetAddBitmap(key, bm)
}

func (b *Bucket) RoaringSetGet(key []byte) (bm *sroar.Bitmap, release func(), err error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return nil, noopRelease, err
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	return b.roaringSetGetFromConsistentView(view, key)
}

func (b *Bucket) roaringSetGetFromConsistentView(
	view BucketConsistentView, key []byte,
) (bm *sroar.Bitmap, release func(), err error) {
	layers, release, err := b.disk.roaringSetGet(key, view.Disk)
	if err != nil {
		return nil, noopRelease, err
	}
	defer func() {
		if err != nil {
			release()
		}
	}()

	if view.Flushing != nil {
		flushing, err := view.Flushing.roaringSetGet(key)
		if err != nil {
			if !errors.Is(err, lsmkv.NotFound) {
				return nil, noopRelease, err
			}
		} else {
			layers = append(layers, flushing)
		}
	}

	activeBM, err := view.Active.roaringSetGet(key)
	if err != nil {
		if !errors.Is(err, lsmkv.NotFound) {
			return nil, noopRelease, err
		}
	} else {
		layers = append(layers, activeBM)
	}

	return layers.Flatten(false), release, nil
}
