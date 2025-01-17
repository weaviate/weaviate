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
	"errors"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (b *Bucket) RoaringSetAddOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetAddOne(key, value)
}

func (b *Bucket) RoaringSetRemoveOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetRemoveOne(key, value)
}

func (b *Bucket) RoaringSetAddList(key []byte, values []uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetAddList(key, values)
}

func (b *Bucket) RoaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	return b.active.roaringSetAddBitmap(key, bm)
}

func (b *Bucket) RoaringSetGet(key []byte) (*sroar.Bitmap, error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return nil, err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	layers, err := b.disk.roaringSetGet(key)
	if err != nil {
		return nil, err
	}

	if b.flushing != nil {
		flushing, err := b.flushing.roaringSetGet(key)
		if err != nil {
			if !errors.Is(err, lsmkv.NotFound) {
				return nil, err
			}
		} else {
			layers = append(layers, flushing)
		}
	}

	active, err := b.active.roaringSetGet(key)
	if err != nil {
		if !errors.Is(err, lsmkv.NotFound) {
			return nil, err
		}
	} else {
		layers = append(layers, active)
	}

	return layers.Flatten(false), nil
}

func (b *Bucket) RoaringSetGetBuffered(key []byte) (bm *sroar.Bitmap, release func(), err error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return nil, nil, err
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	layers, err := b.disk.roaringSetGet(key)
	if err != nil {
		return nil, nil, err
	}

	useBufferedBitmap := len(layers) > 1

	if b.flushing != nil {
		flushing, err := b.flushing.roaringSetGet(key)
		if err != nil {
			if !errors.Is(err, lsmkv.NotFound) {
				return nil, nil, err
			}
		} else {
			layers = append(layers, flushing)
		}
	}

	active, err := b.active.roaringSetGet(key)
	if err != nil {
		if !errors.Is(err, lsmkv.NotFound) {
			return nil, nil, err
		}
	} else {
		layers = append(layers, active)
	}

	release = func() {}
	if useBufferedBitmap {
		var empty *sroar.Bitmap
		empty, release = b.bitmapFactory.GetEmptyBitmap()

		layer := roaringset.BitmapLayer{Additions: empty, Deletions: empty}
		tmpLayers := make(roaringset.BitmapLayers, 0, len(layers)+1)
		tmpLayers = append(tmpLayers, layer)
		tmpLayers = append(tmpLayers, layers...)
		layers = tmpLayers
	}

	return layers.Flatten(false), release, nil
}
