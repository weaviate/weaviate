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

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/concurrency"
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

func (b *Bucket) RoaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release := b.getActiveMemtableForWrite()
	defer release()

	return active.roaringSetAddBitmap(key, bm)
}

// RoaringSetGet consults ctx only for the concurrency budget, not for cancellation.
func (b *Bucket) RoaringSetGet(ctx context.Context, key []byte) (bm *sroar.Bitmap, release func(), err error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return nil, noopRelease, err
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	return b.roaringSetGetFromConsistentView(ctx, view, key)
}

func (b *Bucket) roaringSetGetFromConsistentView(
	ctx context.Context, view BucketConsistentView, key []byte,
) (bm *sroar.Bitmap, release func(), err error) {
	maxConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)

	layers, diskRelease, err := b.disk.roaringSetGet(key, view.Disk, maxConc)
	if err != nil {
		return nil, noopRelease, err
	}
	// diskRelease frees the disk layers' pooled buffer. The defer releases it on
	// any error path so a failed flushing/active read can't leak it back into
	// the pool. It is a dedicated variable rather than the named release return
	// because error paths overwrite the return with noopRelease for the caller;
	// the defer must still see the real release.
	defer func() {
		if err != nil {
			diskRelease()
		}
	}()

	if view.Flushing != nil {
		flushing, flushErr := view.Flushing.roaringSetGet(key)
		if flushErr != nil {
			if !errors.Is(flushErr, lsmkv.NotFound) {
				err = flushErr
				return nil, noopRelease, err
			}
		} else {
			layers = append(layers, flushing)
		}
	}

	activeBM, activeErr := view.Active.roaringSetGet(key)
	if activeErr != nil {
		if !errors.Is(activeErr, lsmkv.NotFound) {
			err = activeErr
			return nil, noopRelease, err
		}
	} else {
		layers = append(layers, activeBM)
	}

	return layers.Flatten(false, maxConc), diskRelease, nil
}
