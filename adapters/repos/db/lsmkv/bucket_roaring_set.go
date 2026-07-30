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
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (b *Bucket) RoaringSetAddOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetAddOne(key, value)
}

func (b *Bucket) RoaringSetRemoveOne(key []byte, value uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetRemoveOne(key, value)
}

func (b *Bucket) RoaringSetAddList(key []byte, values []uint64) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
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

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
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

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetRemoveBatch(entries)
}

func (b *Bucket) RoaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return err
	}

	active, release, err := b.getActiveMemtableForWrite()
	if err != nil {
		return err
	}
	defer release()

	return active.roaringSetAddBitmap(key, bm)
}

// RoaringSetEachDistinctKey calls fn once per distinct key with the key's
// live doc count, in no particular order, giving up once the bucket holds
// more than maxDistinct distinct keys — the return is then (true, nil) and
// fn may not have been called at all. The bloom lower bound rejects clearly
// larger buckets before any scan. Keys whose docs are all deleted count
// toward the limit but are not passed to fn; the key passed to fn aliases
// internal storage, so copy it before retaining.
//
// Distinct keys come from per-segment key walks that collect each key's
// bitmap layers as they pass; only the surviving distinct set pays a layer
// merge, seeded from the already-collected layers. That costs O(index keys)
// plus maxDistinct merges, with none of a merged cursor's per-key
// materialization and none of a per-key get's index seeks.
func (b *Bucket) RoaringSetEachDistinctKey(ctx context.Context, maxDistinct int,
	fn func(key []byte, liveCount int) error,
) (exceeded bool, err error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return false, err
	}

	if lower, err := b.GetKeysCount(); err == nil && lower > uint32(maxDistinct) {
		return true, nil
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	// layers per key in chronological order; a nil slice still marks the key
	// distinct (memtable-only keys)
	layersByKey := map[string][]roaringset.BitmapLayer{}

	const ctxCheckEvery = 1 << 12
	visited := 0
	for _, seg := range view.Disk {
		c := seg.newRoaringSetCursor()
		for {
			key, layer, err := c.Next()
			if err != nil {
				return false, err
			}
			if key == nil {
				break
			}
			if visited++; visited%ctxCheckEvery == 0 {
				if err := ctx.Err(); err != nil {
					return false, err
				}
			}
			layers, ok := layersByKey[string(key)]
			if !ok && len(layersByKey) == maxDistinct {
				return true, nil
			}
			layersByKey[string(key)] = append(layers, layer)
		}
	}

	memtables, count := viewMemtables(view)
	for i := range count {
		keys, err := memtables[i].GetKeys()
		if err != nil {
			return false, err
		}
		for _, key := range keys {
			if _, ok := layersByKey[string(key)]; !ok {
				if len(layersByKey) == maxDistinct {
					return true, nil
				}
				layersByKey[string(key)] = nil
			}
		}
	}

	maxConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	for key, layers := range layersByKey {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		// the collected layers alias segment storage, so the fold's
		// accumulator must start from a clone
		var merger roaringset.LayerMerger
		if len(layers) > 0 {
			merger = roaringset.NewLayerMerger(layers[0].Additions, true, maxConc)
			for _, layer := range layers[1:] {
				merger.Add(layer)
			}
		} else {
			merger = roaringset.NewLayerMerger(nil, false, maxConc)
		}
		if view.Flushing != nil {
			layer, err := view.Flushing.roaringSetGet([]byte(key))
			if err != nil && !errors.Is(err, lsmkv.NotFound) {
				return false, err
			} else if err == nil {
				merger.Add(layer)
			}
		}
		layer, err := view.Active.roaringSetGet([]byte(key))
		if err != nil && !errors.Is(err, lsmkv.NotFound) {
			return false, err
		} else if err == nil {
			merger.Add(layer)
		}

		liveCount := merger.Result().GetCardinality()
		if liveCount == 0 {
			continue
		}
		if err := fn([]byte(key), liveCount); err != nil {
			return false, err
		}
	}

	return false, nil
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

// RoaringSetGetFromView reads key using a caller-held BucketConsistentView,
// skipping the per-call GetConsistentView()/ReleaseView() pair (RLock +
// disk-segment pinning) that RoaringSetGet performs on every invocation.
// Intended for callers that read many keys from the same bucket in one
// logical operation: call GetConsistentView() once, pass the result to every
// RoaringSetGetFromView call, then call view.ReleaseView() exactly once when
// done. The view must come from this bucket's GetConsistentView: a view of
// another bucket is not detected and would silently read that bucket's data.
func (b *Bucket) RoaringSetGetFromView(
	ctx context.Context, view BucketConsistentView, key []byte,
) (bm *sroar.Bitmap, release func(), err error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return nil, noopRelease, err
	}

	return b.roaringSetGetFromConsistentView(ctx, view, key)
}

func (b *Bucket) roaringSetGetFromConsistentView(
	ctx context.Context, view BucketConsistentView, key []byte,
) (bm *sroar.Bitmap, release func(), err error) {
	maxConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)

	diskLayer, diskRelease, err := b.disk.roaringSetGet(key, view.Disk, maxConc)
	if err != nil {
		return nil, noopRelease, err
	}
	// diskRelease (not the named return, which error paths overwrite with
	// noopRelease) is what the defer frees, so a failed flushing/active
	// read can't leak the disk layer's pooled buffer.
	defer func() {
		if err != nil {
			diskRelease()
		}
	}()

	// Fold the disk, flushing and active layers one at a time, oldest first,
	// without materializing a []BitmapLayer. diskLayer.Additions is the pooled
	// base (with headroom); on a disk miss it is nil and the merger adopts the
	// first memtable layer's clone instead of copying it.
	merger := roaringset.NewLayerMerger(diskLayer.Additions, false, maxConc)

	if view.Flushing != nil {
		flushing, flushErr := view.Flushing.roaringSetGet(key)
		if flushErr != nil {
			if !errors.Is(flushErr, lsmkv.NotFound) {
				err = flushErr
				return nil, noopRelease, err
			}
		} else {
			merger.Add(flushing)
		}
	}

	activeBM, activeErr := view.Active.roaringSetGet(key)
	if activeErr != nil {
		if !errors.Is(activeErr, lsmkv.NotFound) {
			err = activeErr
			return nil, noopRelease, err
		}
	} else {
		merger.Add(activeBM)
	}

	return merger.Result(), diskRelease, nil
}
