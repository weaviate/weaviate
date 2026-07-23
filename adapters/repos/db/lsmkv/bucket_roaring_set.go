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
	"time"

	"github.com/sirupsen/logrus"
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

	// The pointer, not the keepMergedSegmentsInMemory flag, is the dispatch
	// predicate: non-nil means this bucket is allow-listed for the in-memory
	// path. Gating on the pointer stays safe if a future eviction design nils
	// the structure independently of the flag.
	if b.disk.roaringSetSegmentInMemory != nil {
		return b.roaringSetGetFromConsistentViewInMemo(key, maxConc)
	}

	layers, diskRelease, err := b.disk.roaringSetGet(key, view.Disk, maxConc)
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

	layers, err = appendRoaringSetMemtableLayers(layers, view.Flushing, view.Active, key)
	if err != nil {
		return nil, noopRelease, err
	}

	return layers.Flatten(false, maxConc), diskRelease, nil
}

// roaringSetGetFromConsistentViewInMemo serves the read from the always-merged
// in-memory segment: the merged root plus one layer per still-pending,
// flushing and active memtable on top.
//
// It must not read the view's memtables: the root is live and advances with
// every completed flush, so top layers pinned to an earlier view can re-apply
// state the root already covers, and once the root has advanced two or more
// flush generations inside the read window that blends generations into a torn
// bitmap (a stale deletion landing after a newer re-add). Instead the top
// layers and the root+pending snapshot are captured under one flushLock.RLock
// hold — a flush completion cannot interleave because the pending-append
// happens under flushLock.Lock in atomicallyAddDiskSegmentAndRemoveFlushing.
// Every layer handed to Flatten is an owned clone, so Flatten's in-place
// mutation of the first layer is safe even when no root layer exists.
func (b *Bucket) roaringSetGetFromConsistentViewInMemo(
	key []byte, maxConc int,
) (bm *sroar.Bitmap, release func(), err error) {
	var active, flushing memtable
	var rootLayer roaringset.BitmapLayer
	var rootRelease func()
	var found bool
	var pending []*roaringset.BinarySearchTree

	func() {
		beforeFlushLock := time.Now()

		b.flushLock.RLock()
		defer b.flushLock.RUnlock()

		// logger nil-guard: test buckets are built as bare literals without one,
		// mirroring GetConsistentView
		if took := time.Since(beforeFlushLock); took > 100*time.Millisecond && b.logger != nil {
			b.logger.WithFields(logrus.Fields{
				"duration": took,
				"action":   "lsm_bucket_get_acquire_flush_lock",
			}).Debugf("Waited more than 100ms to obtain a flush lock during get")
		}

		active, flushing = b.active, b.flushing
		rootLayer, rootRelease, found, pending = b.disk.roaringSetSegmentInMemory.Get(key, b.bitmapBufPool)
	}()

	// rootRelease (not the named return, which error paths overwrite with
	// noopRelease) is what the defer frees, so a failed memtable read can't
	// leak the root layer's pooled buffer.
	defer func() {
		if err != nil {
			rootRelease()
		}
	}()

	var layers roaringset.BitmapLayers
	if found {
		layers = append(layers, rootLayer)
	}

	for _, mt := range pending {
		layer, getErr := mt.Get(key)
		if getErr != nil {
			if !errors.Is(getErr, lsmkv.NotFound) {
				err = getErr
				return nil, noopRelease, err
			}
			continue
		}
		layers = append(layers, layer)
	}

	layers, err = appendRoaringSetMemtableLayers(layers, flushing, active, key)
	if err != nil {
		return nil, noopRelease, err
	}

	return layers.Flatten(false, maxConc), rootRelease, nil
}

// appendRoaringSetMemtableLayers appends the flushing (if any) and active
// memtable layers for key on top of layers, tolerating NotFound, so that the
// caller can flatten persisted and in-memory state into one bitmap.
func appendRoaringSetMemtableLayers(
	layers roaringset.BitmapLayers, flushing, active memtable, key []byte,
) (roaringset.BitmapLayers, error) {
	if flushing != nil {
		flushingLayer, err := flushing.roaringSetGet(key)
		if err != nil {
			if !errors.Is(err, lsmkv.NotFound) {
				return nil, err
			}
		} else {
			layers = append(layers, flushingLayer)
		}
	}

	activeBM, err := active.roaringSetGet(key)
	if err != nil {
		if !errors.Is(err, lsmkv.NotFound) {
			return nil, err
		}
	} else {
		layers = append(layers, activeBM)
	}

	return layers, nil
}
