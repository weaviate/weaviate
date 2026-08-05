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
	"sync/atomic"

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

// RoaringSetGet consults ctx only for the concurrency budget, not for cancellation.
func (b *Bucket) RoaringSetGet(ctx context.Context, key []byte) (bm *sroar.Bitmap, release func(), err error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return nil, noopRelease, err
	}

	view := b.GetConsistentView()
	defer view.ReleaseView()

	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	return b.roaringSetGetFromConsistentView(view, key, mergeConc)
}

// A nil view.Active is skipped, the same way a nil view.Flushing is: the caller
// has established that the layer contributes nothing.
func (b *Bucket) roaringSetGetFromConsistentView(
	view BucketConsistentView, key []byte, mergeConc int,
) (bm *sroar.Bitmap, release func(), err error) {
	diskBM, diskRelease, err := b.disk.roaringSetGet(key, view.Disk, mergeConc)
	if err != nil {
		return nil, noopRelease, err
	}
	// diskRelease (not the named return, which error paths overwrite with
	// noopRelease) is what the defer frees, so a failed flushing/active
	// read can't leak the disk bitmap's pooled buffer.
	defer func() {
		if err != nil {
			diskRelease()
		}
	}()

	// Fold the disk, flushing and active layers one at a time, oldest first,
	// without materializing a []BitmapLayer. diskBM is the pooled base (with
	// headroom); on a disk miss it is nil and the merger adopts the first
	// memtable layer's clone instead of copying it.
	merger := roaringset.NewLayerMerger(diskBM, false, mergeConc)

	// Oldest first: the merger replays each layer's deletions before its
	// additions, so a doc deleted in flushing and re-added in active survives
	// only in this order.
	for _, mt := range [2]memtable{view.Flushing, view.Active} {
		if mt == nil {
			continue
		}
		layer, mtErr := mt.roaringSetGet(key)
		if mtErr != nil {
			if !errors.Is(mtErr, lsmkv.NotFound) {
				err = mtErr
				return nil, noopRelease, err
			}
			continue
		}
		merger.Add(layer)
	}

	return merger.Result(), diskRelease, nil
}

// ErrReaderReleased signals a lifetime bug in the caller, never a storage
// failure.
var ErrReaderReleased = errors.New("roaring set batch reader: Get after Release")

// RoaringSetBatchReader reads many roaringset rows under one view, held until
// Release. It must not outlive the bucket; Get is safe to call concurrently.
//
// An active memtable empty at view time is skipped for the whole batch, so a
// write into it afterwards is invisible to all keys rather than to only the
// tail. Callers needing per-read freshness must use RoaringSetGet.
type RoaringSetBatchReader struct {
	view     BucketConsistentView
	released atomic.Bool
}

// NewRoaringSetBatchReader opens a reader on b. See RoaringSetBatchReader for
// the lifetime and visibility contract.
func (b *Bucket) NewRoaringSetBatchReader() (*RoaringSetBatchReader, error) {
	if err := CheckStrategyRoaringSet(b.strategy); err != nil {
		return nil, err
	}
	view := b.GetConsistentView()
	if view.Active != nil && view.Active.Size() == 0 {
		// Read outside flushLock, which is safe only because a memtable's size
		// never decreases: a racing write leaves it stale-low, never stale-high,
		// so the skip can miss that write but never drops a committed one.
		view.Active = nil
	}
	return &RoaringSetBatchReader{view: view}, nil
}

// Get reads key's row under the held view. mergeConc reaches sroar unclamped —
// pass what concurrency.BudgetFromCtxCapped returned, since SROAR_MERGE itself
// bypasses the query's budget and a non-positive value means unbounded.
// Returns ErrReaderReleased after Release: those segments may be unmapped.
func (r *RoaringSetBatchReader) Get(key []byte, mergeConc int) (*sroar.Bitmap, func(), error) {
	if r.released.Load() {
		return nil, noopRelease, ErrReaderReleased
	}
	return r.view.Bucket.roaringSetGetFromConsistentView(r.view, key, mergeConc)
}

// Release releases the held view. Later calls are no-ops: a second decRef could
// take a concurrent reader's count to zero, and compaction then unmaps and
// deletes a segment that reader is using. The guard detects misuse; it does not
// synchronize a Release racing an in-flight Get.
func (r *RoaringSetBatchReader) Release() {
	if r.released.Swap(true) {
		return
	}
	r.view.ReleaseView()
}
