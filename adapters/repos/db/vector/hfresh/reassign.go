//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hfresh

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

type reassignOperation struct {
	PostingID uint64
	VectorID  uint64
}

func (h *HFresh) doReassign(ctx context.Context, op reassignOperation) error {
	start := time.Now()
	defer h.metrics.ReassignDuration(start)
	defer h.taskQueue.ReassignDone(op.VectorID)

	// check if the vector is still valid
	version, err := h.VersionMap.Get(ctx, op.VectorID)
	if err != nil {
		return errors.Wrapf(err, "failed to get version for vector %d", op.VectorID)
	}
	if version.Deleted() {
		return nil
	}

	// perform a RNG selection to determine the postings where the vector should be
	// reassigned to.
	q, err := h.config.VectorForIDThunk(ctx, op.VectorID)
	if err != nil {
		return errors.Wrap(err, "failed to get vector by index ID")
	}

	replicas, needsReassign, err := h.RNGSelect(q, op.PostingID)
	if err != nil {
		return errors.Wrap(err, "failed to select replicas")
	}
	if !needsReassign {
		return nil
	}

	// increment the vector version. this will invalidate all the existing copies
	// of the vector in other postings.
	version, err = h.VersionMap.Increment(ctx, op.VectorID, version)
	if err != nil {
		h.logger.WithField("vectorID", op.VectorID).
			WithError(err).
			Error("failed to increment version map for vector, skipping reassign operation")
		return nil
	}

	// create a new vector with the updated version
	newVector := NewVector(op.VectorID, version, h.quantizer.Encode(q))

	// append the vector to each replica
	for id := range replicas.Iter() {
		version, err = h.VersionMap.Get(ctx, newVector.ID())
		if err != nil {
			return errors.Wrapf(err, "failed to get version for vector %d", newVector.ID())
		}
		if version.Deleted() || version.Version() > newVector.Version().Version() {
			h.logger.WithField("vectorID", op.VectorID).
				Debug("vector is deleted or has a newer version, skipping reassign operation")
			return nil
		}

		added, err := h.append(ctx, newVector, id, true)
		if err != nil {
			return err
		}
		if !added {
			// the posting has been deleted concurrently,
			// append has enqueued a new reassign operation
			// we can stop here
			break
		}
	}

	return nil
}

// reassignStore is a persistent store for pending reassign operations.
type reassignStore struct {
	bucket *lsmkv.Bucket
}

func newReassignStore(bucket *lsmkv.Bucket) *reassignStore {
	return &reassignStore{
		bucket: bucket,
	}
}

func (v *reassignStore) key(vectorID uint64) [9]byte {
	var buf [9]byte
	buf[0] = reassignBucketPrefix
	binary.LittleEndian.PutUint64(buf[1:], vectorID)
	return buf
}

func (v *reassignStore) Get(ctx context.Context, vectorID uint64) (uint64, error) {
	key := v.key(vectorID)
	data, err := v.bucket.Get(key[:])
	if err != nil {
		return 0, err
	}
	if len(data) != 8 {
		return 0, errors.Errorf("invalid reassign posting data for vector %d", vectorID)
	}

	return binary.LittleEndian.Uint64(data), nil
}

func (v *reassignStore) Set(ctx context.Context, vectorID, postingID uint64) error {
	key := v.key(vectorID)
	return v.bucket.Put(key[:], binary.LittleEndian.AppendUint64(nil, postingID))
}

func (v *reassignStore) Delete(vectorID uint64) error {
	key := v.key(vectorID)
	return v.bucket.Delete(key[:])
}

type reassignDeduplicator struct {
	store *reassignStore
	m     *xsync.Map[uint64, reassignEntry]
}

type reassignEntry struct {
	PostingID uint64
	Dirty     bool
}

func newReassignDeduplicator(bucket *lsmkv.Bucket) *reassignDeduplicator {
	return &reassignDeduplicator{
		store: newReassignStore(bucket),
		m:     xsync.NewMap[uint64, reassignEntry](),
	}
}

func (r *reassignDeduplicator) tryAdd(vectorID, postingID uint64) (bool, error) {
	var newlyAdded bool
	r.m.Compute(vectorID, func(oldValue reassignEntry, loaded bool) (newValue reassignEntry, op xsync.ComputeOp) {
		if loaded {
			return reassignEntry{
				PostingID: oldValue.PostingID,
				Dirty:     true,
			}, xsync.UpdateOp
		}

		newlyAdded = true

		return reassignEntry{
			PostingID: postingID,
		}, xsync.UpdateOp
	})

	if !newlyAdded {
		return false, nil
	}

	err := r.store.Set(context.Background(), vectorID, postingID)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *reassignDeduplicator) done(vectorID uint64) error {
	_, exists := r.m.LoadAndDelete(vectorID)
	if !exists {
		return nil
	}

	return r.store.Delete(vectorID)
}

func (r *reassignDeduplicator) flush(ctx context.Context) (err error) {
	r.m.Range(func(key uint64, value reassignEntry) bool {
		if !value.Dirty {
			return true
		}

		err = r.store.Set(ctx, key, value.PostingID)
		if err != nil {
			return false
		}

		// mark as clean
		r.m.Store(key, reassignEntry{
			PostingID: value.PostingID,
		})

		return true
	})

	return
}

func (r *reassignDeduplicator) getLastKnownPostingID(vectorID uint64) (uint64, error) {
	entry, ok := r.m.Load(vectorID)
	if ok {
		return entry.PostingID, nil
	}

	return r.store.Get(context.Background(), vectorID)
}
