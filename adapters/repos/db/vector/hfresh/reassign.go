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
	newVector := NewVector(op.VectorID, version, h.quantizer.CompressedBytes(h.quantizer.Encode(q)))

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

// reassignDeduplicator is an in-memory deduplicator for reassign operations.
// it ensures that only one reassign operation per vector ID is enqueued at any time.
// it also keeps track of the last known posting ID for each vector ID.
// When a reassign operation is dequeued, it uses the last known posting ID to create the ReassignTask.
// Upon completion of the reassign operation, the entry is removed from the deduplicator.
// During shutdown, all pending reassign operations are flushed to the persistent store in a single write.
// In case of a crash, we may lose the mapping of vector IDs to posting IDs, but since reassign operations are idempotent,
// it is safe to process without this information.
type reassignDeduplicator struct {
	bucket *lsmkv.Bucket
	m      *xsync.Map[uint64, uint64]
}

func newReassignDeduplicator(bucket *lsmkv.Bucket) (*reassignDeduplicator, error) {
	data, err := bucket.Get([]byte(reassignBucketKey))
	if err != nil {
		return nil, err
	}

	r := reassignDeduplicator{
		bucket: bucket,
		m:      xsync.NewMap[uint64, uint64](),
	}

	for i := 0; i < len(data); i += 16 {
		vectorID := binary.LittleEndian.Uint64(data[i : i+8])
		postingID := binary.LittleEndian.Uint64(data[i+8 : i+16])

		r.m.Store(vectorID, postingID)
	}

	return &r, nil
}

// tryAdd tries to add a reassign operation for the given vector ID and posting ID.
// It returns true if the operation was added, false if it was already present.
func (r *reassignDeduplicator) tryAdd(vectorID, postingID uint64) bool {
	_, updated := r.m.LoadAndStore(vectorID, postingID)
	return !updated
}

// marks the reassign operation for the given vector ID as done, removing it from the deduplicator and the persistent store.
func (r *reassignDeduplicator) done(vectorID uint64) {
	r.m.Delete(vectorID)
}

// flush writes all dirty entries to the persistent store.
func (r *reassignDeduplicator) flush() (err error) {
	buf := make([]byte, 0, 16*r.m.Size())
	r.m.Range(func(vectorID uint64, postingID uint64) bool {
		buf = binary.LittleEndian.AppendUint64(buf, vectorID)
		buf = binary.LittleEndian.AppendUint64(buf, postingID)
		return true
	})

	return r.bucket.Put([]byte(reassignBucketKey), buf)
}

// getLastKnownPostingID retrieves the last known posting ID for the given vector ID.
func (r *reassignDeduplicator) getLastKnownPostingID(vectorID uint64) uint64 {
	postingID, _ := r.m.Load(vectorID)
	return postingID
}
