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

package hfresh

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

type reassignOperation struct {
	PostingID uint64
	VectorID  uint64
}

func (h *HFresh) doReassign(ctx context.Context, op reassignOperation) error {
	start := time.Now()
	defer h.metrics.ReassignDuration(start)

	var markedAsDone bool
	defer func() {
		if !markedAsDone {
			h.taskQueue.ReassignDone(op.VectorID)
		}
	}()

	// check if the vector is still valid
	version, err := h.VersionMap.Get(ctx, op.VectorID)
	if err != nil {
		return errors.Wrapf(err, "failed to get version for vector %d", op.VectorID)
	}
	if version.Deleted() {
		return nil
	}

	var q []float32
	if h.muvera.Load() {
		q, err = h.muveraEncoder.GetMuveraVectorForID(op.VectorID, h.id+"_muvera_vectors")
	} else {
		q, err = h.config.VectorForIDThunk(ctx, op.VectorID)
	}
	if err != nil {
		return errors.Wrap(err, "failed to get vector by index ID")
	}
	q = h.normalizeVec(q)

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
	if errors.Is(err, ErrVersionIncrementFailed) {
		h.logger.WithField("vectorID", op.VectorID).
			Debug("version changed concurrently, skipping reassign operation")
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "failed to increment version map for vector %d", op.VectorID)
	}

	// create a new vector with the updated version
	newVector := NewVector(op.VectorID, version, h.quantizer.CompressedBytes(h.quantizer.Encode(q)))

	// append the vector to each replica
	requeued, err := h.appendReassignReplicas(ctx, newVector, replicas)
	if err != nil {
		return err
	}
	if requeued {
		markedAsDone = true
	}

	return nil
}

func (h *HFresh) appendReassignReplicas(ctx context.Context, newVector Vector, replicas *ResultSet) (bool, error) {
	for id := range replicas.Iter() {
		version, err := h.VersionMap.Get(ctx, newVector.ID())
		if err != nil {
			return false, errors.Wrapf(err, "failed to get version for vector %d", newVector.ID())
		}
		if version.Deleted() || version.Version() > newVector.Version().Version() {
			h.logger.WithField("vectorID", newVector.ID()).
				Debug("vector is deleted or has a newer version, skipping reassign operation")
			return false, nil
		}

		added, err := h.append(ctx, newVector, id, true)
		if err != nil {
			return false, err
		}
		if !added {
			// the posting has been deleted concurrently,
			// re-enqueue the vector so it can be reassigned against
			// the current centroid set.
			// Clear the current task's in-flight marker before enqueueing the
			// replacement task; otherwise duplicate suppression would drop it.
			h.taskQueue.ReassignDone(newVector.ID())
			err = h.taskQueue.EnqueueReassign(id, newVector.ID())
			if err != nil {
				return true, err
			}
			return true, nil
		}
	}

	return false, nil
}
