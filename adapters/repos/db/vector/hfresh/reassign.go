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
	Version   uint8
}

func (h *HFresh) doReassign(ctx context.Context, op reassignOperation) error {
	start := time.Now()
	defer h.metrics.ReassignDuration(start)

	// check if the vector is still valid
	version, err := h.VersionMap.Get(ctx, op.VectorID)
	if err != nil {
		return errors.Wrapf(err, "failed to get version for vector %d", op.VectorID)
	}
	if version.Deleted() || version.Version() > op.Version {
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

	// check again if the version is still valid
	version, err = h.VersionMap.Get(ctx, op.VectorID)
	if err != nil {
		return errors.Wrapf(err, "failed to get version for vector %d", op.VectorID)
	}
	if version.Deleted() || version.Version() > op.Version {
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
