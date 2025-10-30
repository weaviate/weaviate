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

package spfresh

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

type reassignOperation struct {
	PostingID uint64
	Vector    Vector
}

func (s *SPFresh) enqueueReassign(ctx context.Context, postingID uint64, vector Vector) error {
	if s.ctx == nil {
		return nil // Not started yet
	}

	if err := s.ctx.Err(); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	err := s.operationsQueue.EnqueueReassign(ctx, postingID, vector.ID(), vector.Version())
	if err != nil {
		return errors.Wrapf(err, "failed to enqueue reassign operation for posting %d", postingID)
	}

	s.metrics.EnqueueReassignTask()

	return nil
}

func (s *SPFresh) doReassign(op reassignOperation) error {
	s.metrics.DequeueReassignTask()
	start := time.Now()
	defer s.metrics.ReassignDuration(start)

	// check if the vector is still valid
	version := s.VersionMap.Get(op.Vector.ID())
	if version.Deleted() || version.Version() > op.Vector.Version().Version() {
		return nil
	}

	// perform a RNG selection to determine the postings where the vector should be
	// reassigned to.
	var q []float32
	if s.config.Compressed {
		q = s.quantizer.Restore(op.Vector.(CompressedVector).Data())
	} else {
		q = op.Vector.(*RawVector).Data()
	}
	if q == nil {
		var err error
		q, err = s.config.VectorByIndexID(s.ctx, op.Vector.ID(), s.config.TargetVector)
		if err != nil {
			return errors.Wrap(err, "failed to get vector by index ID")
		}
	}

	replicas, needsReassign, err := s.RNGSelect(q, op.PostingID)
	if err != nil {
		return errors.Wrap(err, "failed to select replicas")
	}
	if !needsReassign {
		return nil
	}

	// check again if the version is still valid
	version = s.VersionMap.Get(op.Vector.ID())
	if version.Deleted() || version.Version() > op.Vector.Version().Version() {
		return nil
	}

	// increment the vector version. this will invalidate all the existing copies
	// of the vector in other postings.
	version, ok := s.VersionMap.Increment(version, op.Vector.ID())
	if !ok {
		// Increment fails if a concurrent Increment happened (similar to a CAS operation)
		s.logger.WithField("vectorID", op.Vector.ID()).
			Debug("vector version increment failed, skipping reassign operation")
		return nil
	}

	// create a new vector with the updated version
	var newVector Vector
	if s.config.Compressed {
		newVector = NewCompressedVector(op.Vector.ID(), version, op.Vector.(CompressedVector).Data())
	} else {
		newVector = NewRawVector(op.Vector.ID(), version, op.Vector.(*RawVector).Data())
	}

	// append the vector to each replica
	for id := range replicas.Iter() {
		version = s.VersionMap.Get(newVector.ID())
		if version.Deleted() || version.Version() > newVector.Version().Version() {
			s.logger.WithField("vectorID", op.Vector.ID()).
				Debug("vector is deleted or has a newer version, skipping reassign operation")
			return nil
		}

		added, err := s.append(s.ctx, newVector, id, true)
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
