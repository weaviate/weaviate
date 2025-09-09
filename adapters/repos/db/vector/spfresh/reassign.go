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

	// Enqueue the operation to the channel
	select {
	case s.reassignCh <- reassignOperation{PostingID: postingID, Vector: vector}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}

	return nil
}

func (s *SPFresh) reassignWorker() {
	defer s.wg.Done()

	for op := range s.reassignCh {
		if s.ctx.Err() != nil {
			return
		}

		err := s.doReassign(op)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}

			s.Logger.WithError(err).
				WithField("vectorID", op.Vector.ID).
				Error("Failed to process reassign operation")
			continue // Log the error and continue processing other operations
		}
	}
}

func (s *SPFresh) doReassign(op reassignOperation) error {
	// check if the vector is still valid
	version := s.VersionMap.Get(op.Vector.ID())
	if version.Deleted() || version.Version() > op.Vector.Version().Version() {
		s.Logger.WithField("vectorID", op.Vector.ID()).
			Debug("Vector is deleted or has a newer version, skipping reassign operation")
		return nil
	}

	// perform a RNG selection to determine the postings where the vector should be
	// reassigned to.
	replicas, needsReassign, err := s.selectReplicas(op.Vector, op.PostingID)
	if err != nil {
		return errors.Wrap(err, "failed to select replicas")
	}
	if !needsReassign {
		s.Logger.WithField("vectorID", op.Vector.ID()).
			Debug("Vector is already assigned to the best posting, skipping reassign operation")
		return nil
	}
	// check again if the version is still valid
	version = s.VersionMap.Get(op.Vector.ID())
	if version.Deleted() || version.Version() > op.Vector.Version().Version() {
		s.Logger.WithField("vectorID", op.Vector.ID()).
			Debug("Vector is deleted or has a newer version, skipping reassign operation")
		return nil
	}

	// append the vector to each replica
	for _, replica := range replicas {
		ok, err := s.append(s.ctx, op.Vector, replica, true)
		if err != nil {
			return err
		}
		if !ok {
			// s.Logger.WithField("vectorID", op.Vector.ID()).
			// 	WithField("postingID", replica.ID).
			// 	Debug("Posting no longer exists, reassigning")
			continue // Skip if the vector already exists in the replica
		}
	}

	return nil
}

func (s *SPFresh) selectReplicas(query []byte, unless uint64) ([]SearchResult, bool, error) {
	results, err := s.SPTAG.Search(query, s.Config.InternalPostingCandidates)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to search for nearest neighbors")
	}

	replicas := make([]SearchResult, 0, s.Config.Replicas)

LOOP:
	for i := 0; i < len(results) && len(replicas) < s.Config.Replicas; i++ {
		candidate := results[i]

		// determine if the candidate is too close to a pre-existing replica
		for j := range replicas {
			dist, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(s.SPTAG.Get(results[i].ID).Vector, s.SPTAG.Get(replicas[j].ID).Vector)
			if err != nil {
				return nil, false, errors.Wrapf(err, "failed to compute distance for edge %d -> %d", results[i].ID, replicas[j].ID)
			}

			if s.Config.RNGFactor*dist <= candidate.Distance {
				continue LOOP
			}
		}

		// if unless is specified, abort the RNG selection if one of the replicas
		// is the unless posting ID (i.e. the vector is already assigned to this posting)
		if unless != 0 && candidate.ID == unless {
			return nil, false, nil
		}

		replicas = append(replicas, candidate)
	}

	return replicas, true, nil
}
