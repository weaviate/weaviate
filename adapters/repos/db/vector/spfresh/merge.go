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

func (s *SPFresh) EnqueueMerge(ctx context.Context, postingID uint64) error {
	if s.ctx == nil {
		return nil // Not started yet
	}

	// Check if the operation is already in progress
	if !s.mergeList.tryEnqueue(postingID) {
		s.Logger.WithField("postingID", postingID).
			Debug("Merge operation already enqueued, skipping")
		return nil
	}

	// Enqueue the operation to the channel
	select {
	case s.mergeCh <- MergeOperation{PostingID: postingID}:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (s *SPFresh) mergeWorker() {
	defer s.wg.Done()

	for op := range s.mergeCh {
		if s.ctx.Err() != nil {
			return // Exit if the context is cancelled
		}

		err := s.doMerge(op)
		if err != nil {
			s.Logger.WithError(err).
				WithField("postingID", op.PostingID).
				Error("Failed to process merge operation")
			continue // Log the error and continue processing other operations
		}
	}
}

func (s *SPFresh) doMerge(op MergeOperation) error {
	defer s.mergeList.done(op.PostingID)

	var markedAsDone bool
	s.postingLocks.Lock(op.PostingID)
	defer func() {
		if !markedAsDone {
			s.postingLocks.Unlock(op.PostingID)
		}
	}()

	// Ensure the posting exists in the index
	if !s.SPTAG.Exists(op.PostingID) {
		s.Logger.WithField("postingID", op.PostingID).
			Debug("Posting not found, skipping merge operation")
		return nil // Nothing to merge
	}

	p, err := s.Store.Get(s.ctx, op.PostingID)
	if err != nil {
		if err == ErrPostingNotFound {
			s.Logger.WithField("postingID", op.PostingID).
				Debug("Posting not found, skipping merge operation")
			return nil
		}

		return errors.Wrapf(err, "failed to get posting %d for merge operation", op.PostingID)
	}

	// garbage collect the deleted vectors
	vectorSet := make(map[uint64]struct{})
	merged := make(Posting, 0, len(p))
	for _, v := range p {
		version := s.VersionMap.Get(v.ID)
		if version.Deleted() || version.Version() > v.Version.Version() {
			continue
		}
		merged = append(merged, Vector{
			ID:      v.ID,
			Version: version,
			Data:    v.Data,
		})
		vectorSet[v.ID] = struct{}{}
	}
	prevLen := len(merged)

	// skip if the posting is big enough
	if len(merged) >= int(s.UserConfig.MinPostingSize) {
		s.Logger.
			WithField("postingID", op.PostingID).
			WithField("size", len(merged)).
			WithField("min", s.UserConfig.MinPostingSize).
			Debug("Posting is big enough, skipping merge operation")

		if len(merged) == len(p) {
			// no changes, just return
			return nil
		}

		// update the size of the posting
		s.PostingSizes.Set(op.PostingID, uint32(len(merged)))

		// persist the gc'ed posting
		err = s.Store.Put(s.ctx, op.PostingID, merged)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d", op.PostingID)
		}

		return nil
	}

	// get posting centroid
	oldCentroid := s.SPTAG.Get(op.PostingID)
	if oldCentroid == nil {
		return errors.Errorf("centroid not found for posting %d", op.PostingID)
	}

	// search for the closest centroids
	nearest, err := s.SPTAG.Search(oldCentroid, s.UserConfig.InternalPostingCandidates)
	if err != nil {
		return errors.Wrapf(err, "failed to search for nearest centroid for posting %d", op.PostingID)
	}

	if len(nearest) <= 1 {
		s.Logger.WithField("postingID", op.PostingID).
			Debug("No candidates found for merge operation, skipping")

		// update the size of the posting
		s.PostingSizes.Set(op.PostingID, uint32(len(merged)))

		// persist the gc'ed posting
		err = s.Store.Put(s.ctx, op.PostingID, merged)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d", op.PostingID)
		}

		return nil
	}

	// first centroid is the query centroid, the rest are candidates for merging
	for i := 1; i < len(nearest); i++ {
		// check if the combined size of the postings is within limits
		count := s.PostingSizes.Get(nearest[i].ID)
		if int(count)+len(merged) > int(s.UserConfig.MaxPostingSize) || s.mergeList.contains(nearest[i].ID) {
			continue // Skip this candidate
		}

		// lock the candidate posting to ensure no concurrent modifications
		// note: the candidate lock might be the same as the current posting lock
		// so we need to ensure we don't deadlock
		if s.postingLocks.Hash(op.PostingID) != s.postingLocks.Hash(nearest[i].ID) {
			// lock the candidate posting
			s.postingLocks.Lock(nearest[i].ID)
			defer func() {
				if !markedAsDone {
					s.postingLocks.Unlock(nearest[i].ID)
				}
			}()
		}

		// get the candidate posting
		candidate, err := s.Store.Get(s.ctx, nearest[i].ID)
		if err != nil {
			return errors.Wrapf(err, "failed to get candidate posting %d for merge operation on posting %d", nearest[i].ID, op.PostingID)
		}

		var candidateLen int
		for _, v := range candidate {
			version := s.VersionMap.Get(v.ID)
			if version.Deleted() || version.Version() > v.Version.Version() {
				continue
			}
			if _, exists := vectorSet[v.ID]; exists {
				continue // Skip duplicate vectors
			}
			merged = append(merged, Vector{
				ID:      v.ID,
				Version: version,
				Data:    v.Data,
			})
			candidateLen++
		}

		// delete the smallest posting and update the large posting
		smallID, largeID := op.PostingID, nearest[i].ID
		smallPosting := p
		if prevLen > candidateLen {
			smallID, largeID = nearest[i].ID, op.PostingID
			smallPosting = candidate
		}

		err = s.SPTAG.Delete(smallID)
		if err != nil {
			return errors.Wrapf(err, "failed to delete centroid for posting %d", smallID)
		}

		err = s.Store.Put(s.ctx, largeID, merged)
		if err != nil {
			return errors.Wrapf(err, "failed to put merged posting %d after merge operation", op.PostingID)
		}

		// set the small posting size to 0 and update the large posting size
		s.PostingSizes.Set(smallID, 0)
		s.PostingSizes.Set(largeID, uint32(len(merged)))

		// mark the operation as done and unlock everything
		markedAsDone = true
		if s.postingLocks.Hash(op.PostingID) != s.postingLocks.Hash(nearest[i].ID) {
			s.postingLocks.Unlock(nearest[i].ID)
		}
		s.postingLocks.Unlock(op.PostingID)

		// if merged vectors are closer to their old centroid than the new one
		// there may be better centroids for them out there.
		// we need to reassign them in the background.
		smallCentroid := s.SPTAG.Get(smallID)
		largeCentroid := s.SPTAG.Get(largeID)
		for _, v := range smallPosting {
			prevDist, err := s.SPTAG.ComputeDistance(smallCentroid, v.Data)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in small posting %d", v.ID, smallID)
			}

			newDist, err := s.SPTAG.ComputeDistance(largeCentroid, v.Data)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in large posting %d", v.ID, largeID)
			}

			if prevDist < newDist {
				// the vector is closer to the old centroid, we need to reassign it
				err = s.enqueueReassign(s.ctx, largeID, v)
				if err != nil {
					return errors.Wrapf(err, "failed to enqueue reassign for vector %d after merge", v.ID)
				}
			}
		}

		return nil
	}

	// if no candidates were found, just persist the gc'ed posting
	s.PostingSizes.Set(op.PostingID, uint32(len(merged)))
	err = s.Store.Put(s.ctx, op.PostingID, merged)
	if err != nil {
		return errors.Wrapf(err, "failed to put filtered posting %d", op.PostingID)
	}

	return nil
}
