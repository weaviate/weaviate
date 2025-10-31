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

func (s *SPFresh) doMerge(postingID uint64) error {
	s.metrics.DequeueMergeTask()
	start := time.Now()
	defer s.metrics.MergeDuration(start)

	defer s.taskQueue.MergeDone(postingID)

	s.logger.WithField("postingID", postingID).Debug("Merging posting")

	var markedAsDone bool
	if !s.postingLocks.TryLock(postingID) {
		// another merge operation is in progress for this posting
		// re-enqueue the operation to be processed later
		s.taskQueue.MergeDone(postingID) // remove from the in-progress list
		return s.taskQueue.EnqueueMerge(s.ctx, postingID)
	}
	defer func() {
		if !markedAsDone {
			s.postingLocks.Unlock(postingID)
		}
	}()

	// Ensure the posting exists in the index
	if !s.Centroids.Exists(postingID) {
		s.logger.WithField("postingID", postingID).
			Debug("Posting not found, skipping merge operation")
		return nil // Nothing to merge
	}

	p, err := s.PostingStore.Get(s.ctx, postingID)
	if err != nil {
		if errors.Is(err, ErrPostingNotFound) {
			s.logger.WithField("postingID", postingID).
				Debug("Posting not found, skipping merge operation")
			return nil
		}

		return errors.Wrapf(err, "failed to get posting %d for merge operation", postingID)
	}

	initialLen := p.Len()

	// garbage collect the deleted vectors
	newPosting := p.GarbageCollect(s.VersionMap)
	prevLen := newPosting.Len()

	// skip if the posting is big enough
	if prevLen >= int(s.minPostingSize) {
		s.logger.
			WithField("postingID", postingID).
			WithField("size", prevLen).
			WithField("min", s.minPostingSize).
			Debug("Posting is big enough, skipping merge operation")

		if prevLen == initialLen {
			// no changes, just return
			return nil
		}

		// persist the gc'ed posting
		err = s.PostingStore.Put(s.ctx, postingID, newPosting)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d", postingID)
		}
		// update the size of the posting after successful Persist
		return s.PostingSizes.Set(context.TODO(), postingID, uint32(prevLen))
	}

	vectorSet := make(map[uint64]struct{})
	for _, v := range p.Iter() {
		vectorSet[v.ID()] = struct{}{}
	}

	// get posting centroid
	oldCentroid := s.Centroids.Get(postingID)
	if oldCentroid == nil {
		return errors.Errorf("centroid not found for posting %d", postingID)
	}

	// search for the closest centroids
	nearest, err := s.Centroids.Search(oldCentroid.Uncompressed, s.config.InternalPostingCandidates)
	if err != nil {
		return errors.Wrapf(err, "failed to search for nearest centroid for posting %d", postingID)
	}

	if nearest.Len() <= 1 {
		s.logger.WithField("postingID", postingID).
			Debug("No candidates found for merge operation, skipping")

		// persist the gc'ed posting
		err = s.PostingStore.Put(s.ctx, postingID, newPosting)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d", postingID)
		}
		// update the size of the posting after successful Persist
		err = s.PostingSizes.Set(context.TODO(), postingID, uint32(prevLen))
		if err != nil {
			return errors.Wrapf(err, "failed to set size of posting %d to %d", postingID, prevLen)
		}

		return nil
	}

	// first centroid is the query centroid, the rest are candidates for merging
	for candidateID := range nearest.Iter() {
		// check if the combined size of the postings is within limits
		count, err := s.PostingSizes.Get(context.TODO(), candidateID)
		if err != nil {
			return errors.Wrapf(err, "failed to get posting size for candidate %d", candidateID)
		}
		if int(count)+prevLen > int(s.maxPostingSize) || s.taskQueue.MergeContains(candidateID) {
			continue // Skip this candidate
		}

		// lock the candidate posting to ensure no concurrent modifications
		// note: the candidate lock might be the same as the current posting lock
		// so we need to ensure we don't deadlock
		if s.postingLocks.Hash(postingID) != s.postingLocks.Hash(candidateID) {
			// lock the candidate posting
			s.postingLocks.Lock(candidateID)
			defer func() {
				if !markedAsDone {
					s.postingLocks.Unlock(candidateID)
				}
			}()
		}

		// get the candidate posting
		candidate, err := s.PostingStore.Get(s.ctx, candidateID)
		if err != nil {
			return errors.Wrapf(err, "failed to get candidate posting %d for merge operation on posting %d", candidateID, postingID)
		}

		var candidateLen int
		for _, v := range candidate.Iter() {
			version := s.VersionMap.Get(v.ID())
			if version.Deleted() || version.Version() > v.Version().Version() {
				continue
			}
			if _, exists := vectorSet[v.ID()]; exists {
				continue // Skip duplicate vectors
			}
			newPosting.AddVector(v)
			candidateLen++
		}

		// delete the smallest posting and update the large posting
		smallID, largeID := postingID, candidateID
		smallPosting := p
		if prevLen > candidateLen {
			smallID, largeID = candidateID, postingID
			smallPosting = candidate
		}

		// mark the small posting as deleted in the SPTAG
		err = s.Centroids.MarkAsDeleted(smallID)
		if err != nil {
			return errors.Wrapf(err, "failed to delete centroid for posting %d", smallID)
		}

		// persist the merged posting first
		err = s.PostingStore.Put(s.ctx, largeID, newPosting)
		if err != nil {
			return errors.Wrapf(err, "failed to put merged posting %d after merge operation", postingID)
		}

		// set the small posting size to 0 and update the large posting size only after successful persist
		err = s.PostingSizes.Set(context.TODO(), smallID, 0)
		if err != nil {
			return errors.Wrapf(err, "failed to set size of merged posting %d to 0", smallID)
		}
		err = s.PostingSizes.Set(context.TODO(), largeID, uint32(newPosting.Len()))
		if err != nil {
			return errors.Wrapf(err, "failed to set size of merged posting %d to %d", largeID, newPosting.Len())
		}

		// mark the operation as done and unlock everything
		markedAsDone = true
		if s.postingLocks.Hash(postingID) != s.postingLocks.Hash(candidateID) {
			s.postingLocks.Unlock(candidateID)
		}
		s.postingLocks.Unlock(postingID)

		// if merged vectors are closer to their old centroid than the new one
		// there may be better centroids for them out there.
		// we need to reassign them in the background.
		smallCentroid := s.Centroids.Get(smallID)
		largeCentroid := s.Centroids.Get(largeID)
		for _, v := range smallPosting.Iter() {
			prevDist, err := smallCentroid.Distance(s.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in small posting %d", v.ID(), smallID)
			}

			newDist, err := largeCentroid.Distance(s.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in large posting %d", v.ID(), largeID)
			}

			if prevDist < newDist {
				// the vector is closer to the old centroid, we need to reassign it
				err = s.taskQueue.EnqueueReassign(s.ctx, largeID, v.ID(), v.Version())
				if err != nil {
					return errors.Wrapf(err, "failed to enqueue reassign for vector %d after merge", v.ID())
				}
			}
		}

		return nil
	}

	// if no candidates were found, just persist the gc'ed posting
	s.PostingSizes.Set(context.TODO(), postingID, uint32(newPosting.Len()))
	err = s.PostingStore.Put(s.ctx, postingID, newPosting)
	if err != nil {
		return errors.Wrapf(err, "failed to put filtered posting %d", postingID)
	}

	return nil
}
