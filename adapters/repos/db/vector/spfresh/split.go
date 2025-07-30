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

type splitOperation struct {
	PostingID uint64
}

func (s *SPFresh) enqueueSplit(ctx context.Context, postingID uint64) error {
	if s.ctx == nil {
		return nil // Not started yet
	}

	// Check if the operation is already in progress
	if !s.splitList.tryAdd(postingID) {
		s.Logger.WithField("postingID", postingID).
			Debug("Split operation already enqueued, skipping")
		return nil
	}

	// Enqueue the operation to the channel
	select {
	case s.splitCh <- splitOperation{PostingID: postingID}:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (s *SPFresh) splitWorker() {
	defer s.wg.Done()

	for op := range s.splitCh {
		if s.ctx.Err() != nil {
			return
		}

		err := s.doSplit(op.PostingID)
		if err != nil {
			s.Logger.WithError(err).
				WithField("postingID", op.PostingID).
				Error("Failed to process split operation")
			continue // Log the error and continue processing other operations
		}
	}
}

func (s *SPFresh) doSplit(postingID uint64) error {
	s.postingLocks.Lock(postingID)

	var markedAsDone bool
	defer func() {
		if !markedAsDone {
			s.postingLocks.Unlock(postingID)
			s.splitList.done(postingID)
		}
	}()

	p, err := s.Store.Get(s.ctx, postingID)
	if err != nil {
		if errors.Is(err, ErrPostingNotFound) {
			s.Logger.WithField("postingID", postingID).
				Debug("Posting not found, skipping split operation")
			return nil
		}

		return errors.Wrapf(err, "failed to get posting %d for split operation", postingID)
	}

	// garbage collect the deleted vectors
	filtered := p.GarbageCollect(s.VersionMap)

	// skip if the filtered posting is now too small
	if len(filtered) < int(s.UserConfig.MaxPostingSize) {
		s.Logger.
			WithField("postingID", postingID).
			WithField("size", len(filtered)).
			WithField("max", s.UserConfig.MaxPostingSize).
			Debug("Posting has less than max size after garbage collection, skipping split operation")

		if len(filtered) == len(p) {
			// no changes, just return
			return nil
		}

		// update the size of the posting
		// Note: the page associated to this posting is guaranteed to exist in the
		// postingSizes. no need to check for existence.
		s.PostingSizes.Set(postingID, uint32(len(filtered)))

		// persist the gc'ed posting
		err = s.Store.Put(s.ctx, postingID, filtered)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d after split operation", postingID)
		}

		return nil
	}

	// split the vectors into two clusters
	result, err := s.Splitter.Split(filtered)
	if err != nil || len(result) < 2 {
		if !errors.Is(err, ErrIdenticalVectors) {
			return errors.Wrapf(err, "failed to split vectors for posting %d", postingID)
		}

		// If the split fails because the posting contains identical vectors,
		// we override the posting with a single vector
		s.Logger.WithField("postingID", postingID).
			WithError(err).
			Debug("Cannot split posting: contains identical vectors, keeping only one vector")

		s.PostingSizes.Set(postingID, 1)

		err = s.Store.Put(s.ctx, postingID, Posting{filtered[0]})
		if err != nil {
			return errors.Wrapf(err, "failed to put single vector posting %d after split operation", postingID)
		}

		return nil
	}

	newPostingIDs := make([]uint64, 2)
	var postingReused bool
	for i := range 2 {
		// if the centroid of the existing posting is close enough to one of the new centroids
		// we can reuse the existing posting
		if !postingReused {
			existingCentroid := s.SPTAG.Get(postingID)
			dist, err := s.SPTAG.ComputeDistance(existingCentroid, result[i].Centroid)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for split operation on posting %d", postingID)
			}

			if dist < splitReuseEpsilon {
				postingReused = true
				newPostingIDs[i] = postingID
				s.PostingSizes.Set(postingID, uint32(len(result[i].Posting)))
				err = s.Store.Put(s.ctx, postingID, result[i].Posting)
				if err != nil {
					return errors.Wrapf(err, "failed to put reused posting %d after split operation", postingID)
				}

				continue
			}
		}

		// otherwise, we need to create a new posting for the new centroid
		newPostingID := s.IDs.Next()
		newPostingIDs[i] = newPostingID
		err = s.Store.Put(s.ctx, newPostingID, result[i].Posting)
		if err != nil {
			return errors.Wrapf(err, "failed to put new posting %d after split operation", newPostingID)
		}
		s.PostingSizes.AllocPageFor(newPostingID)
		s.PostingSizes.Set(newPostingID, uint32(len(result[i].Posting)))

		// add the new centroid to the SPTAG index
		err = s.SPTAG.Upsert(newPostingID, result[i].Centroid)
		if err != nil {
			return errors.Wrapf(err, "failed to upsert new centroid %d after split operation", newPostingID)
		}
	}

	if !postingReused {
		err = s.SPTAG.Delete(postingID)
		if err != nil {
			return errors.Wrapf(err, "failed to delete old centroid %d after split operation", postingID)
		}
		s.PostingSizes.Set(postingID, 0) // Mark the old posting as deleted
	}

	// Mark the split operation as done
	markedAsDone = true
	s.postingLocks.Unlock(postingID)
	s.splitList.done(postingID)

	err = s.enqueueReassignAfterSplit(postingID, newPostingIDs, result)
	if err != nil {
		return errors.Wrapf(err, "failed to enqueue reassign after split for posting %d", postingID)
	}

	return nil
}

func (s *SPFresh) enqueueReassignAfterSplit(oldPostingID uint64, newPostingIDs []uint64, newPostings []SplitResult) error {
	oldCentroid := s.SPTAG.Get(oldPostingID)

	reassignedVectors := make(map[uint64]struct{})

	// first check: if a vector is closer to one of the new posting centroid than the old centroid,
	// neighboring centroids cannot be better.
	for i := range newPostings {
		// test each vector
		for _, v := range newPostings[i].Posting {
			_, exists := reassignedVectors[v.ID]
			if !exists && !v.Version.Deleted() && s.VersionMap.Get(v.ID) == v.Version {
				// compute distance from v to its new centroid
				newDist, err := s.SPTAG.ComputeDistance(s.SPTAG.Get(newPostingIDs[i]), v.Data)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", v.ID, newPostingIDs[i])
				}

				// compute distance from v to the old centroid
				oldDist, err := s.SPTAG.ComputeDistance(oldCentroid, v.Data)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in old posting %d", v.ID, oldPostingID)
				}

				if newDist >= oldDist {
					// the vector is closer to the old centroid, which means it may be also closer to a neighboring centroid,
					// we need to reassign it
					err = s.enqueueReassign(s.ctx, newPostingIDs[i], v)
					if err != nil {
						return errors.Wrapf(err, "failed to enqueue reassign for vector %d after split", v.ID)
					}
					reassignedVectors[v.ID] = struct{}{}
				}
			}
		}
	}

	// second check: if a vector from a neighboring centroid is closer to one of the new posting centroids than the old centroid,
	// we need to reassign it.
	if s.UserConfig.ReassignNeighbors <= 0 {
		return nil
	}

	// search for neighboring centroids
	nearest, err := s.SPTAG.Search(oldCentroid, s.UserConfig.ReassignNeighbors)
	if err != nil {
		return errors.Wrapf(err, "failed to search for nearest centroids for reassign after split for posting %d", oldPostingID)
	}

	seen := make(map[uint64]struct{})
	for _, id := range newPostingIDs {
		seen[id] = struct{}{}
	}
	// for each neighboring centroid, check if any of its vectors is closer to one of the new centroids
	for _, neighbor := range nearest {
		_, exists := seen[neighbor.ID]
		if exists {
			continue
		}
		seen[neighbor.ID] = struct{}{}

		p, err := s.Store.Get(s.ctx, neighbor.ID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				s.Logger.WithField("postingID", neighbor.ID).
					Debug("Posting not found, skipping reassign after split")
				continue // Skip if the posting is not found
			}

			return errors.Wrapf(err, "failed to get posting %d for reassign after split", neighbor.ID)
		}

		for _, v := range p {
			_, exists := reassignedVectors[v.ID]
			if !exists && !v.Version.Deleted() && s.VersionMap.Get(v.ID) == v.Version {
				distNeighbor, err := s.SPTAG.ComputeDistance(s.SPTAG.Get(neighbor.ID), v.Data)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in neighbor posting %d", v.ID, neighbor.ID)
				}

				distOld, err := s.SPTAG.ComputeDistance(oldCentroid, v.Data)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in old posting %d", v.ID, oldPostingID)
				}

				distA0, err := s.SPTAG.ComputeDistance(s.SPTAG.Get(newPostingIDs[0]), v.Data)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", v.ID, newPostingIDs[0])
				}

				distA1, err := s.SPTAG.ComputeDistance(s.SPTAG.Get(newPostingIDs[1]), v.Data)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", v.ID, newPostingIDs[1])
				}

				if distOld <= distA0 && distOld <= distA1 {
					// the vector is closer to the old centroid, which means the new postings are not better than its current posting
					continue
				}

				if distNeighbor < distA0 && distNeighbor < distA1 {
					// the vector is closer to its current centroid than to the new centroids,
					// no need to reassign it
					continue
				}

				// the vector is closer to one of the new centroids, it needs to be reassigned
				err = s.enqueueReassign(s.ctx, neighbor.ID, v)
				if err != nil {
					return errors.Wrapf(err, "failed to enqueue reassign for vector %d after split", v.ID)
				}
				reassignedVectors[v.ID] = struct{}{}
			}
		}
	}

	return nil
}
