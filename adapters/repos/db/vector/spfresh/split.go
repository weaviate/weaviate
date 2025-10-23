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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

func (s *SPFresh) enqueueSplit(ctx context.Context, postingID uint64) error {
	if s.ctx == nil {
		return nil // Not started yet
	}

	if err := s.ctx.Err(); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// Check if the operation is already in progress
	if !s.splitList.tryAdd(postingID) {
		return nil
	}

	// Enqueue the operation to the channel
	//s.splitCh.Push(postingID)
	err := s.operationsQueue.EnqueueSplit(ctx, postingID)
	if err != nil {
		return errors.Wrapf(err, "failed to enqueue split operation for posting %d", postingID)
	}

	s.metrics.EnqueueSplitTask()

	return nil
}

/*func (s *SPFresh) splitWorker() {
	defer s.wg.Done()

	for postingID := range s.splitCh.Out() {
		if s.ctx.Err() != nil {
			return
		}

		s.metrics.DequeueSplitTask()

		err := s.doSplit(postingID, true)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}

			s.logger.WithError(err).
				WithField("postingID", postingID).
				Error("Failed to process split operation")
			continue // Log the error and continue processing other operations
		}
	}
}*/

func abs(a float32) float32 {
	if a < 0 {
		return -a
	}
	return a
}

// doSplit performs the actual split operation for a given postingID.
// If reassign is true, it will enqueue reassign operations for vectors that
// may need to be moved to other postings after the split.
func (s *SPFresh) doSplit(postingID uint64, reassign bool) error {
	start := time.Now()
	defer s.metrics.SplitDuration(start)

	s.postingLocks.Lock(postingID)

	var markedAsDone bool
	defer func() {
		if !markedAsDone {
			s.postingLocks.Unlock(postingID)
			s.splitList.done(postingID)
		}
	}()

	if !s.Centroids.Exists(postingID) {
		return nil
	}

	// load the posting from disk
	p, err := s.Store.Get(s.ctx, postingID)
	if err != nil {
		if errors.Is(err, ErrPostingNotFound) {
			s.logger.WithField("postingID", postingID).
				Debug("Posting not found, skipping split operation")
			return nil
		}

		return errors.Wrapf(err, "failed to get posting %d for split operation", postingID)
	}

	// garbage collect the deleted vectors
	lp := p.Len()
	filtered := p.GarbageCollect(s.VersionMap)

	// skip if the filtered posting is now too small
	if lf := filtered.Len(); lf < int(s.maxPostingSize) {
		if lf == lp {
			// no changes, just return
			return nil
		}

		// persist the gc'ed posting
		err = s.Store.Put(s.ctx, postingID, filtered)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d after split operation", postingID)
		}

		s.PostingSizes.Set(postingID, uint32(lf))
		return nil
	}

	// split the vectors into two clusters
	result, err := s.splitPosting(filtered)
	if err != nil || len(result) < 2 {
		if !errors.Is(err, ErrIdenticalVectors) {
			return errors.Wrapf(err, "failed to split vectors for posting %d", postingID)
		}

		// If the split fails because the posting contains identical vectors,
		// we override the posting with a single vector
		s.logger.WithField("postingID", postingID).
			WithError(err).
			Debug("cannot split posting: contains identical vectors, keeping only one vector")

		pp := &EncodedPosting{
			vectorSize: int(s.vectorSize),
			compressed: s.config.Compressed,
		}
		pp.AddVector(filtered.GetAt(0))
		err = s.Store.Put(s.ctx, postingID, pp)
		if err != nil {
			return errors.Wrapf(err, "failed to put single vector posting %d after split operation", postingID)
		}
		// update posting size after successful persist
		s.PostingSizes.Set(postingID, 1)

		return nil
	}

	newPostingIDs := make([]uint64, 2)
	var postingReused bool
	for i := range 2 {
		// if the centroid of the existing posting is close enough to one of the new centroids
		// we can reuse the existing posting
		if !postingReused {
			existingCentroid := s.Centroids.Get(postingID)
			dist, err := s.distancer.DistanceBetweenVectors(existingCentroid.Uncompressed, result[i].Uncompressed)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for split operation on posting %d", postingID)
			}

			if abs(dist) < splitReuseEpsilon {
				s.logger.WithField("postingID", postingID).
					WithField("distance", dist).
					Debug("reusing existing posting for split operation")
				postingReused = true
				newPostingIDs[i] = postingID
				err = s.Store.Put(s.ctx, postingID, result[i].Posting)
				if err != nil {
					return errors.Wrapf(err, "failed to put reused posting %d after split operation", postingID)
				}
				// update posting size after successful persist
				s.PostingSizes.Set(postingID, uint32(result[i].Posting.Len()))

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
		// allocate and set posting size after successful persist
		s.PostingSizes.AllocPageFor(newPostingID)
		s.PostingSizes.Set(newPostingID, uint32(result[i].Posting.Len()))

		// add the new centroid to the SPTAG index
		err = s.Centroids.Insert(newPostingID, &Centroid{
			Uncompressed: result[i].Uncompressed,
			Compressed:   result[i].Centroid,
			Deleted:      false,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to upsert new centroid %d after split operation", newPostingID)
		}
	}

	if !postingReused {
		// delete the old centroid if it wasn't reused
		err = s.Centroids.MarkAsDeleted(postingID)
		if err != nil {
			return errors.Wrapf(err, "failed to delete old centroid %d after split operation", postingID)
		}
		s.PostingSizes.Set(postingID, 0)
	}

	// Mark the split operation as done
	markedAsDone = true
	s.postingLocks.Unlock(postingID)
	s.splitList.done(postingID)

	if !reassign {
		return nil
	}

	err = s.enqueueReassignAfterSplit(postingID, newPostingIDs, result)
	if err != nil {
		return errors.Wrapf(err, "failed to enqueue reassign after split for posting %d", postingID)
	}

	return nil
}

// splitPosting takes a posting and returns two groups.
// If the clustering fails because of the content of the posting,
// it returns ErrIdenticalVectors.
func (s *SPFresh) splitPosting(posting Posting) ([]SplitResult, error) {
	enc := compressionhelpers.NewKMeansEncoder(2, int(s.dims), 0)

	var data [][]float32
	if cp, ok := posting.(*EncodedPosting); ok {
		data = cp.Uncompress(s.quantizer)
	}

	err := enc.Fit(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fit KMeans encoder for split operation")
	}

	results := make([]SplitResult, 2)
	for i := range results {
		results[i] = SplitResult{
			Uncompressed: enc.Centroid(byte(i)),
			Posting: &EncodedPosting{
				vectorSize: int(s.vectorSize),
				compressed: s.config.Compressed,
			},
		}

		if s.config.Compressed {
			results[i].Centroid = s.quantizer.Encode(enc.Centroid(byte(i)))
		}
	}

	for i, v := range data {
		// compute the distance to each centroid
		dA, err := s.distancer.DistanceBetweenVectors(v, results[0].Uncompressed)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to compute distance to centroid 0")
		}
		dB, err := s.distancer.DistanceBetweenVectors(v, results[1].Uncompressed)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to compute distance to centroid 1")
		}
		if dA < dB {
			results[0].Posting.AddVector(posting.GetAt(i))
		} else {
			results[1].Posting.AddVector(posting.GetAt(i))
		}
	}

	// check if the clustering failed because the vectors are identical
	if results[0].Posting.Len() == 0 || results[1].Posting.Len() == 0 {
		return nil, ErrIdenticalVectors
	}

	return results, nil
}

type SplitResult struct {
	Centroid     []byte
	Uncompressed []float32
	Posting      Posting
}

func (s *SPFresh) enqueueReassignAfterSplit(oldPostingID uint64, newPostingIDs []uint64, newPostings []SplitResult) error {
	oldCentroid := s.Centroids.Get(oldPostingID)

	reassignedVectors := make(map[uint64]struct{})

	// first check: if a vector is closer to one of the new posting centroid than the old centroid,
	// neighboring centroids cannot be better.
	for i := range newPostings {
		// test each vector
		for _, v := range newPostings[i].Posting.Iter() {
			vid := v.ID()
			_, exists := reassignedVectors[vid]
			if !exists && !v.Version().Deleted() && s.VersionMap.Get(vid) == v.Version() {
				// compute distance from v to its new centroid
				newDist, err := s.Centroids.Get(newPostingIDs[i]).Distance(s.distancer, v)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", vid, newPostingIDs[i])
				}

				// compute distance from v to the old centroid
				oldDist, err := oldCentroid.Distance(s.distancer, v)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in old posting %d", vid, oldPostingID)
				}

				if newDist >= oldDist {
					// the vector is closer to the old centroid, which means it may be also closer to a neighboring centroid,
					// we need to reassign it
					err = s.enqueueReassign(s.ctx, newPostingIDs[i], v)
					if err != nil {
						return errors.Wrapf(err, "failed to enqueue reassign for vector %d after split", vid)
					}
					reassignedVectors[vid] = struct{}{}
				}
			}
		}
	}

	// second check: if a vector from a neighboring centroid is closer to one of the new posting centroids than the old centroid,
	// we need to reassign it.
	if s.config.ReassignNeighbors <= 0 {
		return nil
	}

	// search for neighboring centroids
	nearest, err := s.Centroids.Search(oldCentroid.Uncompressed, s.config.ReassignNeighbors)
	if err != nil {
		return errors.Wrapf(err, "failed to search for nearest centroids for reassign after split for posting %d", oldPostingID)
	}

	seen := make(map[uint64]struct{})
	for _, id := range newPostingIDs {
		seen[id] = struct{}{}
	}
	// for each neighboring centroid, check if any of its vectors is closer to one of the new centroids
	for neighborID := range nearest.Iter() {
		_, exists := seen[neighborID]
		if exists {
			continue
		}
		seen[neighborID] = struct{}{}

		p, err := s.Store.Get(s.ctx, neighborID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				s.logger.WithField("postingID", neighborID).
					Debug("Posting not found, skipping reassign after split")
				continue // Skip if the posting is not found
			}

			return errors.Wrapf(err, "failed to get posting %d for reassign after split", neighborID)
		}

		for _, v := range p.Iter() {
			vid := v.ID()
			version := v.Version()
			_, exists := reassignedVectors[vid]
			if exists || version.Deleted() || s.VersionMap.Get(vid) != version {
				continue
			}

			distNeighbor, err := s.Centroids.Get(neighborID).Distance(s.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in neighbor posting %d", vid, neighborID)
			}

			distOld, err := oldCentroid.Distance(s.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in old posting %d", vid, oldPostingID)
			}

			distA0, err := s.Centroids.Get(newPostingIDs[0]).Distance(s.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", vid, newPostingIDs[0])
			}

			distA1, err := s.Centroids.Get(newPostingIDs[1]).Distance(s.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", vid, newPostingIDs[1])
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
			err = s.enqueueReassign(s.ctx, neighborID, v)
			if err != nil {
				return errors.Wrapf(err, "failed to enqueue reassign for vector %d after split", vid)
			}
			reassignedVectors[vid] = struct{}{}
		}
	}

	return nil
}
