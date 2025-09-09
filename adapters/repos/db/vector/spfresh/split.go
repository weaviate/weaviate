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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

type splitOperation struct {
	PostingID uint64
}

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
	select {
	case s.splitCh <- splitOperation{PostingID: postingID}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}

	return nil
}

func (s *SPFresh) splitWorker() {
	defer s.wg.Done()

	for op := range s.splitCh {
		if s.ctx.Err() != nil {
			return
		}

		err := s.doSplit(op.PostingID, true)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}

			s.Logger.WithError(err).
				WithField("postingID", op.PostingID).
				Error("Failed to process split operation")
			continue // Log the error and continue processing other operations
		}
	}
}

func (s *SPFresh) doSplit(postingID uint64, reassign bool) error {
	s.postingLocks.Lock(postingID)

	var markedAsDone bool
	defer func() {
		if !markedAsDone {
			s.postingLocks.Unlock(postingID)
			s.splitList.done(postingID)
		}
	}()

	if s.SPTAG.IsMarkedAsDeleted(postingID) {
		// s.Logger.WithField("postingID", postingID).
		// 	Debug("Posting is marked as deleted, skipping split operation")
		return nil
	}

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
	lp := p.Len()
	filtered := p.GarbageCollect(s.VersionMap)

	// skip if the filtered posting is now too small
	if lf := filtered.Len(); lf < int(s.Config.MaxPostingSize) {
		s.Logger.
			WithField("postingID", postingID).
			WithField("size", lf).
			WithField("initialSize", lp).
			WithField("max", s.Config.MaxPostingSize).
			Debug("Posting has less than max size after garbage collection, skipping split operation")

		if lf == lp {
			// no changes, just return
			return nil
		}

		// persist the gc'ed posting
		err = s.Store.Put(s.ctx, postingID, filtered)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d after split operation", postingID)
		}
		// Note: the page associated to this posting is guaranteed to exist in the
		// postingSizes. Update the size only after successful persist.
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
		s.Logger.WithField("postingID", postingID).
			WithError(err).
			Debug("Cannot split posting: contains identical vectors, keeping only one vector")

		err = s.Store.Put(s.ctx, postingID, &Posting{
			vectorSize: filtered.vectorSize,
			data:       filtered.GetAt(0),
		})
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
			existingCentroid := s.SPTAG.Get(postingID)
			dist, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(existingCentroid.Vector, result[i].Centroid)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for split operation on posting %d", postingID)
			}

			if dist < splitReuseEpsilon {
				s.Logger.WithField("postingID", postingID).
					Debug("Reusing existing posting for split operation")
				postingReused = true
				newPostingIDs[i] = postingID
				err = s.Store.Put(s.ctx, postingID, &result[i].Posting)
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
		err = s.Store.Put(s.ctx, newPostingID, &result[i].Posting)
		if err != nil {
			return errors.Wrapf(err, "failed to put new posting %d after split operation", newPostingID)
		}
		// allocate and set posting size after successful persist
		s.PostingSizes.AllocPageFor(newPostingID)
		s.PostingSizes.Set(newPostingID, uint32(result[i].Posting.Len()))

		// add the new centroid to the SPTAG index
		err = s.SPTAG.Upsert(newPostingID, &Centroid{
			Vector: result[i].Centroid,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to upsert new centroid %d after split operation", newPostingID)
		}

		s.Logger.WithField("oldPostingID", postingID).
			WithField("postingID", newPostingID).
			Debug("Created new posting for split operation")
	}

	if !postingReused {
		err = s.SPTAG.MarkAsDeleted(postingID)
		if err != nil {
			return errors.Wrapf(err, "failed to delete old centroid %d after split operation", postingID)
		}
		s.PostingSizes.Set(postingID, 0) // Mark the old posting as deleted
		s.Logger.WithField("postingID", postingID).
			Debug("Old posting deleted after split operation")
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
func (s *SPFresh) splitPosting(posting *Posting) ([]SplitResult, error) {
	enc := compressionhelpers.NewKMeansEncoder(2, int(s.dims), 0)

	data := make([][]float32, 0, posting.Len())
	for _, v := range posting.Iter() {
		data = append(data, s.SPTAG.Quantizer().Restore(v.Data()))
	}

	err := enc.Fit(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fit KMeans encoder for split operation")
	}

	results := make([]SplitResult, 2)
	for i := range results {
		results[i] = SplitResult{
			Centroid: s.SPTAG.Quantizer().Encode(enc.Centroid(byte(i))),
			Posting: Posting{
				vectorSize: int(s.vectorSize),
				data:       make([]byte, 0, len(posting.data)),
			},
		}
	}

	for _, v := range posting.Iter() {
		// compute the distance to each centroid
		dA, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(v, results[0].Centroid)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to compute distance for vector %d to centroid 0", v.ID())
		}
		dB, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(v, results[1].Centroid)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to compute distance for vector %d to centroid 1", v.ID())
		}
		if dA < dB {
			results[0].Posting.data = append(results[0].Posting.data, v...)
		} else {
			results[1].Posting.data = append(results[1].Posting.data, v...)
		}
	}

	// check if the clustering failed because the vectors are identical
	if results[0].Posting.Len() == 0 || results[1].Posting.Len() == 0 {
		return nil, ErrIdenticalVectors
	}

	return results, nil
}

type SplitResult struct {
	Centroid []byte
	Posting  Posting
}

func (s *SPFresh) enqueueReassignAfterSplit(oldPostingID uint64, newPostingIDs []uint64, newPostings []SplitResult) error {
	oldCentroid := s.SPTAG.Get(oldPostingID)

	reassignedVectors := make(map[uint64]struct{})

	// first check: if a vector is closer to one of the new posting centroid than the old centroid,
	// neighboring centroids cannot be better.
	for i := range newPostings {
		// test each vector
		for _, v := range newPostings[i].Posting.Iter() {
			vid := v.ID()
			data := v.Data()
			_, exists := reassignedVectors[vid]
			if !exists && !v.Version().Deleted() && s.VersionMap.Get(vid) == v.Version() {
				// compute distance from v to its new centroid
				newDist, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(s.SPTAG.Get(newPostingIDs[i]).Vector, data)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", vid, newPostingIDs[i])
				}

				// compute distance from v to the old centroid
				oldDist, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(oldCentroid.Vector, data)
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
	if s.Config.ReassignNeighbors <= 0 {
		return nil
	}

	// search for neighboring centroids
	nearest, err := s.SPTAG.Search(oldCentroid.Vector, s.Config.ReassignNeighbors)
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

		for _, v := range p.Iter() {
			vid := v.ID()
			version := v.Version()
			data := v.Data()
			_, exists := reassignedVectors[vid]
			if exists || version.Deleted() || s.VersionMap.Get(vid) != version {
				continue
			}

			distNeighbor, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(s.SPTAG.Get(neighbor.ID).Vector, data)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in neighbor posting %d", vid, neighbor.ID)
			}

			distOld, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(oldCentroid.Vector, data)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in old posting %d", vid, oldPostingID)
			}

			distA0, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(s.SPTAG.Get(newPostingIDs[0]).Vector, data)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", vid, newPostingIDs[0])
			}

			distA1, err := s.SPTAG.Quantizer().DistanceBetweenCompressedVectors(s.SPTAG.Get(newPostingIDs[1]).Vector, data)
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
			err = s.enqueueReassign(s.ctx, neighbor.ID, v)
			if err != nil {
				return errors.Wrapf(err, "failed to enqueue reassign for vector %d after split", vid)
			}
			reassignedVectors[vid] = struct{}{}
		}
	}

	return nil
}
