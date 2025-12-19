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
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

// doSplit performs the actual split operation for a given postingID.
// If reassign is true, it will enqueue reassign operations for vectors that
// may need to be moved to other postings after the split.
func (h *HFresh) doSplit(ctx context.Context, postingID uint64, reassign bool) error {
	start := time.Now()
	defer h.metrics.SplitDuration(start)

	h.postingLocks.Lock(postingID)

	var markedAsDone bool
	defer func() {
		if !markedAsDone {
			h.postingLocks.Unlock(postingID)
			h.taskQueue.SplitDone(postingID)
		}
	}()

	if !h.Centroids.Exists(postingID) {
		return nil
	}

	// load the posting from disk
	p, err := h.PostingStore.Get(ctx, postingID)
	if err != nil {
		if errors.Is(err, ErrPostingNotFound) {
			h.logger.WithField("postingID", postingID).
				Debug("posting not found, skipping split operation")
			return nil
		}

		return errors.Wrapf(err, "failed to get posting %d for split operation", postingID)
	}

	// garbage collect the deleted vectors
	lp := len(p)
	filtered, err := p.GarbageCollect(h.VersionMap)
	if err != nil {
		return errors.Wrapf(err, "failed to garbage collect posting %d", postingID)
	}

	// skip if the filtered posting is now too small
	if lf := len(filtered); lf < int(h.maxPostingSize) {
		if lf == lp {
			// no changes, just return
			return nil
		}

		// persist the gc'ed posting
		err = h.PostingStore.Put(ctx, postingID, filtered)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d after split operation", postingID)
		}

		err = h.PostingSizes.Set(ctx, postingID, uint32(lf))
		if err != nil {
			return errors.Wrapf(err, "failed to set posting size for posting %d after split operation", postingID)
		}

		return nil
	}

	// split the vectors into two clusters
	result, err := h.splitPosting(filtered)
	if err != nil {
		return errors.Wrapf(err, "failed to split vectors for posting %d", postingID)
	}
	// if one of the postings is empty, ignore the split
	if len(result[0].Posting) == 0 || len(result[1].Posting) == 0 {
		h.logger.WithField("postingID", postingID).
			Debug("split resulted in empty posting, skipping split operation")
		return nil
	}

	newPostingIDs := make([]uint64, 2)
	for i := range 2 {
		newPostingID, err := h.IDs.Next()
		if err != nil {
			return errors.Wrap(err, "failed to allocate new posting ID during split operation")
		}
		newPostingIDs[i] = newPostingID
		err = h.PostingStore.Put(ctx, newPostingID, result[i].Posting)
		if err != nil {
			return errors.Wrapf(err, "failed to put new posting %d after split operation", newPostingID)
		}
		// allocate and set posting size after successful persist
		err = h.PostingSizes.Set(ctx, newPostingID, uint32(len(result[i].Posting)))
		if err != nil {
			return errors.Wrapf(err, "failed to set posting size for posting %d after split operation", newPostingID)
		}

		// add the new centroid to the SPTAG index
		err = h.Centroids.Insert(newPostingID, &Centroid{
			Uncompressed: result[i].Uncompressed,
			Compressed:   result[i].Centroid,
			Deleted:      false,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to upsert new centroid %d after split operation", newPostingID)
		}
	}

	// delete the old centroid
	err = h.Centroids.MarkAsDeleted(postingID)
	if err != nil {
		return errors.Wrapf(err, "failed to delete old centroid %d after split operation", postingID)
	}
	err = h.PostingSizes.Set(ctx, postingID, 0)
	if err != nil {
		return errors.Wrapf(err, "failed to set posting size for posting %d after split operation", postingID)
	}

	// put empty posting for postingID to increase version and
	// allow cleanup of old vectors on disk
	err = h.PostingStore.Put(ctx, postingID, Posting{})
	if err != nil {
		return errors.Wrapf(err, "failed to put empty posting %d after split operation", postingID)
	}

	// Mark the split operation as done
	markedAsDone = true
	h.postingLocks.Unlock(postingID)
	h.taskQueue.SplitDone(postingID)

	if !reassign {
		return nil
	}

	err = h.enqueueReassignAfterSplit(ctx, postingID, newPostingIDs, result)
	if err != nil {
		return errors.Wrapf(err, "failed to enqueue reassign after split for posting %d", postingID)
	}

	return nil
}

// splitPosting takes a posting and returns two groups.
func (h *HFresh) splitPosting(posting Posting) ([]SplitResult, error) {
	enc := compressionhelpers.NewKMeansEncoder(2, int(h.dims), 0)

	data := posting.Uncompress(h.quantizer)

	idsAssignments, err := enc.FitBalanced(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fit KMeans encoder for split operation")
	}

	results := make([]SplitResult, 2)
	for i := range results {
		results[i] = SplitResult{
			Uncompressed: enc.Centroid(byte(i)),
		}

		results[i].Centroid = h.quantizer.Encode(enc.Centroid(byte(i)))
	}

	for i, v := range idsAssignments {
		results[v].Posting = results[v].Posting.AddVector(posting[i])
	}

	return results, nil
}

type SplitResult struct {
	Centroid     []byte
	Uncompressed []float32
	Posting      Posting
}

func (h *HFresh) enqueueReassignAfterSplit(ctx context.Context, oldPostingID uint64, newPostingIDs []uint64, newPostings []SplitResult) error {
	oldCentroid := h.Centroids.Get(oldPostingID)

	reassignedVectors := make(map[uint64]struct{})

	// first check: if a vector is closer to one of the new posting centroid than the old centroid,
	// neighboring centroids cannot be better.
	for i := range newPostings {
		// test each vector
		for _, v := range newPostings[i].Posting {
			vid := v.ID()
			_, exists := reassignedVectors[vid]
			version, err := h.VersionMap.Get(ctx, vid)
			if err != nil {
				return errors.Wrapf(err, "failed to get version for vector %d", vid)
			}
			if !exists && !v.Version().Deleted() && version == v.Version() {
				// compute distance from v to its new centroid
				newDist, err := h.Centroids.Get(newPostingIDs[i]).Distance(h.distancer, v)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", vid, newPostingIDs[i])
				}

				// compute distance from v to the old centroid
				oldDist, err := oldCentroid.Distance(h.distancer, v)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in old posting %d", vid, oldPostingID)
				}

				if newDist >= oldDist {
					// the vector is closer to the old centroid, which means it may be also closer to a neighboring centroid,
					// we need to reassign it
					err = h.taskQueue.EnqueueReassign(newPostingIDs[i], v.ID(), v.Version())
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
	if h.config.ReassignNeighbors <= 0 {
		return nil
	}

	// search for neighboring centroids
	nearest, err := h.Centroids.Search(oldCentroid.Uncompressed, h.config.ReassignNeighbors)
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

		p, err := h.PostingStore.Get(ctx, neighborID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				h.logger.WithField("postingID", neighborID).
					Debug("posting not found, skipping reassign after split")
				continue // Skip if the posting is not found
			}

			return errors.Wrapf(err, "failed to get posting %d for reassign after split", neighborID)
		}

		for _, v := range p {
			vid := v.ID()
			_, exists := reassignedVectors[vid]
			version, err := h.VersionMap.Get(ctx, vid)
			if err != nil {
				return errors.Wrapf(err, "failed to get version for vector %d", vid)
			}
			if exists || version.Deleted() || version != v.Version() {
				continue
			}

			distNeighbor, err := h.Centroids.Get(neighborID).Distance(h.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in neighbor posting %d", vid, neighborID)
			}

			distOld, err := oldCentroid.Distance(h.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in old posting %d", vid, oldPostingID)
			}

			distA0, err := h.Centroids.Get(newPostingIDs[0]).Distance(h.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", vid, newPostingIDs[0])
			}

			distA1, err := h.Centroids.Get(newPostingIDs[1]).Distance(h.distancer, v)
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
			err = h.taskQueue.EnqueueReassign(neighborID, v.ID(), v.Version())
			if err != nil {
				return errors.Wrapf(err, "failed to enqueue reassign for vector %d after split", vid)
			}
			reassignedVectors[vid] = struct{}{}
		}
	}

	return nil
}
