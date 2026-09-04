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
	"slices"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
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
			// Clear the queue's dedup marker before releasing the posting
			// lock: an append blocked on the lock enqueues its split only
			// after acquiring it, so this ordering keeps that enqueue from
			// being swallowed by a marker that is about to be removed —
			// which would leave an oversized posting with no scheduled
			// split. (Appends that passed their size check before this
			// cleanup can still race the marker; the retry loop below makes
			// failed splits rare enough that this tail is acceptable.)
			h.taskQueue.SplitDone(postingID)
			h.postingLocks.Unlock(postingID)
		}
	}()

	if !h.Centroids.Exists(postingID) {
		h.logger.WithField("postingID", postingID).
			Trace("centroid not found, skipping split operation")
		return nil
	}

	// load the posting from disk
	p, err := h.PostingStore.Get(ctx, postingID)
	if err != nil {
		if errors.Is(err, ErrPostingNotFound) {
			h.logger.WithField("postingID", postingID).
				Trace("posting not found, skipping split operation")
			return nil
		}

		return errors.Wrapf(err, "failed to get posting %d for split operation", postingID)
	}

	// garbage collect the deleted vectors
	lp := len(p)

	if lp == 0 {
		h.logger.WithField("postingID", postingID).
			Debug("posting is empty, skipping split operation")
		return nil
	}

	// splitPosting fetches every entry's full-precision vector, and an entry
	// deleted between garbage collection and that fetch makes it fail. Such
	// failures are transient — the next garbage-collection pass filters the
	// deleted entry — so the collect+split sequence is retried a few times.
	// An error returned past that is treated as permanent by the task queue
	// and discards the task, leaving the oversized posting waiting for a
	// future append to reschedule it.
	const maxSplitAttempts = 3
	var result []SplitResult
	filtered := p
	for attempt := 1; ; attempt++ {
		var err error
		filtered, err = filtered.GarbageCollect(h.VersionMap)
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

			err = h.setPostingVectorIDs(ctx, postingID, filtered)
			if err != nil {
				return errors.Wrapf(err, "failed to set posting size for posting %d after split operation", postingID)
			}

			return nil
		}

		result, err = h.splitPosting(ctx, filtered)
		if err == nil {
			break
		}
		if attempt == maxSplitAttempts {
			return errors.Wrapf(err, "failed to split vectors for posting %d", postingID)
		}

		h.logger.WithField("postingID", postingID).
			WithField("attempt", attempt).
			WithField("error", err).
			Debug("retrying split after vector fetch failure")
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
		err = h.setPostingVectorIDs(ctx, newPostingID, result[i].Posting)
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
	err = h.setPostingVectorIDs(ctx, postingID, Posting{})
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
func (h *HFresh) splitPosting(ctx context.Context, posting Posting) ([]SplitResult, error) {
	dims, quantizer := h.loadQuantizer()
	if quantizer == nil {
		return nil, errors.New("split called on uninitialized index")
	}

	enc := compressionhelpers.NewKMeansEncoder(2, int(dims), 0)

	// Cluster on the full-precision vectors rather than their 1-bit
	// reconstructions: the centroids this split produces become the new
	// postings' routing representatives, and sign-only reconstructions bound
	// their quality.
	//
	// Object-store reads all go through a single bucket view and a pooled
	// slice — the split holds the posting lock, so per-vector view churn
	// would extend the time appends to this posting stay blocked. The pooled
	// container is required, not just an optimization: the production thunk
	// writes the vector ID into the container's Buff8, which only the pool
	// initializes. A muvera index reads the FDE bucket instead and needs
	// neither.
	var (
		view  common.BucketView
		slice *common.VectorSlice
	)
	if !h.muvera.Load() {
		objectsView, releaseView, err := h.objectsBucketView()
		if err != nil {
			return nil, err
		}
		defer releaseView()
		view = objectsView

		slice = h.tempVectors.Get(int(dims))
		defer h.tempVectors.Put(slice)
	}

	data := make([][]float32, len(posting))
	for i, v := range posting {
		// A fetch failure (e.g. a vector deleted between garbage collection
		// and this read) aborts the split rather than degrading it; doSplit
		// retries the collect+split sequence so the deleted entry gets
		// filtered on the next attempt.
		vec, err := h.clusteringVector(ctx, v.ID(), slice, view)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to fetch vector %d for split", v.ID())
		}
		if len(vec) == 0 {
			return nil, errors.Errorf("empty vector %d for split", v.ID())
		}
		data[i] = vec
	}

	idsAssignments, err := enc.FitBalanced(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fit KMeans encoder for split operation")
	}

	results := make([]SplitResult, 2)
	for i := range results {
		results[i] = SplitResult{
			Uncompressed: enc.Centroid(byte(i)),
		}

		results[i].Centroid = quantizer.CompressedBytes(quantizer.Encode(enc.Centroid(byte(i))))
	}

	for i, v := range idsAssignments {
		results[v].Posting = results[v].Posting.AddVector(posting[i])
	}

	return results, nil
}

// clusteringVector returns an owned full-precision vector for id. A muvera
// index routes on the encoded FDE, which lives in the muvera bucket instead of
// the object store the single-vector path reads, so it is read from the same
// source doReassign uses.
func (h *HFresh) clusteringVector(ctx context.Context, id uint64, slice *common.VectorSlice, view common.BucketView) ([]float32, error) {
	if h.muvera.Load() {
		fde, err := h.muveraEncoder.GetMuveraVectorForID(id, helpers.MuveraBucketName(h.id))
		if err != nil {
			return nil, err
		}
		return h.normalizeVec(fde), nil
	}

	vec, err := h.fetchNormalizedVector(ctx, id, slice, view)
	if err != nil {
		return nil, err
	}
	// vec aliases the pooled slice and the next fetch overwrites it;
	// clustering needs every vector at once, so copy it out.
	return slices.Clone(vec), nil
}

type SplitResult struct {
	Centroid     []byte
	Uncompressed []float32
	Posting      Posting
}

func (h *HFresh) enqueueReassignAfterSplit(ctx context.Context, oldPostingID uint64, newPostingIDs []uint64, newPostings []SplitResult) error {
	oldCentroid, err := h.Centroids.Get(oldPostingID)
	if err != nil {
		return errors.Wrapf(err, "failed to get centroid for posting %d", oldPostingID)
	}

	reassignedVectors := make(map[uint64]struct{})

	// Use the split results directly rather than re-fetching through
	// Centroids.Get: they carry the true float centroid (Get returns the
	// centroid HNSW's 8-bit reconstruction) along with its 1-bit code.
	newPostingCentroid0 := &Centroid{
		Uncompressed: newPostings[0].Uncompressed,
		Compressed:   newPostings[0].Centroid,
	}
	newPostingCentroid1 := &Centroid{
		Uncompressed: newPostings[1].Uncompressed,
		Compressed:   newPostings[1].Centroid,
	}
	newPostingCentroids := [2]*Centroid{newPostingCentroid0, newPostingCentroid1}

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
				newDist, err := newPostingCentroids[i].Distance(h.distancer, v)
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
					err = h.taskQueue.EnqueueReassign(newPostingIDs[i], v.ID())
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
	nearest, err := h.Centroids.Search(oldCentroid.Uncompressed, h.config.ReassignNeighbors, nil)
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

		neighborCentroid, err := h.Centroids.Get(neighborID)
		if err != nil {
			return errors.Wrapf(err, "failed to get centroid for posting %d", neighborID)
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

			distNeighbor, err := neighborCentroid.Distance(h.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in neighbor posting %d", vid, neighborID)
			}

			distOld, err := oldCentroid.Distance(h.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in old posting %d", vid, oldPostingID)
			}

			distA0, err := newPostingCentroid0.Distance(h.distancer, v)
			if err != nil {
				return errors.Wrapf(err, "failed to compute distance for vector %d in new posting %d", vid, newPostingIDs[0])
			}

			distA1, err := newPostingCentroid1.Distance(h.distancer, v)
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
			err = h.taskQueue.EnqueueReassign(neighborID, v.ID())
			if err != nil {
				return errors.Wrapf(err, "failed to enqueue reassign for vector %d after split", vid)
			}
			reassignedVectors[vid] = struct{}{}
		}
	}

	return nil
}
