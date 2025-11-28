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
)

func (h *HFresh) doMerge(ctx context.Context, postingID uint64) error {
	start := time.Now()
	defer h.metrics.MergeDuration(start)

	defer h.taskQueue.MergeDone(postingID)

	h.logger.WithField("postingID", postingID).Debug("merging posting")

	var markedAsDone bool
	if !h.postingLocks.TryLock(postingID) {
		// another merge operation is in progress for this posting
		// re-enqueue the operation to be processed later
		h.taskQueue.MergeDone(postingID) // remove from the in-progress list
		return h.taskQueue.EnqueueMerge(postingID)
	}
	defer func() {
		if !markedAsDone {
			h.postingLocks.Unlock(postingID)
		}
	}()

	// Ensure the posting exists in the index
	if !h.Centroids.Exists(postingID) {
		h.logger.WithField("postingID", postingID).
			Debug("posting not found, skipping merge operation")
		return nil // Nothing to merge
	}

	p, err := h.PostingStore.Get(ctx, postingID)
	if err != nil {
		if errors.Is(err, ErrPostingNotFound) {
			h.logger.WithField("postingID", postingID).
				Debug("posting not found, skipping merge operation")
			return nil
		}

		return errors.Wrapf(err, "failed to get posting %d for merge operation", postingID)
	}

	initialLen := p.Len()

	// garbage collect the deleted vectors
	newPosting, err := p.GarbageCollect(h.VersionMap)
	if err != nil {
		return errors.Wrapf(err, "failed to garbage collect posting %d", postingID)
	}
	prevLen := newPosting.Len()

	// skip if the posting is big enough
	if prevLen >= int(h.minPostingSize) {
		h.logger.
			WithField("postingID", postingID).
			WithField("size", prevLen).
			WithField("min", h.minPostingSize).
			Debug("posting is big enough, skipping merge operation")

		if prevLen == initialLen {
			// no changes, just return
			return nil
		}

		// persist the gc'ed posting
		err = h.PostingStore.Put(ctx, postingID, newPosting)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d", postingID)
		}
		// update the size of the posting after successful Persist
		return h.PostingSizes.Set(ctx, postingID, uint32(prevLen))
	}

	vectorSet := make(map[uint64]struct{})
	for _, v := range p.Iter() {
		vectorSet[v.ID()] = struct{}{}
	}

	// get posting centroid
	oldCentroid := h.Centroids.Get(postingID)
	if oldCentroid == nil {
		h.logger.WithField("postingID", postingID).
			Debug("posting centroid not found, skipping merge operation")
		return nil
	}

	// search for the closest centroids
	nearest, err := h.Centroids.Search(oldCentroid.Uncompressed, h.config.InternalPostingCandidates)
	if err != nil {
		return errors.Wrapf(err, "failed to search for nearest centroid for posting %d", postingID)
	}

	if nearest.Len() <= 1 {
		h.logger.WithField("postingID", postingID).
			Debug("no candidates found for merge operation, skipping")

		// persist the gc'ed posting
		err = h.PostingStore.Put(ctx, postingID, newPosting)
		if err != nil {
			return errors.Wrapf(err, "failed to put filtered posting %d", postingID)
		}
		// update the size of the posting after successful Persist
		err = h.PostingSizes.Set(ctx, postingID, uint32(prevLen))
		if err != nil {
			return errors.Wrapf(err, "failed to set size of posting %d to %d", postingID, prevLen)
		}

		return nil
	}

	// first centroid is the query centroid, the rest are candidates for merging
	for candidateID := range nearest.Iter() {
		// check if the combined size of the postings is within limits
		count, err := h.PostingSizes.Get(ctx, candidateID)
		if err != nil {
			return errors.Wrapf(err, "failed to get posting size for candidate %d", candidateID)
		}
		if int(count)+prevLen > int(h.maxPostingSize) || h.taskQueue.MergeContains(candidateID) {
			continue // Skip this candidate
		}

		err = func() error {
			// lock the candidate posting to ensure no concurrent modifications
			// note: the candidate lock might be the same as the current posting lock
			// so we need to ensure we don't deadlock
			if h.postingLocks.Hash(postingID) != h.postingLocks.Hash(candidateID) {
				// lock the candidate posting
				h.postingLocks.Lock(candidateID)
				defer func() {
					if !markedAsDone {
						h.postingLocks.Unlock(candidateID)
					}
				}()
			}

			// get the candidate posting
			candidate, err := h.PostingStore.Get(ctx, candidateID)
			if err != nil {
				return errors.Wrapf(err, "failed to get candidate posting %d for merge operation on posting %d", candidateID, postingID)
			}

			var candidateLen int
			for _, v := range candidate.Iter() {
				version, err := h.VersionMap.Get(ctx, v.ID())
				if err != nil {
					return errors.Wrapf(err, "failed to get version for vector %d", v.ID())
				}
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
			err = h.Centroids.MarkAsDeleted(smallID)
			if err != nil {
				return errors.Wrapf(err, "failed to delete centroid for posting %d", smallID)
			}

			// persist the merged posting first
			err = h.PostingStore.Put(ctx, largeID, newPosting)
			if err != nil {
				return errors.Wrapf(err, "failed to put merged posting %d after merge operation", postingID)
			}

			// set the small posting size to 0 and update the large posting size only after successful persist
			err = h.PostingSizes.Set(ctx, smallID, 0)
			if err != nil {
				return errors.Wrapf(err, "failed to set size of merged posting %d to 0", smallID)
			}
			err = h.PostingSizes.Set(ctx, largeID, uint32(newPosting.Len()))
			if err != nil {
				return errors.Wrapf(err, "failed to set size of merged posting %d to %d", largeID, newPosting.Len())
			}

			// mark the operation as done and unlock everything
			markedAsDone = true
			if h.postingLocks.Hash(postingID) != h.postingLocks.Hash(candidateID) {
				h.postingLocks.Unlock(candidateID)
			}
			h.postingLocks.Unlock(postingID)

			// if merged vectors are closer to their old centroid than the new one
			// there may be better centroids for them out there.
			// we need to reassign them in the background.
			smallCentroid := h.Centroids.Get(smallID)
			largeCentroid := h.Centroids.Get(largeID)
			for _, v := range smallPosting.Iter() {
				prevDist, err := smallCentroid.Distance(h.distancer, v)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in small posting %d", v.ID(), smallID)
				}

				newDist, err := largeCentroid.Distance(h.distancer, v)
				if err != nil {
					return errors.Wrapf(err, "failed to compute distance for vector %d in large posting %d", v.ID(), largeID)
				}

				if prevDist < newDist {
					// the vector is closer to the old centroid, we need to reassign it
					err = h.taskQueue.EnqueueReassign(largeID, v.ID(), v.Version())
					if err != nil {
						return errors.Wrapf(err, "failed to enqueue reassign for vector %d after merge", v.ID())
					}
				}
			}

			return nil
		}()
		if err != nil {
			return errors.Wrapf(err, "failed to merge posting %d with candidate %d", postingID, candidateID)
		}

		return nil
	}

	// if no candidates were found, just persist the gc'ed posting
	h.PostingSizes.Set(ctx, postingID, uint32(newPosting.Len()))
	err = h.PostingStore.Put(ctx, postingID, newPosting)
	if err != nil {
		return errors.Wrapf(err, "failed to put filtered posting %d", postingID)
	}

	return nil
}
