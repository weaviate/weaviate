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

package tachiomfresh

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

// doMerge merges posting postingID with one of its neighbouring postings
// when it is below its minimum size. The code follows
// hfresh.doMerge: the same candidate search, the same size rule, the same
// choice of the surviving posting and the same reassign check. The postings
// are the in-memory lists, and every change is written through to the
// posting store. The reassign check uses float32 distances (see
// centroidDistance in split.go). hfresh's duplicate filter is left out:
// without replicas a token is in one posting only. The candidates are the
// postings of the same type, so a merge never mixes
// types. A posting alone in its type finds no candidate.
func (t *TachiomFresh) doMerge(ctx context.Context, postingID uint64) error {
	start := time.Now()
	defer t.metrics.MergeDuration(start)

	defer t.taskQueue.MergeDone(postingID)

	t.logger.WithField("postingID", postingID).Debug("merging posting")

	var markedAsDone bool
	if !t.postingLocks.TryLock(postingID) {
		// another operation is in progress for this posting
		// re-enqueue the operation to be processed later
		t.taskQueue.MergeDone(postingID) // remove from the in-progress list
		return t.taskQueue.EnqueueMerge(postingID)
	}
	defer func() {
		if !markedAsDone {
			t.postingLocks.Unlock(postingID)
		}
	}()

	// Ensure the posting exists in the index
	if !t.Centroids.Exists(postingID) {
		t.logger.WithField("postingID", postingID).
			Debug("posting not found, skipping merge operation")
		return nil // Nothing to merge
	}

	p := t.postings.get(postingID).decode()
	initialLen := len(p)

	// garbage collect the entries of deleted documents
	newPosting, err := t.garbageCollect(ctx, p)
	if err != nil {
		return errors.Wrapf(err, "failed to garbage collect posting %d", postingID)
	}
	prevLen := len(newPosting)

	if prevLen == 0 {
		return t.deleteEmptyPosting(ctx, postingID)
	}

	// Not in hfresh: the limits are those of the posting's token type (see
	// postingLimits in types.go). The candidates have the same type, so the
	// same maximum bounds the merged posting below.
	minPostingSize, maxPostingSize := t.postingLimits(postingID)

	// skip if the posting is big enough
	if prevLen >= int(minPostingSize) {
		t.logger.
			WithField("postingID", postingID).
			WithField("size", prevLen).
			WithField("min", minPostingSize).
			Debug("posting is big enough, skipping merge operation")

		if prevLen == initialLen {
			// no changes, just return
			return nil
		}

		// persist the garbage-collected posting
		return t.putPosting(ctx, postingID, newPosting)
	}

	// get posting centroid
	oldCentroid, err := t.centroid(postingID)
	if err != nil {
		return errors.Wrapf(err, "failed to get centroid for posting %d", postingID)
	}

	if oldCentroid == nil {
		t.logger.WithField("postingID", postingID).
			Debug("posting centroid not found, skipping merge operation")
		return nil
	}

	// search for the closest centroids
	typ := t.types.typeOf(postingID)
	nearest, err := t.nearestCentroids(oldCentroid, typ, t.config.InternalPostingCandidates)
	if err != nil {
		return errors.Wrapf(err, "failed to search for nearest centroid for posting %d", postingID)
	}

	if nearest.Len() <= 1 {
		t.logger.WithField("postingID", postingID).
			Debug("no candidates found for merge operation, skipping")

		// persist the garbage-collected posting
		return t.putPosting(ctx, postingID, newPosting)
	}

	// first centroid is the query centroid, the rest are candidates for merging
	for candidateID := range nearest.Iter() {
		if candidateID == postingID {
			continue
		}
		if t.taskQueue.MergeContains(candidateID) {
			continue // Skip this candidate
		}

		merged, err := func() (bool, error) {
			// Lock the candidate posting against concurrent changes. The
			// candidate may share its lock stripe with postingID, which is
			// already held, so the lock is taken only when it is a
			// different one, to avoid a deadlock.
			sameLock := t.postingLocks.Hash(postingID) == t.postingLocks.Hash(candidateID)
			if !sameLock {
				// lock the candidate posting
				if t.opts.beforeCandidateLock != nil {
					t.opts.beforeCandidateLock(candidateID)
				}
				t.postingLocks.Lock(candidateID)
				defer func() {
					if !markedAsDone {
						t.postingLocks.Unlock(candidateID)
					}
				}()
			}

			// Get the candidate posting. hfresh reads the candidate's size
			// from PostingSizes before it takes the lock. Here the size is
			// the length of the in-memory list, which is safe to read only
			// under the lock, so the size rule is checked under it. A
			// candidate that was split or merged away since the search is
			// skipped.
			if !t.Centroids.Exists(candidateID) {
				return false, nil
			}
			candidate := t.postings.get(candidateID).decode()
			// check if the combined size of the postings is within limits
			if len(candidate)+prevLen > int(maxPostingSize) {
				return false, nil
			}

			liveCandidate, err := t.garbageCollect(ctx, candidate)
			if err != nil {
				return false, errors.Wrapf(err, "failed to garbage collect candidate posting %d", candidateID)
			}
			candidateLen := len(liveCandidate)
			mergedPosting := append(newPosting, liveCandidate...)

			// delete the smallest posting and update the large posting
			smallID, largeID := postingID, candidateID
			smallPosting := newPosting[:prevLen:prevLen]
			if prevLen > candidateLen {
				smallID, largeID = candidateID, postingID
				smallPosting = liveCandidate
			}

			// hfresh reads both centroids after the small one was deleted.
			// Here they are read before the deletion, for the reassign
			// check below (see doSplit).
			smallCentroid, err := t.centroid(smallID)
			if err != nil {
				return false, errors.Wrapf(err, "failed to get centroid for posting %d", smallID)
			}
			largeCentroid, err := t.centroid(largeID)
			if err != nil {
				return false, errors.Wrapf(err, "failed to get centroid for posting %d", largeID)
			}

			// persist the merged posting first. hfresh deletes the small
			// centroid before it puts the merged posting; here the
			// posting reaches the disk before the delete (see writeWALs),
			// so a crash in between never drops the small posting's
			// tokens.
			prevLarge := t.postings.get(largeID).decode()
			err = t.putPosting(ctx, largeID, mergedPosting)
			if err != nil {
				return false, errors.Wrapf(err, "failed to put merged posting %d after merge operation", largeID)
			}
			if err = t.writeWALs(); err != nil {
				t.unmerge(ctx, largeID, prevLarge)
				return false, err
			}

			t.types.leave(smallID, typ)

			// mark the small posting as deleted in the centroid index
			if t.opts.beforeCentroidDelete != nil {
				err = t.opts.beforeCentroidDelete(smallID)
			}
			if err == nil {
				err = t.Centroids.MarkAsDeleted(smallID)
			}
			if err != nil {
				if t.Centroids.Exists(smallID) {
					// the small posting stays: it rejoins its group, and the
					// large one gives its entries back
					t.types.join(smallID, typ)
					t.unmerge(ctx, largeID, prevLarge)
				}
				return false, errors.Wrapf(err, "failed to delete centroid for posting %d", smallID)
			}

			// hfresh puts an empty posting for smallID, to bump its version
			// and let the old entries be cleaned up on disk. Here the
			// posting is dropped (see dropPosting in posting.go).
			t.dropPosting(smallID)

			// mark the operation as done and unlock everything
			markedAsDone = true
			if !sameLock {
				t.postingLocks.Unlock(candidateID)
			}
			t.postingLocks.Unlock(postingID)

			if t.opts.disableReassign {
				return true, nil
			}

			// A merged token that is closer to its old centroid than to the
			// new one may have a better centroid elsewhere. Such tokens are
			// reassigned in the background.
			for _, ref := range smallPosting {
				token, err := t.fetchToken(ctx, ref)
				if err != nil {
					// deleted or replaced since the garbage collection above
					if live, lerr := t.live(ctx, ref); lerr == nil && !live {
						continue
					}
					return true, errors.Wrapf(err, "failed to fetch token %+v of small posting %d", ref, smallID)
				}

				prevDist, err := t.centroidDistance(smallCentroid, token)
				if err != nil {
					return true, errors.Wrapf(err, "failed to compute distance for token %+v in small posting %d", ref, smallID)
				}

				newDist, err := t.centroidDistance(largeCentroid, token)
				if err != nil {
					return true, errors.Wrapf(err, "failed to compute distance for token %+v in large posting %d", ref, largeID)
				}

				if prevDist < newDist {
					// the token is closer to the old centroid, so it is reassigned
					err = t.taskQueue.EnqueueReassign(largeID, ref)
					if err != nil {
						return true, errors.Wrapf(err, "failed to enqueue reassign for token %+v after merge", ref)
					}
				}
			}

			return true, nil
		}()
		if err != nil {
			return errors.Wrapf(err, "failed to merge posting %d with candidate %d", postingID, candidateID)
		}
		if merged {
			return nil
		}
	}

	// if no candidate was found, just persist the garbage-collected posting
	return t.putPosting(ctx, postingID, newPosting)
}

// deleteEmptyPosting deletes the centroid of a posting with no live entries,
// with the steps a merge uses on the posting it absorbs. hfresh leaves such a
// centroid in place, and every search that reaches it enqueues a merge that
// does nothing. An insert or a reassign racing the delete is routed again by
// append, which checks that the centroid exists. The caller holds
// postingLocks.Lock(postingID).
//
// A posting alone in its token type is deleted too: queries search the
// centroids of every type, so an empty centroid would take a candidate's
// place for nothing. Its type is then left with no centroid, and the next
// token of the type creates the type's first posting again
// (ensureInitialPosting).
func (t *TachiomFresh) deleteEmptyPosting(ctx context.Context, postingID uint64) error {
	t.logger.WithField("postingID", postingID).
		Debug("posting is empty, deleting its centroid")

	// the moves that emptied the posting reach the disk before the delete
	// (see writeWALs)
	if err := t.writeWALs(); err != nil {
		return err
	}
	t.types.leave(postingID, t.types.typeOf(postingID))
	if err := t.Centroids.MarkAsDeleted(postingID); err != nil {
		return errors.Wrapf(err, "failed to delete centroid for empty posting %d", postingID)
	}
	t.dropPosting(postingID)
	return nil
}

// unmerge undoes a merge that failed after it put the merged posting, with
// both postings locked: the large posting gets back the entries it had, so
// that the small posting's tokens are in one posting. A failure here is
// logged; restore removes what it leaves in two postings
// (resolveDuplicates).
func (t *TachiomFresh) unmerge(ctx context.Context, largeID uint64, prev []tokenRef) {
	if err := t.putPosting(ctx, largeID, prev); err != nil {
		t.logger.WithField("postingID", largeID).
			Warnf("failed to undo a failed merge: %v", err)
	}
}
