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

type reassignOperation struct {
	PostingID uint64   // the posting the token was in when the task was enqueued
	Ref       tokenRef // the token, where hfresh has a vector id
}

// doReassign routes a token again. If its nearest centroid is not the
// posting it is in, the token is moved there. The code follows
// hfresh.doReassign with two differences:
//
//   - The target is the single nearest centroid (no replicas). A
//     k=1 centroid search replaces RNGSelect. The task stops when that
//     centroid is the posting the token is already in, as RNGSelect's
//     reassignedFromID does.
//   - There is no version bump. The postings are in memory, so the token is
//     moved from its old posting to the new one directly (moveToken).
//
// The token stays in its type. The type is that of the
// posting the token was in, and the nearest centroid is the nearest one of
// that type.
func (t *TachiomFresh) doReassign(ctx context.Context, op reassignOperation) error {
	start := time.Now()
	defer t.metrics.ReassignDuration(start)

	reenqueued := false
	defer func() {
		if !reenqueued {
			t.taskQueue.ReassignDone(op.PostingID, op.Ref)
		}
	}()

	// check if the entry is still valid: not deleted, not replaced by a
	// re-insert of its document
	live, err := t.live(ctx, op.Ref)
	if err != nil {
		return err
	}
	if !live {
		return nil
	}

	// The posting is gone: a split or a merge moved the token and enqueued
	// its own reassign where needed, or a build started over and places the
	// token again. Routing it here could create a first posting for nothing.
	if !t.Centroids.Exists(op.PostingID) {
		return nil
	}

	q, err := t.fetchToken(ctx, op.Ref)
	if err != nil {
		return errors.Wrap(err, "failed to get vector by index ID")
	}

	typ := t.types.typeOf(op.PostingID)

	for {
		target, err := t.nearestPosting(q, typ)
		if err != nil {
			return errors.Wrap(err, "failed to select replicas")
		}
		if target == op.PostingID {
			return nil
		}

		if t.opts.beforeAppend != nil {
			t.opts.beforeAppend(target)
		}
		outcome, count, err := t.moveToken(op.PostingID, target, op.Ref)
		if err != nil {
			return err
		}
		switch outcome {
		case moveTargetGone:
			// The target posting was deleted concurrently. The token is
			// still in its posting, so it is routed again at once.
			continue
		case moveSourceBusy:
			// The posting the token is in is locked by another operation,
			// which may hold it while it waits for the target's lock (a
			// merge does). The task goes back to the queue, as doMerge's
			// does when its posting is busy.
			t.taskQueue.ReassignDone(op.PostingID, op.Ref)
			reenqueued = true
			return t.taskQueue.EnqueueReassign(op.PostingID, op.Ref)
		case moveNotThere:
			// A split or a merge moved the token meanwhile and enqueued its
			// own reassign where needed, or another reassign of the same
			// token won.
			return nil
		case moveDone:
		}

		// A reassign splits its target at once when it is over size, as
		// hfresh's does, after the move released the locks.
		if _, maxPostingSize := t.postingLimits(target); count > maxPostingSize {
			if err := t.doSplit(ctx, target, false); err != nil {
				return err
			}
		}
		return nil
	}
}

type moveOutcome int

const (
	moveDone moveOutcome = iota
	moveTargetGone
	moveSourceBusy
	moveNotThere
)

// moveToken moves ref from posting source to posting target, on disk and in
// memory, with both postings locked. The target is locked first and the
// source is only tried, so the task never waits for a lock while it holds
// one: doMerge holds one posting's lock while it waits for another's. While
// both are held, no split, merge, cleanup or other reassign can move the
// token or change either posting, and a search sees the token in one posting
// or the other.
//
// On disk the entry is added to the target before it is removed from the
// source. A crash in between leaves the token in both postings, which restore
// resolves; the reverse order would leave it in none. As in hfresh, which may
// have the same problem, nothing waits for the target's centroid to reach
// the commit log's file: a crash can lose a target centroid inserted by a
// recent split, and the token with it (see writeWALs in tachiomfresh.go).
// It returns the target's new length when the token moved.
func (t *TachiomFresh) moveToken(source, target uint64, ref tokenRef) (moveOutcome, uint32, error) {
	t.postingLocks.Lock(target)
	sameLock := t.postingLocks.Hash(source) == t.postingLocks.Hash(target)
	unlock := func() {
		if !sameLock {
			t.postingLocks.Unlock(source)
		}
		t.postingLocks.Unlock(target)
	}
	if !t.Centroids.Exists(target) {
		t.postingLocks.Unlock(target)
		return moveTargetGone, 0, nil
	}
	if !sameLock && !t.postingLocks.TryLock(source) {
		t.postingLocks.Unlock(target)
		return moveSourceBusy, 0, nil
	}
	defer unlock()

	if !t.postings.contains(source, ref) {
		return moveNotThere, 0, nil
	}

	if err := t.PostingStore.Append(target, ref); err != nil {
		return 0, 0, errors.Wrapf(err, "failed to add token %+v to posting %d", ref, target)
	}
	count := uint32(t.postings.append(target, ref))
	t.countEntries(target, 1)

	if err := t.PostingStore.Remove(source, ref); err != nil {
		// The token stays where it was. The copy on the target's disk is
		// removed if the store takes it, otherwise restore resolves it.
		t.postings.remove(target, ref)
		t.countEntries(target, -1)
		if undoErr := t.PostingStore.Remove(target, ref); undoErr != nil {
			t.logger.WithField("postingID", target).
				Warnf("failed to undo the move of token %+v: %v", ref, undoErr)
		}
		return 0, 0, errors.Wrapf(err, "failed to remove token %+v from posting %d", ref, source)
	}
	t.postings.remove(source, ref)
	t.countEntries(source, -1)
	return moveDone, count, nil
}

// removeToken removes ref from posting postingID, on disk and in memory,
// and reports whether it was there. The tombstone is written first: a store
// error leaves the entry in memory as it is on disk.
func (t *TachiomFresh) removeToken(ctx context.Context, postingID uint64, ref tokenRef) (bool, error) {
	t.postingLocks.Lock(postingID)
	defer t.postingLocks.Unlock(postingID)

	if !t.postings.contains(postingID, ref) {
		return false, nil
	}
	// on disk this writes one tombstone (see postingStore in store.go)
	if err := t.PostingStore.Remove(postingID, ref); err != nil {
		return false, errors.Wrapf(err, "failed to remove token %+v from posting %d", ref, postingID)
	}
	t.postings.remove(postingID, ref)
	t.countEntries(postingID, -1)
	return true, nil
}
