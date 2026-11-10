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

// doCleanup removes the entries of deleted documents from posting
// postingID. hfresh has no such task. Its only rule is that a posting is
// cleaned when a split or a merge rewrites it. Under churn the dead
// entries pile up in the postings that neither touches. In the update
// tests they reached a large share of all entries, and the largest share
// right after a delete phase. Each dead entry costs its bytes in the
// packed posting, and one decode and one version check in every scan
// that meets it. The scan that meets the dead entries
// enqueues the task (scanPostings, search.go). The task does what a split
// and a merge do before their own work: decode the posting, garbage
// collect it, and write it whole (putPosting fits the packed widths
// again). When the posting is busy, the lock is tried and the task is
// re-enqueued, as doMerge does; the dedup marker stays set across the
// re-enqueue. A posting whose centroid is gone is skipped. A posting with
// nothing dead is left as it is. A posting that fell below its minimum
// size gets the merge the next scan would enqueue for it. Splits and
// merges are unchanged.
func (t *TachiomFresh) doCleanup(ctx context.Context, postingID uint64) error {
	start := time.Now()
	defer t.metrics.CleanupDuration(start)

	if !t.postingLocks.TryLock(postingID) {
		// another operation is in progress for this posting
		// re-enqueue the operation to be processed later
		t.taskQueue.CleanupDone(postingID) // remove from the in-progress list
		return t.taskQueue.EnqueueCleanup(postingID)
	}
	defer func() {
		// the marker is cleared before the lock is released, as doSplit
		// does. A scan that enqueues in between finds the posting clean,
		// which costs one decode.
		t.taskQueue.CleanupDone(postingID)
		t.postingLocks.Unlock(postingID)
	}()

	if !t.Centroids.Exists(postingID) {
		t.logger.WithField("postingID", postingID).
			Debug("posting not found, skipping cleanup operation")
		return nil
	}

	refs := t.postings.get(postingID).decode()
	if len(refs) == 0 {
		return nil
	}

	live, err := t.garbageCollect(ctx, refs)
	if err != nil {
		return errors.Wrapf(err, "failed to garbage collect posting %d", postingID)
	}
	if len(live) == len(refs) {
		// nothing dead: a split, a merge or an earlier cleanup removed the
		// entries already
		return nil
	}

	if err := t.putPosting(ctx, postingID, live); err != nil {
		return err
	}
	// the cleaned posting reaches the disk as a split's does (see writeWALs)
	if err := t.writeWALs(); err != nil {
		return err
	}
	t.metrics.IncCleanupCount()

	// the merge trigger of scanPostings: the posting may have fallen below
	// the minimum of its token type, and a scan would enqueue the merge
	// the next time it reaches the posting anyway
	if minPostingSize, _ := t.postingLimits(postingID); len(live) < int(minPostingSize) && (len(live) == 0 || !t.aloneInGroup(postingID)) {
		if err := t.taskQueue.EnqueueMerge(postingID); err != nil {
			return errors.Wrapf(err, "failed to enqueue merge for posting %d", postingID)
		}
	}
	return nil
}
