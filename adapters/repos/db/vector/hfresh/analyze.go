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
	"time"

	"github.com/pkg/errors"
)

// doAnalyze keeps the mapping postingID -> []vectorID in sync with the posting store by analyzing
// the posting and updating the posting map accordingly.
func (h *HFresh) doAnalyze(ctx context.Context, postingID uint64) error {
	start := time.Now()
	defer h.metrics.AnalyzeDuration(start)

	h.postingLocks.Lock(postingID)
	var markedAsDone bool
	defer func() {
		if !markedAsDone {
			h.postingLocks.Unlock(postingID)
			h.taskQueue.AnalyzeDone(postingID)
		}
	}()

	if !h.Centroids.Exists(postingID) {
		return nil
	}

	// load the posting metadata
	meta, release, err := h.PostingMap.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return errors.Wrapf(err, "failed to get posting %d for analyze operation", postingID)
	}
	defer release()

	// if the metadata was loaded from disk or the posting doesn't have a mapping entry yet, it might not be in sync with
	// the posting store. load the posting from disk to do the analysis.
	if err != nil || meta.fromDisk {
		p, err := h.PostingStore.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				h.logger.WithField("postingID", postingID).
					Debug("posting not found, skipping analyze operation")
				return nil
			}

			return errors.Wrapf(err, "failed to get posting %d for analyze operation", postingID)
		}

		// update the posting map in-memory cache and persist the vector IDs
		err = h.PostingMap.SetVectorIDs(ctx, postingID, p)
		if err != nil {
			return errors.Wrapf(err, "failed to set vector IDs for posting %d", postingID)
		}
	} else {
		// only persist the in-memory cache to disk
		err = h.PostingMap.Persist(ctx, postingID)
		if err != nil {
			return errors.Wrapf(err, "failed to persist posting %d", postingID)
		}
	}

	markedAsDone = true
	h.postingLocks.Unlock(postingID)
	h.taskQueue.AnalyzeDone(postingID)

	// check if the posting needs to be split
	size, err := h.PostingMap.CountVectorIDs(ctx, postingID)
	if err != nil {
		return errors.Wrapf(err, "failed to get size of posting %d", postingID)
	}

	if size > h.maxPostingSize {
		err = h.taskQueue.EnqueueSplit(postingID)
		if err != nil {
			return errors.Wrapf(err, "failed to enqueue split for posting %d", postingID)
		}
	}

	return nil
}
