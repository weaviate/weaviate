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
	_, err := h.PostingMap.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return errors.Wrapf(err, "failed to get posting %d for analyze operation", postingID)
	}
	// if the posting has been removed, split or merge have updated the posting map and persisted it.
	if errors.Is(err, ErrPostingNotFound) {
		return nil
	}

	// load the posting and update the posting metadata on-disk and in-memory
	p, err := h.PostingStore.Get(ctx, postingID)
	if err != nil {
		return errors.Wrapf(err, "failed to get posting %d for analyze operation", postingID)
	}

	// update the posting map in-memory cache and persist the vector IDs
	err = h.PostingMap.SetVectorIDs(ctx, postingID, p)
	if err != nil {
		return errors.Wrapf(err, "failed to set vector IDs for posting %d", postingID)
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
