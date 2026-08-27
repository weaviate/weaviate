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

	"github.com/pkg/errors"
)

// ReassignAllStats reports what an EnqueueReassignAll scan covered. Enqueued
// counts enqueue requests, not distinct vectors: a vector stored in several
// postings is requested once per live copy and deduplicated by the task
// queue.
type ReassignAllStats struct {
	Postings int `json:"postings"`
	Entries  int `json:"entries"`
	Enqueued int `json:"enqueued"`
}

// EnqueueReassignAll walks every posting and enqueues a reassignment task for
// each live vector entry. The reassignment task re-routes a vector through
// RNGSelect and aborts before writing anything when the vector's current
// posting is still among the selected targets, so the write cost of this
// operation is proportional to how many vectors are actually misplaced, not
// to the corpus size. Stale entries (version mismatch) and deleted vectors
// are skipped so each reassignment is anchored to the vector's live copy.
//
// This exists to repair placement damage persisted by indexes built before
// the reassignment-gate fixes: an idle index runs no maintenance that could
// correct it. It is deliberately not part of the VectorIndex interface and is
// only reachable through the debug API.
func (h *HFresh) EnqueueReassignAll(ctx context.Context) (ReassignAllStats, error) {
	var stats ReassignAllStats

	_, quantizer := h.loadQuantizer()
	if quantizer == nil {
		return stats, errors.New("index is not initialized")
	}

	// The posting map enumerates the allocated posting IDs; the posting is
	// still read from the store because only its entries carry the per-copy
	// version byte that distinguishes a vector's live copy from stale ones.
	for postingID := range h.PostingMap.Iter() {
		err := ctx.Err()
		if err != nil {
			return stats, err
		}

		posting, err := h.PostingStore.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				continue
			}
			return stats, errors.Wrapf(err, "failed to get posting %d", postingID)
		}
		stats.Postings++

		for _, v := range posting {
			version, err := h.VersionMap.Get(ctx, v.ID())
			if err != nil {
				return stats, errors.Wrapf(err, "failed to get version for vector %d", v.ID())
			}
			if version.Deleted() || version != v.Version() {
				continue
			}
			stats.Entries++

			err = h.taskQueue.EnqueueReassign(postingID, v.ID())
			if err != nil {
				return stats, errors.Wrapf(err, "failed to enqueue reassign for vector %d", v.ID())
			}
			stats.Enqueued++
		}
	}

	return stats, nil
}
