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
// queue. The skipped counters measure index bloat: SkippedStale entries are
// invalidated copies left behind by earlier reassignments and not yet
// garbage collected, SkippedDeleted entries belong to deleted vectors.
type ReassignAllStats struct {
	Postings       int `json:"postings"`
	Enqueued       int `json:"enqueued"`
	SkippedDeleted int `json:"skippedDeleted"`
	SkippedStale   int `json:"skippedStale"`
}

// EnqueueReassignAll walks every posting and enqueues a reassignment task for
// each live vector entry. The reassignment task re-routes a vector through
// RNGSelect and aborts before writing anything when the vector's current
// posting is still among the selected targets, so the write cost of this
// operation is proportional to how many vectors are actually misplaced, not
// to the corpus size. Stale entries (version mismatch) and deleted vectors
// are skipped so each reassignment is anchored to the vector's live copy.
// Before enqueueing the first live vector, ensure a destination exists so an
// index whose centroid graph was lost can recover its surviving postings.
//
// This exists to repair placement damage persisted by indexes built before
// the reassignment-gate fixes: an idle index runs no maintenance that could
// correct it. It is deliberately not part of the VectorIndex interface and is
// only reachable through the debug API.
//
// The scan runs without a shard reference, so alongside the caller's ctx it
// watches the index's own lifecycle context, like the version map warmup: a
// shard shutdown or drop stops it cooperatively at the next iteration (one
// in-flight store read or enqueue may race the close, as with warmup).
func (h *HFresh) EnqueueReassignAll(ctx context.Context) (ReassignAllStats, error) {
	var stats ReassignAllStats

	_, quantizer := h.loadQuantizer()
	if quantizer == nil {
		return stats, errors.New("index is not initialized")
	}
	var destinationChecked bool

	// The posting map enumerates the allocated posting IDs; the posting is
	// still read from the store because only its entries carry the per-copy
	// version byte that distinguishes a vector's live copy from stale ones.
	for postingID := range h.PostingMap.Iter() {
		err := ctx.Err()
		if err == nil {
			err = h.ctx.Err()
		}
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
			if version.Deleted() {
				stats.SkippedDeleted++
				continue
			}
			if version != v.Version() {
				stats.SkippedStale++
				continue
			}

			if !destinationChecked {
				vector, err := h.config.VectorForIDThunk(ctx, v.ID())
				if err != nil {
					return stats, errors.Wrapf(err, "failed to read vector %d for initial posting", v.ID())
				}
				if err := h.ValidateBeforeInsert(vector); err != nil {
					return stats, errors.Wrapf(err, "invalid vector %d for initial posting", v.ID())
				}
				if err := ctx.Err(); err != nil {
					return stats, err
				}
				if err := h.ctx.Err(); err != nil {
					return stats, err
				}
				vector = h.normalizeVec(vector)
				compressed := quantizer.CompressedBytes(quantizer.Encode(vector))
				if _, err := h.ensureInitialPosting(vector, compressed); err != nil {
					return stats, errors.Wrap(err, "failed to ensure initial posting for reassignment")
				}
				destinationChecked = true
			}

			err = h.taskQueue.EnqueueReassign(postingID, v.ID())
			if err != nil {
				return stats, errors.Wrapf(err, "failed to enqueue reassign for vector %d", v.ID())
			}
			stats.Enqueued++
		}
	}

	return stats, nil
}
