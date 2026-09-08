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

// PostingEntry is one stored copy of a vector inside a posting, as reported
// by DumpPostings. Live is false for copies the index no longer considers
// current: the vector was deleted, or this copy's version is behind the
// vector's live version (a stale copy left behind by a reassignment and not
// yet garbage collected).
type PostingEntry struct {
	ID      uint64 `json:"id"`
	Version uint8  `json:"version"`
	Live    bool   `json:"live"`
}

// DumpCentroids calls fn for every posting that has a centroid, passing the
// centroid vector as the router sees it: the centroid HNSW stores 8-bit
// codes, so this is the decoded representation that the posting selection
// at search time is based on, not the float32 mean the split computed.
//
// Read-only. It is not part of the VectorIndex interface and is reachable
// only through the debug API; it exists so that placement and routing can be
// analyzed offline, together with DumpPostings. Like EnqueueReassignAll it
// watches the index's own lifecycle context alongside the caller's, so a
// shard shutdown or drop stops it at the next iteration.
func (h *HFresh) DumpCentroids(ctx context.Context, fn func(postingID uint64, centroid []float32) error) error {
	dims, quantizer := h.loadQuantizer()
	if quantizer == nil {
		return errors.New("index is not initialized")
	}

	for postingID := range h.PostingMap.Iter() {
		err := ctx.Err()
		if err == nil {
			err = h.ctx.Err()
		}
		if err != nil {
			return err
		}

		if !h.Centroids.Exists(postingID) {
			continue
		}
		centroid, err := h.Centroids.Get(postingID)
		if err != nil {
			return errors.Wrapf(err, "failed to get centroid %d", postingID)
		}

		// the centroid HNSW pads vectors to its rotation block size (a
		// multiple of 64); the padding carries no information
		vec := centroid.Uncompressed
		if len(vec) > int(dims) {
			vec = vec[:dims]
		}

		err = fn(postingID, vec)
		if err != nil {
			return err
		}
	}

	return nil
}

// DumpPostings calls fn for every posting with the copies it stores. Every
// stored copy is reported, including deleted and stale ones, each flagged
// through PostingEntry.Live, so the caller can reproduce both what the index
// scans and what it treats as current. Read-only; see DumpCentroids for the
// lifecycle and reachability notes, which apply here too.
func (h *HFresh) DumpPostings(ctx context.Context, fn func(postingID uint64, entries []PostingEntry) error) error {
	_, quantizer := h.loadQuantizer()
	if quantizer == nil {
		return errors.New("index is not initialized")
	}

	for postingID := range h.PostingMap.Iter() {
		err := ctx.Err()
		if err == nil {
			err = h.ctx.Err()
		}
		if err != nil {
			return err
		}

		posting, err := h.PostingStore.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				continue
			}
			return errors.Wrapf(err, "failed to get posting %d", postingID)
		}

		entries := make([]PostingEntry, 0, len(posting))
		for _, v := range posting {
			version, err := h.VersionMap.Get(ctx, v.ID())
			if err != nil {
				return errors.Wrapf(err, "failed to get version for vector %d", v.ID())
			}
			entries = append(entries, PostingEntry{
				ID:      v.ID(),
				Version: uint8(v.Version()),
				Live:    !version.Deleted() && version == v.Version(),
			})
		}

		err = fn(postingID, entries)
		if err != nil {
			return err
		}
	}

	return nil
}
