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
	"sync"
)

// Posting expansion parameters - internal constants for now.
// These may become configurable in future versions.
const (
	// topKDocsForExpansion is the number of top approximate candidates
	// whose associated postings are used for expansion.
	topKDocsForExpansion = 50

	// maxAdditionalPostings is the maximum number of additional postings
	// to scan during expansion.
	maxAdditionalPostings = 50
)

// DocToPostings is a reverse map from document IDs to their associated posting IDs.
// It is built lazily from the PostingMap on first MUVERA search.
// The map is read-only after initialization and safe for concurrent access.
type DocToPostings struct {
	mu        sync.RWMutex
	data      map[uint64][]uint64 // docID -> postingIDs
	built     bool
	buildOnce sync.Once
}

// NewDocToPostings creates a new empty reverse map.
func NewDocToPostings() *DocToPostings {
	return &DocToPostings{
		data: make(map[uint64][]uint64),
	}
}

// Build constructs the reverse map from the PostingMap.
// This is called lazily on first MUVERA search via sync.Once.
// It iterates over all postings and records which documents appear in each.
func (d *DocToPostings) Build(ctx context.Context, postingMap *PostingMap) error {
	var buildErr error

	d.buildOnce.Do(func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		if d.built {
			return
		}

		// Iterate over all postings in the PostingMap
		for postingID, metadata := range postingMap.Iter() {
			if ctx.Err() != nil {
				buildErr = ctx.Err()
				return
			}

			if metadata == nil {
				continue
			}

			// For each vector in this posting, record the posting association
			for docID := range metadata.Iter() {
				d.data[docID] = append(d.data[docID], postingID)
			}
		}

		d.built = true
	})

	return buildErr
}

// GetPostings returns the posting IDs associated with a document.
// Returns nil if the document has no recorded postings or the map is not built.
func (d *DocToPostings) GetPostings(docID uint64) []uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if !d.built {
		return nil
	}

	return d.data[docID]
}

// IsBuilt returns whether the reverse map has been built.
func (d *DocToPostings) IsBuilt() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.built
}

// Size returns the number of documents in the reverse map.
func (d *DocToPostings) Size() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.data)
}

// EstimatedMemoryBytes returns an estimate of memory usage in bytes.
// This includes the map overhead and the slice allocations.
func (d *DocToPostings) EstimatedMemoryBytes() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if !d.built {
		return 0
	}

	// Estimate: 8 bytes per docID key + 24 bytes slice header + 8 bytes per postingID
	var totalPostings int64
	for _, postings := range d.data {
		totalPostings += int64(len(postings))
	}

	// Map overhead: ~48 bytes per entry (key + value + bucket overhead)
	mapOverhead := int64(len(d.data)) * 48
	// Slice data: 8 bytes per uint64 posting ID
	sliceData := totalPostings * 8

	return mapOverhead + sliceData
}
