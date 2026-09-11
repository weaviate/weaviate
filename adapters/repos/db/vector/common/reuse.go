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

package common

// ReuseCleanliness is the OPTIONAL per-index docID-reuse surface. A vector
// index that can prove a deleted docID left no trace implements it; the shard
// only ever reissues a docID once EVERY index of the shard reports it clean.
//
// The zero behavior is deliberate: an index that does NOT implement this
// interface is treated as never-clean, which scopes docID reuse to the index
// types that opted in (HNSW, flat, dynamic delegating to them) without
// maintaining an explicit allow list. HFresh, noop, and any future index type
// are excluded by construction until they implement it.
//
// CleanForReuse is a STATE query, not an event: it must report the id's
// current condition in the index ("no vector, no tombstone, no pending
// per-id state"), so that ops discarded by queue quarantine/salvage paths
// leave the id blocked (the index still holds the old life) rather than
// falsely clean.
type ReuseCleanliness interface {
	// CleanForReuse reports whether docID has no remaining trace in this
	// index: no vector/node, no tombstone, no per-id bookkeeping that a
	// re-issued id could collide with. It must be safe to call concurrently
	// with writes; a conservative false is always allowed.
	CleanForReuse(docID uint64) bool
}
