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

package state

import (
	"strings"
	"sync"
	"time"
)

// reindexDeleteMarkerTTL bounds how long a recorded DELETE is remembered.
// The only consumer is the GET /indexes finalize-window suppression, which
// matters for at most one finalize window (2× scheduler tick, clamped to
// ≤10s) after a task FINISHED. A delete older than that can never suppress
// anything (the driving FINISHED task is already outside its window), so a
// generous 30s TTL is ample and keeps the map bounded.
const reindexDeleteMarkerTTL = 30 * time.Second

// ReindexDeleteMarkers records, per (collection, property, indexType), the
// most recent time a property-index DELETE was accepted.
//
// It exists to suppress the post-DELETE finalize-window bleed on
// GET /v1/schema/{class}/indexes: mergeReindexStatus keeps a recently-FINISHED
// task's index visible as "indexing@100%" for a short window to bridge the gap
// between "task FINISHED" and "schema flag flipped". After a DELETE the flag
// is intentionally off, so that override wrongly resurrects the just-deleted
// index. The GET handler consults these markers to distinguish "index was
// deleted after its task finished" (suppress) from a live re-enable (which is
// driven by a STARTED task and never touches this path).
//
// Node-local and best-effort: the marker lives on the node that served the
// DELETE. A GET served by a different node in a multi-node cluster may still
// show the bleed for the (short) finalize window — acceptable, since the
// finalize-window override itself is already a per-node, cosmetic mitigation.
type ReindexDeleteMarkers struct {
	mu      sync.Mutex
	deleted map[string]time.Time
}

// NewReindexDeleteMarkers returns an initialised tracker.
func NewReindexDeleteMarkers() *ReindexDeleteMarkers {
	return &ReindexDeleteMarkers{deleted: map[string]time.Time{}}
}

func reindexDeleteMarkerKey(collection, property, indexType string) string {
	return strings.ToLower(collection) + "/" + property + "/" + indexType
}

// Record notes that a DELETE for (collection, property, indexType) was
// accepted now. It opportunistically prunes expired entries so the map stays
// bounded by the delete rate within the TTL.
//
// indexType is the canonical status-type spelling ("searchable" /
// "filterable" / "rangeFilters") so it matches what GET /indexes queries.
func (m *ReindexDeleteMarkers) Record(collection, property, indexType string) {
	if m == nil {
		return
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, ts := range m.deleted {
		if now.Sub(ts) > reindexDeleteMarkerTTL {
			delete(m.deleted, k)
		}
	}
	m.deleted[reindexDeleteMarkerKey(collection, property, indexType)] = now
}

// LastDeleted returns the most recent recorded DELETE time for
// (collection, property, indexType), or the zero time if none is recorded or
// it has expired.
func (m *ReindexDeleteMarkers) LastDeleted(collection, property, indexType string) time.Time {
	if m == nil {
		return time.Time{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, ok := m.deleted[reindexDeleteMarkerKey(collection, property, indexType)]
	if !ok || time.Since(ts) > reindexDeleteMarkerTTL {
		return time.Time{}
	}
	return ts
}
