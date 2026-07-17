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

// reindexDeleteMarkerTTL bounds how long a recorded DELETE is remembered. A
// delete can only matter within the finalize window (≤10s after FINISHED),
// so 30s is generous headroom that also keeps the map bounded.
const reindexDeleteMarkerTTL = 30 * time.Second

// ReindexDeleteMarkers records, per (collection, property, indexType), the
// most recent time a property-index DELETE was accepted, so GET /indexes can
// tell "index was deleted after its task finished" (suppress the
// finalize-window "indexing@100%" bleed) from a live re-enable (STARTED
// task, never suppressed).
//
// Node-local and best-effort: a GET served by a different node in a
// multi-node cluster may still show the bleed for the short finalize
// window — acceptable, since the finalize-window override itself is
// already a per-node, cosmetic mitigation.
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
