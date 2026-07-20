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

// FinalizeWindowMax is the upper clamp on the GET-indexes finalize-window
// bleed. It lives beside the marker (whose only job is to suppress that bleed)
// so the two move together; adapters/handlers/rest clamps to this value.
const FinalizeWindowMax = 10 * time.Second

// reindexDeleteMarkerTTL must outlive the finalize window it suppresses, so it
// is derived from FinalizeWindowMax rather than independently encoded — raising
// the window can't silently let a post-DELETE phantom outlive its marker.
const reindexDeleteMarkerTTL = 3 * FinalizeWindowMax

// ReindexDeleteMarkers tracks the last accepted DELETE per (collection,
// property, indexType) so GET /indexes can suppress the finalize-window
// bleed for a deleted index without suppressing a live re-enable.
// Node-local and best-effort: a GET on a different node may briefly still
// show the bleed, same as the per-node finalize-window mitigation itself.
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

// Record marks a DELETE for (collection, property, indexType) as accepted
// now, pruning expired entries so the map stays bounded. indexType must be
// the canonical status-type spelling ("searchable"/"filterable"/
// "rangeFilters") to match what GET /indexes queries.
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
