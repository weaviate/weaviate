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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// White-box on purpose: the TTL constant, the key builder, and the backing
// map are all package-private, and pinning TTL expiry / prune eviction /
// the case-fold asymmetry requires reaching them directly rather than
// waiting 30s of wall-clock per boundary case.
func TestReindexDeleteMarkers(t *testing.T) {
	t.Run("record then LastDeleted round-trip", func(t *testing.T) {
		m := NewReindexDeleteMarkers()
		require.True(t, m.LastDeleted("Foo", "title", "searchable").IsZero(),
			"an unrecorded lookup must return the zero time")

		before := time.Now()
		m.Record("Foo", "title", "searchable")
		after := time.Now()

		got := m.LastDeleted("Foo", "title", "searchable")
		require.False(t, got.IsZero(), "a freshly recorded marker must be live")
		assert.False(t, got.Before(before), "stored time must not predate the Record call")
		assert.False(t, got.After(after), "stored time must not postdate the Record call")

		// A second Record for the same tuple advances the remembered time
		// (LastDeleted returns the most recent DELETE).
		time.Sleep(2 * time.Millisecond)
		m.Record("Foo", "title", "searchable")
		assert.True(t, m.LastDeleted("Foo", "title", "searchable").After(got),
			"re-recording must advance the remembered DELETE time")
	})

	t.Run("TTL boundary: within TTL is live, beyond TTL is expired", func(t *testing.T) {
		// Margins are ±1s against a 30s TTL — orders of magnitude above any
		// test-execution jitter, so the boundary is deterministic. Ages are
		// derived from the constant so the test tracks a TTL change.
		cases := []struct {
			name string
			age  time.Duration
			live bool
		}{
			{"just recorded is live", time.Second, true},
			{"just under TTL is live", reindexDeleteMarkerTTL - time.Second, true},
			{"just over TTL is expired", reindexDeleteMarkerTTL + time.Second, false},
			{"far past TTL is expired", 10 * reindexDeleteMarkerTTL, false},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				m := NewReindexDeleteMarkers()
				seeded := time.Now().Add(-tc.age)
				m.deleted[reindexDeleteMarkerKey("Foo", "title", "searchable")] = seeded

				got := m.LastDeleted("Foo", "title", "searchable")
				if tc.live {
					require.False(t, got.IsZero(), "within TTL must return the stored time")
					assert.Equal(t, seeded, got, "a live lookup returns the exact stored time")
				} else {
					assert.True(t, got.IsZero(), "beyond TTL must return the zero time")
				}
			})
		}
	})

	t.Run("Record prunes expired markers (map shrinks)", func(t *testing.T) {
		expired := time.Now().Add(-(reindexDeleteMarkerTTL + time.Second))
		m := NewReindexDeleteMarkers()
		m.deleted["a/x/searchable"] = expired
		m.deleted["b/y/filterable"] = expired
		m.deleted["c/z/rangeFilters"] = expired
		require.Len(t, m.deleted, 3, "precondition: three expired markers seeded")

		m.Record("Live", "title", "searchable")

		// Record evicts all three expired entries and adds exactly one fresh
		// one: net map size shrinks from 3 to 1.
		assert.Len(t, m.deleted, 1, "prune must evict every expired marker, leaving only the fresh one")
		_, freshPresent := m.deleted[reindexDeleteMarkerKey("Live", "title", "searchable")]
		assert.True(t, freshPresent, "the freshly recorded marker must remain")
		for _, k := range []string{"a/x/searchable", "b/y/filterable", "c/z/rangeFilters"} {
			_, ok := m.deleted[k]
			assert.False(t, ok, "expired marker %q must be evicted", k)
		}
	})

	t.Run("Record prunes only expired markers, not live ones", func(t *testing.T) {
		m := NewReindexDeleteMarkers()
		m.deleted["old/x/searchable"] = time.Now().Add(-(reindexDeleteMarkerTTL + time.Second))
		m.deleted["fresh/y/filterable"] = time.Now().Add(-time.Second)

		m.Record("New", "title", "searchable")

		assert.Len(t, m.deleted, 2, "the within-TTL marker plus the fresh one survive; only the expired one is evicted")
		_, expiredGone := m.deleted["old/x/searchable"]
		assert.False(t, expiredGone, "expired marker must be evicted")
		_, liveKept := m.deleted["fresh/y/filterable"]
		assert.True(t, liveKept, "a within-TTL marker must survive the prune")
	})

	t.Run("case-fold asymmetry is pinned (current behavior, not an endorsement)", func(t *testing.T) {
		// reindexDeleteMarkerKey lowercases `collection` but leaves `property`
		// and `indexType` verbatim. That asymmetry is deliberate today: the
		// GET-status bleed suppression keys off the exact canonical indexType
		// spelling ("searchable"/"filterable"/"rangeFilters"). This test pins
		// the asymmetry so a future change that starts or stops folding any of
		// the three fields is a conscious edit here, not a silent behavior drift.
		assert.Equal(t, "mycollection/Title/searchable",
			reindexDeleteMarkerKey("MyCollection", "Title", "searchable"),
			"collection must fold to lower-case; property and indexType must be preserved verbatim")

		const (
			col  = "MyCollection"
			prop = "Title"
			idx  = "searchable"
		)
		cases := []struct {
			name        string
			lookupCol   string
			lookupProp  string
			lookupIdx   string
			shouldMatch bool
		}{
			{"exact match", col, prop, idx, true},
			{"collection lower-cased still matches", "mycollection", prop, idx, true},
			{"collection upper-cased still matches", "MYCOLLECTION", prop, idx, true},
			{"property case differs does NOT match", col, "title", idx, false},
			{"indexType case differs does NOT match", col, prop, "Searchable", false},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				m := NewReindexDeleteMarkers()
				m.Record(col, prop, idx)
				got := m.LastDeleted(tc.lookupCol, tc.lookupProp, tc.lookupIdx)
				if tc.shouldMatch {
					assert.False(t, got.IsZero(), "expected the recorded marker to match this lookup")
				} else {
					assert.True(t, got.IsZero(), "expected NO match: property/indexType are case-sensitive")
				}
			})
		}
	})

	t.Run("nil receiver is safe", func(t *testing.T) {
		var m *ReindexDeleteMarkers
		assert.NotPanics(t, func() { m.Record("Foo", "title", "searchable") },
			"Record on a nil tracker must be a no-op, not a panic")
		assert.True(t, m.LastDeleted("Foo", "title", "searchable").IsZero(),
			"LastDeleted on a nil tracker must return the zero time")
	})
}
