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

package db

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShard_TokenizationOverlay_* pin the per-shard tokenization overlay
// lifecycle introduced for 0-weaviate-issues#216 (Gap B). The overlay
// bridges the per-replica window between a change-tokenization
// migration's local bucket swap (in OnGroupCompleted.RunSwapOnShard) and
// the cluster-wide schema flip (in OnTaskCompleted's
// flipSemanticMigrationSchema). See the [tokenizationOverlay] field
// godoc on Shard for the full rationale.
//
// We exercise the helper methods directly against a zero-valued Shard
// struct because they touch only the per-shard map + mutex — no other
// shard wiring is required.

func TestShard_TokenizationOverlay_NotSet_FallsBackToLive(t *testing.T) {
	s := &Shard{}
	// No overlay entries → fall back to liveTokenization.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
	assert.Equal(t, "field", s.TokenizationFor("path", "field"))
	// Empty propName is a no-op.
	assert.Equal(t, "word", s.TokenizationFor("", "word"))
}

func TestShard_TokenizationOverlay_SetAndRead(t *testing.T) {
	s := &Shard{}
	s.SetTokenizationOverlay("name", "field")

	// Overlay value wins while the live schema hasn't caught up.
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))

	// Unrelated propNames are unaffected.
	assert.Equal(t, "word", s.TokenizationFor("other", "word"))
}

func TestShard_TokenizationOverlay_SetEmptyValues_NoOp(t *testing.T) {
	s := &Shard{}
	s.SetTokenizationOverlay("", "field") // empty propName
	s.SetTokenizationOverlay("name", "")  // empty target
	// Neither call should have populated the overlay.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestShard_TokenizationOverlay_ClearExplicit(t *testing.T) {
	s := &Shard{}
	s.SetTokenizationOverlay("name", "field")
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))

	s.ClearTokenizationOverlay("name")
	// Cleared → fall back to liveTokenization.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestShard_TokenizationOverlay_ClearUnsetIsNoOp(t *testing.T) {
	s := &Shard{}
	// Clearing a never-set entry is safe.
	s.ClearTokenizationOverlay("name")
	s.ClearTokenizationOverlay("")
	// Live fallback still works.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestShard_TokenizationOverlay_SelfClearOnSchemaCatchup(t *testing.T) {
	// This is the defensive branch — if the schema-update callback hasn't
	// fired yet but the live schema has already caught up to the overlay's
	// target, the next TokenizationFor call self-clears the overlay and
	// returns the live value.
	s := &Shard{}
	s.SetTokenizationOverlay("name", "field")

	// First call: live schema still has the OLD value → overlay wins.
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))

	// Live schema catches up to the overlay's target.
	got := s.TokenizationFor("name", "field")
	assert.Equal(t, "field", got, "live should match overlay → return overlay value")

	// The self-clear should have fired; subsequent calls take the fast
	// path and return whatever live the caller passes — even if it's
	// changed back (operator did a downstream migration). Verifies the
	// overlay is actually removed, not just shadowed.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"),
		"after self-clear, overlay must no longer override")
}

func TestShard_TokenizationOverlay_SnapshotEmpty(t *testing.T) {
	s := &Shard{}
	// No overlay → nil snapshot regardless of how many props requested.
	assert.Nil(t, s.SnapshotTokenizationOverlay(nil))
	assert.Nil(t, s.SnapshotTokenizationOverlay([]string{}))
	assert.Nil(t, s.SnapshotTokenizationOverlay([]string{"a", "b"}))
}

func TestShard_TokenizationOverlay_SnapshotSubset(t *testing.T) {
	s := &Shard{}
	s.SetTokenizationOverlay("a", "field")
	s.SetTokenizationOverlay("b", "lowercase")
	// "c" is not in the overlay.

	// Asking for only the props the caller cares about.
	snap := s.SnapshotTokenizationOverlay([]string{"a", "c"})
	require.NotNil(t, snap)
	assert.Equal(t, "field", snap["a"])
	_, present := snap["c"]
	assert.False(t, present, "non-overlaid prop must not appear in snapshot")
	assert.Len(t, snap, 1)

	// A request that hits no overlay entries returns nil so the analyzer
	// can take its fast path.
	assert.Nil(t, s.SnapshotTokenizationOverlay([]string{"c", "d"}))
}

func TestShard_TokenizationOverlay_ConcurrentAccess(t *testing.T) {
	// Pin the RWMutex contract: many concurrent readers + a writer must
	// not race. Run under `go test -race` to catch any regression in the
	// lock discipline.
	s := &Shard{}
	s.SetTokenizationOverlay("name", "field")

	const goroutines = 32
	const iterations = 200
	var wg sync.WaitGroup

	// Readers
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = s.TokenizationFor("name", "word")
				_ = s.SnapshotTokenizationOverlay([]string{"name"})
			}
		}()
	}

	// Writers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				s.SetTokenizationOverlay("name", "field")
				s.ClearTokenizationOverlay("name")
			}
		}()
	}

	wg.Wait()
}
