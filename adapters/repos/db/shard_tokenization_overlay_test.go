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

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
)

// TestShard_TokenizationOverlay_* pin the per-shard property overlay
// lifecycle introduced for https://github.com/weaviate/0-weaviate-issues/issues/216 (Gap B). The overlay
// is documented on Shard's [propertyOverlay] field.
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
	s.SetPropertyOverlay("name", inverted.PropertyOverlay{Tokenization: "field"})

	// Overlay value wins while the live schema hasn't caught up.
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))

	// Unrelated propNames are unaffected.
	assert.Equal(t, "word", s.TokenizationFor("other", "word"))
}

func TestShard_TokenizationOverlay_SetEmptyValues_NoOp(t *testing.T) {
	s := &Shard{}
	s.SetPropertyOverlay("", inverted.PropertyOverlay{Tokenization: "field"}) // empty propName
	s.SetPropertyOverlay("name", inverted.PropertyOverlay{})                  // nothing to override
	// Neither call should have populated the overlay.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestShard_TokenizationOverlay_ClearExplicit(t *testing.T) {
	s := &Shard{}
	s.SetPropertyOverlay("name", inverted.PropertyOverlay{Tokenization: "field"})
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))

	s.ClearPropertyOverlay("name", inverted.PropertyOverlay{Tokenization: "field"})
	// Cleared → fall back to liveTokenization.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestShard_TokenizationOverlay_ClearUnsetIsNoOp(t *testing.T) {
	s := &Shard{}
	// Clearing a never-set entry is safe.
	s.ClearPropertyOverlay("name", inverted.PropertyOverlay{Tokenization: "field"})
	s.ClearPropertyOverlay("", inverted.PropertyOverlay{Tokenization: "field"})
	// Live fallback still works.
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestShard_TokenizationOverlay_SnapshotEmpty(t *testing.T) {
	s := &Shard{}
	// No overlay → nil snapshot regardless of how many props requested.
	assert.Nil(t, s.SnapshotPropertyOverlay(nil))
	assert.Nil(t, s.SnapshotPropertyOverlay([]string{}))
	assert.Nil(t, s.SnapshotPropertyOverlay([]string{"a", "b"}))
}

func TestShard_TokenizationOverlay_SnapshotSubset(t *testing.T) {
	s := &Shard{}
	s.SetPropertyOverlay("a", inverted.PropertyOverlay{Tokenization: "field"})
	s.SetPropertyOverlay("b", inverted.PropertyOverlay{ForceFilterable: true})
	// "c" is not in the overlay.

	// Asking for only the props the caller cares about.
	snap := s.SnapshotPropertyOverlay([]string{"a", "c"})
	require.NotNil(t, snap)
	assert.Equal(t, inverted.PropertyOverlay{Tokenization: "field"}, snap["a"])
	_, present := snap["c"]
	assert.False(t, present, "non-overlaid prop must not appear in snapshot")
	assert.Len(t, snap, 1)

	// A request that hits no overlay entries returns nil so the analyzer
	// can take its fast path.
	assert.Nil(t, s.SnapshotPropertyOverlay([]string{"c", "d"}))
}

func TestShard_TokenizationOverlay_ConcurrentAccess(t *testing.T) {
	// Pin the RWMutex contract: many concurrent readers + a writer must
	// not race. Run under `go test -race` to catch any regression in the
	// lock discipline.
	s := &Shard{}
	s.SetPropertyOverlay("name", inverted.PropertyOverlay{Tokenization: "field"})

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
				_ = s.SnapshotPropertyOverlay([]string{"name"})
			}
		}()
	}

	// Writers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				s.SetPropertyOverlay("name", inverted.PropertyOverlay{Tokenization: "field"})
				s.ClearPropertyOverlay("name", inverted.PropertyOverlay{Tokenization: "field"})
			}
		}()
	}

	wg.Wait()
}
