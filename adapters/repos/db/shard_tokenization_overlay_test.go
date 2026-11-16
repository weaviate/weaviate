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
	"context"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
)

// TestShard_TokenizationOverlay_* pin the per-shard property overlay
// lifecycle introduced for https://github.com/weaviate/0-weaviate-issues/issues/216 (Gap B). The overlay
// is documented on Shard's [propertyOverlay] field.

// overlayShard holds an overlay and an empty store, which is all the pin reads.
func overlayShard(t *testing.T) *Shard {
	dir := t.TempDir()
	store, err := lsmkv.New(dir, dir, logrus.New(), nil, nil, cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })
	return &Shard{store: store}
}

// queryTokenization is the tokenization a query on propName gets.
func queryTokenization(s *Shard, propName, live string) string {
	tok, _, release := s.PinTokenizationAndBucket(propName, "", live)
	release()
	return tok
}

func TestShard_TokenizationOverlay_NotSet_FallsBackToLive(t *testing.T) {
	s := overlayShard(t)
	// No overlay entries → fall back to liveTokenization.
	assert.Equal(t, "word", queryTokenization(s, "name", "word"))
	assert.Equal(t, "field", queryTokenization(s, "path", "field"))
	// Empty propName is a no-op.
	assert.Equal(t, "word", queryTokenization(s, "", "word"))
}

func TestShard_TokenizationOverlay_SetAndRead(t *testing.T) {
	s := overlayShard(t)
	s.SetPropertyOverlay("name", inverted.PropertyOverlay{Tokenization: "field"})

	// Overlay value wins while the live schema hasn't caught up.
	assert.Equal(t, "field", queryTokenization(s, "name", "word"))

	// Unrelated propNames are unaffected.
	assert.Equal(t, "word", queryTokenization(s, "other", "word"))
}

func TestShard_TokenizationOverlay_SetEmptyValues_NoOp(t *testing.T) {
	s := overlayShard(t)
	s.SetPropertyOverlay("", inverted.PropertyOverlay{Tokenization: "field"}) // empty propName
	s.SetPropertyOverlay("name", inverted.PropertyOverlay{})                  // nothing to override
	// Neither call should have populated the overlay.
	assert.Equal(t, "word", queryTokenization(s, "name", "word"))
}

// Which fields go is decided by the index type whose bucket is being removed,
// so the buckets that stay keep the entry that describes them.
func TestShard_PropertyOverlay_RetireByIndexType(t *testing.T) {
	full := inverted.PropertyOverlay{
		ForceFilterable: true,
		ForceSearchable: true,
		ForceRangeable:  true,
		Tokenization:    "field",
	}

	tests := []struct {
		name      string
		propName  string
		indexType string
		want      inverted.PropertyOverlay
	}{
		{
			name:      "filterable",
			propName:  "name",
			indexType: "filterable",
			want: inverted.PropertyOverlay{
				ForceSearchable: true, ForceRangeable: true, Tokenization: "field",
			},
		},
		{
			name:      "searchable takes the tokenization with it",
			propName:  "name",
			indexType: "searchable",
			want: inverted.PropertyOverlay{
				ForceFilterable: true, ForceRangeable: true,
			},
		},
		{
			name:      "rangeable",
			propName:  "name",
			indexType: "rangeable",
			want: inverted.PropertyOverlay{
				ForceFilterable: true, ForceSearchable: true, Tokenization: "field",
			},
		},
		{
			name:      "an index type with no bucket of its own changes nothing",
			propName:  "name",
			indexType: "propertyLength",
			want:      full,
		},
		{
			name:      "an empty property name is a no-op",
			propName:  "",
			indexType: "filterable",
			want:      full,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := overlayShard(t)
			s.SetPropertyOverlay("name", full)

			s.retirePropertyOverlay(tc.propName, tc.indexType)

			assert.Equal(t, tc.want, s.SnapshotPropertyOverlay([]string{"name"})["name"])
		})
	}
}

func TestShard_PropertyOverlay_RetireDropsTheEmptiedEntry(t *testing.T) {
	s := overlayShard(t)
	s.SetPropertyOverlay("name", inverted.PropertyOverlay{Tokenization: "field"})

	s.retirePropertyOverlay("name", "searchable")

	assert.Nil(t, s.SnapshotPropertyOverlay([]string{"name"}))
	assert.Equal(t, "word", queryTokenization(s, "name", "word"))
}

func TestShard_PropertyOverlay_RetireUnsetIsNoOp(t *testing.T) {
	s := overlayShard(t)
	s.retirePropertyOverlay("name", "filterable")
	assert.Equal(t, "word", queryTokenization(s, "name", "word"))
}

func TestShard_TokenizationOverlay_SnapshotEmpty(t *testing.T) {
	s := overlayShard(t)
	// No overlay → nil snapshot regardless of how many props requested.
	assert.Nil(t, s.SnapshotPropertyOverlay(nil))
	assert.Nil(t, s.SnapshotPropertyOverlay([]string{}))
	assert.Nil(t, s.SnapshotPropertyOverlay([]string{"a", "b"}))
}

func TestShard_TokenizationOverlay_SnapshotSubset(t *testing.T) {
	s := overlayShard(t)
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
	s := overlayShard(t)
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
				_ = queryTokenization(s, "name", "word")
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
				s.retirePropertyOverlay("name", "searchable")
			}
		}()
	}

	wg.Wait()
}
