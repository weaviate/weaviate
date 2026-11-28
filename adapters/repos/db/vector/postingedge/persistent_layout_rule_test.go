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

package postingedge

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// An index that is being ingested into lays its vectors out less often than
// one at rest: a pass writes every exact vector again, and competes with the
// inserts for the disk. With an insert in the last minute a pass is due once
// the index holds four times the vectors of the last pass; a minute after
// its last insert, once it has doubled, as before. Whoever settles the index
// gets the rule of an index at rest at once. The clock is the test's.
func TestPersistentLaysOutLessOftenWhileItIsIngestedInto(t *testing.T) {
	t.Parallel()
	const base = doublingBase
	const total = 4*base + 20
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	f := newDoublingFixture(t, total, ent.CacheModeRouting, func(cfg *PersistentConfig) {
		cfg.noBackground = true
		cfg.layoutQuiet = 0
		cfg.now = now
	})
	require.Equal(t, DefaultLayoutQuiet, f.index.cfg.layoutQuiet, "the default")
	ctx := persistentCtx()
	state := func() (gen, laidOut int, ingesting, due, dueAtRest bool) {
		f.index.mu.RLock()
		defer f.index.mu.RUnlock()
		g := f.index.g
		return g.meta.VectorGen, g.meta.VectorsLaidOut, f.index.ingesting(), f.index.layoutDue(false), f.index.layoutDue(true)
	}
	// background is what the index does by itself, without anybody settling
	// it.
	background := func() {
		t.Helper()
		require.NoError(t, f.index.maintain(ctx, false))
	}

	// The index doubles while it is ingested into: no pass.
	f.add(t, 0, 2*base+10)
	gen, laidOut, ingesting, due, dueAtRest := state()
	require.Equal(t, 1, gen)
	require.Equal(t, base, laidOut)
	require.True(t, ingesting, "an insert was just applied")
	require.False(t, due, "it has doubled, and is being ingested into")
	require.True(t, dueAtRest, "an index at rest would lay its vectors out")
	background()
	gen, _, _, _, _ = state()
	require.Equal(t, 1, gen, "no pass while the index is ingested into and has not grown to four times")

	// Just short of a minute later it still counts as ingested into.
	clock.Add(int64(DefaultLayoutQuiet - time.Second))
	background()
	gen, _, ingesting, _, _ = state()
	require.True(t, ingesting)
	require.Equal(t, 1, gen)

	// It grows to four times what the last pass laid out, still ingested
	// into: a pass is due, and takes every vector.
	f.add(t, 2*base+10, 4*base)
	_, _, ingesting, due, _ = state()
	require.True(t, ingesting)
	require.True(t, due, "it holds four times the vectors of the last pass")
	background()
	gen, laidOut, _, _, _ = state()
	require.Equal(t, 2, gen)
	require.Equal(t, 4*base, laidOut)

	// A few more, and a minute with no insert: the rule of an index at rest,
	// which has not doubled since the pass.
	f.add(t, 4*base, total)
	clock.Add(int64(DefaultLayoutQuiet))
	_, _, ingesting, due, dueAtRest = state()
	require.False(t, ingesting, "a minute after its last insert")
	require.False(t, due)
	require.False(t, dueAtRest)
	f.covered(t, "at the end")
	f.vectorsIntact(t, "at the end")
}

// An index that doubled while it was ingested into lays its vectors out a
// minute after its last insert, by itself, and at once for whoever settles
// it.
func TestPersistentLaysOutOnceTheIngestEnds(t *testing.T) {
	t.Parallel()
	const base = doublingBase
	const total = 2*base + 10
	tests := []struct {
		name string
		// settle has the test settle the index right after its last insert;
		// otherwise a minute passes and the index does what it does by itself.
		settle bool
	}{
		{name: "a minute after its last insert"},
		{name: "settled right after its last insert", settle: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var clock atomic.Int64
			clock.Store(time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC).UnixNano())
			f := newDoublingFixture(t, total, ent.CacheModeRouting, func(cfg *PersistentConfig) {
				cfg.noBackground = true
				cfg.layoutQuiet = 0
				cfg.now = func() time.Time { return time.Unix(0, clock.Load()) }
			})
			ctx := persistentCtx()
			f.add(t, 0, total)
			require.NoError(t, f.index.maintain(ctx, false))
			require.Zero(t, f.index.layouts.Load(), "no pass while it is ingested into")
			if tt.settle {
				require.NoError(t, f.index.Settle(ctx))
			} else {
				clock.Add(int64(DefaultLayoutQuiet))
				require.NoError(t, f.index.maintain(ctx, false))
			}
			require.Equal(t, int64(1), f.index.layouts.Load(), "the pass an index at rest that has doubled is due")
			f.index.mu.RLock()
			laidOut := f.index.g.meta.VectorsLaidOut
			f.index.mu.RUnlock()
			assert.Equal(t, total, laidOut)
			f.vectorsIntact(t, "laid out")
		})
	}
}

// The default of the index a shard opens is the rule that lays out less often
// during an ingest.
func TestPersistentConfigLaysOutLessOftenByDefault(t *testing.T) {
	t.Parallel()
	cfg := PersistentConfig{
		ID: "vectors_test", RootPath: t.TempDir(), DistanceProvider: distancer.NewCosineDistanceProvider(),
	}
	require.NoError(t, cfg.validate())
	assert.Equal(t, time.Minute, cfg.layoutQuiet)
	assert.NotNil(t, cfg.now)
	assert.Equal(t, 3, layoutIngestFactor, "a pass during an ingest once the index holds four times as many")
}
