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
	"context"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// filteredAnswers searches every query of the fixture with a filter, checks
// that no answer holds a doc id the filter leaves out or one held tells is
// not held, and returns the answers and their recall against brute force
// over the first n vectors.
func (f *persistentFixture) filteredAnswers(t *testing.T, n, k int, allow helpers.AllowList, held func(uint64) bool,
	at string,
) ([][]uint64, float64) {
	t.Helper()
	ctx := context.Background()
	answers := make([][]uint64, len(f.queries))
	hits, want := 0, 0
	for i, query := range f.queries {
		ids, dists, err := f.index.SearchByVector(ctx, query, k, allow)
		require.NoError(t, err, "%s, query %d", at, i)
		require.True(t, slices.IsSorted(dists), "%s, query %d", at, i)
		for _, id := range ids {
			require.True(t, allow.Contains(id), "%s, query %d: doc %d is left out by the filter", at, i, id)
			require.True(t, held == nil || held(id), "%s, query %d: doc %d is not held", at, i, id)
		}
		allowed := func(id uint64) bool { return allow.Contains(id) && (held == nil || held(id)) }
		truth := f.truth(t, query, n, k, allowed)
		for _, id := range truth {
			if slices.Contains(ids, id) {
				hits++
			}
		}
		want += len(truth)
		answers[i] = ids
	}
	if want == 0 {
		return answers, 1
	}
	return answers, float64(hits) / float64(want)
}

// pathOf tells whether the index searches the given filter flat.
func (f *persistentFixture) pathOf(t *testing.T, allow helpers.AllowList) bool {
	t.Helper()
	f.index.mu.RLock()
	defer f.index.mu.RUnlock()
	stats := &diskSearchStats{}
	_, _, err := f.index.searchIndex(context.Background(), f.queries[0], 10, allow, stats)
	require.NoError(t, err)
	return stats.Flat
}

// The index a shard opens takes a filter: with fewer allowed vectors than its
// cutoff it searches them flat, and with more it searches the index with the
// filter. Before its base it searches exactly, among the allowed. Neither way
// returns a doc id the filter leaves out, nor one deleted, and both find what
// the filter leaves: the flat search every time, the search of the index
// where the filter leaves a tenth or more at random, as the measure asks. An
// empty filter finds nothing. Opened again the index answers the same. So in
// both ways of keeping the codes: with no codes in memory the flat search
// ranks the allowed vectors by the full codes of their postings
// (integration-plan.md, section 8, amendment 3).
func TestPersistentFilteredSearch(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) { persistentFilteredSearch(t, cacheMode) })
	}
}

func persistentFilteredSearch(t *testing.T, cacheMode string) {
	ctx := context.Background()
	const total, k = 3000, 10
	// Under the cutoff of the test, a tenth of the vectors is searched flat
	// and half of them through the index.
	const cutoff = 500
	f := newPersistentFixtureMode(t, total, cacheMode, func(cfg *PersistentConfig) { cfg.flatCutoff = cutoff })
	near, far := f.normalized[0], f.normalized[0]
	worst := float32(0)
	for _, vector := range f.normalized {
		dist, err := f.provider.SingleDist(f.normalized[0], vector)
		require.NoError(t, err)
		if dist > worst {
			far, worst = vector, dist
		}
	}

	// Before the base: the vectors kept aside, searched exactly.
	f.add(t, 0, persistentBase/2)
	require.Nil(t, f.index.g)
	aside := testFilters(t, f.provider, f.normalized[:persistentBase/2], near, far, 0.5, 0.1)
	for _, filter := range aside {
		_, recall := f.filteredAnswers(t, persistentBase/2, k, filter.allow, nil, "kept aside, "+filter.name)
		require.Equal(t, 1.0, recall, "kept aside, %s: exact", filter.name)
	}

	f.add(t, persistentBase/2, total)
	require.NotNil(t, f.index.g)
	filters := testFilters(t, f.provider, f.normalized, near, far, 0.5, 0.1, 0.01)
	paths := []struct {
		name   string
		cutoff int
	}{
		{name: "the cutoff of the test", cutoff: cutoff},
		{name: "never flat", cutoff: -1},
		{name: "always flat", cutoff: math.MaxInt},
	}
	flatOn := func(cutoff int, allow helpers.AllowList) bool {
		return cutoff == math.MaxInt || cutoff > 0 && allow.Len() < cutoff
	}
	for _, path := range paths {
		f.index.cfg.flatCutoff = path.cutoff
		for _, filter := range filters {
			at := path.name + ", " + filter.name
			flat := flatOn(path.cutoff, filter.allow)
			require.Equal(t, flat, f.pathOf(t, filter.allow), "%s: %d allowed", at, filter.allow.Len())
			_, recall := f.filteredAnswers(t, total, k, filter.allow, nil, at)
			t.Logf("%s: recall %.3f, flat %v", at, recall, flat)
			if flat {
				assert.GreaterOrEqual(t, recall, 0.95, at)
			} else if filter.name == "0.5 at random" || filter.name == "0.1 at random" {
				assert.GreaterOrEqual(t, recall, 0.9, at)
			}
		}
	}
	empty, _, err := f.index.SearchByVector(ctx, f.queries[0], k, helpers.NewAllowList())
	require.NoError(t, err)
	require.Empty(t, empty, "an empty filter")
	f.covered(t, "searched with filters")

	// A third of the vectors deleted: none comes back through a filter, by
	// either way.
	deleted := map[uint64]bool{}
	var ids []uint64
	for id := uint64(0); id < total; id += 3 {
		deleted[id] = true
		ids = append(ids, id)
	}
	require.NoError(t, f.index.Delete(ids...))
	held := func(id uint64) bool { return !deleted[id] }
	for _, path := range paths[1:] {
		f.index.cfg.flatCutoff = path.cutoff
		for _, filter := range filters {
			at := path.name + ", deleted, " + filter.name
			_, recall := f.filteredAnswers(t, total, k, filter.allow, held, at)
			t.Logf("%s: recall %.3f", at, recall)
			if flatOn(path.cutoff, filter.allow) {
				assert.GreaterOrEqual(t, recall, 0.95, at)
			}
		}
	}

	// Opened again, the same answers, by both ways.
	before := map[string][][]uint64{}
	for _, path := range paths[1:] {
		f.index.cfg.flatCutoff = path.cutoff
		for _, filter := range filters {
			at := path.name + ", " + filter.name
			before[at], _ = f.filteredAnswers(t, total, k, filter.allow, held, "before the restart, "+at)
		}
	}
	f.restart(t)
	for _, path := range paths[1:] {
		f.index.cfg.flatCutoff = path.cutoff
		for _, filter := range filters {
			at := path.name + ", " + filter.name
			require.Equal(t, flatOn(path.cutoff, filter.allow), f.pathOf(t, filter.allow), "after the restart, %s", at)
			after, _ := f.filteredAnswers(t, total, k, filter.allow, held, "after the restart, "+at)
			assert.Equal(t, before[at], after, "%s: the same answers after a restart", at)
		}
	}
}

// The flat search finds about as much with no codes in memory as with them.
// Two indexes of the same vectors, one each way, search flat with a filter at
// random and with one in a ball near a vector and far from it, at shares that
// allow more than they rescore: their recall against brute force is the same
// within 0.02, and at least 0.95. With no codes in memory the flat search
// ranks by the full codes of the postings (integration-plan.md, section 8,
// amendment 3); at the 64 dimensions of the fixture the routing code is the
// full code, so they differ only in how ties of the code fall.
func TestPersistentFlatSearchTwins(t *testing.T) {
	const total, k = 3000, 10
	twins := map[string]*persistentFixture{}
	for _, cacheMode := range cacheModes {
		f := newPersistentFixtureMode(t, total, cacheMode, func(cfg *PersistentConfig) { cfg.flatCutoff = math.MaxInt })
		f.add(t, 0, total)
		twins[cacheMode] = f
	}
	routing := twins[ent.CacheModeRouting]
	near, far, worst := routing.normalized[0], routing.normalized[0], float32(0)
	for _, vector := range routing.normalized {
		dist, err := routing.provider.SingleDist(near, vector)
		require.NoError(t, err)
		if dist > worst {
			far, worst = vector, dist
		}
	}
	// The fixture rescores 100: both shares allow more.
	filters := testFilters(t, routing.provider, routing.normalized, near, far, 0.1, 0.05)
	for _, filter := range filters {
		recall := map[string]float64{}
		for _, cacheMode := range cacheModes {
			f := twins[cacheMode]
			at := cacheMode + ", " + filter.name
			require.True(t, f.pathOf(t, filter.allow), at)
			_, recall[cacheMode] = f.filteredAnswers(t, total, k, filter.allow, nil, at)
			assert.GreaterOrEqual(t, recall[cacheMode], 0.95, at)
		}
		t.Logf("%s: recall %.3f with the codes in memory, %.3f with none", filter.name,
			recall[ent.CacheModeRouting], recall[ent.CacheModeNone])
		assert.InDelta(t, recall[ent.CacheModeRouting], recall[ent.CacheModeNone], 0.02, filter.name)
	}
}

// The flat search finds the vectors still kept aside next to the base, both
// ways of keeping the codes. With the base the index and every vector that
// came as it was built still aside, a search with a filter returns only what
// the filter allows, finds each of those vectors that the filter allows as
// its own nearest, and finds what brute force does; and so once the index has
// taken them all.
func TestPersistentFlatSearchWithVectorsKeptAside(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) {
			ctx := context.Background()
			const base, total, k = persistentBase, persistentBase + 250, 10
			steps := newBaseSteps()
			// The index is held before it takes the first vector kept aside,
			// with no lock held, until the test lets it go.
			held, hold := make(chan struct{}), make(chan struct{})
			var holding, letGo sync.Once
			release := func() { letGo.Do(func() { close(hold) }) }
			f := newPersistentFixtureMode(t, total, cacheMode, func(cfg *PersistentConfig) {
				cfg.baseHook = steps.hook
				cfg.drainHook = func() { holding.Do(func() { close(held); <-hold }) }
				cfg.flatCutoff = math.MaxInt
			})
			// A check that fails leaves the build stopped: it is let go before
			// the index closes, or the close waits for it forever.
			t.Cleanup(func() {
				release()
				steps.stopping.Store(false)
				for {
					select {
					case <-steps.reached:
					case steps.proceed <- struct{}{}:
					case <-time.After(200 * time.Millisecond):
						return
					}
				}
			})
			filter := randomFilter(total, 0.5, 11)
			check := func(at string) {
				t.Helper()
				for id := uint64(base); id < total; id++ {
					if !filter.Contains(id) {
						continue
					}
					ids, _, err := f.index.SearchByVector(ctx, f.vectors[id], 1, filter)
					require.NoError(t, err, "%s: doc %d", at, id)
					require.Equal(t, []uint64{id}, ids, "%s: doc %d is its own nearest", at, id)
				}
				_, recall := f.filteredAnswers(t, total, k, filter, nil, at)
				t.Logf("%s: recall %.3f", at, recall)
				assert.GreaterOrEqual(t, recall, 0.95, at)
			}

			f.addBatches(t, 0, base)
			steps.at(t, "the vectors of the base are read")
			f.addBatches(t, base, total)
			steps.proceed <- struct{}{}
			steps.at(t, "the base is written")
			steps.proceed <- struct{}{}
			steps.at(t, "the base is the index")
			steps.stopping.Store(false)
			steps.proceed <- struct{}{}
			select {
			case <-held:
			case <-time.After(2 * time.Minute):
				require.FailNow(t, "the index did not get to the vectors kept aside")
			}
			f.index.mu.RLock()
			require.NotNil(t, f.index.g)
			aside := len(f.index.staged)
			f.index.mu.RUnlock()
			require.Equal(t, total-base, aside, "every vector that came as the base was built is aside")
			require.True(t, f.pathOf(t, filter))
			check("with vectors kept aside")

			release()
			require.NoError(t, f.index.settleBase(ctx))
			f.index.mu.RLock()
			require.Empty(t, f.index.staged)
			f.index.mu.RUnlock()
			check("with every vector taken")
		})
	}
}

// The flat search rescores what the rescore limit of the index gives for the
// width of the code that ranks it, never a number of its own: with the
// automatic limit the rule of the rescore at that width, 507 for the routing
// code of 512 bits and 64 for a full code of 1 536, with four times k and ef
// over it; with a limit of the class, that limit, with k and ef over it
// (integration-plan.md, section 8, amendment 3). R is the larger of that limit
// and ef, as the user decided on 2026-11-23: it grows with ef past 507, and a
// limit set by hand is kept.
func TestFlatRescore(t *testing.T) {
	tests := []struct {
		name         string
		rescoreLimit int
		bits, k, ef  int
		wantRescored int
	}{
		{name: "automatic, the routing code of 512 bits", rescoreLimit: ent.RescoreLimitAuto, bits: 512, k: 10, ef: 20, wantRescored: 507},
		{name: "automatic, a full code of 1 536 bits", rescoreLimit: ent.RescoreLimitAuto, bits: 1536, k: 10, ef: 20, wantRescored: 64},
		{name: "automatic, 768 bits", rescoreLimit: ent.RescoreLimitAuto, bits: 768, k: 10, ef: 20, wantRescored: 150},
		{name: "automatic, four times k over the rule", rescoreLimit: ent.RescoreLimitAuto, bits: 512, k: 200, ef: 20, wantRescored: 800},
		{name: "automatic, ef over the rule", rescoreLimit: ent.RescoreLimitAuto, bits: 512, k: 10, ef: 1000, wantRescored: 1000},
		{name: "automatic, the rule over ef", rescoreLimit: ent.RescoreLimitAuto, bits: 512, k: 10, ef: 500, wantRescored: 507},
		{name: "automatic, ef 800 over 507", rescoreLimit: ent.RescoreLimitAuto, bits: 512, k: 10, ef: 800, wantRescored: 800},
		{name: "a limit of 100 set by hand, ef 20", rescoreLimit: 100, bits: 512, k: 10, ef: 20, wantRescored: 100},
		{name: "a limit of 100 set by hand, ef 300", rescoreLimit: 100, bits: 512, k: 10, ef: 300, wantRescored: 300},
		{name: "a limit of the class", rescoreLimit: 300, bits: 512, k: 10, ef: 20, wantRescored: 300},
		{name: "a limit of the class, k over it", rescoreLimit: 300, bits: 512, k: 400, ef: 20, wantRescored: 400},
		{name: "a limit of the class, ef over it", rescoreLimit: 300, bits: 512, k: 10, ef: 2000, wantRescored: 2000},
		{name: "no rescore asked: still k and ef", rescoreLimit: 0, bits: 512, k: 10, ef: 20, wantRescored: 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := ent.NewDefaultUserConfig()
			uc.RQ.RescoreLimit = tt.rescoreLimit
			p := &Persistent{uc: uc, g: &growIndex{ix: &Index{}}}
			p.g.ix.ef.Store(int64(tt.ef))
			require.Equal(t, tt.wantRescored, p.flatRescore(tt.k, tt.bits))
		})
	}
}
