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

//go:build postingedgebench

package postingedge

import (
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// The filters at random beyond those of the measure (-bench.filters.extrarandom)
// look for the crossover of the flat search by measuring it: the
// pre-registration "La búsqueda plana y el punto de cruce, otra vez, con HNSW
// centrado" of bench-report.md asks for 7 %, 5 % and 3 %. They are drawn by
// the same code as the four of the measure, randomFilterIDs, with a seed of
// their own that only their share sets, and go after the filters of the
// measure in the cache, which they extend without changing them.

// randomFilterIDs draws the doc ids of a filter at random: the given share of
// the n vectors, rounded, by a generator of the given seed, sorted.
func randomFilterIDs(n int, share float64, seed uint64) []uint64 {
	rng := rand.New(rand.NewPCG(seed, 7))
	ids := make([]uint64, int(math.Round(share*float64(n))))
	for i, id := range rng.Perm(n)[:len(ids)] {
		ids[i] = uint64(id)
	}
	slices.Sort(ids)
	return ids
}

// extraRandomSeed is the seed of a filter at random beyond those of the
// measure, which take 1000 to 1003 (the balls take 2000 to 2013): 3000 and
// the share in hundredths of a percent.
func extraRandomSeed(share float64) uint64 {
	return 3000 + uint64(math.Round(share*1e4))
}

// parseExtraShares reads -bench.filters.extrarandom: shares in percent, comma
// separated, each above 0 and below 100, in hundredths of a percent at most,
// none twice and none a share of the measure.
func parseExtraShares(spec string) ([]float64, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	var shares []float64
	for _, field := range strings.Split(spec, ",") {
		field = strings.TrimSpace(field)
		percent, err := strconv.ParseFloat(field, 64)
		if err != nil || percent <= 0 || percent >= 100 || math.IsNaN(percent) {
			return nil, errors.Errorf("%q is not a share in percent above 0 and below 100", field)
		}
		if hundredths := percent * 100; math.Abs(hundredths-math.Round(hundredths)) > 1e-9 {
			return nil, errors.Errorf("%q has more than two decimals", field)
		}
		share := percent / 100
		if slices.ContainsFunc(filterShares, func(s float64) bool { return math.Abs(s-share) < 1e-12 }) {
			return nil, errors.Errorf("%s %% is a share of the measure already", field)
		}
		if slices.ContainsFunc(shares, func(s float64) bool { return math.Abs(s-share) < 1e-12 }) {
			return nil, errors.Errorf("%s %% is named twice", field)
		}
		shares = append(shares, share)
	}
	return shares, nil
}

// drawRandomFilters draws filters at random at the given shares, each with
// the given seed, and finds their ground truth by brute force over the train
// set, which is in memory and normalized, as buildFilters does for its own:
// the same distance, the same order of the pushes and the same top k.
func drawRandomFilters(data *benchData, k int, shares []float64, seeds []uint64) []*benchFilter {
	n, q := len(data.train), len(data.queries)
	provider := distancer.NewCosineDistanceProvider()
	ids := make([][]uint64, len(shares))
	in := make([][]bool, len(shares))
	inAny := make([]bool, n)
	for s, share := range shares {
		ids[s] = randomFilterIDs(n, share, seeds[s])
		in[s] = make([]bool, n)
		for _, id := range ids[s] {
			in[s][id], inAny[id] = true, true
		}
	}
	truth := make([][][]uint64, len(shares))
	for s := range truth {
		truth[s] = make([][]uint64, q)
	}
	parallelFor(q, func(i int) {
		within := make([]topK, len(shares))
		for s := range within {
			within[s].reset(k)
		}
		for id, vec := range data.train {
			if !inAny[id] {
				continue
			}
			dist, err := provider.SingleDist(data.queries[i], vec)
			if err != nil {
				panic(err)
			}
			for s := range within {
				if in[s][id] {
					within[s].push(uint64(id), dist)
				}
			}
		}
		for s := range within {
			truth[s][i] = idsOf(within[s].sorted())
		}
	})
	filters := make([]*benchFilter, len(shares))
	for s, share := range shares {
		filters[s] = &benchFilter{
			Name: filterName("random", share), Kind: "random", Share: share, Allowed: len(ids[s]),
			allow: helpers.NewAllowListFromBitmap(bitmapOf(ids[s])), Truth: truth[s],
		}
	}
	return filters
}

// measureFilterNames are the names of the filters of the measure, those
// buildFilters draws.
func measureFilterNames() []string {
	names := []string{filterName("none", 0)}
	for _, kind := range filterKinds {
		for _, share := range filterShares {
			names = append(names, filterName(kind, share))
		}
	}
	return names
}

// withExtraRandom returns the filters of the measure, as they are, followed
// by those at random at the given shares, in their order: those the filters
// hold already (from the cache) as they are, the others drawn. Filters beyond
// the measure that are not asked for are left out. drawn tells whether any
// was drawn, and so whether the cache has to be written again.
func withExtraRandom(data *benchData, k int, filters []*benchFilter, shares []float64) (
	out []*benchFilter, drawn bool,
) {
	measure := measureFilterNames()
	byName := map[string]*benchFilter{}
	for _, f := range filters {
		byName[f.Name] = f
		if slices.Contains(measure, f.Name) {
			out = append(out, f)
		}
	}
	var missing []float64
	var seeds []uint64
	for _, share := range shares {
		if byName[filterName("random", share)] == nil {
			missing, seeds = append(missing, share), append(seeds, extraRandomSeed(share))
		}
	}
	for _, f := range drawRandomFilters(data, k, missing, seeds) {
		byName[f.Name] = f
	}
	for _, share := range shares {
		out = append(out, byName[filterName("random", share)])
	}
	return out, len(missing) > 0
}

// requireSameFilter fails unless two filters are the same: name, kind,
// share, allowed, the allow list of every query and the ground truth.
func requireSameFilter(t *testing.T, want, got *benchFilter, queries int) {
	t.Helper()
	require.Equal(t, want.Name, got.Name)
	require.Equal(t, want.Kind, got.Kind)
	require.Equal(t, want.Share, got.Share)
	require.Equal(t, want.Allowed, got.Allowed, want.Name)
	require.Equal(t, want.Truth, got.Truth, want.Name)
	for q := range queries {
		if want.of(q) == nil {
			require.Nil(t, got.of(q), "%s, query %d", want.Name, q)
			continue
		}
		require.Equal(t, want.of(q).Slice(), got.of(q).Slice(), "%s, query %d", want.Name, q)
	}
}

// parseTimedSearchers reads -bench.filters.timed: searchers by index and
// path, comma separated, each a searcher of the measure and one that is
// measured, none twice. Empty for every searcher measured (nil).
func parseTimedSearchers(spec string, searchers, measured []string) (map[string]bool, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	timed := map[string]bool{}
	for _, field := range strings.Split(spec, ",") {
		name := strings.TrimSpace(field)
		switch {
		case !slices.Contains(searchers, name):
			return nil, errors.Errorf("%q is no searcher (%s)", name, strings.Join(searchers, ", "))
		case !slices.Contains(measured, name):
			return nil, errors.Errorf("%s is not measured (no HNSW, or left out by another flag)", name)
		case timed[name]:
			return nil, errors.Errorf("%s is named twice", name)
		}
		timed[name] = true
	}
	return timed, nil
}

func TestParseTimedSearchers(t *testing.T) {
	all := []string{"ours graph", "ours flat", "hnsw acorn", "hnsw sweeping"}
	for _, tc := range []struct {
		name     string
		spec     string
		measured []string
		want     map[string]bool
		err      string
	}{
		{name: "none", spec: "", measured: all},
		{
			name: "ours, the reproduction of the night of the 24th", spec: "ours graph, ours flat", measured: all,
			want: map[string]bool{"ours graph": true, "ours flat": true},
		},
		{name: "no searcher", spec: "ours graph,ours", measured: all, err: `"ours" is no searcher`},
		{name: "not measured", spec: "hnsw acorn", measured: all[:2], err: "hnsw acorn is not measured"},
		{name: "twice", spec: "ours flat,ours flat", measured: all, err: "named twice"},
		{name: "a trailing comma", spec: "ours flat,", measured: all, err: `"" is no searcher`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTimedSearchers(tc.spec, filterSearcherNames, tc.measured)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseExtraShares(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec string
		want []float64
		err  string
	}{
		{name: "none", spec: ""},
		{name: "those of the pre-registration", spec: "7,5,3", want: []float64{0.07, 0.05, 0.03}},
		{name: "spaces and decimals", spec: " 2.5 , 0.25", want: []float64{0.025, 0.0025}},
		{name: "a share of the measure", spec: "7,10", err: "a share of the measure"},
		{name: "a small share of the measure", spec: "0.1", err: "a share of the measure"},
		{name: "twice", spec: "7,7", err: "named twice"},
		{name: "zero", spec: "0", err: "above 0"},
		{name: "a hundred", spec: "100", err: "below 100"},
		{name: "not a number", spec: "7,x", err: `"x" is not a share`},
		{name: "a trailing comma", spec: "7,", err: `"" is not a share`},
		{name: "three decimals", spec: "0.125", err: "more than two decimals"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseExtraShares(tc.spec)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tc.want))
			for i := range got {
				require.InDelta(t, tc.want[i], got[i], 1e-12)
			}
		})
	}
}

func TestFilterNames(t *testing.T) {
	require.Equal(t, []string{
		"none", "random 50 %", "random 10 %", "random 1 %", "random 0.1 %",
		"near 50 %", "near 10 %", "near 1 %", "near 0.1 %", "far 50 %", "far 10 %", "far 1 %", "far 0.1 %",
	}, measureFilterNames(), "the names of the measure, those of every result file")
	for _, percent := range []float64{7, 5, 3, 2.5, 0.25} {
		require.Equal(t, "random "+strconv.FormatFloat(percent, 'f', -1, 64)+" %", filterName("random", percent/100))
	}
}

// TestRandomFilterIDs pins the draw of the filters at random: the rounding of
// randomFilterIDs gives the sizes the truncation of the draw that built the
// filters of the measure gave, at the sizes of the measure and of its smoke,
// so the filters in the caches are the ones it draws; and a seed draws the
// same ids every time.
func TestRandomFilterIDs(t *testing.T) {
	for _, n := range []int{990_000, 30_000} {
		for _, share := range filterShares {
			require.Len(t, randomFilterIDs(n, share, 1000), int(share*float64(n)), "n %d, share %g", n, share)
		}
	}
	a, b := randomFilterIDs(10_000, 0.03, extraRandomSeed(0.03)), randomFilterIDs(10_000, 0.03, extraRandomSeed(0.03))
	require.Equal(t, a, b)
	require.Len(t, a, 300)
	require.True(t, slices.IsSorted(a))
	require.NotEqual(t, a, randomFilterIDs(10_000, 0.03, 1000), "another seed")
	seeds := map[uint64]bool{}
	for s := range filterShares {
		seeds[1000+uint64(s)] = true
		for kind := range filterKinds[1:] {
			seeds[2000+uint64(10*kind+s)] = true
		}
	}
	for _, share := range []float64{0.07, 0.05, 0.03, 0.025, 0.0025} {
		require.False(t, seeds[extraRandomSeed(share)], "the seed of %g is taken", share)
		seeds[extraRandomSeed(share)] = true
	}
}

// TestExtraRandomFilters draws the filters of the measure on a small train set
// and checks that drawRandomFilters, with the seeds of the measure, gives its
// four filters at random, sets and ground truth; and that the cache extended
// with filters at random beyond the measure keeps the filters of the measure
// as they were, holds the new ones after them, and is not drawn again.
func TestExtraRandomFilters(t *testing.T) {
	const n, q, dims, k = 3000, 24, 16, 10
	rng := rand.New(rand.NewPCG(5, 5))
	vector := func() []float32 {
		v := make([]float32, dims)
		var norm float64
		for i := range v {
			v[i] = float32(rng.NormFloat64())
			norm += float64(v[i]) * float64(v[i])
		}
		for i := range v {
			v[i] /= float32(math.Sqrt(norm))
		}
		return v
	}
	data := &benchData{name: "synthetic", dims: dims, rows: n}
	for range n {
		data.train = append(data.train, vector())
	}
	for range q {
		data.queries = append(data.queries, vector())
	}
	measure := buildFilters(t, data, k)
	require.Len(t, measure, 13)

	seeds := make([]uint64, len(filterShares))
	for s := range seeds {
		seeds[s] = 1000 + uint64(s)
	}
	same := func(t *testing.T, want, got *benchFilter) {
		t.Helper()
		requireSameFilter(t, want, got, q)
	}
	for s, f := range drawRandomFilters(data, k, filterShares, seeds) {
		same(t, measure[1+s], f)
	}

	path := filepath.Join(t.TempDir(), "filters.gob")
	writeFilterCache(t, path, data, k, measure)
	cached, ok := readFilterCache(t, path, data, k)
	require.True(t, ok)
	extra := []float64{0.07, 0.05, 0.03}
	extended, drawn := withExtraRandom(data, k, cached, extra)
	require.True(t, drawn)
	writeFilterCache(t, path, data, k, extended)
	read, ok := readFilterCache(t, path, data, k)
	require.True(t, ok)
	require.Len(t, read, 16)
	for i := range measure {
		same(t, measure[i], read[i])
	}
	for i, share := range extra {
		f := read[13+i]
		require.Equal(t, filterName("random", share), f.Name)
		same(t, drawRandomFilters(data, k, []float64{share}, []uint64{extraRandomSeed(share)})[0], f)
		require.Equal(t, int(math.Round(share*n)), f.Allowed)
	}

	again, drawn := withExtraRandom(data, k, read, []float64{0.03, 0.07})
	require.False(t, drawn, "both are in the cache")
	require.Len(t, again, 15)
	require.Equal(t, []string{"random 3 %", "random 7 %"}, []string{again[13].Name, again[14].Name})
	without, drawn := withExtraRandom(data, k, read, nil)
	require.False(t, drawn)
	require.Len(t, without, 13, "the filters beyond the measure are left out unless asked for")
	for i := range measure {
		require.Same(t, read[i], without[i])
	}
}
