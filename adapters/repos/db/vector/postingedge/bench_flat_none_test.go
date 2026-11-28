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
	"context"
	"encoding/json"
	"flag"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

var benchFlatNoneShares = flag.String("bench.flatnone.shares", "0.01,0.001", "TestFlatNoneCost: the shares of "+
	"the filters of the cache measured")

// flatNoneResult is what TestFlatNoneCost writes to -bench.out.
type flatNoneResult struct {
	Dataset  string           `json:"dataset"`
	Index    string           `json:"index"`
	Vectors  int              `json:"vectors"`
	Dims     int              `json:"dims"`
	Queries  int              `json:"queries"`
	K        int              `json:"k"`
	EF       int              `json:"ef"`
	Postings int              `json:"postings"`
	Filters  []flatNoneFilter `json:"filters"`
}

type flatNoneFilter struct {
	Name    string        `json:"name"`
	Kind    string        `json:"kind"`
	Share   float64       `json:"share"`
	Allowed int           `json:"allowed"`
	Ways    []flatNoneWay `json:"ways"`
}

// flatNoneWay is the flat search of one filter one way, per query: the
// routing codes in memory, or no codes in memory (rq.cacheMode none).
type flatNoneWay struct {
	CacheMode string  `json:"cacheMode"`
	Bits      int     `json:"bits"`
	Rescore   int     `json:"rescore"`
	Recall    float64 `json:"recall"`
	// Loads are the postings read, each its row of members alone, and
	// Members and MemberBytes what those rows hold.
	Loads       float64 `json:"loads"`
	LoadsMin    int     `json:"loadsMin"`
	LoadsMax    int     `json:"loadsMax"`
	Members     float64 `json:"members"`
	MemberBytes float64 `json:"memberBytes"`
	Scored      float64 `json:"scored"`
	Rescored    float64 `json:"rescored"`
}

// TestFlatNoneCost measures what the flat search of a filter costs with no
// codes in memory, in recall and in postings read per query, untimed: the
// cost of a first version that nobody has made quick yet (integration-plan.md,
// section 8, amendment 3). It opens the index of -bench.filters.index, which
// should be a copy, with nothing of what the index does by itself, and forces
// every filtered search flat. Each filter of the cache of the measure of
// filters (-bench.filters.cache, never drawn here) at the shares of
// -bench.flatnone.shares is searched both ways over the same index: with the
// routing codes in memory, and as an index with no codes in memory searches
// flat, which reads only the posting map, which is in memory either way, the
// rows of members of the postings and the exact vectors. The rows of members
// are the same whichever way an index keeps its codes, so the second way over
// an index grown with its codes in memory reads what the same index with none
// would. No query is timed: the volume is not cold, and nothing else is
// measured with it.
func TestFlatNoneCost(t *testing.T) {
	if *benchDatasetDir == "" || *benchFiltersCache == "" || *benchFiltersIndex == "" {
		t.Skip("no -bench.dataset, -bench.filters.cache and -bench.filters.index given")
	}
	require.NotEmpty(t, *benchOut, "TestFlatNoneCost writes its results to -bench.out")
	ctx := context.Background()
	k := *benchK
	var shares []float64
	for _, field := range strings.Split(*benchFlatNoneShares, ",") {
		share, err := strconv.ParseFloat(strings.TrimSpace(field), 64)
		require.NoError(t, err, "-bench.flatnone.shares")
		shares = append(shares, share)
	}

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, k, false)
	queries := data.queries
	filters, cached := readFilterCache(t, *benchFiltersCache, data, k)
	require.True(t, cached, "the filters come from the cache of the measure: %s", *benchFiltersCache)

	ix := openFilteredIndex(t, ctx, logger, *benchFiltersIndex)
	defer ix.shut()
	ix.cutoff = math.MaxInt
	ix.p.cfg.flatCutoff = math.MaxInt
	g := ix.p.g
	require.False(t, g.noCodes, "the index opens with its routing codes in memory")
	result := &flatNoneResult{
		Dataset: data.name, Index: *benchFiltersIndex, Vectors: len(data.train), Dims: data.dims,
		Queries: len(queries), K: k, EF: int(g.ix.ef.Load()), Postings: g.meta.Postings,
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}

	for _, f := range filters {
		if f.Kind == "none" || !slices.Contains(shares, f.Share) {
			continue
		}
		measured := flatNoneFilter{Name: f.Name, Kind: f.Kind, Share: f.Share, Allowed: f.Allowed}
		for _, cacheMode := range []string{ent.CacheModeRouting, ent.CacheModeNone} {
			// Only the flat search runs, and nothing in the background: what
			// it reads with no codes in memory is all in the index either way.
			ix.p.mu.Lock()
			g.noCodes = cacheMode == ent.CacheModeNone
			ix.p.mu.Unlock()
			way := flatNoneWay{CacheMode: cacheMode, Bits: g.flatBits(), LoadsMin: math.MaxInt}
			way.Rescore = ix.p.flatRescore(k, way.Bits)
			begin := time.Now()
			hits, want := 0, 0
			for q := range queries {
				allow := f.of(q)
				var stats diskSearchStats
				ix.p.mu.RLock()
				ids, _, err := ix.p.searchIndex(ctx, queries[q], k, allow, &stats)
				ix.p.mu.RUnlock()
				require.NoError(t, err, "%s, %s, query %d", f.Name, cacheMode, q)
				require.True(t, stats.Flat, "%s, %s, query %d", f.Name, cacheMode, q)
				for _, id := range ids {
					require.True(t, allow.Contains(id), "%s, %s, query %d: doc %d is left out by the filter",
						f.Name, cacheMode, q, id)
				}
				truth := f.Truth[q][:min(k, len(f.Truth[q]))]
				for _, id := range truth {
					if slices.Contains(ids, id) {
						hits++
					}
				}
				want += len(truth)
				way.Loads += float64(stats.Postings)
				way.LoadsMin, way.LoadsMax = min(way.LoadsMin, stats.Postings), max(way.LoadsMax, stats.Postings)
				way.Members += float64(stats.Members)
				way.MemberBytes += float64(stats.MemberBytes)
				way.Scored += float64(stats.Scored)
				way.Rescored += float64(stats.Rescored)
			}
			n := float64(len(queries))
			way.Recall = float64(hits) / float64(want)
			way.Loads, way.Members, way.MemberBytes = way.Loads/n, way.Members/n, way.MemberBytes/n
			way.Scored, way.Rescored = way.Scored/n, way.Rescored/n
			t.Logf("%s, %s: R %d, recall %.4f, %.1f loads a query (%d to %d), %.0f members, %.0f KB, %.0f scored, "+
				"%.0f rescored (untimed, %s)", f.Name, cacheMode, way.Rescore, way.Recall, way.Loads, way.LoadsMin,
				way.LoadsMax, way.Members, way.MemberBytes/1024, way.Scored, way.Rescored,
				time.Since(begin).Round(time.Second))
			measured.Ways = append(measured.Ways, way)
		}
		ix.p.mu.Lock()
		g.noCodes = false
		ix.p.mu.Unlock()
		result.Filters = append(result.Filters, measured)
		write()
	}
	require.NotEmpty(t, result.Filters, "no filter of the cache at the shares %v", shares)
}
