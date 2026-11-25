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
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var benchDirectCounts = flag.String("bench.direct.counts", "500,1000,2000,5000,10000", "TestNoneDirectRead: the "+
	"allowed vectors of the filters at random it draws, by exact count")

// noneDirectResult is what TestNoneDirectRead writes to -bench.out.
type noneDirectResult struct {
	Dataset string             `json:"dataset"`
	Index   string             `json:"index"`
	Vectors int                `json:"vectors"`
	Dims    int                `json:"dims"`
	Queries int                `json:"queries"`
	K       int                `json:"k"`
	Filters []noneDirectFilter `json:"filters"`
}

type noneDirectFilter struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Allowed int    `json:"allowed"`
	// Drawn tells a filter at random drawn here, by exact count, from one
	// of the cache of the measure.
	Drawn  bool           `json:"drawn"`
	Seed   uint64         `json:"seed,omitempty"`
	Direct noneDirectWay  `json:"direct"`
	Index  []noneIndexWay `json:"index"`
	Grid   []noneIndexWay `json:"grid"`
	Best   float64        `json:"bestRecall"`
	Missed []float64      `json:"missedTargets,omitempty"`
}

// noneDirectWay is the direct read of one filter, per query: the exact
// vectors read and their bytes, and the recall against brute force.
type noneDirectWay struct {
	Recall   float64 `json:"recall"`
	Reads    float64 `json:"reads"`
	ReadsMin int     `json:"readsMin"`
	ReadsMax int     `json:"readsMax"`
	Bytes    float64 `json:"bytes"`
	Seconds  float64 `json:"seconds"`
}

// noneIndexWay is the search of the index of one filter at one ef, with its
// routing codes in memory, per query. Reads are the blocks the store
// returned and the exact vectors read. NoneReads adds one block of halo per
// posting loaded, which an index with no codes in memory reads with every
// posting and this one does not hold: computed, not measured. Its bytes are
// not computed.
type noneIndexWay struct {
	Target     float64 `json:"target,omitempty"`
	EF         int     `json:"ef"`
	Recall     float64 `json:"recall"`
	Loads      float64 `json:"loads"`
	Blocks     float64 `json:"blocks"`
	BlockBytes float64 `json:"blockBytes"`
	Rescored   float64 `json:"rescored"`
	Reads      float64 `json:"routingReads"`
	Bytes      float64 `json:"routingBytes"`
	NoneReads  float64 `json:"noneReadsComputed"`
	Seconds    float64 `json:"seconds"`
}

// TestNoneDirectRead measures, untimed, what the direct read of the flat
// search with no codes in memory reads against the search of the index, to
// propose the cutoff of an index with no codes in memory (the user's order of
// 2026-11-24; pre-registration of 2026-11-24 at 21:50 in bench-report.md of
// the experiments branch). It opens the index of -bench.filters.index, which
// should be a copy, with nothing of what the index does by itself. Filters:
// at random by the exact counts of -bench.direct.counts, drawn here with the
// seed 4 000 plus the count in hundreds and their ground truth by brute
// force; and the filters near and far at 0.1 % and 1 % and at random at
// 0.1 % and 1 % of the cache of the measure of filters. For each:
//   - the direct read, as an index with no codes in memory does it: the
//     index set to none, every filtered search flat;
//   - the search of the index with its routing codes in memory, never flat,
//     on the grid of -bench.filters.efs, and the smallest ef that reaches
//     0.95 and 0.99, found as TestIntegrationFilters finds them: between two
//     values of the grid, by halves.
//
// The copy was grown with its routing codes in memory, so it has no halo:
// the reads of the index with none are computed from those of the index with
// its codes, one block more per posting loaded. No query is timed: the
// queries run GOMAXPROCS at a time and the volume is not cold.
func TestNoneDirectRead(t *testing.T) {
	if *benchDatasetDir == "" || *benchFiltersCache == "" || *benchFiltersIndex == "" {
		t.Skip("no -bench.dataset, -bench.filters.cache and -bench.filters.index given")
	}
	require.NotEmpty(t, *benchOut, "TestNoneDirectRead writes its results to -bench.out")
	ctx := context.Background()
	k := *benchK
	counts := parseInts(t, *benchDirectCounts)
	efs := parseInts(t, *benchFiltersEFs)

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, k, false)
	queries := data.queries
	n := len(data.train)
	cached, fromCache := readFilterCache(t, *benchFiltersCache, data, k)
	require.True(t, fromCache, "the filters come from the cache of the measure: %s", *benchFiltersCache)

	var filters []*benchFilter
	drawn := map[*benchFilter]uint64{}
	shares, seeds := make([]float64, len(counts)), make([]uint64, len(counts))
	for i, count := range counts {
		require.Zero(t, count%100, "a count in hundreds: %d", count)
		shares[i], seeds[i] = float64(count)/float64(n), 4000+uint64(count/100)
	}
	for i, f := range drawRandomFilters(data, k, shares, seeds) {
		require.Equal(t, counts[i], f.Allowed, "the filter of %d allowed", counts[i])
		f.Name = fmt.Sprintf("random %d", counts[i])
		filters = append(filters, f)
		drawn[f] = seeds[i]
	}
	for _, f := range cached {
		if f.Kind != "none" && (f.Share == 0.01 || f.Share == 0.001) {
			filters = append(filters, f)
		}
	}
	require.Len(t, filters, len(counts)+6, "the six filters of the cache at 1 % and 0.1 %")

	ix := openFilteredIndex(t, ctx, logger, *benchFiltersIndex)
	defer ix.shut()
	g := ix.p.g
	require.False(t, g.noCodes, "the index opens with its routing codes in memory")
	require.Equal(t, n, len(g.live), "the index holds the vectors of the dataset")
	result := &noneDirectResult{
		Dataset: data.name, Index: *benchFiltersIndex, Vectors: n, Dims: data.dims, Queries: len(queries), K: k,
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}
	vectorBytes := 4 * data.dims
	index := ix.searcher("graph", -1, queries, k)

	// recallOf is the share of the ground truth of a filter the answers find.
	recallOf := func(f *benchFilter, answers [][]uint64) float64 {
		hits, want := 0, 0
		for q, ids := range answers {
			truth := f.Truth[q][:min(k, len(f.Truth[q]))]
			for _, id := range truth {
				if slices.Contains(ids, id) {
					hits++
				}
			}
			want += len(truth)
		}
		return float64(hits) / float64(want)
	}
	// run answers every query of the filter, GOMAXPROCS at a time, checks that no
	// answer holds a doc id the filter leaves out, and returns the answers
	// and the stats of each query.
	run := func(f *benchFilter, search func(q int) ([]uint64, diskSearchStats, error)) ([][]uint64, []diskSearchStats) {
		answers := make([][]uint64, len(queries))
		stats := make([]diskSearchStats, len(queries))
		var failed atomic.Value
		parallelFor(len(queries), func(q int) {
			ids, s, err := search(q)
			if err != nil {
				failed.CompareAndSwap(nil, fmt.Sprintf("%s, query %d: %v", f.Name, q, err))
				return
			}
			allow := f.of(q)
			for _, id := range ids {
				if !allow.Contains(id) {
					failed.CompareAndSwap(nil, fmt.Sprintf("%s, query %d: doc %d is left out by the filter", f.Name, q, id))
				}
			}
			answers[q], stats[q] = ids, s
		})
		if msg := failed.Load(); msg != nil {
			require.FailNow(t, msg.(string))
		}
		return answers, stats
	}

	for _, f := range filters {
		measured := noneDirectFilter{Name: f.Name, Kind: f.Kind, Allowed: f.Allowed}
		if seed, ok := drawn[f]; ok {
			measured.Drawn, measured.Seed = true, seed
		}

		// The direct read: the index as one with no codes in memory, which
		// reads only the posting map and the exact vectors, both there
		// whichever way the index keeps its codes.
		ix.p.mu.Lock()
		g.noCodes = true
		ix.p.cfg.noneFlatCutoff = math.MaxInt
		ix.p.mu.Unlock()
		begin := time.Now()
		answers, stats := run(f, func(q int) ([]uint64, diskSearchStats, error) {
			ix.p.mu.RLock()
			defer ix.p.mu.RUnlock()
			var s diskSearchStats
			ids, _, err := ix.p.searchIndex(ctx, queries[q], k, f.of(q), &s)
			return ids, s, err
		})
		direct := noneDirectWay{Recall: recallOf(f, answers), ReadsMin: math.MaxInt, Seconds: time.Since(begin).Seconds()}
		for q, s := range stats {
			require.True(t, s.Flat, "%s, query %d: flat", f.Name, q)
			require.Zero(t, s.Postings, "%s, query %d: no posting loaded", f.Name, q)
			require.Zero(t, s.Scored, "%s, query %d: no code scored", f.Name, q)
			direct.Reads += float64(s.Rescored)
			direct.ReadsMin, direct.ReadsMax = min(direct.ReadsMin, s.Rescored), max(direct.ReadsMax, s.Rescored)
		}
		direct.Reads /= float64(len(queries))
		direct.Bytes = direct.Reads * float64(vectorBytes)
		measured.Direct = direct
		ix.p.mu.Lock()
		g.noCodes = false
		ix.p.mu.Unlock()
		t.Logf("%s (%d allowed), direct read: recall %.4f, %.1f vectors read a query (%d to %d), %.1f MB (untimed, %.0f s)",
			f.Name, f.Allowed, direct.Recall, direct.Reads, direct.ReadsMin, direct.ReadsMax, direct.Bytes/(1<<20),
			direct.Seconds)

		// The search of the index, never flat, with the routing codes in
		// memory.
		index.prepare()
		at := map[int]noneIndexWay{}
		measure := func(ef int) noneIndexWay {
			if p, done := at[ef]; done {
				return p
			}
			index.setEF(ef)
			begin := time.Now()
			answers, stats := run(f, func(q int) ([]uint64, diskSearchStats, error) {
				ids, s, err := index.search(ctx, q, f.of(q))
				return ids, s, err
			})
			p := noneIndexWay{EF: ef, Recall: recallOf(f, answers), Seconds: time.Since(begin).Seconds()}
			for q, s := range stats {
				require.False(t, s.Flat, "%s, ef %d, query %d: the index", f.Name, ef, q)
				p.Loads += float64(s.Postings)
				p.Blocks += float64(s.Blocks)
				p.BlockBytes += float64(s.BlockBytes)
				p.Rescored += float64(s.Rescored)
			}
			queriesN := float64(len(queries))
			p.Loads, p.Blocks, p.BlockBytes, p.Rescored = p.Loads/queriesN, p.Blocks/queriesN, p.BlockBytes/queriesN,
				p.Rescored/queriesN
			p.Reads = p.Blocks + p.Rescored
			p.Bytes = p.BlockBytes + p.Rescored*float64(vectorBytes)
			p.NoneReads = p.Reads + p.Loads
			at[ef] = p
			t.Logf("%s, index, ef %d: recall %.4f | %.1f loads, %.1f blocks, %.1f vectors read: %.1f reads with the "+
				"codes in memory, %.1f with none (computed), %.2f MB | %.0f s", f.Name, ef, p.Recall, p.Loads, p.Blocks,
				p.Rescored, p.Reads, p.NoneReads, p.Bytes/(1<<20), p.Seconds)
			return p
		}
		for _, ef := range efs {
			p := measure(ef)
			measured.Grid = append(measured.Grid, p)
			measured.Best = max(measured.Best, p.Recall)
			if p.Recall >= 0.999 {
				break
			}
		}
		for _, target := range filterTargets {
			low := 0
			found := false
			for _, ef := range efs {
				if _, done := at[ef]; !done {
					break
				}
				if at[ef].Recall >= target {
					high := ef
					for high-low > 1 {
						middle := (low + high) / 2
						if middle < k || measure(middle).Recall < target {
							low = middle
						} else {
							high = middle
						}
					}
					p := measure(high)
					p.Target = target
					measured.Index = append(measured.Index, p)
					found = true
					t.Logf("%s, index at recall %.2f: ef %d, recall %.4f, %.1f reads with the codes in memory, %.1f with "+
						"none (computed), against %.1f of the direct read", f.Name, target, p.EF, p.Recall, p.Reads,
						p.NoneReads, direct.Reads)
					break
				}
				low = ef
			}
			if !found {
				measured.Missed = append(measured.Missed, target)
				t.Logf("%s, index: recall %.2f is not reached, the best is %.4f", f.Name, target, measured.Best)
			}
		}
		result.Filters = append(result.Filters, measured)
		write()
	}
	names := make([]string, len(result.Filters))
	for i, f := range result.Filters {
		names[i] = f.Name
	}
	t.Logf("measured: %s", strings.Join(names, ", "))
}
