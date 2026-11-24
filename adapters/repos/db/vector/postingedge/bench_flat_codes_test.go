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
	"encoding/json"
	"flag"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

var (
	benchFlatCodesRescore = flag.String("bench.flatcodes.rescore", "64,150,507", "TestFlatRankingCode: the "+
		"numbers of allowed vectors the flat search rescores, R")
	benchFlatCodesBits = flag.Int("bench.flatcodes.bits", DefaultRoutingBits, "TestFlatRankingCode: the width "+
		"in bits of the routing code")
	benchFlatCodesShare = flag.Float64("bench.flatcodes.share", 0.01, "TestFlatRankingCode: the largest share "+
		"of a filter measured")
)

// flatCodesResult is what TestFlatRankingCode writes to -bench.out.
type flatCodesResult struct {
	Dataset string `json:"dataset"`
	Vectors int    `json:"vectors"`
	Dims    int    `json:"dims"`
	Queries int    `json:"queries"`
	K       int    `json:"k"`
	// Training is how many vectors, the first of the train set, the mean of
	// the quantizer is taken over.
	Training      int               `json:"training"`
	FilterSeconds float64           `json:"filterSeconds,omitempty"`
	Codes         []flatCodesCode   `json:"codes"`
	Rescore       []int             `json:"rescore"`
	Filters       []flatCodesFilter `json:"filters"`
}

type flatCodesCode struct {
	Name  string `json:"name"`
	Bits  int    `json:"bits"`
	Words int    `json:"words"`
}

type flatCodesFilter struct {
	Name    string  `json:"name"`
	Kind    string  `json:"kind"`
	Share   float64 `json:"share"`
	Allowed int     `json:"allowed"`
	// Recall[c][r] is the recall at k when code c ranks the allowed vectors
	// and the best Rescore[r] of them are rescored.
	Recall [][]float64 `json:"recall"`
}

// TestFlatRankingCode measures what the code that ranks the allowed vectors
// of a flat search costs in recall, against the R of its rescore. The flat
// search of the index is a function of the vectors, the allowed ids and that
// code: it scores every allowed vector with its code, rescores the best R
// with their exact vectors and answers the best k of those (flatSearch in
// persistent_filter.go). This copies it over the dataset, with no index
// opened: the quantizer as the base of an index trains it, over the first
// vectors, and the routing code as the index caps it, the first sign bits of
// the same rotation. Each filter of the measure of filters up to
// -bench.flatcodes.share is ranked with the routing code and with the full
// code, and rescored at every R of -bench.flatcodes.rescore. The filters come
// from -bench.filters.cache, drawn and written there when it does not exist.
// A diagnostic of the hypothesis that the flat search needs an R of 507 at
// 1 536 dimensions because its code is capped at 512 bits; untimed.
func TestFlatRankingCode(t *testing.T) {
	if *benchDatasetDir == "" || *benchFiltersCache == "" {
		t.Skip("no -bench.dataset and -bench.filters.cache given")
	}
	require.NotEmpty(t, *benchOut, "TestFlatRankingCode writes its results to -bench.out")
	k := *benchK
	var rescore []int
	for _, field := range strings.Split(*benchFlatCodesRescore, ",") {
		r, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoError(t, err, "-bench.flatcodes.rescore")
		require.GreaterOrEqual(t, r, k, "an R under k")
		rescore = append(rescore, r)
	}
	widest := 0
	for _, r := range rescore {
		widest = max(widest, r)
	}

	// The data and the filters as the measure of filters takes them: the
	// train set and the queries normalized by the loader.
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, k, false)
	n, queries := len(data.train), data.queries
	result := &flatCodesResult{
		Dataset: data.name, Vectors: n, Dims: data.dims, Queries: len(queries), K: k, Rescore: rescore,
	}
	begin := time.Now()
	filters, cached := readFilterCache(t, *benchFiltersCache, data, k)
	if !cached {
		filters = buildFilters(t, data, k)
		result.FilterSeconds = time.Since(begin).Seconds()
		writeFilterCache(t, *benchFiltersCache, data, k, filters)
	}
	t.Logf("filters: %d, %s, in %s", len(filters), map[bool]string{true: "from the cache", false: "drawn"}[cached],
		time.Since(begin).Round(time.Second))

	// The vectors as the index stores them, and the two codes of each.
	vectors := asTheIndexStoresThem(data)
	cfg := Config{DistanceProvider: distancer.NewCosineDistanceProvider()}
	result.Training = min(DefaultTrainingLimit, n)
	full, err := trainQuantizer(vectors[:result.Training], data.dims, result.Training, 0, &cfg)
	require.NoError(t, err)
	routing, err := full.withBits(*benchFlatCodesBits)
	require.NoError(t, err)
	codecs := []codec{routing, full}
	codes := make([][]uint64, len(codecs))
	words := make([]int, len(codecs))
	for c, quantizer := range codecs {
		words[c] = len(quantizer.Encode(vectors[0]))
		codes[c] = make([]uint64, n*words[c])
		parallelFor(n, func(i int) {
			code := quantizer.Encode(vectors[i])
			if len(code) != words[c] {
				panic("a code of another width")
			}
			copy(codes[c][i*words[c]:], code)
		})
	}
	result.Codes = []flatCodesCode{
		{Name: "routing", Bits: *benchFlatCodesBits, Words: words[0]},
		{Name: "full", Bits: data.dims, Words: words[1]},
	}
	t.Logf("codes: routing of %d words and full of %d, in %s", words[0], words[1],
		time.Since(begin).Round(time.Second))

	provider := distancer.NewCosineDistanceProvider()
	for _, f := range filters {
		if f.Kind == "none" || f.Share > *benchFlatCodesShare {
			continue
		}
		// hits[c][r][q] are the true neighbors query q finds with code c
		// and the R of index r.
		hits := make([][][]int, len(codecs))
		for c := range hits {
			hits[c] = make([][]int, len(rescore))
			for r := range hits[c] {
				hits[c][r] = make([]int, len(queries))
			}
		}
		parallelFor(len(queries), func(q int) {
			query := distancer.Normalize(queries[q])
			truth := make(map[uint64]bool, len(f.Truth[q]))
			for _, id := range f.Truth[q] {
				truth[id] = true
			}
			it := f.of(q).Iterator()
			var ids []uint64
			for id, ok := it.Next(); ok; id, ok = it.Next() {
				ids = append(ids, id)
			}
			it.Stop()
			var ranked, answer topK
			for c, quantizer := range codecs {
				estimator := quantizer.NewEstimator(query)
				ranked.reset(widest)
				for _, id := range ids {
					dist, err := estimator.Distance(codes[c][int(id)*words[c] : (int(id)+1)*words[c]])
					if err != nil {
						panic(err)
					}
					ranked.push(id, dist)
				}
				best := ranked.sorted()
				for r, limit := range rescore {
					answer.reset(k)
					for _, cand := range best[:min(limit, len(best))] {
						dist, err := provider.SingleDist(query, vectors[cand.id])
						if err != nil {
							panic(err)
						}
						answer.push(cand.id, dist)
					}
					for _, e := range answer.sorted() {
						if truth[e.id] {
							hits[c][r][q]++
						}
					}
				}
			}
		})
		measured := flatCodesFilter{
			Name: f.Name, Kind: f.Kind, Share: f.Share, Allowed: f.Allowed,
			Recall: make([][]float64, len(codecs)),
		}
		for c := range codecs {
			measured.Recall[c] = make([]float64, len(rescore))
			for r := range rescore {
				sum := 0
				for _, h := range hits[c][r] {
					sum += h
				}
				measured.Recall[c][r] = float64(sum) / float64(k*len(queries))
			}
		}
		result.Filters = append(result.Filters, measured)
		t.Logf("%s (%d allowed): routing %v, full %v at R %v", f.Name, f.Allowed, measured.Recall[0],
			measured.Recall[1], rescore)
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}
	require.NotEmpty(t, result.Filters, "no filter up to -bench.flatcodes.share")
}
