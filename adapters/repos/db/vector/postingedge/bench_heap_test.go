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
	"runtime"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

var (
	benchHeapOut = flag.String("bench.heap.out", "", "TestFilteredIndexHeap: the file the heap profile is "+
		"written to, after opening the index of -bench.filters.index and collecting; run it with "+
		"-test.memprofilerate=1 for every allocation")
	benchHeapCacheMode = flag.String("bench.heap.cachemode", ent.CacheModeRouting, "TestFilteredIndexHeap: the "+
		"rq.cacheMode the index is opened with, routing or none")
)

// TestFilteredIndexHeap opens the index a shard opens, as the filters
// measure does (openFilteredIndex), in the cache mode given, and writes the
// profile of the heap in use with nothing else of a harness loaded: no
// dataset, no queries, no filters. It logs what the index holds in memory
// by count, for the breakdown of the heap by component.
func TestFilteredIndexHeap(t *testing.T) {
	if *benchFiltersIndex == "" || *benchHeapOut == "" {
		t.Skip("no -bench.filters.index and -bench.heap.out given")
	}
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	uc := ent.NewDefaultUserConfig()
	uc.RQ.CacheMode = *benchHeapCacheMode
	ix := &filteredIndex{t: t, ctx: ctx, logger: logger, dir: *benchFiltersIndex, uc: uc, closed: true}
	ix.open()
	defer ix.shut()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	require.NoError(t, writeHeapProfile(*benchHeapOut))

	g := ix.p.g
	meta, err := json.Marshal(g.meta)
	require.NoError(t, err)
	deleted := 0
	for _, d := range g.deleted {
		if d {
			deleted++
		}
	}
	t.Logf("cache mode %s: heap in use %.1f MB, allocated and in use %.1f MB, after opening and collecting",
		*benchHeapCacheMode, float64(mem.HeapInuse)/1e6, float64(mem.HeapAlloc)/1e6)
	t.Logf("no codes in memory: %v; keys %d, live slots %d, deleted %d (of a []bool of %d); meta %s",
		g.noCodes, g.meta.Keys, len(g.live), deleted, len(g.deleted), meta)
	logCentroidGraph(t, g.st.centroids)
}

// logCentroidGraph logs the graph over the centroids as HNSW tells it: its
// nodes by layer, its tombstones, its cache and its compression, and how many
// centroids the index holds in float32 (none with codes).
func logCentroidGraph(t *testing.T, c *centroidIndex) {
	stats, err := c.graph.Stats()
	require.NoError(t, err)
	held := 0
	c.mu.RLock()
	for _, v := range c.vectors {
		if v != nil {
			held++
		}
	}
	c.mu.RUnlock()
	raw, err := json.Marshal(struct {
		Float32Held int          `json:"float32Held"`
		Codes       bool         `json:"codes"`
		Dims        int          `json:"dims"`
		Layers      map[int]uint `json:"nodesByLayer"`
		Tombstones  int          `json:"tombstones"`
		CacheSize   int32        `json:"cacheSize"`
		Compressed  bool         `json:"compressed"`
		Type        string       `json:"compressionType"`
	}{
		held, c.codes, int(stats.Dimensions), stats.DistributionLayers, stats.NumTombstones, stats.CacheSize,
		stats.Compressed, stats.CompressionType,
	})
	require.NoError(t, err)
	t.Logf("centroid graph: %s", raw)
}
