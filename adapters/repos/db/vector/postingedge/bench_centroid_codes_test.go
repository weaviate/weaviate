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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// codesPoint is one point of a cell, measured with the centroids one way.
type codesPoint struct {
	Target       float64 `json:"target"`
	EF           int     `json:"ef"`
	RescoreLimit int     `json:"rescoreLimit"`
	Recall       float64 `json:"recall"`
	Loads        float64 `json:"loads"`
}

// codesMode is a cell opened with its centroids one way.
type codesMode struct {
	Codes      bool `json:"codes"`
	Compressed bool `json:"compressed"`
	// HeapInuse is the heap in use after opening and collecting.
	HeapInuse uint64 `json:"heapInuse"`
	// Centroids is how many the graph holds, and CentroidBytes what they
	// take: in float32 the vectors held, with codes the bytes of a code by
	// the compression ratio of the graph, times the nodes.
	Centroids     int          `json:"centroids"`
	CentroidBytes int64        `json:"centroidBytes"`
	Points        []codesPoint `json:"points"`
}

type codesResult struct {
	Dataset string      `json:"dataset"`
	Cell    string      `json:"cell"`
	Queries int         `json:"queries"`
	K       int         `json:"k"`
	Modes   []codesMode `json:"modes"`
}

// TestDiskCentroidCodes is the pre-registered measure of the centroids as
// 8-bit RQ codes with no rescore (bench-report.md, "Centroides en RQ8"),
// untimed. A cell of the manifest, from a copy of its directory, opens with
// its centroids in float32 and then with codes, which converts it. At the ef
// of each of its points it gives the recall and the loads over the queries of
// the manifest, and after opening the heap in use and what the centroids take.
func TestDiskCentroidCodes(t *testing.T) {
	if *benchDiskManifest == "" || *benchDiskCell == "" || *benchDiskDir == "" || *benchOut == "" {
		t.Skip("no -bench.diskmanifest, -bench.diskcell, -bench.diskdir and -bench.out given")
	}
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	raw, err := os.ReadFile(*benchDiskManifest)
	require.NoError(t, err)
	var manifest diskManifest
	require.NoError(t, json.Unmarshal(raw, &manifest))
	at := slices.IndexFunc(manifest.Cells, func(c diskCellManifest) bool { return c.Spec.Name == *benchDiskCell })
	require.GreaterOrEqualf(t, at, 0, "the manifest has no cell %q", *benchDiskCell)
	cell := manifest.Cells[at]
	data := loadBenchData(t, *benchDatasetDir, 0, manifest.Queries, manifest.K, true)
	require.Len(t, data.queries, len(manifest.Truth))
	queries, truth, k := data.queries, manifest.Truth, manifest.K
	dir := filepath.Join(*benchDiskDir, cell.Dir)
	provider := distancer.NewCosineDistanceProvider()
	uc := NewDefaultUserConfig()
	uc.LoadBatch = DefaultLoadBatch
	uc.SourceMargin = float32(cell.Spec.Margin)

	result := codesResult{Dataset: manifest.Dataset, Cell: cell.Spec.Name, Queries: len(queries), K: k}
	defer func() { diskCentroidCodes = false }()
	for _, codes := range []bool{false, true} {
		diskCentroidCodes = codes
		disk, err := openDiskIndex(ctx, logger, dir, provider, uc, nil, false)
		require.NoError(t, err)
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		centroids := disk.st.centroids
		mode := codesMode{Codes: codes, Compressed: centroids.graph.Compressed(), HeapInuse: mem.HeapInuse}
		if codes {
			stats, err := centroids.graph.Stats()
			require.NoError(t, err)
			for _, n := range stats.DistributionLayers {
				mode.Centroids += int(n)
			}
			ratio := centroids.graph.CompressionStats().CompressionRatio(disk.meta.Dims)
			mode.CentroidBytes = int64(math.Round(float64(4*disk.meta.Dims) / ratio * float64(mode.Centroids)))
		} else {
			for _, v := range centroids.vectors {
				if v != nil {
					mode.Centroids++
					mode.CentroidBytes += int64(4 * len(v))
				}
			}
		}
		for _, point := range cell.Points {
			if !point.Reached {
				continue
			}
			pointUC := uc
			pointUC.EF, pointUC.RescoreLimit = point.EF, point.RescoreLimit
			require.NoError(t, disk.tunables().UpdateUserConfig(pointUC, func() {}))
			hits, loads := 0, 0
			for q, query := range queries {
				var stats diskSearchStats
				ids, _, err := disk.search(ctx, query, k, &stats)
				require.NoError(t, err)
				for _, id := range truth[q][:k] {
					if slices.Contains(ids, id) {
						hits++
					}
				}
				loads += stats.Postings
			}
			measured := codesPoint{
				Target: point.Target, EF: point.EF, RescoreLimit: point.RescoreLimit,
				Recall: float64(hits) / float64(len(queries)*k), Loads: float64(loads) / float64(len(queries)),
			}
			mode.Points = append(mode.Points, measured)
			t.Logf("cell %s, codes %v, at %.2f (ef %d): recall %.4f, %.2f loads | the manifest: %.4f, %.2f",
				cell.Spec.Name, codes, point.Target, point.EF, measured.Recall, measured.Loads, point.Recall, point.Loads)
		}
		t.Logf("cell %s, codes %v (graph compressed %v): heap in use %.1f MB after opening; %d centroids, %.1f MB",
			cell.Spec.Name, codes, mode.Compressed, float64(mode.HeapInuse)/1e6, mode.Centroids, float64(mode.CentroidBytes)/1e6)
		require.NoError(t, disk.close(ctx))
		result.Modes = append(result.Modes, mode)
	}
	out, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(*benchOut, out, 0o644))
}
