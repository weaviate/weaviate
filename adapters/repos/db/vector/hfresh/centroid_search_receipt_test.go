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

package hfresh

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/datasets"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Experiment, not a regression test: measures how many of the exact
// top-probe centroids the centroid HNSW returns, as configured by HFresh,
// against a dump of real centroids and a real query set. Skipped unless
//
//	HFRESH_CENTROIDS_DUMP=centroids.ndjson  (output of /debug/index/hfresh/centroids)
//	HFRESH_TEST_PARQUET=test/test.parquet   (benchmarker dataset, the query vectors)
//
// are set. Run with -v to see the table.
func TestCentroidSearchReceipt(t *testing.T) {
	dump := os.Getenv("HFRESH_CENTROIDS_DUMP")
	testFile := os.Getenv("HFRESH_TEST_PARQUET")
	if dump == "" || testFile == "" {
		t.Skip("set HFRESH_CENTROIDS_DUMP and HFRESH_TEST_PARQUET to run")
	}

	// centroids, as the router sees them
	f, err := os.Open(dump)
	require.NoError(t, err)
	defer f.Close()
	var ids []uint64
	var centroids [][]float32
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var line struct {
			ID     uint64    `json:"id"`
			Vector []float32 `json:"vector"`
		}
		require.NoError(t, json.Unmarshal(sc.Bytes(), &line))
		ids = append(ids, line.ID)
		centroids = append(centroids, line.Vector)
	}
	require.NoError(t, sc.Err())
	require.NotEmpty(t, centroids)
	dims := len(centroids[0])

	// queries
	reader, err := datasets.NewLocalDataReader(testFile, datasets.TestSplit, 0, -1, 1000)
	require.NoError(t, err)
	defer reader.Close()
	ds, err := reader.ReadAllRows()
	require.NoError(t, err)
	queries := ds.Vectors
	require.Len(t, queries[0], dims)

	// the centroid index exactly as HFresh builds it (NewHNSWIndex: ef 64,
	// efConstruction 64, 8-bit RQ, no rescore)
	tf := createHFreshIndex(t)
	initializeDimensions(t, &tf, centroids[0])
	for i, c := range centroids {
		require.NoError(t, tf.Index.Centroids.Insert(ids[i], &Centroid{Uncompressed: c}))
	}
	t.Logf("centroids %d x %d, queries %d", len(centroids), dims, len(queries))

	// exact ranking of the same centroids, by cosine (unit vectors: 1 - dot)
	exact := make([][]uint64, len(queries))
	for qi, q := range queries {
		type scored struct {
			id uint64
			s  float32
		}
		all := make([]scored, len(centroids))
		for i, c := range centroids {
			var dot float32
			for d := range q {
				dot += q[d] * c[d]
			}
			all[i] = scored{ids[i], dot}
		}
		sort.Slice(all, func(a, b int) bool { return all[a].s > all[b].s })
		exact[qi] = make([]uint64, len(all))
		for i := range all {
			exact[qi][i] = all[i].id
		}
	}

	probes := []int{64, 256, 512, 1024}
	measure := func(label string) {
		var sb string
		for _, p := range probes {
			if p > len(centroids) {
				continue
			}
			hits := 0
			for qi, q := range queries {
				want := map[uint64]struct{}{}
				for _, id := range exact[qi][:p] {
					want[id] = struct{}{}
				}
				res, err := tf.Index.Centroids.Search(q, p, nil)
				require.NoError(t, err)
				for id := range res.Iter() {
					if _, ok := want[id]; ok {
						hits++
					}
				}
			}
			sb += fmt.Sprintf("  probe %4d: centroid-search recall %.4f", p, float64(hits)/float64(len(queries)*p))
			sb += "\n"
		}
		t.Logf("%s\n%s", label, sb)
	}

	measure("current HFresh settings (ef = max(64, probe), efConstruction 64, RQ-8, no rescore)")

	// same graph, wider beam: ef fixed at 4096 for every probe
	uc := ent.UserConfig{}
	uc.SetDefaults()
	uc.EF = 4096
	uc.EFConstruction = 64
	uc.RQ.Enabled = true
	uc.RQ.Bits = 8
	uc.RQ.RescoreLimit = 0
	uc.FilterStrategy = ent.FilterStrategyAcorn
	require.NoError(t, tf.Index.Centroids.hnsw.UpdateUserConfig(uc, func() {}))
	measure("same graph, ef = 4096 for every probe")
}
