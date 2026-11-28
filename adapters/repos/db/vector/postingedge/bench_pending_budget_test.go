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
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// TestMeasurePendingBudget measures the writes the budget of what waits of
// the edges adds: an index a shard opens, with the defaults, grows by
// inserts from its base, once without a budget and once with each budget
// given per live posting of the run without one. 64 MB over the ~42 000
// postings of 10M is about 1 600 B a posting. Untimed: it counts rows and
// distances written. PE_N (60 000), PE_DIMS (768), PE_PER_POSTING (comma
// list of bytes, "800,1600,3200") and PE_OUT (a JSON file) tune it.
func TestMeasurePendingBudget(t *testing.T) {
	n := envOr("PE_N", 60000)
	dims := envOr("PE_DIMS", 768)
	perPosting := []int64{800, 1600, 3200}
	if s := os.Getenv("PE_PER_POSTING"); s != "" {
		perPosting = nil
		for _, f := range splitComma(s) {
			v, err := strconv.ParseInt(f, 10, 64)
			require.NoError(t, err)
			perPosting = append(perPosting, v)
		}
	}
	vectors := latentVectors(n, dims, 9101)
	type run struct {
		Budget          int64   `json:"budget"`
		PerPosting      int64   `json:"perPosting"`
		Inserts         int     `json:"inserts"`
		LivePostings    int     `json:"livePostings"`
		Postings        int     `json:"postings"`
		RowWrites       growIO  `json:"rowWrites"`
		RowWritesFull   int     `json:"rowWritesFull"`
		RowWritesSplit  int     `json:"rowWritesSplit"`
		RowWritesBudget int     `json:"rowWritesBudget"`
		RowDists        growIO  `json:"rowDists"`
		PendingLog      growIO  `json:"pendingLog"`
		PendingPeak     int64   `json:"pendingPeakBytes"`
		PendingHeap     int64   `json:"pendingHeapAtEnd"`
		RowBytesInsert  float64 `json:"rowBytesPerInsert"`
		DistBytesInsert float64 `json:"distBytesPerInsert"`
		Seconds         float64 `json:"seconds"`
	}
	grow := func(budget int64) run {
		root := t.TempDir()
		noop := cyclemanager.NewCallbackGroupNoop()
		store, err := lsmkv.New(filepath.Join(root, "lsm"), root, testLogger(), nil, nil, noop, noop, noop)
		require.NoError(t, err)
		defer func() { require.NoError(t, store.Shutdown(context.Background())) }()
		uc := ent.NewDefaultUserConfig()
		cfg := PersistentConfig{
			ID: "vectors_measure", ShardName: "shard", ClassName: "Class", RootPath: root,
			Logger: testLogger(), DistanceProvider: distancer.NewCosineDistanceProvider(),
			noBackground: true, pendingBudget: budget,
		}
		index, err := Open(cfg, uc, store)
		require.NoError(t, err)
		defer func() { require.NoError(t, index.Shutdown(context.Background())) }()
		ctx := context.Background()
		start := time.Now()
		const batch = 100
		for from := 0; from < n; from += batch {
			to := min(from+batch, n)
			require.NoError(t, index.AddBatch(ctx, sequentialIDs(to-from, uint64(from)), vectors[from:to]))
		}
		require.NoError(t, index.Settle(ctx))
		index.mu.RLock()
		defer index.mu.RUnlock()
		g := index.g
		s := g.stats
		inserts := n - int(DefaultBaseSize)
		r := run{
			Budget: budget, Inserts: inserts, LivePostings: livePostings(g), Postings: g.meta.Postings,
			RowWrites: s.RowWrites, RowWritesFull: s.RowWritesFull, RowWritesSplit: s.RowWritesSplit,
			RowWritesBudget: s.RowWritesBudget, RowDists: s.RowDists, PendingLog: s.PendingLog,
			PendingPeak: s.PendingPeakBytes, PendingHeap: g.pendingHeap(),
			Seconds: time.Since(start).Seconds(),
		}
		r.RowBytesInsert = float64(s.RowWrites.Bytes) / float64(inserts)
		r.DistBytesInsert = float64(s.RowDists.Bytes) / float64(inserts)
		return r
	}
	free := grow(-1)
	t.Logf("no budget: %d live postings, rows %d (%d B, %.0f B an insert), dists %.0f B an insert, pending heap at the end %d B",
		free.LivePostings, free.RowWrites.N, free.RowWrites.Bytes, free.RowBytesInsert, free.DistBytesInsert, free.PendingHeap)
	runs := []run{free}
	for _, per := range perPosting {
		r := grow(per * int64(free.LivePostings))
		r.PerPosting = per
		runs = append(runs, r)
		t.Logf("budget %d B (%d B a posting): rows %d (%d by the budget, %.0f B an insert, %.2fx), dists %.0f B an insert (%.2fx), pending heap at the end %d B",
			r.Budget, per, r.RowWrites.N, r.RowWritesBudget, r.RowBytesInsert, r.RowBytesInsert/free.RowBytesInsert,
			r.DistBytesInsert, r.DistBytesInsert/free.DistBytesInsert, r.PendingHeap)
	}
	if out := os.Getenv("PE_OUT"); out != "" {
		raw, err := json.MarshalIndent(map[string]any{"n": n, "dims": dims, "runs": runs}, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(out, raw, 0o644))
	}
}

func envOr(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
