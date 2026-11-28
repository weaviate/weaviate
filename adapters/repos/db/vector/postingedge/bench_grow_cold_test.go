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
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// growColdDisk is what the system read from disk for the cold queries of one
// point, with the rows of every load read one after another, in one order.
type growColdDisk struct {
	// First is the row of a load that is read first: "members" or "edges".
	First string `json:"first"`
	// By load: the page-ins and the bytes read from disk while its row of
	// members was read, and while its row of edges was.
	MembersPageins float64 `json:"membersPageins"`
	MembersBytes   float64 `json:"membersBytes"`
	EdgesPageins   float64 `json:"edgesPageins"`
	EdgesBytes     float64 `json:"edgesBytes"`
	// By query: what the rest of the search read from disk, which is its
	// rescore.
	RestPageins float64 `json:"restPageins"`
	RestBytes   float64 `json:"restBytes"`
	Batches     int     `json:"batches"`
}

// growColdPoint is what the loads of the cold queries of one operating point
// read.
type growColdPoint struct {
	Target  float64 `json:"target"`
	EF      int     `json:"ef"`
	Queries int     `json:"queries"`
	// Loads is the postings a query loads, on average.
	Loads float64 `json:"loads"`
	// By load, what the store counts in pread mode: the reads and the bytes
	// of its row of members and of its row of edges. It is the same cold as
	// warm: the store asks for the same reads whatever is cached.
	StoreMembersReads float64 `json:"storeMembersReads"`
	StoreMembersBytes float64 `json:"storeMembersBytes"`
	StoreEdgesReads   float64 `json:"storeEdgesReads"`
	StoreEdgesBytes   float64 `json:"storeEdgesBytes"`
	// Disk is what the system read from disk, the store reading through its
	// mapping as the timed curves do, once with the members read first and
	// once with the edges. Empty where the system does not tell.
	Disk []growColdDisk `json:"disk,omitempty"`
}

type growColdResult struct {
	Dataset     string           `json:"dataset"`
	Dir         string           `json:"dir"`
	Queries     int              `json:"queries"`
	K           int              `json:"k"`
	Consolidate int              `json:"consolidate"`
	Ordered     bool             `json:"vectorsInPostingOrder"`
	Batch       int              `json:"batch"`
	Drop        string           `json:"drop"`
	Files       map[string]int64 `json:"files"`
	Segments    map[string]int   `json:"segments"`
	// NoDisk says why the system's count is missing, when it is.
	NoDisk string          `json:"noDisk,omitempty"`
	Points []growColdPoint `json:"points"`
}

// TestGrowColdReads counts what a load reads in the cold queries of an index
// in the format that grows, row by row: the combined control of v5.1
// (bench-report.md, "control combinado"). Untimed, one query at a time, with
// the rows of a load read one after another, which is what tells them apart.
// First what the store counts, in pread mode. Then what the system reads
// from disk for the process, in the cold protocol of the timed curves: the
// first -bench.diskcold queries in batches of ten, each batch after dropping
// the cache with -bench.diskdrop and opening the index again, the store
// reading through its mapping. That count is taken twice, with the members
// read first and with the edges read first: where the two rows of a posting
// lie together on disk, the one read first brings part of the other.
func TestGrowColdReads(t *testing.T) {
	if *benchDatasetDir == "" || *benchGrowDir == "" || *benchGrowManifest == "" {
		t.Skip("no -bench.dataset, -bench.growdir and -bench.growmanifest given")
	}
	require.NotEmpty(t, *benchOut, "TestGrowColdReads writes its result to -bench.out")
	require.NotEmpty(t, *benchDiskDrop, "TestGrowColdReads needs -bench.diskdrop")
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	raw, err := os.ReadFile(*benchGrowManifest)
	require.NoError(t, err)
	var manifest growManifest
	require.NoError(t, json.Unmarshal(raw, &manifest))
	require.NotEmpty(t, manifest.Points, "the manifest has the operating points of the index")
	data := loadBenchData(t, *benchDatasetDir, 0, manifest.Queries, manifest.K, true)
	require.Len(t, data.queries, len(manifest.Truth))
	dir, k := *benchGrowDir, manifest.K
	queries := data.queries[:min(*benchDiskCold, len(data.queries))]
	const batch = 10
	provider := distancer.NewCosineDistanceProvider()
	uc := NewDefaultUserConfig()
	metrics, err := diskStoreMetrics()
	require.NoError(t, err)

	result := &growColdResult{Dataset: manifest.Dataset, Dir: dir, Queries: len(queries), K: k, Batch: batch, Drop: *benchDiskDrop}
	result.Files, result.Segments, err = growFiles(dir)
	require.NoError(t, err)
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}
	// search runs the given queries one at a time and returns the postings
	// they loaded.
	search := func(g *growIndex, which [][]float32) int {
		loads := 0
		for _, query := range which {
			stats := &diskSearchStats{Work: &SearchStats{}}
			_, _, err := g.search(ctx, query, k, stats)
			require.NoError(t, err)
			loads += stats.Postings
		}
		return loads
	}

	// What the store counts.
	g, err := openGrowIndex(ctx, logger, dir, provider, uc, growOpen{metrics: metrics, pread: true})
	require.NoError(t, err)
	result.Consolidate, result.Ordered = g.meta.Consolidate, g.slotOf != nil
	for _, point := range manifest.Points {
		if !point.Reached {
			continue
		}
		pointUC := uc
		pointUC.EF, pointUC.RescoreLimit = point.EF, point.RescoreLimit
		require.NoError(t, g.ix.UpdateUserConfig(pointUC, func() {}))
		count := &growRowCount{read: func() ([growCounters]uint64, error) {
			reads, bytes, err := diskStoreReads()
			return [growCounters]uint64{reads, bytes}, err
		}}
		g.rowCount = count
		loads := search(g, queries)
		g.rowCount = nil
		require.Equal(t, loads, count.loads)
		n := float64(loads)
		members, edges := count.rows[diskBlockMembers], count.rows[diskBlockEdges]
		out := growColdPoint{
			Target: point.Target, EF: point.EF, Queries: len(queries), Loads: n / float64(len(queries)),
			StoreMembersReads: float64(members[0]) / n, StoreMembersBytes: float64(members[1]) / n,
			StoreEdgesReads: float64(edges[0]) / n, StoreEdgesBytes: float64(edges[1]) / n,
		}
		result.Points = append(result.Points, out)
		t.Logf("recall %.2f (ef %d), %d queries, %.2f loads | the store, a load: its members in %.2f reads and %.1f KB, its "+
			"edges in %.2f reads and %.1f KB", point.Target, point.EF, len(queries), out.Loads, out.StoreMembersReads,
			out.StoreMembersBytes/1e3, out.StoreEdgesReads, out.StoreEdgesBytes/1e3)
	}
	require.NoError(t, g.close(ctx))
	write()

	// What the system reads from disk.
	if _, err := processDiskReads(); err != nil {
		result.NoDisk = err.Error()
		write()
		t.Logf("what the system reads from disk is not counted: %v", err)
		return
	}
	storeDir := filepath.Join(dir, diskStoreDir)
	at := 0
	for _, point := range manifest.Points {
		if !point.Reached {
			continue
		}
		pointUC := uc
		pointUC.EF, pointUC.RescoreLimit = point.EF, point.RescoreLimit
		for _, edgesFirst := range []bool{false, true} {
			count := &growRowCount{edgesFirst: edgesFirst, read: processDiskReads}
			var total [growCounters]uint64
			disk := growColdDisk{First: "members"}
			if edgesFirst {
				disk.First = "edges"
			}
			loads := 0
			for from := 0; from < len(queries); from += batch {
				which := queries[from:min(from+batch, len(queries))]
				out, err := exec.CommandContext(ctx, "sh", "-c", *benchDiskDrop).CombinedOutput()
				require.NoErrorf(t, err, "drop the cache: %s", out)
				g, err := openGrowIndex(ctx, logger, dir, provider, uc, growOpen{})
				require.NoError(t, err)
				require.NoError(t, g.ix.UpdateUserConfig(pointUC, func() {}))
				_, err = warmSegmentIndexes(storeDir)
				require.NoError(t, err)
				g.rowCount = count
				before, err := processDiskReads()
				require.NoError(t, err)
				loads += search(g, which)
				after, err := processDiskReads()
				require.NoError(t, err)
				g.rowCount = nil
				for c := range total {
					total[c] += after[c] - before[c]
				}
				require.NoError(t, g.close(ctx))
				disk.Batches++
			}
			require.Equal(t, loads, count.loads)
			n, q := float64(loads), float64(len(queries))
			members, edges := count.rows[diskBlockMembers], count.rows[diskBlockEdges]
			disk.MembersPageins, disk.MembersBytes = float64(members[0])/n, float64(members[1])/n
			disk.EdgesPageins, disk.EdgesBytes = float64(edges[0])/n, float64(edges[1])/n
			disk.RestPageins = float64(total[0]-members[0]-edges[0]) / q
			disk.RestBytes = float64(total[1]-members[1]-edges[1]) / q
			result.Points[at].Disk = append(result.Points[at].Disk, disk)
			write()
			t.Logf("recall %.2f (ef %d), cold, the %s first | from disk, a load: its members in %.2f page-ins and %.1f KB, its "+
				"edges in %.2f and %.1f KB; the two, %.2f and %.1f KB | the rest of a query: %.1f page-ins and %.1f KB | %d "+
				"batches of %d", point.Target, point.EF, disk.First, disk.MembersPageins, disk.MembersBytes/1e3,
				disk.EdgesPageins, disk.EdgesBytes/1e3, disk.MembersPageins+disk.EdgesPageins,
				(disk.MembersBytes+disk.EdgesBytes)/1e3, disk.RestPageins, disk.RestBytes/1e3, disk.Batches, batch)
		}
		at++
	}
}
