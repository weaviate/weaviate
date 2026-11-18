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
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// The inserts on disk of v5.0 (bench-report.md, "v5.0: inserts en disco"):
// an index built in batch over the first tenth of dbpedia 1M, written in the
// format that grows, and grown to the whole of it by inserts that read and
// write the store.
//
// TestGrowValidate is the validation, untimed. The index grows next to its
// twin in memory, insert by insert, with the store counting what it reads.
// At every checkpoint the coverage is checked from the store and the index
// is compared with the twin: either failing stops the run. It leaves what an
// insert reads and writes, the size of the store, and what a search reads
// from it before and after it is compacted.

var (
	benchGrowDir = flag.String("bench.growdir", "", "the inserts on disk of v5.0 (TestGrowValidate): the directory the "+
		"index grows in. It must not exist, or be empty")
	benchGrowStart = flag.Int("bench.growstart", 10, "with TestGrowValidate: the share of the train set, in percent, "+
		"built in batch as the base")
	benchGrowCheckpoints = flag.String("bench.growcheckpoints", "20,40,60,80,100", "with TestGrowValidate: the shares of "+
		"the train set the index is checked at")
	benchGrowThreshold = flag.Int("bench.growthreshold", 360, "with TestGrowValidate: the size past which a posting splits")
	benchGrowInsertEF  = flag.Int("bench.growinsertef", 160, "with TestGrowValidate: the ef of the search for a new vector's edges")
	benchGrowBits      = flag.Int("bench.growbits", 512, "with TestGrowValidate: the bits of the code the endpoints are "+
		"ranked by, which stays in memory")
)

// growStoreWrites returns what the store has written to its files so far,
// by strategy and operation: the log, the flushes and the compactions.
func growStoreWrites() (map[string]growIO, error) {
	ch := make(chan prometheus.Metric, 4096)
	monitoring.GetMetrics().FileIOWrites.Collect(ch)
	close(ch)
	out := make(map[string]growIO)
	for m := range ch {
		var metric dto.Metric
		if err := m.Write(&metric); err != nil {
			return nil, errors.Wrap(err, "read the store's write summary")
		}
		var strategy, operation string
		for _, label := range metric.GetLabel() {
			switch label.GetName() {
			case "strategy":
				strategy = label.GetValue()
			case "operation":
				operation = label.GetValue()
			}
		}
		key := strategy + "/" + operation
		io := out[key]
		io.N += int64(metric.GetSummary().GetSampleCount())
		io.Bytes += int64(metric.GetSummary().GetSampleSum())
		out[key] = io
	}
	return out, nil
}

// growFiles returns the bytes of every bucket of the store of an index, of
// its vectors and of its centroid graph, and the segments of every bucket.
func growFiles(dir string) (map[string]int64, map[string]int, error) {
	sizes, segments := make(map[string]int64), make(map[string]int)
	for _, bucket := range []string{growBucketPostings, growBucketMap, growBucketRouting, growBucketDists} {
		root := filepath.Join(dir, diskStoreDir, bucket)
		size, err := sizeOnDisk(root)
		if err != nil {
			return nil, nil, err
		}
		sizes[bucket] = size
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, nil, err
		}
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".db") {
				segments[bucket]++
			}
		}
	}
	for name, path := range map[string]string{"centroids": diskGraphDir, "centroidVectors": diskCentroidFile} {
		size, err := sizeOnDisk(filepath.Join(dir, path))
		if err != nil {
			return nil, nil, err
		}
		sizes[name] = size
	}
	return sizes, segments, nil
}

// growCheckpoint is the index at one share of the train set. Its counts are
// those of the whole run so far.
type growCheckpoint struct {
	Percent  int     `json:"percent"`
	Live     int     `json:"live"`
	Postings int     `json:"postings"`
	Splits   int     `json:"splits"`
	Seconds  float64 `json:"seconds"`
	// Coverage and Equal say what the two checks of the checkpoint found.
	Coverage string           `json:"coverage"`
	Equal    string           `json:"equal"`
	Stats    growStats        `json:"stats"`
	Files    map[string]int64 `json:"files"`
	Segments map[string]int   `json:"segments"`
	// Written is what the store wrote to its files, by strategy and
	// operation.
	Written map[string]growIO `json:"written"`
}

// growSearch is what the searches of one operating point read from the
// store, per query, and whether they returned what the twin returns.
type growSearch struct {
	Target   float64 `json:"target"`
	Reached  bool    `json:"reached"`
	EF       int     `json:"ef"`
	Recall   float64 `json:"recall"`
	Compared int     `json:"compared"`
	Loads    float64 `json:"loads"`
	Rounds   float64 `json:"rounds"`
	// StoreReads and StoreBytes are what the store counted, and MemberBytes
	// and EdgeBytes what it handed back, by row.
	StoreReads  float64 `json:"storeReads"`
	StoreBytes  float64 `json:"storeBytes"`
	MemberBytes float64 `json:"memberBytes"`
	EdgeBytes   float64 `json:"edgeBytes"`
	// ModelBytes is what the postings loaded take by the byte model of v3.0,
	// 208 B a member and 4 B an edge: what the twin in memory reads.
	ModelBytes float64 `json:"modelBytes"`
	Rescored   float64 `json:"rescored"`
}

type growResult struct {
	Dataset     string           `json:"dataset"`
	Vectors     int              `json:"vectors"`
	Queries     int              `json:"queries"`
	K           int              `json:"k"`
	Start       int              `json:"start"`
	Threshold   int              `json:"threshold"`
	InsertEF    int              `json:"insertEF"`
	RoutingBits int              `json:"routingBits"`
	Config      map[string]any   `json:"config"`
	Base        growCheckpoint   `json:"base"`
	Checkpoints []growCheckpoint `json:"checkpoints"`
	// Uncompacted and Compacted are the searches over the whole index,
	// before and after the store is compacted for good.
	Uncompacted      []growSearch   `json:"uncompacted,omitempty"`
	Compacted        []growSearch   `json:"compacted,omitempty"`
	CompactionRounds int            `json:"compactionRounds"`
	Final            growCheckpoint `json:"final"`
	// Reopened says how the index, closed and opened again from its files,
	// compares with the twin over every query.
	Reopened string `json:"reopened,omitempty"`
}

// growRecall returns the recall of an index in memory over the queries, with
// the tunables it has.
func growRecall(ctx context.Context, ix *Index, queries [][]float32, truth [][]uint64, k int) (float64, error) {
	var hits atomic.Int64
	var next atomic.Int64
	eg := enterrors.NewErrorGroupWrapper(logrus.New())
	for range 8 {
		eg.Go(func() error {
			for {
				i := int(next.Add(1)) - 1
				if i >= len(queries) {
					return nil
				}
				ids, _, err := ix.search(ctx, queries[i], k, nil, nil)
				if err != nil {
					return err
				}
				found := 0
				for _, want := range truth[i] {
					if slices.Contains(ids, want) {
						found++
					}
				}
				hits.Add(int64(found))
			}
		})
	}
	if err := eg.Wait(); err != nil {
		return 0, err
	}
	return float64(hits.Load()) / float64(len(queries)*k), nil
}

func TestGrowValidate(t *testing.T) {
	if *benchDatasetDir == "" || *benchGrowDir == "" {
		t.Skip("no -bench.dataset and -bench.growdir given")
	}
	require.NotEmpty(t, *benchOut, "TestGrowValidate writes its result to -bench.out")
	ctx := context.Background()
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, *benchK, false)
	if *benchLimit > 0 {
		data.neighbors = bruteNeighbors(data, streamTruthDepth)
	}
	require.NotNil(t, data.neighbors, "the ground truth is drawn from the shipped one")
	n, k := len(data.train), *benchK
	// The order of v2.x: the train set shuffled with seed 1.
	order := make([]uint64, n)
	for i := range order {
		order[i] = uint64(i)
	}
	rand.New(rand.NewPCG(1, 1)).Shuffle(n, func(i, j int) { order[i], order[j] = order[j], order[i] })
	require.True(t, *benchGrowStart > 0 && *benchGrowStart < 100, "-bench.growstart is a share of the train set")
	built := n * *benchGrowStart / 100
	var stops []int
	for _, field := range strings.Split(*benchGrowCheckpoints, ",") {
		percent, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoError(t, err)
		require.True(t, percent > *benchGrowStart && percent <= 100 && (len(stops) == 0 || percent > stops[len(stops)-1]),
			"the checkpoints go up from the start to 100")
		stops = append(stops, percent)
	}
	require.Equal(t, 100, stops[len(stops)-1], "the last checkpoint is the whole train set")
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	provider := distancer.NewCosineDistanceProvider()
	vectorOf := func(id uint64) []float32 { return data.train[id] }

	uc := NewDefaultUserConfig()
	uc.TargetPostingSize = *benchPosting
	uc.BuildProbe = *benchProbe
	uc.RescueLimit = *benchRescue
	uc.TrainingLimit = *benchTraining
	result := &growResult{
		Dataset: data.name, Vectors: n, Queries: len(data.queries), K: k, Start: *benchGrowStart,
		Threshold: *benchGrowThreshold, InsertEF: *benchGrowInsertEF, RoutingBits: *benchGrowBits,
		Config: map[string]any{
			"targetPostingSize": uc.TargetPostingSize, "buildProbe": uc.BuildProbe, "rescueLimit": uc.RescueLimit,
			"edges": uc.Edges, "centroidEF": centroidEF, "rescoreLimit": uc.RescoreLimit, "loadBatch": uc.LoadBatch,
			"sourceMargin": uc.SourceMargin, "cycleEvery": growCycleEvery,
		},
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}

	// The base, in batch, with the endpoints ranked by the routing code.
	begin := time.Now()
	_, tempVectorForID := benchThunks(data)
	index, err := New(Config{
		ID:                           "grow-twin",
		RootPath:                     t.TempDir(),
		Logger:                       logger,
		DistanceProvider:             provider,
		TempVectorForIDWithViewThunk: tempVectorForID,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
		Seed:                         1,
	}, uc)
	require.NoError(t, err)
	const chunk = 10000
	for from := 0; from < built; from += chunk {
		to := min(from+chunk, built)
		vectors := make([][]float32, to-from)
		for i, id := range order[from:to] {
			vectors[i] = data.train[id]
		}
		require.NoError(t, index.AddBatch(ctx, order[from:to], vectors))
	}
	require.NoError(t, index.Build(ctx))
	st := index.state.Load()
	if *benchGrowBits > 0 {
		routing, err := st.quantizer.withBits(*benchGrowBits)
		require.NoError(t, err)
		st, err = haloState(ctx, logger, st, routing, vectorOf)
		require.NoError(t, err)
		index.state.Store(st)
	}
	t.Logf("built the base of %d vectors in %s", built, time.Since(begin).Round(time.Second))

	dir := *benchGrowDir
	require.NoError(t, os.MkdirAll(dir, 0o755))
	begin = time.Now()
	cfg := growConfig{keys: n, neighbors: uc.Edges, threshold: *benchGrowThreshold, seed: 1}
	require.NoError(t, writeGrowIndex(ctx, logger, dir, st, cfg, provider, vectorOf))
	t.Logf("wrote the base in %s", time.Since(begin).Round(time.Second))

	twin, err := newStreamIndex(index, n, streamConfig{
		threshold: *benchGrowThreshold, inverse: true, seed: 1, followGraph: true,
	}, vectorOf)
	require.NoError(t, err)
	own := twin.st.centroids
	var written struct {
		docID   uint64
		posting uint32
		edges   []uint32
	}
	twin.onWrite = func(docID uint64, p uint32, edges []uint32) error {
		written.docID, written.posting, written.edges = docID, p, append(written.edges[:0], edges...)
		return nil
	}

	insertUC := uc
	insertUC.EF = *benchGrowInsertEF
	metrics, err := diskStoreMetrics()
	require.NoError(t, err)
	var g *growIndex
	open := func() {
		g, err = openGrowIndex(ctx, logger, dir, provider, insertUC, metrics, true)
		require.NoError(t, err)
		g.storeReads = diskStoreReads
		twin.st.centroids = g.st.centroids
	}
	open()
	defer func() {
		if g != nil {
			require.NoError(t, g.close(ctx))
		}
		twin.st.centroids = own
		require.NoError(t, index.Shutdown(ctx))
	}()
	require.NoError(t, twin.ix.UpdateUserConfig(insertUC, func() {}))

	checkpoint := func(percent int, seconds float64) growCheckpoint {
		cp := growCheckpoint{
			Percent: percent, Live: g.meta.Live, Postings: g.meta.Postings, Splits: g.meta.Splits, Seconds: seconds,
			Coverage: "complete", Equal: "the same as the twin", Stats: g.stats,
		}
		require.NoError(t, g.cleanCentroidGraph())
		// The checks read what is on disk and in the memtables.
		if err := g.checkCoverage(); err != nil {
			cp.Coverage = err.Error()
		}
		if err := twin.checkCoverage(); err != nil {
			cp.Equal = "the twin lost its coverage: " + err.Error()
		} else if err := g.sameAs(twin); err != nil {
			cp.Equal = err.Error()
		}
		require.NoError(t, g.sync())
		cp.Files, cp.Segments, err = growFiles(dir)
		require.NoError(t, err)
		cp.Written, err = growStoreWrites()
		require.NoError(t, err)
		return cp
	}
	stop := func(cp growCheckpoint) {
		write()
		require.Equal(t, "complete", cp.Coverage, "STOP: the coverage at %d %%", cp.Percent)
		require.Equal(t, "the same as the twin", cp.Equal, "STOP: the index on disk against the twin at %d %%", cp.Percent)
	}
	result.Base = checkpoint(*benchGrowStart, 0)
	stop(result.Base)
	t.Logf("base: %d vectors in %d postings, coverage %s, %s", g.meta.Live, g.meta.Postings, result.Base.Coverage, result.Base.Equal)

	// The inserts, the twin first: it leaves the graph as it found it.
	inserted := built
	run := time.Now()
	for _, percent := range stops {
		upto := n * percent / 100
		for ; inserted < upto; inserted++ {
			id := order[inserted]
			require.NoError(t, twin.insert(ctx, id, data.train[id]))
			require.NoError(t, g.insert(ctx, id, data.train[id]))
			if written.docID != id || written.posting != g.lastPosting || !slices.Equal(written.edges, g.lastEdges) {
				write()
				t.Fatalf("STOP: doc %d, insert %d, went to posting %d with edges %v on disk, and to %d with %v in memory",
					id, inserted, g.lastPosting, g.lastEdges, written.posting, written.edges)
			}
			if twin.stats.Splits != g.stats.Splits || twin.stats.InverseEdges != g.stats.InverseEdges ||
				twin.stats.SearchLoads != g.stats.SearchLoads || twin.stats.SearchRounds != g.stats.SearchRounds {
				write()
				t.Fatalf("STOP: after insert %d, %d splits, %d inverse edges and %d loads in %d rounds on disk; %d, %d and %d in %d in memory",
					inserted, g.stats.Splits, g.stats.InverseEdges, g.stats.SearchLoads, g.stats.SearchRounds,
					twin.stats.Splits, twin.stats.InverseEdges, twin.stats.SearchLoads, twin.stats.SearchRounds)
			}
			if (inserted-built+1)%50000 == 0 {
				t.Logf("%d inserts in %s: %d postings, %d splits, %.2f inverse edges an insert",
					inserted-built+1, time.Since(run).Round(time.Second), g.meta.Postings, g.stats.Splits,
					float64(g.stats.InverseEdges)/float64(g.stats.Inserted))
			}
		}
		cp := checkpoint(percent, time.Since(run).Seconds())
		result.Checkpoints = append(result.Checkpoints, cp)
		stop(cp)
		s := &cp.Stats
		per := func(x int64) float64 { return float64(x) / float64(s.Inserted) }
		t.Logf("%d %%: %d live in %d postings, %d splits | coverage %s, %s | an insert so far: %.1f loads, "+
			"store reads %.1f (%.2f MB) for its search, %.1f for its inverse edges, %.2f for its splits | "+
			"%.2f inverse edges in %.2f postings | postings bucket %.1f MB in %d segments",
			percent, cp.Live, cp.Postings, cp.Splits, cp.Coverage, cp.Equal, per(int64(s.SearchLoads)),
			per(s.StoreSearch.N), per(s.StoreSearch.Bytes)/1e6, per(s.StoreInverse.N), per(s.StoreSplit.N),
			per(int64(s.InverseEdges)), per(int64(s.InversePostings)),
			float64(cp.Files[growBucketPostings])/1e6, cp.Segments[growBucketPostings])
	}

	// The searches over the whole index: the operating points from the twin,
	// which is the same index, and what the store reads for them.
	truth := liveTruth(t, data, nil, k)
	searchUC := uc
	setEF := func(ef int) {
		searchUC.EF = ef
		require.NoError(t, twin.ix.UpdateUserConfig(searchUC, func() {}))
		require.NoError(t, g.ix.UpdateUserConfig(searchUC, func() {}))
	}
	recalls := make(map[int]float64)
	recall := func(ef int) float64 {
		if _, done := recalls[ef]; !done {
			setEF(ef)
			r, err := growRecall(ctx, twin.ix, data.queries, truth, k)
			require.NoError(t, err)
			recalls[ef] = r
			t.Logf("the twin at ef %d: recall %.4f", ef, r)
		}
		return recalls[ef]
	}
	points := make([]growSearch, len(diskTargets))
	for i, target := range diskTargets {
		points[i] = growSearch{Target: target}
		low := 0
		for _, ef := range diskGrid {
			if recall(ef) >= target {
				high := ef
				for high-low > 1 {
					middle := (low + high) / 2
					if middle < k || recall(middle) < target {
						low = middle
					} else {
						high = middle
					}
				}
				points[i].Reached, points[i].EF, points[i].Recall = true, high, recall(high)
				break
			}
			low = ef
		}
	}

	// measure searches every query in the twin and on disk at every point:
	// what they return must be the same, and the store tells what it read.
	measure := func(at string) []growSearch {
		out := slices.Clone(points)
		for i := range out {
			point := &out[i]
			if !point.Reached {
				continue
			}
			setEF(point.EF)
			reads, bytes, err := diskStoreReads()
			require.NoError(t, err)
			var loads, rounds, memberBytes, edgeBytes, rescored atomic.Int64
			var next atomic.Int64
			var mu sync.Mutex
			difference := ""
			eg := enterrors.NewErrorGroupWrapper(logger)
			for range 8 {
				eg.Go(func() error {
					for {
						q := int(next.Add(1)) - 1
						if q >= len(data.queries) {
							return nil
						}
						var memory SearchStats
						wantIDs, wantDists, err := twin.ix.search(ctx, data.queries[q], k, nil, &memory)
						if err != nil {
							return err
						}
						stats := &diskSearchStats{Work: &SearchStats{}}
						ids, dists, err := g.search(ctx, data.queries[q], k, stats)
						if err != nil {
							return err
						}
						if !slices.Equal(wantIDs, ids) || !slices.Equal(wantDists, dists) || diskWorkOf(&memory) != diskWorkOf(stats.Work) {
							mu.Lock()
							if difference == "" {
								difference = fmt.Sprintf("query %d: ids %v, distances %v and work %+v from disk; %v, %v and %+v in memory",
									q, ids, dists, diskWorkOf(stats.Work), wantIDs, wantDists, diskWorkOf(&memory))
							}
							mu.Unlock()
						}
						loads.Add(int64(stats.Postings))
						rounds.Add(int64(stats.Rounds))
						memberBytes.Add(int64(stats.MemberBytes))
						edgeBytes.Add(int64(stats.EdgeBytes))
						rescored.Add(int64(stats.Rescored))
					}
				})
			}
			require.NoError(t, eg.Wait())
			readsAfter, bytesAfter, err := diskStoreReads()
			require.NoError(t, err)
			queries := float64(len(data.queries))
			members := float64(memberBytes.Load()) / float64(8*(1+g.meta.ScanCodeWords))
			point.Compared = len(data.queries)
			point.Loads, point.Rounds = float64(loads.Load())/queries, float64(rounds.Load())/queries
			point.StoreReads, point.StoreBytes = float64(readsAfter-reads)/queries, float64(bytesAfter-bytes)/queries
			point.MemberBytes, point.EdgeBytes = float64(memberBytes.Load())/queries, float64(edgeBytes.Load())/queries
			// A list of edges carries the id of its member: 4 B a member
			// that the model does not count.
			point.ModelBytes = (float64(memberBytes.Load()) + float64(edgeBytes.Load()) - 4*members) / queries
			point.Rescored = float64(rescored.Load()) / queries
			t.Logf("%s, recall %.2f (ef %d, %.4f): %.2f loads in %.2f rounds | the store read %.1f times, %.3f MB, "+
				"for %.3f MB by the model: %.3f | members %.3f MB, edges %.3f MB", at, point.Target, point.EF, point.Recall,
				point.Loads, point.Rounds, point.StoreReads, point.StoreBytes/1e6, point.ModelBytes/1e6,
				point.StoreBytes/point.ModelBytes, point.MemberBytes/1e6, point.EdgeBytes/1e6)
			if difference != "" {
				write()
				t.Fatalf("STOP: %s, the index on disk does not return what the twin returns: %s", at, difference)
			}
		}
		return out
	}
	result.Uncompacted = measure("before compacting")
	write()

	begin = time.Now()
	result.CompactionRounds, err = g.compactAll()
	require.NoError(t, err)
	t.Logf("compacted in %d rounds and %s", result.CompactionRounds, time.Since(begin).Round(time.Second))
	result.Final = checkpoint(100, time.Since(run).Seconds())
	stop(result.Final)
	result.Compacted = measure("compacted")
	write()

	// Closed and opened again from its files, it is still the twin.
	require.NoError(t, g.close(ctx))
	g = nil
	open()
	require.NoError(t, g.checkCoverage(), "STOP: the coverage of the index opened again")
	require.NoError(t, g.sameAs(twin), "STOP: the index opened again against the twin")
	measure("opened again")
	result.Reopened = "the same as the twin"
	write()
}
