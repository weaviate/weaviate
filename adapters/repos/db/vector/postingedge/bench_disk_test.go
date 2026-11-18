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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

var (
	benchDiskDir = flag.String("bench.diskdir", "", "the disk prototype of v4.0 (TestDiskWrite, TestDiskTimed): the "+
		"directory the cells are written under and read from, one directory per cell. For cold measures it lives on a "+
		"volume whose cache -bench.diskdrop drops")
	benchDiskCells = flag.String("bench.diskcells", "full,r512,r384,halo512,halo512p", "with TestDiskWrite: the cells "+
		"to write and check, of full, r512, r384 (the routing code in memory: the full code, or a sketch of 512 or 384 "+
		"bits), halo512 (a halo of 512 bits in the postings) and halo512p (that halo over the edges pruned to one per "+
		"target posting and capped at 8, with margin 0.08)")
	benchDiskManifest = flag.String("bench.diskmanifest", "", "what TestDiskWrite leaves for TestDiskTimed: the cells, "+
		"their operating points, the checks they passed and the ground truth. It must not live under -bench.diskdir")
	benchDiskCell = flag.String("bench.diskcell", "", "with TestDiskTimed: the cell to time, one per process")
	benchDiskDrop = flag.String("bench.diskdrop", "", "with TestDiskTimed: the shell command that drops the system's "+
		"cache of the files under -bench.diskdir, run with the index closed before every cold batch. Empty, no cold "+
		"measure is taken")
	benchDiskCold   = flag.Int("bench.diskcold", 500, "with TestDiskTimed: the cold queries of every point")
	benchDiskBudget = flag.Int("bench.diskbudget", 8, "with TestDiskTimed: the concurrency budget of one query, which "+
		"the rescore reads its vectors within")
	benchDiskPasses = flag.Int("bench.diskpasses", 3, "with TestDiskTimed: the timed warm passes of every point")
)

// diskTargets are the recalls a cell is measured at.
var diskTargets = []float64{0.95, 0.99}

// diskGrid is where the operating point of a cell is first looked for,
// before the search narrows between two of its values.
var diskGrid = []int{10, 14, 20, 28, 40, 56, 80, 113, 160, 226, 320, 452, 640, 1024, 2048}

// diskModel is the byte model of v3.0: what a member, an edge and a halo
// entry take, the last one apart from its sketch.
const (
	diskModelIDBytes    = 8
	diskModelEdgeBytes  = 4
	diskModelEntryBytes = 4
	// diskStoreNodeBytes is what the store wraps a block in, apart from its
	// key: a tombstone byte, the length of the value and that of the key.
	diskStoreNodeBytes = 13
)

// diskCellSpec is one cell of v4.0.
type diskCellSpec struct {
	Name string `json:"name"`
	// Bits is the width of the code the endpoints are ranked by, 0 for the
	// full code, and Halo tells that it lives in the postings.
	Bits int  `json:"bits"`
	Halo bool `json:"halo"`
	// OnePerTarget and DegreeCap prune the edges, as in v3.1.
	OnePerTarget bool    `json:"onePerTarget"`
	DegreeCap    int     `json:"degreeCap"`
	Margin       float64 `json:"margin"`
}

func diskCellSpecs(t *testing.T, names string) []diskCellSpec {
	known := map[string]diskCellSpec{
		"full":     {Name: "full", Margin: DefaultCosineSourceMargin},
		"r512":     {Name: "r512", Bits: 512, Margin: DefaultCosineSourceMargin},
		"r384":     {Name: "r384", Bits: 384, Margin: DefaultCosineSourceMargin},
		"halo512":  {Name: "halo512", Bits: 512, Halo: true, Margin: DefaultCosineSourceMargin},
		"halo512p": {Name: "halo512p", Bits: 512, Halo: true, OnePerTarget: true, DegreeCap: 8, Margin: 0.08},
	}
	var specs []diskCellSpec
	for _, name := range strings.Split(names, ",") {
		spec, ok := known[strings.TrimSpace(name)]
		require.Truef(t, ok, "-bench.diskcells: unknown cell %q", name)
		specs = append(specs, spec)
	}
	return specs
}

// diskPoint is the operating point of a cell for one recall: the smallest ef
// that reaches it, and what a query does and reads there, per query.
type diskPoint struct {
	Target       float64 `json:"target"`
	Reached      bool    `json:"reached"`
	EF           int     `json:"ef"`
	RescoreLimit int     `json:"rescoreLimit"`
	Recall       float64 `json:"recall"`
	// Compared is the queries whose results from disk were compared with
	// those of the index in memory, all of them equal.
	Compared int     `json:"compared"`
	Loads    float64 `json:"loads"`
	Rounds   float64 `json:"rounds"`
	// Blocks and the bytes by block are what the index decoded, per query.
	Blocks      float64 `json:"blocks"`
	MemberBytes float64 `json:"memberBytes"`
	EdgeBytes   float64 `json:"edgeBytes"`
	HaloBytes   float64 `json:"haloBytes"`
	// StoreReads and StoreBytes are what the store counted, reading with
	// pread, per query.
	StoreReads float64 `json:"storeReads"`
	StoreBytes float64 `json:"storeBytes"`
	// ModelBytes is the byte model of v3.0 over the postings loaded.
	ModelBytes    float64 `json:"modelBytes"`
	RealOverModel float64 `json:"realOverModel"`
	// Rescored is the exact vectors read, one read of 4 bytes per dimension
	// each.
	Rescored float64 `json:"rescored"`
}

type diskCellManifest struct {
	Spec diskCellSpec `json:"spec"`
	Dir  string       `json:"dir"`
	Meta diskMeta     `json:"meta"`
	// MaxRecall is the best recall over the grid, with MaxRescoreLimit.
	MaxRecall       float64     `json:"maxRecall"`
	MaxRescoreLimit int         `json:"maxRescoreLimit"`
	Points          []diskPoint `json:"points"`
	// Files is what the cell takes on disk, by what it is.
	Files map[string]int64 `json:"files"`
}

type diskManifest struct {
	Dataset string             `json:"dataset"`
	Vectors int                `json:"vectors"`
	Queries int                `json:"queries"`
	K       int                `json:"k"`
	Dims    int                `json:"dims"`
	Config  map[string]any     `json:"config"`
	Build   *buildWork         `json:"build"`
	Cells   []diskCellManifest `json:"cells"`
	// Truth is the ground truth of every query, for the timed runs, which do
	// not load the corpus.
	Truth [][]uint64 `json:"truth"`
}

// diskFiles sums what an index takes on disk: the store with the postings,
// the exact vectors, and what a reader loads into memory.
func diskFiles(dir string) (map[string]int64, error) {
	files := map[string]int64{"store": 0, "vectors": 0, "resident": 0}
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		switch {
		case strings.HasPrefix(rel, diskStoreDir+string(filepath.Separator)):
			files["store"] += info.Size()
		case rel == diskVectorFile:
			files["vectors"] += info.Size()
		default:
			files["resident"] += info.Size()
		}
		return nil
	})
	return files, err
}

// diskRecall searches every query in the index in memory, untimed, and tells
// the recall.
func diskRecall(b *haloBench, ef, rescore int, margin float32) float64 {
	t, data := b.t, b.data
	uc := b.uc
	uc.EF, uc.RescoreLimit, uc.SourceMargin = ef, rescore, margin
	require.NoError(t, b.index.UpdateUserConfig(uc, func() {}))
	searchCtx := concurrency.CtxWithBudget(b.ctx, 1)
	var hits, total atomic.Int64
	err := parallelChunks(b.ctx, b.logger, len(data.queries), 8, func(_, start, end int) error {
		found, wanted := 0, 0
		for i := start; i < end; i++ {
			ids, _, err := b.index.SearchByVector(searchCtx, data.queries[i], *benchK, nil)
			if err != nil {
				return err
			}
			wanted += len(data.truth[i])
			for _, want := range data.truth[i] {
				if slices.Contains(ids, want) {
					found++
				}
			}
		}
		hits.Add(int64(found))
		total.Add(int64(wanted))
		return nil
	})
	require.NoError(t, err)
	return float64(hits.Load()) / float64(total.Load())
}

// diskOperatingPoints finds, for every target, the smallest ef whose recall
// reaches it: first over the grid, then between the two values of the grid
// around it. The index in memory has the state of the cell.
func diskOperatingPoints(b *haloBench, name string, rescore int, margin float32) ([]diskPoint, float64) {
	recalls := make(map[int]float64)
	recall := func(ef int) float64 {
		if _, done := recalls[ef]; !done {
			recalls[ef] = diskRecall(b, ef, rescore, margin)
			b.t.Logf("%s, rescore %d, ef %d: recall %.4f", name, rescore, ef, recalls[ef])
		}
		return recalls[ef]
	}
	best := 0.0
	points := make([]diskPoint, len(diskTargets))
	for i, target := range diskTargets {
		points[i] = diskPoint{Target: target, RescoreLimit: rescore}
		low := 0
		for _, ef := range diskGrid {
			r := recall(ef)
			best = max(best, r)
			if r >= target {
				// The smallest ef between the last one below the target
				// and this one.
				high := ef
				for high-low > 1 {
					middle := (low + high) / 2
					if middle < *benchK || recall(middle) < target {
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
	return points, best
}

// TestDiskWrite is the untimed part of v4.0 (bench-report.md, "v4.0: primer
// prototipo que lee de disco"): one batch build of the dataset of
// -bench.dataset, every cell of -bench.diskcells written under
// -bench.diskdir, and the two checks that come before any timing. A cell on
// disk must return, for every query at its two operating points, what its
// state in memory returns; and what the store reads for it must be within a
// quarter of the byte model of v3.0. Either check failing stops the run. What
// passes goes into -bench.diskmanifest, for TestDiskTimed.
func TestDiskWrite(t *testing.T) {
	if *benchDatasetDir == "" || *benchDiskDir == "" {
		t.Skip("no -bench.dataset and -bench.diskdir given")
	}
	require.NotEmpty(t, *benchDiskManifest, "TestDiskWrite leaves its manifest in -bench.diskmanifest")
	specs := diskCellSpecs(t, *benchDiskCells)
	require.NoError(t, os.MkdirAll(*benchDiskDir, 0o755))
	b := newHaloBench(t, "disk")
	ctx, index, built, data := b.ctx, b.index, b.built, b.data
	provider := distancer.NewCosineDistanceProvider()
	vectorOf := func(id uint64) []float32 { return data.train[id] }
	manifest := &diskManifest{
		Dataset: data.name, Vectors: len(data.train), Queries: len(data.queries), K: *benchK, Dims: data.dims,
		Config: b.config(), Build: workOfBuild(t, index, 0), Truth: data.truth,
	}
	write := func() {
		raw, err := json.Marshal(manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchDiskManifest, raw, 0o644))
	}
	fullStart, fullEdges := built.postings.edgeStart, built.postings.edges
	metrics, err := diskStoreMetrics()
	require.NoError(t, err)

	shareFrom := ""
	for _, spec := range specs {
		// The state of the cell in memory: its code for the endpoints over
		// its edges.
		st := built
		if spec.Bits > 0 {
			st = b.sketch(spec.Bits)
		}
		if spec.OnePerTarget || spec.DegreeCap > 0 {
			start, kept := pruneEdges(fullStart, fullEdges, built.side.postingOf, spec.OnePerTarget, spec.DegreeCap)
			require.NoError(t, checkPruned(fullStart, fullEdges, start, kept, built.side.postingOf, spec.OnePerTarget, spec.DegreeCap))
			index.state.Store(st)
			require.NoError(t, index.replaceEdges(start, kept))
			st = index.state.Load()
		}
		dir := filepath.Join(*benchDiskDir, spec.Name)
		require.NoError(t, os.RemoveAll(dir))
		require.NoError(t, os.Mkdir(dir, 0o755))
		begin := time.Now()
		meta, err := writeDiskIndex(ctx, b.logger, dir, st, diskWriteOptions{Halo: spec.Halo, ShareFrom: shareFrom},
			provider, vectorOf)
		require.NoError(t, err)
		if shareFrom == "" {
			shareFrom = dir
		}
		files, err := diskFiles(dir)
		require.NoError(t, err)
		t.Logf("cell %s: written in %s | %d postings, %d edges, %d halo entries | store %.1f MB, vectors %.1f MB, "+
			"resident %.1f MB", spec.Name, time.Since(begin).Round(time.Second), meta.Postings, meta.Edges, meta.HaloEntries,
			float64(files["store"])/1e6, float64(files["vectors"])/1e6, float64(files["resident"])/1e6)
		cell := diskCellManifest{Spec: spec, Dir: spec.Name, Meta: *meta, Files: files}

		// Reopened, not the one just written. The index in memory enters
		// through the graph the one on disk was reopened with.
		disk, err := openDiskIndex(ctx, b.logger, dir, provider, b.uc, nil, false)
		require.NoError(t, err)
		index.state.Store(withCentroids(st, disk.st.centroids))
		margin := float32(spec.Margin)

		cell.MaxRescoreLimit = b.uc.RescoreLimit
		cell.Points, cell.MaxRecall = diskOperatingPoints(b, spec.Name, b.uc.RescoreLimit, margin)
		if last := cell.Points[len(cell.Points)-1]; !last.Reached && *benchHaloRescore > b.uc.RescoreLimit {
			t.Logf("cell %s: the best recall is %.4f with rescore %d: looked for again with %d",
				spec.Name, cell.MaxRecall, b.uc.RescoreLimit, *benchHaloRescore)
			again, best := diskOperatingPoints(b, spec.Name, *benchHaloRescore, margin)
			if again[len(again)-1].Reached {
				cell.Points, cell.MaxRecall, cell.MaxRescoreLimit = again, best, *benchHaloRescore
			}
		}

		for i := range cell.Points {
			point := &cell.Points[i]
			if !point.Reached {
				t.Logf("cell %s: recall %.2f is not reached, the best is %.4f", spec.Name, point.Target, cell.MaxRecall)
				continue
			}
			uc := b.uc
			uc.EF, uc.RescoreLimit, uc.SourceMargin = point.EF, point.RescoreLimit, margin
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
			require.NoError(t, disk.ix.UpdateUserConfig(uc, func() {}))

			// The first check: the same results as in memory, query by query.
			searchCtx := concurrency.CtxWithBudget(ctx, 1)
			var mu sync.Mutex
			var first string
			sums := &diskSearchStats{}
			err := parallelChunks(ctx, b.logger, len(data.queries), 8, func(_, start, end int) error {
				for q := start; q < end; q++ {
					difference, stats, err := diskDifference(searchCtx, index, disk, data.queries[q], *benchK)
					if err != nil {
						return err
					}
					mu.Lock()
					if difference != "" && first == "" {
						first = fmt.Sprintf("query %d: %s", q, difference)
					}
					sums.Postings += stats.Postings
					sums.Rounds += stats.Rounds
					sums.Blocks += stats.Blocks
					sums.MemberBytes += stats.MemberBytes
					sums.EdgeBytes += stats.EdgeBytes
					sums.HaloBytes += stats.HaloBytes
					sums.Rescored += stats.Rescored
					mu.Unlock()
				}
				return nil
			})
			require.NoError(t, err)
			if first != "" {
				t.Fatalf("STOP: cell %s at recall %.2f (ef %d) does not return from disk what it returns in memory: %s",
					spec.Name, point.Target, point.EF, first)
			}
			q := float64(len(data.queries))
			point.Compared = len(data.queries)
			point.Loads, point.Rounds, point.Blocks = float64(sums.Postings)/q, float64(sums.Rounds)/q, float64(sums.Blocks)/q
			point.MemberBytes, point.EdgeBytes = float64(sums.MemberBytes)/q, float64(sums.EdgeBytes)/q
			point.HaloBytes, point.Rescored = float64(sums.HaloBytes)/q, float64(sums.Rescored)/q

			// The model of v3.0 over the same postings: a member is its id
			// and its code, an edge 4 bytes, a halo entry 4 bytes and its
			// sketch. The edges block has a byte per member besides.
			memberBytes := diskModelIDBytes + 8*meta.ScanCodeWords
			members := sums.MemberBytes / memberBytes
			model := sums.MemberBytes + (sums.EdgeBytes - members) + sums.HaloBytes
			require.Equal(t, 0, (sums.EdgeBytes-members)%diskModelEdgeBytes)
			if spec.Halo {
				require.Equal(t, 0, sums.HaloBytes%(diskModelEntryBytes+8*meta.RoutingCodeWords))
			}
			point.ModelBytes = float64(model) / q
			t.Logf("cell %s at recall %.2f: ef %d, recall %.4f | the %d queries return from disk what they return in "+
				"memory | %.2f loads, %.2f rounds, %.1f blocks, %.3f MB of blocks per query (model %.3f MB), %.1f rescored",
				spec.Name, point.Target, point.EF, point.Recall, point.Compared, point.Loads, point.Rounds, point.Blocks,
				(point.MemberBytes+point.EdgeBytes+point.HaloBytes)/1e6, point.ModelBytes/1e6, point.Rescored)
		}
		require.NoError(t, disk.close(ctx))

		// The second check: what the store reads, counted by the store,
		// which only counts when it reads with pread.
		counted, err := openDiskIndex(ctx, b.logger, dir, provider, b.uc, metrics, true)
		require.NoError(t, err)
		for i := range cell.Points {
			point := &cell.Points[i]
			if !point.Reached {
				continue
			}
			uc := b.uc
			uc.EF, uc.RescoreLimit, uc.SourceMargin = point.EF, point.RescoreLimit, margin
			require.NoError(t, counted.ix.UpdateUserConfig(uc, func() {}))
			readsBefore, bytesBefore, err := diskStoreReads()
			require.NoError(t, err)
			searchCtx := concurrency.CtxWithBudget(ctx, 1)
			var blocks, blockBytes atomic.Int64
			err = parallelChunks(ctx, b.logger, len(data.queries), 8, func(_, start, end int) error {
				for q := start; q < end; q++ {
					stats := &diskSearchStats{}
					if _, _, err := counted.search(searchCtx, data.queries[q], *benchK, stats); err != nil {
						return err
					}
					blocks.Add(int64(stats.Blocks))
					blockBytes.Add(int64(stats.BlockBytes))
				}
				return nil
			})
			require.NoError(t, err)
			readsAfter, bytesAfter, err := diskStoreReads()
			require.NoError(t, err)
			q := float64(len(data.queries))
			reads, bytes := readsAfter-readsBefore, bytesAfter-bytesBefore
			// The store reads a block with one read, of the block and
			// what it wraps it in.
			require.Equal(t, uint64(blocks.Load()), reads, "the store counts one read per block")
			require.Equal(t, uint64(blockBytes.Load()+blocks.Load()*(diskStoreNodeBytes+diskKeyBytes)), bytes)
			require.InDelta(t, point.Blocks, float64(blocks.Load())/q, 1e-9, "the same blocks as the search over the mapped store")
			point.StoreReads, point.StoreBytes = float64(reads)/q, float64(bytes)/q
			point.RealOverModel = point.StoreBytes / point.ModelBytes
			t.Logf("cell %s at recall %.2f: the store counts %.1f reads and %.3f MB per query, %.3f of the model",
				spec.Name, point.Target, point.StoreReads, point.StoreBytes/1e6, point.RealOverModel)
			if point.RealOverModel < 0.75 || point.RealOverModel > 1.25 {
				t.Fatalf("STOP: cell %s at recall %.2f reads %.3f MB per query, the model says %.3f MB: more than a "+
					"quarter apart", spec.Name, point.Target, point.StoreBytes/1e6, point.ModelBytes/1e6)
			}
		}
		require.NoError(t, counted.close(ctx))

		index.state.Store(built)
		manifest.Cells = append(manifest.Cells, cell)
		write()
	}
	require.NoError(t, index.Shutdown(ctx))
}

// diskLatencies summarizes the latencies of a set of queries.
type diskLatencies struct {
	Queries int     `json:"queries"`
	P50     float64 `json:"p50ms"`
	P95     float64 `json:"p95ms"`
	P99     float64 `json:"p99ms"`
	Mean    float64 `json:"meanms"`
	// QPS is the queries over the time they took all together.
	QPS float64 `json:"qps"`
	// The parts of the mean latency: the centroid graph, the wait for the
	// blocks, the scan and the ranking of what they hold, and the rescore.
	CentroidMs float64 `json:"centroidms"`
	FetchMs    float64 `json:"fetchms"`
	VisitMs    float64 `json:"visitms"`
	RescoreMs  float64 `json:"rescorems"`
}

func diskLatenciesOf(latencies []time.Duration, stats []diskSearchStats, wall time.Duration) diskLatencies {
	sorted := make([]float64, len(latencies))
	out := diskLatencies{Queries: len(latencies)}
	for i, l := range latencies {
		sorted[i] = ms(l)
		out.Mean += ms(l) / float64(len(latencies))
		out.CentroidMs += ms(stats[i].CentroidTime) / float64(len(latencies))
		out.FetchMs += ms(stats[i].FetchTime) / float64(len(latencies))
		out.VisitMs += ms(stats[i].VisitTime) / float64(len(latencies))
		out.RescoreMs += ms(stats[i].RescoreTime) / float64(len(latencies))
	}
	slices.Sort(sorted)
	out.P50, out.P95, out.P99 = percentileOf(sorted, 0.50), percentileOf(sorted, 0.95), percentileOf(sorted, 0.99)
	out.QPS = float64(len(latencies)) / wall.Seconds()
	return out
}

// diskTimedPoint is one cell at one operating point with one thread count.
type diskTimedPoint struct {
	Target  float64 `json:"target"`
	EF      int     `json:"ef"`
	Threads int     `json:"threads"`
	Recall  float64 `json:"recall"`
	// Warm has every timed pass over all the queries, after an untimed one.
	Warm []diskLatencies `json:"warm"`
	// Cold is the queries of all the cold batches together, and ColdHalves
	// the two halves of the batches apart, to see how far a cold measure is
	// from itself.
	Cold       *diskLatencies  `json:"cold,omitempty"`
	ColdHalves []diskLatencies `json:"coldHalves,omitempty"`
	// ColdBatches is the cold batches and ColdBatch their size.
	// ColdRepeated is the part of the posting loads of a batch that load a
	// posting already loaded since the cache was dropped. ColdRecall is the
	// recall of the cold queries, a part of all of them.
	ColdBatches  int     `json:"coldBatches,omitempty"`
	ColdBatch    int     `json:"coldBatch,omitempty"`
	ColdRepeated float64 `json:"coldRepeated,omitempty"`
	ColdRecall   float64 `json:"coldRecall,omitempty"`
	// ReopenMs is the mean time to open the index after the cache is
	// dropped, and IndexBytes what warming the index of its segment reads.
	ReopenMs   float64 `json:"reopenms,omitempty"`
	IndexBytes int64   `json:"indexBytes,omitempty"`
}

type diskTimedResult struct {
	Dataset string       `json:"dataset"`
	Cell    diskCellSpec `json:"cell"`
	Meta    diskMeta     `json:"meta"`
	Queries int          `json:"queries"`
	K       int          `json:"k"`
	Budget  int          `json:"budget"`
	Procs   int          `json:"gomaxprocs"`
	Access  string       `json:"access"`
	Drop    string       `json:"drop"`
	// HeapBytes is the heap in use right after the index is opened and
	// collected, and FootprintBytes the most the process has had resident,
	// read then.
	HeapBytes      uint64           `json:"heapBytes"`
	FootprintBytes int64            `json:"footprintBytes"`
	Files          map[string]int64 `json:"files"`
	Points         []diskTimedPoint `json:"points"`
}

// TestDiskTimed times one cell of v4.0, written and checked by TestDiskWrite:
// at each of its operating points, with 1 and with 8 threads, warm and, when
// -bench.diskdrop is given, cold. It loads the queries and the manifest, not
// the corpus, so that what the process holds is what the cell keeps in
// memory. The store reads through its mapping, as it does by default.
func TestDiskTimed(t *testing.T) {
	if *benchDatasetDir == "" || *benchDiskDir == "" || *benchDiskCell == "" {
		t.Skip("no -bench.dataset, -bench.diskdir and -bench.diskcell given")
	}
	require.NotEmpty(t, *benchOut, "TestDiskTimed writes its results to -bench.out")
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
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

	open := func() *diskIndex {
		disk, err := openDiskIndex(ctx, logger, dir, provider, uc, nil, false)
		require.NoError(t, err)
		return disk
	}
	disk := open()
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var usage syscall.Rusage
	require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	footprint := int64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		footprint *= 1024 // kilobytes there, bytes on darwin
	}
	files, err := diskFiles(dir)
	require.NoError(t, err)
	result := &diskTimedResult{
		Dataset: manifest.Dataset, Cell: cell.Spec, Meta: cell.Meta, Queries: len(queries), K: k, Budget: *benchDiskBudget,
		Procs: runtime.GOMAXPROCS(0), Access: "mmap", Drop: *benchDiskDrop, HeapBytes: mem.HeapInuse,
		FootprintBytes: footprint, Files: files,
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}
	write()
	t.Logf("cell %s: heap in use %.1f MB after opening, footprint %.1f MB | store %.1f MB, vectors %.1f MB, resident "+
		"files %.1f MB", cell.Spec.Name, float64(mem.HeapInuse)/1e6, float64(footprint)/1e6,
		float64(files["store"])/1e6, float64(files["vectors"])/1e6, float64(files["resident"])/1e6)

	// run searches the given queries with the given number of threads, each
	// thread taking the next query, and tells the latency and the stats of
	// each and the time of all.
	run := func(disk *diskIndex, which []int, threads int) ([]time.Duration, []diskSearchStats, int, time.Duration) {
		latencies := make([]time.Duration, len(which))
		stats := make([]diskSearchStats, len(which))
		var next, hits atomic.Int64
		var wg sync.WaitGroup
		errs := make([]error, threads)
		begin := time.Now()
		for worker := 0; worker < threads; worker++ {
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				searchCtx := concurrency.CtxWithBudget(ctx, *benchDiskBudget)
				for {
					i := int(next.Add(1)) - 1
					if i >= len(which) {
						return
					}
					q := which[i]
					stats[i].KeepLoaded = true
					start := time.Now()
					ids, _, err := disk.search(searchCtx, queries[q], k, &stats[i])
					latencies[i] = time.Since(start)
					if err != nil {
						errs[worker] = err
						return
					}
					found := 0
					for _, want := range truth[q] {
						if slices.Contains(ids, want) {
							found++
						}
					}
					hits.Add(int64(found))
				}
			}, logger)
		}
		wg.Wait()
		wall := time.Since(begin)
		for _, err := range errs {
			require.NoError(t, err)
		}
		return latencies, stats, int(hits.Load()), wall
	}
	all := make([]int, len(queries))
	for i := range all {
		all[i] = i
	}

	for _, point := range cell.Points {
		if !point.Reached {
			t.Logf("cell %s does not reach recall %.2f: nothing to time there", cell.Spec.Name, point.Target)
			continue
		}
		pointUC := uc
		pointUC.EF, pointUC.RescoreLimit = point.EF, point.RescoreLimit
		require.NoError(t, disk.ix.UpdateUserConfig(pointUC, func() {}))
		for _, threads := range []int{1, 8} {
			timed := diskTimedPoint{Target: point.Target, EF: point.EF, Threads: threads}
			// Warm: after a pass that nobody times.
			_, _, hits, _ := run(disk, all, 8)
			timed.Recall = float64(hits) / float64(len(queries)*k)
			require.InDelta(t, point.Recall, timed.Recall, 1e-9, "the recall of the manifest")
			for pass := 0; pass < *benchDiskPasses; pass++ {
				latencies, stats, _, wall := run(disk, all, threads)
				timed.Warm = append(timed.Warm, diskLatenciesOf(latencies, stats, wall))
			}
			warm := timed.Warm[len(timed.Warm)/2]
			t.Logf("cell %s at recall %.2f (ef %d), %d threads, warm: p50 %.3f ms, p95 %.3f, p99 %.3f | %.0f QPS | "+
				"fetch %.3f ms, visit %.3f, rescore %.3f, centroids %.3f", cell.Spec.Name, point.Target, point.EF, threads,
				warm.P50, warm.P95, warm.P99, warm.QPS, warm.FetchMs, warm.VisitMs, warm.RescoreMs, warm.CentroidMs)

			if *benchDiskDrop != "" {
				batch := 10
				if threads > 1 {
					batch = 2 * threads
				}
				var latencies []time.Duration
				var stats []diskSearchStats
				var halves [2]struct {
					latencies []time.Duration
					stats     []diskSearchStats
					wall      time.Duration
				}
				var wall, reopen time.Duration
				hits, loads, repeated := 0, 0, 0
				for from := 0; from < *benchDiskCold; from += batch {
					which := all[from:min(from+batch, *benchDiskCold)]
					require.NoError(t, disk.close(ctx))
					out, err := exec.CommandContext(ctx, "sh", "-c", *benchDiskDrop).CombinedOutput()
					require.NoErrorf(t, err, "drop the cache: %s", out)
					begin := time.Now()
					disk = open()
					require.NoError(t, disk.ix.UpdateUserConfig(pointUC, func() {}))
					timed.IndexBytes, err = warmSegmentIndexes(filepath.Join(dir, diskStoreDir))
					require.NoError(t, err)
					reopen += time.Since(begin)

					batchLatencies, batchStats, batchHits, batchWall := run(disk, which, threads)
					seen := map[uint32]bool{}
					for _, s := range batchStats {
						for _, p := range s.Loaded {
							loads++
							if seen[p] {
								repeated++
							}
							seen[p] = true
						}
					}
					half := &halves[timed.ColdBatches%2]
					half.latencies, half.stats = append(half.latencies, batchLatencies...), append(half.stats, batchStats...)
					half.wall += batchWall
					latencies, stats = append(latencies, batchLatencies...), append(stats, batchStats...)
					wall += batchWall
					hits += batchHits
					timed.ColdBatches++
				}
				cold := diskLatenciesOf(latencies, stats, wall)
				timed.Cold, timed.ColdBatch = &cold, batch
				for _, half := range halves {
					timed.ColdHalves = append(timed.ColdHalves, diskLatenciesOf(half.latencies, half.stats, half.wall))
				}
				timed.ColdRepeated = float64(repeated) / float64(loads)
				timed.ColdRecall = float64(hits) / float64(len(latencies)*k)
				timed.ReopenMs = ms(reopen) / float64(timed.ColdBatches)
				t.Logf("cell %s at recall %.2f (ef %d), %d threads, cold: p50 %.3f ms, p95 %.3f, p99 %.3f | %.0f QPS | "+
					"fetch %.3f ms, visit %.3f, rescore %.3f | %d batches of %d, %.1f%% of the loads repeat a posting, "+
					"halves at p50 %.3f and %.3f ms | reopened in %.0f ms", cell.Spec.Name, point.Target, point.EF, threads,
					cold.P50, cold.P95, cold.P99, cold.QPS, cold.FetchMs, cold.VisitMs, cold.RescoreMs, timed.ColdBatches,
					batch, 100*timed.ColdRepeated, timed.ColdHalves[0].P50, timed.ColdHalves[1].P50, timed.ReopenMs)
			}
			result.Points = append(result.Points, timed)
			write()
		}
	}
	require.NoError(t, disk.close(ctx))
}
