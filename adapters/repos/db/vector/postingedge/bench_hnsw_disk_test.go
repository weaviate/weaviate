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
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/queryadmission"
)

var (
	benchHNSWDiskDir = flag.String("bench.hnswdisk.dir", "", "TestHNSWDiskTimed: the directory, on the disk under "+
		"test, of the file with the vectors of the dataset that HNSW reads to rescore. It is written there if it is "+
		"not there yet")
	benchHNSWDiskRounds = flag.Int("bench.hnswdisk.rounds", 2, "TestHNSWDiskTimed: the timed rounds, one after "+
		"the other, in the process that built the index")
	benchHNSWDiskWait = flag.String("bench.hnswdisk.wait", "", "TestHNSWDiskTimed: a shell command run once the "+
		"index is built and its operating points are found, before anything is timed. It returns when the machine "+
		"may be timed, and fails if it may not")
	benchHNSWDiskEFs = flag.String("bench.hnswdisk.efs", "", "TestHNSWDiskTimed: ef values, comma-separated, to "+
		"tell the recall at once the index is built, instead of finding its operating points and timing them")
	benchHNSWDiskCentering = flag.Bool("bench.hnswdisk.centering", false, "TestHNSWDiskTimed: the RQ codes "+
		"centered, as in the HNSW of v1.4. A class has them not centered by default")
)

// hnswDiskVectors is the vectors of a dataset in a file of float32, read one
// at a time with pread, as HNSW reads the vector of a node to rescore it.
// Nothing of them stays in memory. close and open let the cache of the file
// be dropped between two batches of queries.
type hnswDiskVectors struct {
	path  string
	dims  int
	rows  uint64
	file  atomic.Pointer[os.File]
	reads atomic.Int64
	bufs  sync.Pool
}

func newHNSWDiskVectors(path string, rows, dims int) *hnswDiskVectors {
	v := &hnswDiskVectors{path: path, dims: dims, rows: uint64(rows)}
	v.bufs.New = func() any {
		buf := make([]byte, 4*dims)
		return &buf
	}
	return v
}

func (v *hnswDiskVectors) open() error {
	f, err := os.Open(v.path)
	if err != nil {
		return err
	}
	v.file.Store(f)
	return nil
}

func (v *hnswDiskVectors) close() error {
	if f := v.file.Swap(nil); f != nil {
		return f.Close()
	}
	return nil
}

// read reads the vector of id into into, which it grows as it needs, and
// returns it.
func (v *hnswDiskVectors) read(id uint64, into []float32) ([]float32, error) {
	if id >= v.rows {
		return nil, storobj.NewErrNotFoundf(id, "out of range")
	}
	f := v.file.Load()
	if f == nil {
		return nil, fmt.Errorf("read vector %d: the file of the vectors is closed", id)
	}
	buf := v.bufs.Get().(*[]byte)
	defer v.bufs.Put(buf)
	if _, err := f.ReadAt(*buf, int64(id)*int64(len(*buf))); err != nil {
		return nil, fmt.Errorf("read vector %d: %w", id, err)
	}
	v.reads.Add(1)
	into = slices.Grow(into[:0], v.dims)[:v.dims]
	for i := range into {
		into[i] = math.Float32frombits(binary.LittleEndian.Uint32((*buf)[4*i:]))
	}
	return into, nil
}

// writeHNSWDiskVectors writes the train set of the dataset, normalized, to
// path, unless a file of its size is there already.
func writeHNSWDiskVectors(t *testing.T, data *benchData, path string) {
	want := int64(data.rows) * int64(data.dims) * 4
	if info, err := os.Stat(path); err == nil && info.Size() == want {
		t.Logf("the vectors are in %s already: %d of %d dimensions", path, data.rows, data.dims)
		return
	}
	begin := time.Now()
	f, err := os.Create(path + ".tmp")
	require.NoError(t, err)
	w := bufio.NewWriterSize(f, 16<<20)
	buf := make([]byte, 4*data.dims)
	rows := 0
	err = testinghelpers.ReadRawTrainChunks(data.dir, 100_000, func(first int, chunk [][]float32) error {
		if first != rows {
			return fmt.Errorf("a chunk from row %d, where %d was next", first, rows)
		}
		for _, vec := range chunk {
			distancer.NormalizeInPlace(vec)
			for i, x := range vec {
				binary.LittleEndian.PutUint32(buf[4*i:], math.Float32bits(x))
			}
			if _, err := w.Write(buf); err != nil {
				return err
			}
		}
		rows += len(chunk)
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	require.Equal(t, data.rows, rows)
	require.NoError(t, os.Rename(path+".tmp", path))
	t.Logf("the vectors written to %s in %s: %d of %d dimensions", path, time.Since(begin).Round(time.Second),
		rows, data.dims)
}

// hnswDiskRound is one timed round of TestHNSWDiskTimed.
type hnswDiskRound struct {
	Round  int              `json:"round"`
	Start  string           `json:"start"`
	End    string           `json:"end"`
	Points []diskTimedPoint `json:"points"`
	// Reads is the vectors read from the file per query, by point, in the
	// warm passes.
	Reads []float64 `json:"reads"`
}

type hnswDiskResult struct {
	Dataset   string         `json:"dataset"`
	Vectors   int            `json:"vectors"`
	Dims      int            `json:"dims"`
	Queries   int            `json:"queries"`
	K         int            `json:"k"`
	Config    map[string]any `json:"config"`
	Admission bool           `json:"admission,omitempty"`
	Budget    int            `json:"budget"`
	Procs     int            `json:"gomaxprocs"`
	Drop      string         `json:"drop"`
	// WriteSeconds is what writing the file of the vectors took, 0 if it was
	// there, and BuildSeconds what building the index took.
	WriteSeconds float64            `json:"writeSeconds"`
	BuildSeconds float64            `json:"buildSeconds"`
	BuildStages  map[string]float64 `json:"buildStages"`
	// AfterBuild is the memory of the process once the index is built and
	// collected: the index, the queries and the ground truth, and nothing of
	// the vectors.
	AfterBuild memSnapshot `json:"afterBuild"`
	// CodeBytes is the codes HNSW keeps of the vectors in memory, and
	// VectorBytes the file of the vectors.
	CodeBytes   int64   `json:"codeBytes"`
	VectorBytes int64   `json:"vectorBytes"`
	MaxRecall   float64 `json:"maxRecall"`
	// Recalls is the recall at every ef of -bench.hnswdisk.efs, when given.
	Recalls map[int]float64 `json:"recalls,omitempty"`
	Points  []diskPoint     `json:"points"`
	Rounds  []hnswDiskRound `json:"rounds"`
}

// TestHNSWDiskTimed times HNSW with its default configuration and 1-bit RQ,
// next to the cells of v4.0 (bench-report.md, "10M en disco"): the graph and
// the codes in memory, as HNSW keeps them, and the vectors it rescores with
// in a file on the disk under test, read with pread at every rescore, with
// none of them in memory. The index is built in the process, as the bench of
// v1.4 builds it, and timed there: at each of its two operating points, with
// 1 thread, warm and, when -bench.diskdrop is given, cold, in the batches and
// with the drops of TestDiskTimed. Only the file of the vectors is closed for
// a drop: the graph and the codes stay where HNSW keeps them.
func TestHNSWDiskTimed(t *testing.T) {
	if *benchDatasetDir == "" || *benchHNSWDiskDir == "" {
		t.Skip("no -bench.dataset and -bench.hnswdisk.dir given")
	}
	require.NotEmpty(t, *benchOut, "TestHNSWDiskTimed writes its results to -bench.out")
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	data := loadBenchData(t, *benchDatasetDir, 0, *benchQueries, *benchK, true)
	n, k := data.rows, *benchK
	queries, truth := data.queries, data.truth
	require.NoError(t, os.MkdirAll(*benchHNSWDiskDir, 0o755))
	path := *benchHNSWDiskDir + "/vectors.f32"

	result := &hnswDiskResult{
		Dataset: data.name, Vectors: n, Dims: data.dims, Queries: len(queries), K: k,
		Admission: *benchDiskAdmission, Budget: *benchDiskBudget, Procs: runtime.GOMAXPROCS(0), Drop: *benchDiskDrop,
	}
	write := func() {
		raw, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
	}
	begin := time.Now()
	writeHNSWDiskVectors(t, data, path)
	result.WriteSeconds = time.Since(begin).Seconds()
	info, err := os.Stat(path)
	require.NoError(t, err)
	result.VectorBytes = info.Size()

	vectors := newHNSWDiskVectors(path, n, data.dims)
	require.NoError(t, vectors.open())
	defer func() { require.NoError(t, vectors.close()) }()
	vectorForID := func(_ context.Context, id uint64) ([]float32, error) {
		return vectors.read(id, nil)
	}
	tempVectorForID := func(_ context.Context, id uint64, container *common.VectorSlice, _ common.BucketView,
	) ([]float32, error) {
		vec, err := vectors.read(id, container.Slice)
		if err != nil {
			return nil, err
		}
		container.Slice = vec
		return vec, nil
	}

	// The defaults of a class, with 1-bit RQ: 32 connections, ef
	// construction 128, a rescore limit of 512, no centering unless
	// -bench.hnswdisk.centering asks for it, and a training limit of 10 000.
	uc := enthnsw.UserConfig{}
	uc.SetDefaults()
	uc.RQ = enthnsw.RQConfig{
		Enabled:       true,
		Bits:          1,
		Centering:     enthnsw.DefaultRQCentering || *benchHNSWDiskCentering,
		TrainingLimit: enthnsw.DefaultRQTrainingLimit,
		RescoreLimit:  enthnsw.DefaultBRQRescoreLimit,
	}
	result.Config = map[string]any{
		"maxConnections": uc.MaxConnections, "efConstruction": uc.EFConstruction, "distance": uc.Distance,
		"rqBits": uc.RQ.Bits, "rqCentering": uc.RQ.Centering, "rqTrainingLimit": uc.RQ.TrainingLimit,
		"rqRescoreLimit": uc.RQ.RescoreLimit,
	}
	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)
	const id = "hnswdisk"
	index, err := hnsw.New(hnsw.Config{
		RootPath:                     t.TempDir(),
		ID:                           id,
		Logger:                       logger,
		MakeCommitLoggerThunk:        hnsw.MakeNoopCommitLogger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		VectorForIDThunk:             vectorForID,
		TempVectorForIDWithViewThunk: tempVectorForID,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	defer index.Shutdown(ctx)
	index.PostStartup(ctx)

	// The production order, as in the bench of v1.4: uncompressed until the
	// training limit, the upgrade, and the rest against the codes. Every
	// vector is read from the file, a new slice each, since the index may
	// keep what it is given.
	insert := func(from, to int) {
		var next atomic.Int64
		next.Store(int64(from))
		workers := runtime.GOMAXPROCS(0)
		errs := make([]error, workers)
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				for {
					i := int(next.Add(1)) - 1
					if i >= to {
						return
					}
					vec, err := vectors.read(uint64(i), nil)
					if err == nil {
						err = index.Add(ctx, uint64(i), vec)
					}
					if err != nil {
						errs[worker] = err
						return
					}
					if done := i + 1 - from; done%1_000_000 == 0 {
						t.Logf("%d of %d vectors in, %s", i+1, to, time.Since(begin).Round(time.Second))
					}
				}
			}, logger)
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(t, err)
		}
	}
	begin = time.Now()
	warm := min(uc.RQ.TrainingLimit, n)
	insert(0, warm)
	uncompressed := time.Since(begin)
	stage := time.Now()
	upgraded := make(chan struct{})
	require.NoError(t, index.Upgrade(func() { close(upgraded) }))
	<-upgraded
	require.True(t, index.Compressed(), "the index must be compressed after the upgrade")
	upgrade := time.Since(stage)
	stage = time.Now()
	insert(warm, n)
	result.BuildSeconds = time.Since(begin).Seconds()
	result.BuildStages = map[string]float64{
		"insertUncompressed": uncompressed.Seconds(), "upgrade": upgrade.Seconds(),
		"insertCompressed": time.Since(stage).Seconds(),
	}
	bucket := store.Bucket(helpers.CompressedBucketNameForID(id))
	require.NotNil(t, bucket)
	require.NoError(t, bucket.FlushAndSwitch())
	runtime.GC()
	result.AfterBuild = takeMemSnapshot(t)
	// One header word and a bit per padded dimension, as the bench of v1.4
	// counts them.
	result.CodeBytes = int64(n) * int64(8*(1+(max(data.dims, 256)+63)/64))
	t.Logf("built: %d vectors in %s %v | heap in use %.1f MB, rss %.1f MB, footprint %.1f MB | codes %.1f MB, "+
		"vectors on disk %.2f GB", n, time.Duration(result.BuildSeconds*float64(time.Second)).Round(time.Second),
		result.BuildStages, result.AfterBuild.HeapInuseMB, result.AfterBuild.RSSMB, result.AfterBuild.FootprintMB,
		float64(result.CodeBytes)/(1<<20), float64(result.VectorBytes)/1e9)
	write()

	setEF := func(ef int) {
		uc.EF = ef
		updated := make(chan struct{})
		require.NoError(t, index.UpdateUserConfig(uc, func() { close(updated) }))
		<-updated
	}
	found := func(q int, ids []uint64) int {
		hits := 0
		for _, want := range truth[q] {
			if slices.Contains(ids, want) {
				hits++
			}
		}
		return hits
	}

	// The operating points, untimed, with 8 queries at a time.
	recallAt := func(ef int) float64 {
		setEF(ef)
		searchCtx := concurrency.CtxWithBudget(ctx, 1)
		var hits atomic.Int64
		err := parallelChunks(ctx, logger, len(queries), 8, func(_, start, end int) error {
			for q := start; q < end; q++ {
				ids, _, err := index.SearchByVector(searchCtx, queries[q], k, nil)
				if err != nil {
					return err
				}
				hits.Add(int64(found(q, ids)))
			}
			return nil
		})
		require.NoError(t, err)
		return float64(hits.Load()) / float64(len(queries)*k)
	}
	if *benchHNSWDiskEFs != "" {
		result.Recalls = make(map[int]float64)
		for _, ef := range parseInts(t, *benchHNSWDiskEFs) {
			result.Recalls[ef] = recallAt(ef)
			t.Logf("hnsw, rescore %d, ef %d: recall %.4f", uc.RQ.RescoreLimit, ef, result.Recalls[ef])
		}
		write()
		return
	}
	result.Points, result.MaxRecall = diskPointsOver(t, "hnsw", uc.RQ.RescoreLimit, recallAt)
	for _, point := range result.Points {
		if !point.Reached {
			t.Logf("hnsw: recall %.2f is not reached with rescore %d, the best is %.4f", point.Target,
				uc.RQ.RescoreLimit, result.MaxRecall)
			continue
		}
		t.Logf("hnsw at recall %.2f: ef %d, recall %.4f", point.Target, point.EF, point.Recall)
	}
	write()

	if *benchHNSWDiskWait != "" {
		out, err := exec.CommandContext(ctx, "sh", "-c", *benchHNSWDiskWait).CombinedOutput()
		t.Logf("the wait before the timed rounds: %s", out)
		require.NoErrorf(t, err, "the wait before the timed rounds: %s", out)
	}

	// The budget of a query: fixed, or what the limiter of a node grants it,
	// as in timeDisk.
	var limiter *queryadmission.Limiter
	if *benchDiskAdmission {
		limiter = queryadmission.New(prometheus.NewRegistry(), queryadmission.Config{})
	}
	budgeted := func() (context.Context, func(), error) {
		if limiter == nil {
			return concurrency.CtxWithBudget(ctx, *benchDiskBudget), func() {}, nil
		}
		return limiter.Admit(ctx, concurrency.TimesGOMAXPROCS(2))
	}
	// run searches the queries of which one after the other, and tells the
	// latency of each, the vectors each read, the hits and the time of all.
	run := func(which []int) ([]time.Duration, []diskSearchStats, int, time.Duration) {
		latencies := make([]time.Duration, len(which))
		stats := make([]diskSearchStats, len(which))
		hits := 0
		start := time.Now()
		for i, q := range which {
			before := vectors.reads.Load()
			queryBegin := time.Now()
			searchCtx, release, err := budgeted()
			require.NoError(t, err)
			stats[i].Budget = concurrency.BudgetFromCtx(searchCtx, 0)
			ids, _, err := index.SearchByVector(searchCtx, queries[q], k, nil)
			release()
			latencies[i] = time.Since(queryBegin)
			require.NoError(t, err)
			stats[i].Rescored = int(vectors.reads.Load() - before)
			hits += found(q, ids)
		}
		return latencies, stats, hits, time.Since(start)
	}
	all := make([]int, len(queries))
	for i := range all {
		all[i] = i
	}
	meanReads := func(stats []diskSearchStats) float64 {
		total := 0
		for _, s := range stats {
			total += s.Rescored
		}
		return float64(total) / float64(len(stats))
	}

	for round := 1; round <= *benchHNSWDiskRounds; round++ {
		timedRound := hnswDiskRound{Round: round, Start: time.Now().Format("2006-01-02 15:04:05")}
		for _, point := range result.Points {
			if !point.Reached {
				continue
			}
			setEF(point.EF)
			timed := diskTimedPoint{Target: point.Target, EF: point.EF, Threads: 1}
			// Warm: after an untimed pass.
			_, _, hits, _ := run(all)
			timed.Recall = float64(hits) / float64(len(queries)*k)
			// HNSW does not answer every query the same way twice: its
			// rescore runs on as many workers as the budget of the query,
			// and a tie at the k-th place goes to whichever comes first.
			// The points were found with a budget of 1 and 8 queries at a
			// time; here every query takes what the limiter grants it.
			if timed.Recall != point.Recall {
				t.Logf("round %d, hnsw at recall %.2f (ef %d): recall %.4f in the untimed pass, %.4f when the "+
					"point was found", round, point.Target, point.EF, timed.Recall, point.Recall)
			}
			require.InDelta(t, point.Recall, timed.Recall, 1e-3, "the recall the point was found with")
			var reads float64
			for pass := 0; pass < *benchDiskPasses; pass++ {
				latencies, stats, _, wall := run(all)
				timed.Warm = append(timed.Warm, diskLatenciesOf(latencies, stats, wall))
				reads = meanReads(stats)
			}
			warmPass := timed.Warm[len(timed.Warm)/2]
			t.Logf("round %d, hnsw at recall %.2f (ef %d), 1 thread, warm: p50 %.3f ms, p95 %.3f, p99 %.3f | %.0f QPS "+
				"| %.1f vectors read a query", round, point.Target, point.EF, warmPass.P50, warmPass.P95, warmPass.P99,
				warmPass.QPS, reads)
			timedRound.Reads = append(timedRound.Reads, reads)

			if *benchDiskDrop != "" {
				const batch = 10
				var latencies []time.Duration
				var stats []diskSearchStats
				var halves [2]struct {
					latencies []time.Duration
					stats     []diskSearchStats
					wall      time.Duration
				}
				var wall, reopen time.Duration
				coldHits := 0
				for from := 0; from < *benchDiskCold; from += batch {
					which := all[from:min(from+batch, *benchDiskCold)]
					require.NoError(t, vectors.close())
					out, err := exec.CommandContext(ctx, "sh", "-c", *benchDiskDrop).CombinedOutput()
					require.NoErrorf(t, err, "drop the cache: %s", out)
					reopenBegin := time.Now()
					require.NoError(t, vectors.open())
					reopen += time.Since(reopenBegin)
					batchLatencies, batchStats, batchHits, batchWall := run(which)
					half := &halves[timed.ColdBatches%2]
					half.latencies = append(half.latencies, batchLatencies...)
					half.stats = append(half.stats, batchStats...)
					half.wall += batchWall
					latencies, stats = append(latencies, batchLatencies...), append(stats, batchStats...)
					wall += batchWall
					coldHits += batchHits
					timed.ColdBatches++
				}
				cold := diskLatenciesOf(latencies, stats, wall)
				timed.Cold, timed.ColdBatch = &cold, batch
				for _, half := range halves {
					timed.ColdHalves = append(timed.ColdHalves, diskLatenciesOf(half.latencies, half.stats, half.wall))
				}
				timed.ColdRecall = float64(coldHits) / float64(len(latencies)*k)
				timed.ReopenMs = ms(reopen) / float64(timed.ColdBatches)
				t.Logf("round %d, hnsw at recall %.2f (ef %d), 1 thread, cold: p50 %.3f ms, p95 %.3f, p99 %.3f | "+
					"%.0f QPS | %d batches of %d, halves at p50 %.3f and %.3f ms | %.1f vectors read a query", round,
					point.Target, point.EF, cold.P50, cold.P95, cold.P99, cold.QPS, timed.ColdBatches, batch,
					timed.ColdHalves[0].P50, timed.ColdHalves[1].P50, meanReads(stats))
			}
			timedRound.Points = append(timedRound.Points, timed)
			write()
		}
		timedRound.End = time.Now().Format("2006-01-02 15:04:05")
		result.Rounds = append(result.Rounds, timedRound)
		write()
	}
}
