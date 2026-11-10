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

// In-process end-to-end benchmark of the posting-edge index against HNSW:
// build time, memory by component, and recall vs QPS over an ef sweep.
//
// One invocation measures one arm, so that memory readings are not polluted
// by the other index:
//
//	go test -tags postingedgebench -run TestBenchmark -timeout 0 -v \
//	    ./adapters/repos/db/vector/postingedge/ \
//	    -bench.dataset ~/datasets/dbpedia-openai-1000k-angular.bin \
//	    -bench.arm postingedge -bench.out postingedge.json
//
// The dataset is a directory with meta.txt ("train <rows> <dims>", "test
// <rows> <dims>", "neighbors <rows> <k>" lines) and the raw little-endian
// train.f32, test.f32 and neighbors.i32 next to it. Both arms hold the
// full-precision vectors in the same in-memory array and read them through
// the same thunk, use the same quantization (centered 1-bit RQ) and the same
// rescore limit, and run with a per-query concurrency budget of 1, so a
// thread is a thread for both.

package postingedge

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

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
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var (
	benchDatasetDir = flag.String("bench.dataset", "", "dataset directory (meta.txt, train.f32, test.f32, neighbors.i32)")
	benchArm        = flag.String("bench.arm", "postingedge", "index to measure: postingedge or hnsw")
	benchOut        = flag.String("bench.out", "", "path of the JSON result file")
	benchLimit      = flag.Int("bench.limit", 0, "use only the first n train vectors (0 = all); ground truth is then brute-forced")
	benchQueries    = flag.Int("bench.queries", 2000, "number of test queries")
	benchK          = flag.Int("bench.k", 10, "top k")
	benchEFs        = flag.String("bench.efs", "10,20,40,80,160,320,640", "ef sweep")
	benchThreads    = flag.String("bench.threads", "1,8", "query threads sweep")
	benchRuns       = flag.Int("bench.runs", 3, "measured passes per operating point, after one warm-up pass")
	benchRescores   = flag.String("bench.rescores", strconv.Itoa(enthnsw.DefaultBRQRescoreLimit), "rescore limit sweep, for both arms")
	benchTraining   = flag.Int("bench.training", enthnsw.DefaultRQTrainingLimit, "quantizer training limit of both arms")

	benchBatches = flag.String("bench.batches", "4", "postingedge: load batch sweep")
	benchEntries = flag.Int("bench.entries", DefaultEntryPostings, "postingedge: entry postings")
	benchProbe   = flag.Int("bench.probe", DefaultBuildProbe, "postingedge: build probe")
	benchPosting = flag.Int("bench.posting", 0, "postingedge: target posting size (0 = default budget)")
	benchSample  = flag.Int("bench.edgesample", 1000, "postingedge: vectors sampled to measure edge recall")

	benchMaxConn = flag.Int("bench.maxconn", enthnsw.DefaultMaxConnections, "hnsw: max connections")
	benchEFC     = flag.Int("bench.efc", enthnsw.DefaultEFConstruction, "hnsw: ef construction")

	benchSweeps sweepFlag
)

func init() {
	flag.Var(&benchSweeps, "bench.sweep", "one set of operating points, e.g. \"ef=10,20 threads=1,8 rescore=512 batch=4\"; "+
		"repeatable, measured in order on the same index; keys left out take -bench.efs, -bench.threads, "+
		"-bench.rescores and -bench.batches")
}

// sweepFlag collects the -bench.sweep values.
type sweepFlag []string

func (s *sweepFlag) String() string { return strings.Join(*s, "; ") }

func (s *sweepFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// sweep is one set of operating points: the cross product of its lists.
type sweep struct {
	efs, threads, rescores, batches []int
}

// benchSweepPlan resolves the -bench.sweep flags, or a single sweep of the
// defaults if there are none.
func benchSweepPlan(t *testing.T) []sweep {
	specs := []string(benchSweeps)
	if len(specs) == 0 {
		specs = []string{""}
	}
	plan := make([]sweep, 0, len(specs))
	for _, spec := range specs {
		sw := sweep{
			efs:      parseInts(t, *benchEFs),
			threads:  parseInts(t, *benchThreads),
			rescores: parseInts(t, *benchRescores),
			batches:  parseInts(t, *benchBatches),
		}
		for _, field := range strings.Fields(spec) {
			key, list, found := strings.Cut(field, "=")
			require.Truef(t, found, "sweep field %q is not key=list", field)
			switch key {
			case "ef":
				sw.efs = parseInts(t, list)
			case "threads":
				sw.threads = parseInts(t, list)
			case "rescore":
				sw.rescores = parseInts(t, list)
			case "batch":
				sw.batches = parseInts(t, list)
			default:
				t.Fatalf("unknown sweep key %q", key)
			}
		}
		plan = append(plan, sw)
	}
	return plan
}

type benchData struct {
	name    string
	dims    int
	train   [][]float32
	queries [][]float32
	truth   [][]uint64
}

type memSnapshot struct {
	HeapAllocMB float64 `json:"heapAllocMB"`
	HeapInuseMB float64 `json:"heapInuseMB"`
	RSSMB       float64 `json:"rssMB"`
}

type statsSummary struct {
	RoundsMean    float64 `json:"roundsMean"`
	RoundsP50     float64 `json:"roundsP50"`
	RoundsP95     float64 `json:"roundsP95"`
	RoundsMax     float64 `json:"roundsMax"`
	LoadsMean     float64 `json:"loadsMean"`
	LoadsP95      float64 `json:"loadsP95"`
	ScannedMean   float64 `json:"scannedMean"`
	EndpointsMean float64 `json:"endpointsMean"`
	RescoredMean  float64 `json:"rescoredMean"`
	CentroidUs    float64 `json:"centroidUs"`
	ScanUs        float64 `json:"scanUs"`
	ExpandUs      float64 `json:"expandUs"`
	RescoreUs     float64 `json:"rescoreUs"`
}

type benchPoint struct {
	EF           int           `json:"ef"`
	Threads      int           `json:"threads"`
	RescoreLimit int           `json:"rescoreLimit"`
	LoadBatch    int           `json:"loadBatch,omitempty"`
	Recall       float64       `json:"recall"`
	QPS          float64       `json:"qps"`
	QPSRuns      []float64     `json:"qpsRuns"`
	MeanMs       float64       `json:"meanMs"`
	P50Ms        float64       `json:"p50Ms"`
	P95Ms        float64       `json:"p95Ms"`
	P99Ms        float64       `json:"p99Ms"`
	Stats        *statsSummary `json:"stats,omitempty"`
}

type benchResult struct {
	Arm        string             `json:"arm"`
	Dataset    string             `json:"dataset"`
	Vectors    int                `json:"vectors"`
	Dims       int                `json:"dims"`
	Queries    int                `json:"queries"`
	K          int                `json:"k"`
	GoVersion  string             `json:"goVersion"`
	GOARCH     string             `json:"goarch"`
	GOMAXPROCS int                `json:"gomaxprocs"`
	Config     map[string]any     `json:"config"`
	BuildSec   float64            `json:"buildSec"`
	BuildStage map[string]float64 `json:"buildStageSec,omitempty"`

	Baseline    memSnapshot        `json:"memBaseline"`
	AfterBuild  memSnapshot        `json:"memAfterBuild"`
	IndexHeapMB float64            `json:"indexHeapMB"`
	PeakRSSMB   float64            `json:"peakRSSMB"`
	Components  map[string]float64 `json:"componentsMB"`
	Diagnostics map[string]float64 `json:"diagnostics,omitempty"`

	Points []benchPoint `json:"points"`
}

func TestBenchmark(t *testing.T) {
	if *benchDatasetDir == "" {
		t.Skip("no -bench.dataset given")
	}
	ctx := context.Background()
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, *benchK)

	result := &benchResult{
		Arm:        *benchArm,
		Dataset:    data.name,
		Vectors:    len(data.train),
		Dims:       data.dims,
		Queries:    len(data.queries),
		K:          *benchK,
		GoVersion:  runtime.Version(),
		GOARCH:     runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		Baseline:   takeMemSnapshot(t),
	}
	t.Logf("dataset %s: %d x %d, %d queries, baseline heap %.0f MB",
		data.name, len(data.train), data.dims, len(data.queries), result.Baseline.HeapAllocMB)

	switch *benchArm {
	case "postingedge":
		benchPostingEdge(t, ctx, data, result)
	case "hnsw":
		benchHNSW(t, ctx, data, result)
	default:
		t.Fatalf("unknown -bench.arm %q", *benchArm)
	}
}

// benchThunks serves the full-precision vectors of the dataset, the same way
// for both arms: ids are positions in the train set.
func benchThunks(data *benchData) (common.VectorForID[float32], common.TempVectorForIDWithView[float32]) {
	vectorForID := func(_ context.Context, id uint64) ([]float32, error) {
		if id >= uint64(len(data.train)) {
			return nil, storobj.NewErrNotFoundf(id, "out of range")
		}
		return data.train[id], nil
	}
	tempVectorForID := func(_ context.Context, id uint64, container *common.VectorSlice,
		_ common.BucketView,
	) ([]float32, error) {
		if id >= uint64(len(data.train)) {
			return nil, storobj.NewErrNotFoundf(id, "out of range")
		}
		container.Slice = append(container.Slice[:0], data.train[id]...)
		return container.Slice, nil
	}
	return vectorForID, tempVectorForID
}

func benchPostingEdge(t *testing.T, ctx context.Context, data *benchData, result *benchResult) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	_, tempVectorForID := benchThunks(data)

	uc := NewDefaultUserConfig()
	uc.TargetPostingSize = *benchPosting
	uc.BuildProbe = *benchProbe
	uc.TrainingLimit = *benchTraining
	uc.EntryPostings = *benchEntries
	result.Config = map[string]any{
		"targetPostingSize": uc.TargetPostingSize, "edges": uc.Edges, "buildProbe": uc.BuildProbe,
		"rescueLimit": uc.RescueLimit, "trainingLimit": uc.TrainingLimit,
		"entryPostings": uc.EntryPostings,
		"quantization":  "rq bits=1 centering=true", "distance": uc.Distance,
	}

	index, err := New(Config{
		ID:                           "bench",
		RootPath:                     t.TempDir(),
		Logger:                       logger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		TempVectorForIDWithViewThunk: tempVectorForID,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
		Seed:                         1,
	}, uc)
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	begin := time.Now()
	const batch = 10_000
	ids := make([]uint64, batch)
	for start := 0; start < len(data.train); start += batch {
		end := min(start+batch, len(data.train))
		for i := start; i < end; i++ {
			ids[i-start] = uint64(i)
		}
		require.NoError(t, index.AddBatch(ctx, ids[:end-start], data.train[start:end]))
	}
	addTime := time.Since(begin)
	require.NoError(t, index.Build(ctx))
	result.BuildSec = time.Since(begin).Seconds()
	result.PeakRSSMB = peakRSSMB()

	build, _ := index.BuildStats()
	result.BuildStage = map[string]float64{
		"add":           addTime.Seconds(),
		"partition":     build.PartitionTime.Seconds(),
		"assign":        build.AssignTime.Seconds(),
		"encode":        build.EncodeTime.Seconds(),
		"edges":         build.EdgeTime.Seconds(),
		"centroidGraph": build.CentroidGraphTime.Seconds(),
	}
	t.Logf("build: %.1fs %v", result.BuildSec, result.BuildStage)

	result.AfterBuild = takeMemSnapshot(t)
	result.IndexHeapMB = result.AfterBuild.HeapAllocMB - result.Baseline.HeapAllocMB
	memory, _ := index.MemoryStats()
	analytic := float64(memory.Total()) / (1 << 20)
	result.Components = map[string]float64{
		"postings":        float64(memory.PostingBytes) / (1 << 20),
		"codes":           float64(memory.CodeBytes) / (1 << 20),
		"edges":           float64(memory.EdgeBytes) / (1 << 20),
		"centroidVectors": float64(memory.CentroidVectorBytes) / (1 << 20),
		// what the heap holds beyond the arrays the index accounts for:
		// the graph over the centroids, the presence bitmap, pools
		"otherMeasured": result.IndexHeapMB - analytic,
		"f32":           float64(len(data.train)) * float64(data.dims) * 4 / (1 << 20),
	}
	t.Logf("memory: index heap %.1f MB, analytic %.1f MB, components %v, rss %.0f MB, peak rss %.0f MB",
		result.IndexHeapMB, analytic, result.Components, result.AfterBuild.RSSMB, result.PeakRSSMB)

	result.Diagnostics = postingEdgeDiagnostics(t, index, data, build, *benchSample)
	t.Logf("diagnostics: %v", result.Diagnostics)
	writeBenchResult(t, result)

	search := func(ctx context.Context, query []float32) ([]uint64, error) {
		ids, _, err := index.SearchByVector(ctx, query, *benchK, nil)
		return ids, err
	}
	for _, sw := range benchSweepPlan(t) {
		for _, rescore := range sw.rescores {
			for _, loadBatch := range sw.batches {
				for _, threads := range sw.threads {
					for _, ef := range sw.efs {
						uc.EF, uc.LoadBatch, uc.RescoreLimit = ef, loadBatch, rescore
						require.NoError(t, index.UpdateUserConfig(uc, func() {}))

						point := measurePoint(t, data, search, threads, *benchRuns)
						point.EF, point.LoadBatch, point.RescoreLimit = ef, loadBatch, rescore
						point.Stats = collectSearchStats(t, index, data)
						result.Points = append(result.Points, point)
						s := point.Stats
						t.Logf("ef=%d rescore=%d batch=%d threads=%d recall=%.4f qps=%.0f p50=%.3fms p95=%.3fms | rounds %.1f (p95 %.0f) loads %.1f scanned %.0f endpoints %.0f | us: centroid %.0f scan %.0f expand %.0f rescore %.0f",
							ef, rescore, loadBatch, threads, point.Recall, point.QPS, point.P50Ms, point.P95Ms,
							s.RoundsMean, s.RoundsP95, s.LoadsMean, s.ScannedMean, s.EndpointsMean,
							s.CentroidUs, s.ScanUs, s.ExpandUs, s.RescoreUs)
						writeBenchResult(t, result)
					}
				}
			}
		}
	}
}

func benchHNSW(t *testing.T, ctx context.Context, data *benchData, result *benchResult) {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	vectorForID, tempVectorForID := benchThunks(data)
	store := testinghelpers.NewDummyStore(t)
	defer store.Shutdown(ctx)

	uc := enthnsw.UserConfig{}
	uc.SetDefaults()
	uc.MaxConnections = *benchMaxConn
	uc.EFConstruction = *benchEFC
	uc.EF = 64
	uc.RQ = enthnsw.RQConfig{
		Enabled:       true,
		Bits:          1,
		Centering:     true,
		TrainingLimit: *benchTraining,
		RescoreLimit:  parseInts(t, *benchRescores)[0],
	}
	result.Config = map[string]any{
		"maxConnections": uc.MaxConnections, "efConstruction": uc.EFConstruction,
		"trainingLimit": uc.RQ.TrainingLimit,
		"quantization":  "rq bits=1 centering=true", "distance": uc.Distance,
	}

	const id = "bench"
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

	// The production order: vectors are indexed uncompressed until the
	// training limit is reached, the index is upgraded, and everything
	// after that is indexed against the codes.
	begin := time.Now()
	insert := func(from, to int) {
		var next atomic.Int64
		next.Store(int64(from))
		var wg sync.WaitGroup
		errs := make(chan error, runtime.GOMAXPROCS(0))
		for range runtime.GOMAXPROCS(0) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1)) - 1
					if i >= to {
						return
					}
					if err := index.Add(ctx, uint64(i), data.train[i]); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
	}
	warm := min(uc.RQ.TrainingLimit, len(data.train))
	insert(0, warm)
	uncompressedTime := time.Since(begin)

	stage := time.Now()
	upgraded := make(chan struct{})
	require.NoError(t, index.Upgrade(func() { close(upgraded) }))
	<-upgraded
	require.True(t, index.Compressed(), "the index must be compressed after the upgrade")
	upgradeTime := time.Since(stage)

	stage = time.Now()
	insert(warm, len(data.train))
	result.BuildSec = time.Since(begin).Seconds()
	result.BuildStage = map[string]float64{
		"insertUncompressed": uncompressedTime.Seconds(),
		"upgrade":            upgradeTime.Seconds(),
		"insertCompressed":   time.Since(stage).Seconds(),
	}
	result.PeakRSSMB = peakRSSMB()
	t.Logf("build: %.1fs %v", result.BuildSec, result.BuildStage)

	// The dummy store never flushes on its own: without this the codes
	// would be counted twice, once in the cache and once in the memtable.
	bucket := store.Bucket(helpers.CompressedBucketNameForID(id))
	require.NotNil(t, bucket)
	require.NoError(t, bucket.FlushAndSwitch())

	result.AfterBuild = takeMemSnapshot(t)
	result.IndexHeapMB = result.AfterBuild.HeapAllocMB - result.Baseline.HeapAllocMB
	// The codes are the same centered 1-bit codes: one header word and one
	// bit per (padded) dimension.
	codeBytes := 8 * (1 + (max(data.dims, 256)+63)/64)
	codes := float64(len(data.train)) * float64(codeBytes) / (1 << 20)
	result.Components = map[string]float64{
		"codes": codes,
		// everything else the index holds: vertices, connections, locks,
		// the per-code slice headers of the cache
		"graphMeasured": result.IndexHeapMB - codes,
		"f32":           float64(len(data.train)) * float64(data.dims) * 4 / (1 << 20),
	}
	t.Logf("memory: index heap %.1f MB, components %v, rss %.0f MB, peak rss %.0f MB",
		result.IndexHeapMB, result.Components, result.AfterBuild.RSSMB, result.PeakRSSMB)
	writeBenchResult(t, result)

	search := func(ctx context.Context, query []float32) ([]uint64, error) {
		ids, _, err := index.SearchByVector(ctx, query, *benchK, nil)
		return ids, err
	}
	for _, sw := range benchSweepPlan(t) {
		for _, rescore := range sw.rescores {
			for _, threads := range sw.threads {
				for _, ef := range sw.efs {
					uc.EF, uc.RQ.RescoreLimit = ef, rescore
					updated := make(chan struct{})
					require.NoError(t, index.UpdateUserConfig(uc, func() { close(updated) }))
					<-updated

					point := measurePoint(t, data, search, threads, *benchRuns)
					point.EF, point.RescoreLimit = ef, rescore
					result.Points = append(result.Points, point)
					t.Logf("ef=%d rescore=%d threads=%d recall=%.4f qps=%.0f p50=%.3fms p95=%.3fms",
						ef, rescore, threads, point.Recall, point.QPS, point.P50Ms, point.P95Ms)
					writeBenchResult(t, result)
				}
			}
		}
	}
}

// measurePoint runs the query set once to warm up and then runs times more,
// and reports the median over the measured passes.
func measurePoint(t *testing.T, data *benchData, search func(context.Context, []float32) ([]uint64, error),
	threads, runs int,
) benchPoint {
	// A budget of 1 keeps each query on the thread that issued it: neither
	// index fans its rescoring out to other cores.
	ctx := concurrency.CtxWithBudget(context.Background(), 1)
	results := make([][]uint64, len(data.queries))
	latencies := make([]time.Duration, len(data.queries))

	pass := func() time.Duration {
		var next atomic.Int64
		var failed atomic.Pointer[error]
		var wg sync.WaitGroup
		runtime.GC()
		begin := time.Now()
		for range threads {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1)) - 1
					if i >= len(data.queries) {
						return
					}
					start := time.Now()
					ids, err := search(ctx, data.queries[i])
					latencies[i] = time.Since(start)
					if err != nil {
						failed.Store(&err)
						return
					}
					results[i] = ids
				}
			}()
		}
		wg.Wait()
		wall := time.Since(begin)
		if err := failed.Load(); err != nil {
			require.NoError(t, *err)
		}
		return wall
	}

	pass()
	point := benchPoint{Threads: threads}
	var means, p50s, p95s, p99s []float64
	for range runs {
		wall := pass()
		point.QPSRuns = append(point.QPSRuns, float64(len(data.queries))/wall.Seconds())

		sorted := slices.Clone(latencies)
		slices.Sort(sorted)
		var sum time.Duration
		for _, l := range sorted {
			sum += l
		}
		means = append(means, ms(sum)/float64(len(sorted)))
		p50s = append(p50s, ms(percentileOf(sorted, 0.50)))
		p95s = append(p95s, ms(percentileOf(sorted, 0.95)))
		p99s = append(p99s, ms(percentileOf(sorted, 0.99)))
	}
	point.QPS = median(point.QPSRuns)
	point.MeanMs, point.P50Ms, point.P95Ms, point.P99Ms = median(means), median(p50s), median(p95s), median(p99s)

	hits, total := 0, 0
	for i, ids := range results {
		for _, want := range data.truth[i] {
			if slices.Contains(ids, want) {
				hits++
			}
		}
		total += len(data.truth[i])
	}
	point.Recall = float64(hits) / float64(total)
	return point
}

// collectSearchStats runs the query set once more, single-threaded, with
// the per-search stats on. It is kept out of the timed passes because
// collecting the stats reads the clock.
func collectSearchStats(t *testing.T, index *Index, data *benchData) *statsSummary {
	ctx := concurrency.CtxWithBudget(context.Background(), 1)
	n := float64(len(data.queries))
	rounds := make([]float64, 0, len(data.queries))
	loads := make([]float64, 0, len(data.queries))
	var sum statsSummary
	for _, query := range data.queries {
		_, _, stats, err := index.SearchByVectorWithStats(ctx, query, *benchK, nil)
		require.NoError(t, err)
		rounds = append(rounds, float64(stats.Rounds))
		loads = append(loads, float64(stats.PostingsLoaded))
		sum.RoundsMean += float64(stats.Rounds) / n
		sum.LoadsMean += float64(stats.PostingsLoaded) / n
		sum.ScannedMean += float64(stats.VectorsScanned) / n
		sum.EndpointsMean += float64(stats.EndpointsScored) / n
		sum.RescoredMean += float64(stats.Rescored) / n
		sum.CentroidUs += float64(stats.CentroidTime.Nanoseconds()) / 1e3 / n
		sum.ScanUs += float64(stats.ScanTime.Nanoseconds()) / 1e3 / n
		sum.ExpandUs += float64(stats.ExpandTime.Nanoseconds()) / 1e3 / n
		sum.RescoreUs += float64(stats.RescoreTime.Nanoseconds()) / 1e3 / n
	}
	slices.Sort(rounds)
	slices.Sort(loads)
	sum.RoundsP50 = percentileOf(rounds, 0.50)
	sum.RoundsP95 = percentileOf(rounds, 0.95)
	sum.RoundsMax = rounds[len(rounds)-1]
	sum.LoadsP95 = percentileOf(loads, 0.95)
	return &sum
}

// postingEdgeDiagnostics measures what the search cost depends on but no
// operating point shows: how the postings turned out, how good the edges
// are, and how often the centroid graph finds the true nearest centroid.
func postingEdgeDiagnostics(t *testing.T, index *Index, data *benchData, build BuildStats, sample int,
) map[string]float64 {
	st := index.state.Load()
	store, side := st.postings, st.side
	n := store.vectors()

	sizes := make([]float64, store.postings())
	for p := range sizes {
		sizes[p] = float64(store.start[p+1] - store.start[p])
	}
	slices.Sort(sizes)
	out := map[string]float64{
		"postings":            float64(store.postings()),
		"postingSizeMean":     float64(n) / float64(store.postings()),
		"postingSizeMin":      sizes[0],
		"postingSizeP50":      percentileOf(sizes, 0.50),
		"postingSizeP95":      percentileOf(sizes, 0.95),
		"postingSizeMax":      sizes[len(sizes)-1],
		"edgesPerVector":      float64(len(store.edges)) / float64(n),
		"candidatesPerVector": float64(build.CandidatesScored) / float64(n),
	}

	// Edge recall: of the true nearest neighbors of a vector that live in
	// another posting, how many is it linked to.
	slotOf := make([]uint32, n)
	for slot, docID := range store.docIDs {
		slotOf[docID] = uint32(slot)
	}
	provider := distancer.NewCosineDistanceProvider()
	rng := rand.New(rand.NewPCG(7, 7))
	sampled := make([]int, min(sample, n))
	for i := range sampled {
		sampled[i] = rng.IntN(n)
	}
	var captured, wanted, sameCell atomic.Int64
	parallelFor(len(sampled), func(i int) {
		doc := sampled[i]
		source := data.train[doc]
		var nearest topK
		nearest.reset(index.uc.Edges)
		for other, vec := range data.train {
			if other == doc {
				continue
			}
			dist, err := provider.SingleDist(source, vec)
			if err != nil {
				panic(err)
			}
			nearest.push(uint64(other), dist)
		}
		edges := store.edgesOf(slotOf[doc])
		for _, nb := range nearest.items {
			if side.postingOf[slotOf[nb.id]] == side.postingOf[slotOf[doc]] {
				sameCell.Add(1)
				continue
			}
			wanted.Add(1)
			if slices.Contains(edges, slotOf[nb.id]) {
				captured.Add(1)
			}
		}
	})
	out["edgeRecall"] = float64(captured.Load()) / float64(wanted.Load())
	out["neighborsInOwnPosting"] = float64(sameCell.Load()) / float64(sameCell.Load()+wanted.Load())

	// Entry accuracy: how often the centroid graph returns the exact
	// nearest centroid of a query.
	l2 := distancer.NewL2SquaredProvider()
	var exactEntry atomic.Int64
	parallelFor(len(data.queries), func(i int) {
		query := data.queries[i]
		best, bestDist := 0, float32(math.Inf(1))
		for c, centroid := range st.centroids.vectors {
			dist, err := l2.SingleDist(query, centroid)
			if err != nil {
				panic(err)
			}
			if dist < bestDist {
				best, bestDist = c, dist
			}
		}
		got, err := st.centroids.nearest(context.Background(), query, 1)
		if err != nil {
			panic(err)
		}
		if len(got) == 1 && got[0] == uint64(best) {
			exactEntry.Add(1)
		}
	})
	out["entryIsNearestCentroid"] = float64(exactEntry.Load()) / float64(len(data.queries))
	return out
}

func loadBenchData(t *testing.T, dir string, limit, queries, k int) *benchData {
	meta := map[string][2]int{}
	file, err := os.Open(filepath.Join(dir, "meta.txt"))
	require.NoError(t, err)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 3 {
			continue
		}
		rows, errRows := strconv.Atoi(fields[1])
		cols, errCols := strconv.Atoi(fields[2])
		if errRows == nil && errCols == nil {
			meta[fields[0]] = [2]int{rows, cols}
		}
	}
	require.NoError(t, scanner.Err())
	require.NoError(t, file.Close())

	train, test, neighbors := meta["train"], meta["test"], meta["neighbors"]
	require.NotZero(t, train[0], "meta.txt must describe the train set")
	require.NotZero(t, test[0], "meta.txt must describe the test set")
	dims := train[1]
	require.Equal(t, dims, test[1])

	data := &benchData{name: filepath.Base(dir), dims: dims}
	n := train[0]
	if limit > 0 && limit < n {
		n = limit
	}
	data.train = readVectors(t, filepath.Join(dir, "train.f32"), n, dims)
	q := min(queries, test[0])
	data.queries = readVectors(t, filepath.Join(dir, "test.f32"), q, dims)
	// The benchmark is a cosine benchmark. Unit vectors up front let the
	// brute-force passes below use the distance provider directly; both
	// indexes normalize what they are given anyway.
	parallelFor(n, func(i int) { distancer.NormalizeInPlace(data.train[i]) })
	parallelFor(q, func(i int) { distancer.NormalizeInPlace(data.queries[i]) })

	data.truth = make([][]uint64, q)
	if n == train[0] && neighbors[0] >= q && neighbors[1] >= k {
		raw := make([]int32, q*neighbors[1])
		readRaw(t, filepath.Join(dir, "neighbors.i32"), unsafe.Slice((*byte)(unsafe.Pointer(&raw[0])), len(raw)*4))
		for i := range data.truth {
			data.truth[i] = make([]uint64, k)
			for j := range data.truth[i] {
				data.truth[i][j] = uint64(raw[i*neighbors[1]+j])
			}
		}
		return data
	}

	// A truncated corpus invalidates the shipped ground truth.
	t.Logf("brute-forcing the ground truth over %d vectors", n)
	provider := distancer.NewCosineDistanceProvider()
	parallelFor(q, func(i int) {
		var nearest topK
		nearest.reset(k)
		for id, vec := range data.train {
			dist, err := provider.SingleDist(data.queries[i], vec)
			if err != nil {
				panic(err)
			}
			nearest.push(uint64(id), dist)
		}
		for _, e := range nearest.sorted() {
			data.truth[i] = append(data.truth[i], e.id)
		}
	})
	return data
}

func readVectors(t *testing.T, path string, rows, dims int) [][]float32 {
	flat := make([]float32, rows*dims)
	readRaw(t, path, unsafe.Slice((*byte)(unsafe.Pointer(&flat[0])), len(flat)*4))
	out := make([][]float32, rows)
	for i := range out {
		out[i] = flat[i*dims : (i+1)*dims : (i+1)*dims]
	}
	return out
}

func readRaw(t *testing.T, path string, into []byte) {
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	_, err = io.ReadFull(file, into)
	require.NoError(t, err)
}

func writeBenchResult(t *testing.T, result *benchResult) {
	if *benchOut == "" {
		return
	}
	raw, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
}

// takeMemSnapshot reads the live heap and the resident set after returning
// everything collectable to the OS.
func takeMemSnapshot(t *testing.T) memSnapshot {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	snapshot := memSnapshot{
		HeapAllocMB: float64(stats.HeapAlloc) / (1 << 20),
		HeapInuseMB: float64(stats.HeapInuse) / (1 << 20),
	}
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Logf("cannot read rss: %v", err)
		return snapshot
	}
	kb, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err == nil {
		snapshot.RSSMB = kb / 1024
	}
	return snapshot
}

// peakRSSMB is the high-water mark of the resident set so far.
func peakRSSMB() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return float64(usage.Maxrss) / (1 << 20) // bytes
	}
	return float64(usage.Maxrss) / 1024 // kilobytes
}

func parallelFor(n int, fn func(i int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				fn(i)
			}
		}()
	}
	wg.Wait()
}

func parseInts(t *testing.T, list string) []int {
	var out []int
	for _, field := range strings.Split(list, ",") {
		value, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoErrorf(t, err, "not a list of integers: %q", list)
		out = append(out, value)
	}
	return out
}

func percentileOf[T any](sorted []T, q float64) T {
	return sorted[max(0, min(len(sorted)-1, int(math.Ceil(q*float64(len(sorted))))-1))]
}

func median(values []float64) float64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted[len(sorted)/2]
}

func ms(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}
