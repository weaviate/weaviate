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
//	go test -tags postingedgebench -run 'TestBenchmark$' -timeout 0 -v \
//	    ./adapters/repos/db/vector/postingedge/ \
//	    -bench.dataset ~/datasets/dbpedia-openai-1000k-angular.bin \
//	    -bench.arm postingedge -bench.out postingedge.json
//
// The dataset is a directory in the layout testinghelpers.ReadRawDataset
// reads. Both arms hold the full-precision vectors in the same in-memory
// array and read them through the same thunk, use the same quantization
// (centered 1-bit RQ) and the same rescore limit, and run with a per-query
// concurrency budget of 1, so a thread is a thread for both.
//
// The operating points come from -bench.sweep, which can be given several
// times. With -bench.mode work the posting-edge arm skips the timed passes
// and only reports the work of a search at each point. The HNSW counterpart
// of that, which needs the internals of the index, is TestSearchWork of the
// hnsw package. TestBenchmarkReport turns the result files into tables and
// charts.

package postingedge

import (
	"context"
	"encoding/json"
	"flag"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
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
	benchReach   = flag.Int("bench.reach", 0, "postingedge: queries sampled to measure what a triangle bound on the edge lengths could prune (0 = skip)")

	benchMaxConn = flag.Int("bench.maxconn", enthnsw.DefaultMaxConnections, "hnsw: max connections")
	benchEFC     = flag.Int("bench.efc", enthnsw.DefaultEFConstruction, "hnsw: ef construction")

	benchMode   = flag.String("bench.mode", "full", "full: timed passes plus work stats; work: postingedge only, the work stats without the timed passes")
	benchSettle = flag.Int("bench.settle", 1000, "queries run before the after-queries memory reading")

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

// memSnapshot is one reading of the process memory. RSS is what ps reports.
// On macOS that includes the pages the Go runtime has already handed back as
// reusable, so the physical footprint (what the process is charged for) is
// read as well where the footprint tool exists.
type memSnapshot struct {
	HeapAllocMB float64 `json:"heapAllocMB"`
	HeapInuseMB float64 `json:"heapInuseMB"`
	RSSMB       float64 `json:"rssMB"`
	FootprintMB float64 `json:"footprintMB,omitempty"`
}

// statsSummary is the work of a search at one operating point, over the
// query set. The means are per query, the times in microseconds.
type statsSummary struct {
	RoundsMean    float64 `json:"roundsMean"`
	RoundsP50     float64 `json:"roundsP50"`
	RoundsP95     float64 `json:"roundsP95"`
	RoundsMax     float64 `json:"roundsMax"`
	LoadsMean     float64 `json:"loadsMean"`
	LoadsP50      float64 `json:"loadsP50"`
	LoadsP95      float64 `json:"loadsP95"`
	ScannedMean   float64 `json:"scannedMean"`
	AgainMean     float64 `json:"scannedAgainMean"`
	EdgesMean     float64 `json:"edgesFollowedMean"`
	InLoadedMean  float64 `json:"endpointsInLoadedMean"`
	RepeatedMean  float64 `json:"endpointsRepeatedMean"`
	EndpointsMean float64 `json:"endpointsMean"`
	RescoredMean  float64 `json:"rescoredMean"`
	CentroidUs    float64 `json:"centroidUs"`
	ScanUs        float64 `json:"scanUs"`
	ExpandUs      float64 `json:"expandUs"`
	SelectUs      float64 `json:"selectUs"`
	RescoreUs     float64 `json:"rescoreUs"`
	// TotalUs is the whole search as timed from outside, so that what the
	// phases do not cover can be told.
	TotalUs float64 `json:"totalUs"`
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

	Baseline   memSnapshot `json:"memBaseline"`
	AfterBuild memSnapshot `json:"memAfterBuild"`
	// AfterQueries is read after SettleQueries searches, without forcing a
	// collection first: the process as it stands while serving.
	AfterQueries  memSnapshot        `json:"memAfterQueries"`
	SettleQueries int                `json:"settleQueries"`
	IndexHeapMB   float64            `json:"indexHeapMB"`
	PeakRSSMB     float64            `json:"peakRSSMB"`
	Components    map[string]float64 `json:"componentsMB"`
	Diagnostics   map[string]float64 `json:"diagnostics,omitempty"`

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

	search := func(ctx context.Context, query []float32) ([]uint64, error) {
		ids, _, err := index.SearchByVector(ctx, query, *benchK, nil)
		return ids, err
	}
	plan := benchSweepPlan(t)
	uc.EF, uc.LoadBatch, uc.RescoreLimit = plan[0].efs[0], plan[0].batches[0], plan[0].rescores[0]
	require.NoError(t, index.UpdateUserConfig(uc, func() {}))
	settle(t, data, search, result)

	result.Diagnostics = postingEdgeDiagnostics(t, index, data, build, *benchSample)
	if *benchReach > 0 {
		for key, value := range edgeReachDiagnostics(t, index, data, *benchReach) {
			result.Diagnostics[key] = value
		}
	}
	t.Logf("diagnostics: %v", result.Diagnostics)
	writeBenchResult(t, result)

	for _, sw := range plan {
		for _, rescore := range sw.rescores {
			for _, loadBatch := range sw.batches {
				for _, threads := range sw.threads {
					for _, ef := range sw.efs {
						uc.EF, uc.LoadBatch, uc.RescoreLimit = ef, loadBatch, rescore
						require.NoError(t, index.UpdateUserConfig(uc, func() {}))

						// The work of a search does not depend on timing or
						// on the thread count, so the work mode skips the
						// timed passes and reports each point once.
						point := benchPoint{Threads: threads}
						if *benchMode != "work" {
							point = measurePoint(t, data, search, threads, *benchRuns)
						}
						point.EF, point.LoadBatch, point.RescoreLimit = ef, loadBatch, rescore
						var recall float64
						point.Stats, recall = collectSearchStats(t, index, data)
						if *benchMode == "work" {
							point.Recall = recall
						}
						result.Points = append(result.Points, point)
						s := point.Stats
						t.Logf("ef=%d rescore=%d batch=%d threads=%d recall=%.4f qps=%.0f p50=%.3fms p95=%.3fms | rounds %.1f (p95 %.0f) loads %.1f/%.0f/%.0f scanned %.0f (again %.0f) edges %.0f (in loaded %.0f, repeated %.0f) endpoints %.0f rescored %.0f | us: centroid %.0f scan %.0f expand %.0f select %.0f rescore %.0f total %.0f",
							ef, rescore, loadBatch, threads, point.Recall, point.QPS, point.P50Ms, point.P95Ms,
							s.RoundsMean, s.RoundsP95, s.LoadsMean, s.LoadsP50, s.LoadsP95, s.ScannedMean, s.AgainMean,
							s.EdgesMean, s.InLoadedMean, s.RepeatedMean, s.EndpointsMean, s.RescoredMean,
							s.CentroidUs, s.ScanUs, s.ExpandUs, s.SelectUs, s.RescoreUs, s.TotalUs)
						writeBenchResult(t, result)
					}
					if *benchMode == "work" {
						break
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

	search := func(ctx context.Context, query []float32) ([]uint64, error) {
		ids, _, err := index.SearchByVector(ctx, query, *benchK, nil)
		return ids, err
	}
	settle(t, data, search, result)
	writeBenchResult(t, result)

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
// the per-search stats on, and returns them with the recall of that pass. It
// is kept out of the timed passes because collecting the stats reads the
// clock and counts in passes of its own.
func collectSearchStats(t *testing.T, index *Index, data *benchData) (*statsSummary, float64) {
	ctx := concurrency.CtxWithBudget(context.Background(), 1)
	n := float64(len(data.queries))
	rounds := make([]float64, 0, len(data.queries))
	loads := make([]float64, 0, len(data.queries))
	var sum statsSummary
	hits, total := 0, 0
	for i, query := range data.queries {
		begin := time.Now()
		ids, _, stats, err := index.SearchByVectorWithStats(ctx, query, *benchK, nil)
		took := time.Since(begin)
		require.NoError(t, err)
		for _, want := range data.truth[i] {
			if slices.Contains(ids, want) {
				hits++
			}
		}
		total += len(data.truth[i])

		rounds = append(rounds, float64(stats.Rounds))
		loads = append(loads, float64(stats.PostingsLoaded))
		sum.RoundsMean += float64(stats.Rounds) / n
		sum.LoadsMean += float64(stats.PostingsLoaded) / n
		sum.ScannedMean += float64(stats.VectorsScanned) / n
		sum.AgainMean += float64(stats.ScannedAgain) / n
		sum.EdgesMean += float64(stats.EdgesFollowed) / n
		sum.InLoadedMean += float64(stats.EndpointsInLoaded) / n
		sum.RepeatedMean += float64(stats.EndpointsRepeated()) / n
		sum.EndpointsMean += float64(stats.EndpointsScored) / n
		sum.RescoredMean += float64(stats.Rescored) / n
		sum.CentroidUs += float64(stats.CentroidTime.Nanoseconds()) / 1e3 / n
		sum.ScanUs += float64(stats.ScanTime.Nanoseconds()) / 1e3 / n
		sum.ExpandUs += float64(stats.ExpandTime.Nanoseconds()) / 1e3 / n
		sum.SelectUs += float64(stats.SelectTime.Nanoseconds()) / 1e3 / n
		sum.RescoreUs += float64(stats.RescoreTime.Nanoseconds()) / 1e3 / n
		sum.TotalUs += float64(took.Nanoseconds()) / 1e3 / n
	}
	slices.Sort(rounds)
	slices.Sort(loads)
	sum.RoundsP50 = percentileOf(rounds, 0.50)
	sum.RoundsP95 = percentileOf(rounds, 0.95)
	sum.RoundsMax = rounds[len(rounds)-1]
	sum.LoadsP50 = percentileOf(loads, 0.50)
	sum.LoadsP95 = percentileOf(loads, 0.95)
	return &sum, float64(hits) / float64(total)
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

// edgeReachDiagnostics measures what pruning sources by a triangle bound on
// their edges could save, without the index implementing it.
//
// The bound: an endpoint e of a source s cannot be nearer to the query than
// angle(q, s) - angle(s, e). So if angle(q, s) minus the longest edge of s is
// still at least the angle of the stop threshold, no edge of s can matter and
// s can be skipped. (The angle is the tightest metric for cosine: the bound
// is attained when the three vectors lie on one great circle.)
//
// The diagnostic gives the bound its best case: the exact, unquantized
// length of the longest edge, and the lowest threshold a search can reach,
// the ef-th best estimate over the whole corpus. It reports the share of
// sources that could then be skipped, over the whole corpus and over the
// postings nearest to the query, and the distributions behind it.
//
// It also reports what subtracting the cosine distances directly would skip.
// That is not a bound, cosine distance does not obey the triangle
// inequality, but it is the reading of the rule that prunes the most.
//
// Last, it measures what a plain cut on the distance of the source would
// keep: expanding only the sources within a margin of the stop threshold. An
// endpoint is useful if its estimate is inside the threshold, so that its
// posting has to be loaded. For each margin the diagnostic reports how many
// sources, edges and distinct endpoints the cut keeps, and how many of the
// useful endpoints are still reached through at least one kept source. This
// is no bound either: it shows how much there is to gain and to lose.
func edgeReachDiagnostics(t *testing.T, index *Index, data *benchData, queries int) map[string]float64 {
	st := index.state.Load()
	store, side := st.postings, st.side
	n := store.vectors()
	provider := distancer.NewCosineDistanceProvider()
	angle := func(dist float32) float64 { return math.Acos(max(-1, min(1, 1-float64(dist)))) }
	toCosine := func(radians float64) float64 { return 1 - math.Cos(radians) }
	degrees := func(radians float64) float64 { return radians * 180 / math.Pi }

	// The exact length of every edge, and the longest per source.
	reach := make([]float64, n)
	edgeDist := make([]float64, len(store.edges))
	parallelFor(n, func(slot int) {
		source := data.train[store.docIDs[slot]]
		for i := store.edgeStart[slot]; i < store.edgeStart[slot+1]; i++ {
			dist, err := provider.SingleDist(source, data.train[store.docIDs[store.edges[i]]])
			if err != nil {
				panic(err)
			}
			edgeDist[i] = float64(dist)
			reach[slot] = max(reach[slot], angle(dist))
		}
	})
	var reaches []float64
	for slot := 0; slot < n; slot++ {
		if store.edgeStart[slot+1] > store.edgeStart[slot] {
			reaches = append(reaches, reach[slot])
		}
	}
	slices.Sort(reaches)
	slices.Sort(edgeDist)

	const nearPostings = 16
	efs := []int{*benchK, 2 * *benchK}
	margins := []float64{0, 0.02, 0.04, 0.06, 0.08, 0.10, 0.15}
	// marginCut counts, for the cut at one margin: the sources, edges and
	// distinct endpoints it keeps, and the useful endpoints among those.
	type marginCut struct {
		sources, edges, endpoints, useful float64
	}
	type perQuery struct {
		threshold      [2]float64
		corpus, near   [2]float64 // prunable share of the sources with edges
		naiveNear      [2]float64 // the same over the near postings, subtracting cosine distances
		nearSourceDist []float64
		// all counts what there is without a cut, cuts what each margin keeps
		all  [2]marginCut
		cuts [2][]marginCut
	}
	queries = min(queries, len(data.queries))
	out := make([]perQuery, queries)
	l2 := distancer.NewL2SquaredProvider()
	parallelFor(queries, func(qi int) {
		query := data.queries[qi]
		estimator := st.quantizer.NewDistancer(query)
		dists := make([]float32, n)
		best := make([]topK, len(efs))
		for i, ef := range efs {
			best[i].reset(ef)
		}
		for slot := 0; slot < n; slot++ {
			dist, err := estimator.Distance(side.code(uint32(slot)))
			if err != nil {
				panic(err)
			}
			dists[slot] = dist
			for i := range best {
				best[i].push(uint64(slot), dist)
			}
		}

		var nearest topK
		nearest.reset(nearPostings)
		for c, centroid := range st.centroids.vectors {
			dist, err := l2.SingleDist(query, centroid)
			if err != nil {
				panic(err)
			}
			nearest.push(uint64(c), dist)
		}
		isNear := make([]bool, store.postings())
		for _, e := range nearest.items {
			isNear[e.id] = true
		}

		result := &out[qi]
		for i := range efs {
			threshold := best[i].worst()
			limit := angle(threshold)
			result.threshold[i] = float64(threshold)
			var sources, prunable, nearSources, nearPrunable, naivePrunable float64
			for slot := 0; slot < n; slot++ {
				if store.edgeStart[slot+1] == store.edgeStart[slot] {
					continue
				}
				skip := angle(dists[slot])-reach[slot] >= limit
				sources++
				if skip {
					prunable++
				}
				if isNear[side.postingOf[slot]] {
					nearSources++
					if skip {
						nearPrunable++
					}
					if float64(dists[slot])-toCosine(reach[slot]) >= float64(threshold) {
						naivePrunable++
					}
					if i == 0 {
						result.nearSourceDist = append(result.nearSourceDist, float64(dists[slot]))
					}
				}
			}
			result.corpus[i] = prunable / sources
			result.near[i] = nearPrunable / max(nearSources, 1)
			result.naiveNear[i] = naivePrunable / max(nearSources, 1)

			// The cut on the distance of the source. nearestSource is,
			// per endpoint outside the near postings, the distance of
			// the nearest source that links to it.
			nearestSource := map[uint32]float32{}
			result.cuts[i] = make([]marginCut, len(margins))
			for slot := 0; slot < n; slot++ {
				edges := store.edgesOf(uint32(slot))
				if len(edges) == 0 || !isNear[side.postingOf[slot]] {
					continue
				}
				leaving := 0.0
				for _, endpoint := range edges {
					if isNear[side.postingOf[endpoint]] {
						continue
					}
					leaving++
					if known, ok := nearestSource[endpoint]; !ok || dists[slot] < known {
						nearestSource[endpoint] = dists[slot]
					}
				}
				result.all[i].sources++
				result.all[i].edges += leaving
				for j, margin := range margins {
					if float64(dists[slot]) <= float64(threshold)+margin {
						result.cuts[i][j].sources++
						result.cuts[i][j].edges += leaving
					}
				}
			}
			for endpoint, sourceDist := range nearestSource {
				useful := dists[endpoint] < threshold
				result.all[i].endpoints++
				if useful {
					result.all[i].useful++
				}
				for j, margin := range margins {
					if float64(sourceDist) <= float64(threshold)+margin {
						result.cuts[i][j].endpoints++
						if useful {
							result.cuts[i][j].useful++
						}
					}
				}
			}
		}
	})

	var thresholds [2][]float64
	var corpus, near, naiveNear [2]float64
	var nearSourceDist []float64
	for _, q := range out {
		for i := range efs {
			thresholds[i] = append(thresholds[i], q.threshold[i])
			corpus[i] += q.corpus[i] / float64(queries)
			near[i] += q.near[i] / float64(queries)
			naiveNear[i] += q.naiveNear[i] / float64(queries)
		}
		nearSourceDist = append(nearSourceDist, q.nearSourceDist...)
	}
	slices.Sort(nearSourceDist)
	for i := range thresholds {
		slices.Sort(thresholds[i])
	}

	medianReach := percentileOf(reaches, 0.50)
	medianThreshold := percentileOf(thresholds[0], 0.50)
	diagnostics := map[string]float64{}
	for i, suffix := range []string{"", "2k"} {
		var all marginCut
		cuts := make([]marginCut, len(margins))
		for _, q := range out {
			all.sources += q.all[i].sources
			all.edges += q.all[i].edges
			all.endpoints += q.all[i].endpoints
			all.useful += q.all[i].useful
			for j := range margins {
				cuts[j].sources += q.cuts[i][j].sources
				cuts[j].edges += q.cuts[i][j].edges
				cuts[j].endpoints += q.cuts[i][j].endpoints
				cuts[j].useful += q.cuts[i][j].useful
			}
		}
		diagnostics["cutUsefulEndpointsPerQuery"+suffix] = all.useful / float64(queries)
		diagnostics["cutEndpointsPerQuery"+suffix] = all.endpoints / float64(queries)
		for j, margin := range margins {
			key := "cutMargin" + strconv.FormatFloat(margin, 'f', 2, 64) + suffix
			diagnostics[key+"Sources"] = cuts[j].sources / all.sources
			diagnostics[key+"Edges"] = cuts[j].edges / all.edges
			diagnostics[key+"Endpoints"] = cuts[j].endpoints / all.endpoints
			diagnostics[key+"Useful"] = cuts[j].useful / max(all.useful, 1)
		}
	}
	for key, value := range map[string]float64{
		"reachQueries":          float64(queries),
		"reachDegreesP05":       degrees(percentileOf(reaches, 0.05)),
		"reachDegreesP50":       degrees(medianReach),
		"reachDegreesP95":       degrees(percentileOf(reaches, 0.95)),
		"reachCosineMin":        toCosine(reaches[0]),
		"reachCosineP01":        toCosine(percentileOf(reaches, 0.01)),
		"reachCosineP05":        toCosine(percentileOf(reaches, 0.05)),
		"reachCosineP25":        toCosine(percentileOf(reaches, 0.25)),
		"reachCosineP50":        toCosine(medianReach),
		"reachCosineP75":        toCosine(percentileOf(reaches, 0.75)),
		"reachCosineP95":        toCosine(percentileOf(reaches, 0.95)),
		"reachCosineP99":        toCosine(percentileOf(reaches, 0.99)),
		"reachCosineMax":        toCosine(reaches[len(reaches)-1]),
		"edgeCosineP05":         percentileOf(edgeDist, 0.05),
		"edgeCosineP50":         percentileOf(edgeDist, 0.50),
		"edgeCosineP95":         percentileOf(edgeDist, 0.95),
		"thresholdCosineP50":    medianThreshold,
		"thresholdCosineP95":    percentileOf(thresholds[0], 0.95),
		"threshold2kCosineP50":  percentileOf(thresholds[1], 0.50),
		"nearSourceCosineP05":   percentileOf(nearSourceDist, 0.05),
		"nearSourceCosineP50":   percentileOf(nearSourceDist, 0.50),
		"nearSourceCosineP95":   percentileOf(nearSourceDist, 0.95),
		"nearSourceCosineP99":   percentileOf(nearSourceDist, 0.99),
		"nearSourceCosineMax":   nearSourceDist[len(nearSourceDist)-1],
		"prunableAtCosine":      toCosine(angle(float32(medianThreshold)) + medianReach),
		"prunableCorpusShare":   corpus[0],
		"prunableNearShare":     near[0],
		"prunableCorpusShare2k": corpus[1],
		"prunableNearShare2k":   near[1],
		"naiveNearShare":        naiveNear[0],
		"naiveNearShare2k":      naiveNear[1],
	} {
		diagnostics[key] = value
	}
	return diagnostics
}

func loadBenchData(t *testing.T, dir string, limit, queries, k int) *benchData {
	raw, err := testinghelpers.ReadRawDataset(dir, limit, queries)
	require.NoError(t, err)
	data := &benchData{name: raw.Name, dims: raw.Dims, train: raw.Train, queries: raw.Queries}
	n, q := len(data.train), len(data.queries)

	// The benchmark is a cosine benchmark. Unit vectors up front let the
	// brute-force passes below use the distance provider directly; both
	// indexes normalize what they are given anyway.
	parallelFor(n, func(i int) { distancer.NormalizeInPlace(data.train[i]) })
	parallelFor(q, func(i int) { distancer.NormalizeInPlace(data.queries[i]) })

	data.truth = make([][]uint64, q)
	if raw.Neighbors != nil && len(raw.Neighbors[0]) >= k {
		for i := range data.truth {
			data.truth[i] = raw.Neighbors[i][:k]
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

func writeBenchResult(t *testing.T, result *benchResult) {
	if *benchOut == "" {
		return
	}
	raw, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(*benchOut, raw, 0o644))
}

// takeMemSnapshot reads the process memory after returning everything
// collectable to the OS: what the process needs to hold.
func takeMemSnapshot(t *testing.T) memSnapshot {
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	return readProcessMemory(t)
}

// readProcessMemory reads the process memory as it stands.
func readProcessMemory(t *testing.T) memSnapshot {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	snapshot := memSnapshot{
		HeapAllocMB: float64(stats.HeapAlloc) / (1 << 20),
		HeapInuseMB: float64(stats.HeapInuse) / (1 << 20),
	}

	pid := strconv.Itoa(os.Getpid())
	out, err := exec.Command("ps", "-o", "rss=", "-p", pid).Output()
	if err != nil {
		t.Logf("cannot read rss: %v", err)
	} else if kb, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64); err == nil {
		snapshot.RSSMB = kb / 1024
	}

	if runtime.GOOS == "darwin" {
		// "<name> [<pid>]: 64-bit    Footprint: 6329 MB (16384 bytes per page)"
		out, err := exec.Command("footprint", pid).Output()
		if err != nil {
			t.Logf("cannot read the physical footprint: %v", err)
			return snapshot
		}
		_, after, found := strings.Cut(string(out), "Footprint: ")
		fields := strings.Fields(after)
		if found && len(fields) >= 2 {
			value, err := strconv.ParseFloat(fields[0], 64)
			unit := map[string]float64{"KB": 1.0 / 1024, "MB": 1, "GB": 1024}[fields[1]]
			if err == nil && unit != 0 {
				snapshot.FootprintMB = value * unit
			}
		}
	}
	return snapshot
}

// settle runs the first queries of the set on one thread and then reads the
// process memory, to show what serving adds on top of the built index.
func settle(t *testing.T, data *benchData, search func(context.Context, []float32) ([]uint64, error),
	result *benchResult,
) {
	ctx := concurrency.CtxWithBudget(context.Background(), 1)
	result.SettleQueries = *benchSettle
	for i := 0; i < *benchSettle; i++ {
		_, err := search(ctx, data.queries[i%len(data.queries)])
		require.NoError(t, err)
	}
	result.AfterQueries = readProcessMemory(t)
	t.Logf("memory after build: rss %.0f MB, footprint %.0f MB, heap %.0f MB; after %d queries: rss %.0f MB, footprint %.0f MB, heap %.0f MB",
		result.AfterBuild.RSSMB, result.AfterBuild.FootprintMB, result.AfterBuild.HeapAllocMB,
		result.SettleQueries, result.AfterQueries.RSSMB, result.AfterQueries.FootprintMB, result.AfterQueries.HeapAllocMB)
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
