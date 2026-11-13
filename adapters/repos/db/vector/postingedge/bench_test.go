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
	"fmt"
	"maps"
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

	benchBatches       = flag.String("bench.batches", "4", "postingedge: load batch sweep")
	benchMargins       = flag.String("bench.margins", "0", "postingedge: source margin sweep (0 = no cut)")
	benchEntries       = flag.Int("bench.entries", DefaultEntryPostings, "postingedge: entry postings")
	benchProbe         = flag.Int("bench.probe", DefaultBuildProbe, "postingedge: build probe")
	benchRescue        = flag.Int("bench.rescue", DefaultRescueLimit, "postingedge: code-ranked candidates the edge builder re-ranks exactly")
	benchPosting       = flag.Int("bench.posting", 0, "postingedge: target posting size (0 = default budget)")
	benchSample        = flag.Int("bench.edgesample", 1000, "postingedge: vectors sampled to measure edge recall")
	benchCodeBits      = flag.Int("bench.codebits", 0, "postingedge: sign bits kept per code (0 = the full code)")
	benchScanCodeBits  = flag.Int("bench.scancodebits", 0, "postingedge: give the postings codes of their own for the scan, of this many sign bits, next to the codes of -bench.codebits in the sidecar (0 = the scan reads the codes of the sidecar)")
	benchEdgeCodeBits  = flag.Int("bench.edgecodebits", -1, "postingedge: build the edges with codes of this width instead of the codes of the index, 0 for full codes (-1 = the codes of the index); a diagnostic that tells what a search loses to its edges from what it loses to its codes")
	benchEdgeRescue    = flag.Int("bench.edgerescue", DefaultRescueLimit, "postingedge: the rescue limit that goes with -bench.edgecodebits")
	benchMinEdgeRecall = flag.Float64("bench.minedgerecall", 0, "postingedge: fail before the sweeps if the edge recall of the build is below this")
	benchAutoMargins   = flag.String("bench.automargins", "0.04,0.06,0.08,0.10,0.12", "postingedge: the margins a sweep with margin=auto chooses from")
	benchAutoTolerance = flag.Float64("bench.autotolerance", 0.002, "postingedge: the recall a margin=auto margin may cost at any ef, against no margin")
	benchAutoEFs       = flag.String("bench.autoefs", "", "postingedge: the ef values a margin=auto margin is calibrated at (empty = those of its sweep)")
	benchWorkThreads   = flag.Int("bench.workthreads", 1, "postingedge: threads of the passes that are not timed (calibration, timed=0 sweeps, the recall ceiling); recall and work counts do not depend on it, the phase times are kept only with 1")
	benchCeiling       = flag.Float64("bench.ceiling", 0, "postingedge: for every timed configuration that stays below this recall, keep doubling ef in untimed passes until the recall is reached or stops growing (0 = skip)")
	benchReach         = flag.Int("bench.reach", 0, "postingedge: queries sampled to measure what a triangle bound on the edge lengths could prune (0 = skip)")

	benchMaxConn = flag.Int("bench.maxconn", enthnsw.DefaultMaxConnections, "hnsw: max connections")
	benchEFC     = flag.Int("bench.efc", enthnsw.DefaultEFConstruction, "hnsw: ef construction")

	benchMode = flag.String("bench.mode", "full", "full: timed passes plus work stats; work: postingedge only, the work stats without the timed passes; "+
		"probes: postingedge only, the edge builder at each width of -bench.probes, no index; "+
		"edgerecall: postingedge only, the edge recall of a sample over -bench.codebitslist x -bench.probes x -bench.rescues, no index")
	benchProbes       = flag.String("bench.probes", "32,64,128,256", "postingedge: build probe widths of the probes and edgerecall modes")
	benchRescues      = flag.String("bench.rescues", strconv.Itoa(DefaultRescueLimit), "postingedge: rescue limits of the edgerecall mode")
	benchCodeList     = flag.String("bench.codebitslist", "0", "postingedge: code widths of the edgerecall mode (0 = the full code)")
	benchSampleProbes = flag.String("bench.sampleprobes", "", "postingedge, edgerecall mode: probe widths beyond those of -bench.probes, measured with the "+
		"nearest centroids of the sampled vectors alone. What a wider build probe would find, without the assignment of the whole train set at that width")
	benchSettle = flag.Int("bench.settle", 1000, "queries run before the after-queries memory reading")
	benchSelf   = flag.Int("bench.selfsample", 10_000, "postingedge: vectors searched for with themselves as the query, to tell that none is lost to the search")

	benchRepeat = flag.Bool("bench.repeat", false, "time every timed point a second time once the whole plan is done, in the order of the first, "+
		"and write the second round next to -bench.out as <name>-repeat.json: a stretch of points taken while the machine was slow shows "+
		"when the two rounds are set against each other (TestCellReport -bench.report.cells.repeats), and not in the passes of a point")

	benchStreamTrain = flag.Bool("bench.streamtrain", false, "postingedge, full mode: hand the train set to the index straight from the dataset files "+
		"and load the copy the benchmark reads from once the index is built. An index holds a copy of every vector while it builds, "+
		"and a large train set does not fit in memory twice")

	benchDegreeCaps = flag.String("bench.degreecaps", "", "postingedge, full mode: measure the plan once per cap of this list, in order, on the one build, "+
		"every vector keeping its k nearest edges (0 = all of them). Each cap writes next to -bench.out as <name>-cap<k>.json, "+
		"and as <name>-cap<k>-<n>.json the n-th time the list names it; -bench.out itself keeps the build and its diagnostics")
	benchCapMaxDrop = flag.Float64("bench.capmaxdrop", 0.01, "postingedge, with -bench.degreecaps: a cap whose edge recall falls more than this "+
		"below that of the full edges is reported and not measured. The recall of a cap is against the k nearest of the true edges")

	benchCentroidEFs = flag.String("bench.centroidefs", "", "postingedge, full mode: after the plan, efs of the centroid graph search below the default, "+
		"largest first. Each is measured untimed at -bench.centroidpoints, next to -bench.out as <name>-centroidef<ef>.json, after the default; "+
		"the walk stops at the first that loses more than -bench.centroidmaxdrop of recall at any point, and the last one that did not "+
		"gets the plan, timed, in its file")
	benchCentroidPoints = flag.String("bench.centroidpoints", "20,160", "postingedge, with -bench.centroidefs: the search efs the centroid efs are "+
		"compared at, with the rescore limit, load batch and margin of the first sweep")
	benchCentroidMaxDrop = flag.Float64("bench.centroidmaxdrop", 0.0005, "postingedge, with -bench.centroidefs: the most recall a centroid ef may lose "+
		"against the default at a point and still count as keeping it")

	benchSweeps sweepFlag
)

func init() {
	flag.Var(&benchSweeps, "bench.sweep", "one set of operating points, e.g. \"ef=10,20 threads=1,8 rescore=512 batch=4 margin=0,0.08\"; "+
		"repeatable, measured in order on the same index; keys left out take -bench.efs, -bench.threads, "+
		"-bench.rescores, -bench.batches and -bench.margins; timed=0 measures recall and work only")
}

// sweepFlag collects the -bench.sweep values.
type sweepFlag []string

func (s *sweepFlag) String() string { return strings.Join(*s, "; ") }

func (s *sweepFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// autoMargin stands in a sweep for the source margin the calibration picks,
// see calibrateMargin.
const autoMargin = -1

// sweep is one set of operating points: the cross product of its lists.
type sweep struct {
	efs, threads, rescores, batches []int
	margins                         []float64
	untimed                         bool // recall and work only, no timed passes
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
			margins:  parseFloats(t, *benchMargins),
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
			case "margin":
				if list == "auto" {
					sw.margins = []float64{autoMargin}
				} else {
					sw.margins = parseFloats(t, list)
				}
			case "timed":
				sw.untimed = list == "0"
			default:
				t.Fatalf("unknown sweep key %q", key)
			}
		}
		plan = append(plan, sw)
	}
	return plan
}

type benchData struct {
	name string
	dims int
	// dir is where the dataset was read from and rows the size of its train
	// set. With -bench.streamtrain the train set itself is not in memory
	// until the index is built.
	dir     string
	rows    int
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
	SourcesCut    float64 `json:"sourcesCutMean"`
	EdgesCut      float64 `json:"edgesCutMean"`
	EdgesMean     float64 `json:"edgesFollowedMean"`
	InLoadedMean  float64 `json:"endpointsInLoadedMean"`
	RepeatedMean  float64 `json:"endpointsRepeatedMean"`
	EndpointsMean float64 `json:"endpointsMean"`
	RescoredMean  float64 `json:"rescoredMean"`
	// Where the rescored candidates live: the loaded postings that hold at
	// least one of them, and the candidates that are in no loaded posting
	// (none, as long as candidates only come from scans).
	RescoredPostingsMean  float64 `json:"rescoredPostingsMean"`
	RescoredPostingsP95   float64 `json:"rescoredPostingsP95"`
	RescoredElsewhereMean float64 `json:"rescoredElsewhereMean"`
	RescoredElsewhereMax  float64 `json:"rescoredElsewhereMax"`
	CentroidUs            float64 `json:"centroidUs"`
	ScanUs                float64 `json:"scanUs"`
	ExpandUs              float64 `json:"expandUs"`
	SelectUs              float64 `json:"selectUs"`
	RescoreUs             float64 `json:"rescoreUs"`
	// TotalUs is the whole search as timed from outside, so that what the
	// phases do not cover can be told.
	TotalUs float64 `json:"totalUs"`
}

type benchPoint struct {
	EF           int           `json:"ef"`
	Threads      int           `json:"threads"`
	RescoreLimit int           `json:"rescoreLimit"`
	LoadBatch    int           `json:"loadBatch,omitempty"`
	SourceMargin float64       `json:"sourceMargin,omitempty"`
	Recall       float64       `json:"recall"`
	QPS          float64       `json:"qps"`
	QPSRuns      []float64     `json:"qpsRuns"`
	MeanMs       float64       `json:"meanMs"`
	P50Ms        float64       `json:"p50Ms"`
	P95Ms        float64       `json:"p95Ms"`
	P99Ms        float64       `json:"p99Ms"`
	Stats        *statsSummary `json:"stats,omitempty"`
	// Untimed points carry recall and work but no QPS or latencies.
	Untimed bool `json:"untimed,omitempty"`
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
	// EdgeRedundancy is how the edges of the vectors spread over the
	// postings they lead to.
	EdgeRedundancy *edgeRedundancy `json:"edgeRedundancy,omitempty"`

	Points     []benchPoint      `json:"points"`
	ProbeSweep []probePoint      `json:"probeSweep,omitempty"`
	EdgeRecall []edgeRecallPoint `json:"edgeRecall,omitempty"`
	// Calibration holds the untimed points the source margins of margin=auto
	// sweeps were chosen from, and AutoMargins what was chosen for each
	// rescore limit. AutoMargin is what result files written before the
	// margin was calibrated per rescore limit hold: one margin for all.
	Calibration []benchPoint    `json:"calibration,omitempty"`
	AutoMargins map[int]float64 `json:"autoMargins,omitempty"`
	AutoMargin  *float64        `json:"autoMargin,omitempty"`
}

func TestBenchmark(t *testing.T) {
	if *benchDatasetDir == "" {
		t.Skip("no -bench.dataset given")
	}
	ctx := context.Background()
	stream := *benchStreamTrain && *benchArm == "postingedge" && *benchMode == "full"
	if stream {
		require.Zero(t, *benchLimit, "-bench.streamtrain reads the whole train set")
	}
	data := loadBenchData(t, *benchDatasetDir, *benchLimit, *benchQueries, *benchK, stream)

	result := &benchResult{
		Arm:        *benchArm,
		Dataset:    data.name,
		Vectors:    data.rows,
		Dims:       data.dims,
		Queries:    len(data.queries),
		K:          *benchK,
		GoVersion:  runtime.Version(),
		GOARCH:     runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		Baseline:   takeMemSnapshot(t),
	}
	t.Logf("dataset %s: %d x %d, %d queries, baseline heap %.0f MB",
		data.name, data.rows, data.dims, len(data.queries), result.Baseline.HeapAllocMB)

	switch *benchArm {
	case "postingedge":
		if *benchMode == "probes" {
			benchProbeSweep(t, ctx, data, result)
			return
		}
		if *benchMode == "edgerecall" {
			benchEdgeRecallGrid(t, ctx, data, result)
			return
		}
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
	uc.CodeBits = *benchCodeBits
	uc.ScanCodeBits = *benchScanCodeBits
	uc.BuildProbe = *benchProbe
	uc.RescueLimit = *benchRescue
	uc.TrainingLimit = *benchTraining
	uc.EntryPostings = *benchEntries
	result.Config = map[string]any{
		"targetPostingSize": uc.TargetPostingSize, "codeBits": uc.CodeBits, "edges": uc.Edges,
		"buildProbe": uc.BuildProbe, "rescueLimit": uc.RescueLimit, "trainingLimit": uc.TrainingLimit,
		"entryPostings": uc.EntryPostings,
		"quantization":  "rq bits=1 centering=true", "distance": uc.Distance,
	}
	if uc.ScanCodeBits != 0 {
		result.Config["scanCodeBits"] = uc.ScanCodeBits
	}
	if *benchWorkThreads > 1 {
		result.Config["workThreads"] = *benchWorkThreads
	}

	// buildIndex adds the train set to a new index and builds it.
	buildIndex := func(uc UserConfig) (index *Index, addTime, total time.Duration) {
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

		// With -bench.buildcache the partition and the assignment come from
		// an earlier build of this train set, or are left for the next one.
		var share *buildShare
		var cacheDir, train string
		if *benchBuildCache != "" {
			cacheProbe := max(*benchCacheProbe, uc.BuildProbe)
			cacheDir = buildCacheDir(data, uc.TargetPostingSize, cacheProbe)
			train, err = trainFingerprint(data.dir)
			require.NoError(t, err)
			share, err = loadBuildShare(cacheDir, train)
			require.NoError(t, err)
			// A build computes probes as wide as its own and no wider.
			require.Truef(t, share.filled() || cacheProbe == uc.BuildProbe,
				"%s holds no assignment: the edge recall grid with -bench.probes %d computes it", cacheDir, cacheProbe)
			result.Config["cacheProbe"] = cacheProbe
			index.shared = share
		}
		taken := share != nil && share.filled()

		// Only the time inside the index counts: with -bench.streamtrain the
		// batches come from the dataset files as they are added.
		const batch = 10_000
		ids := make([]uint64, batch)
		add := func(first int, vectors [][]float32) error {
			for i := range vectors {
				ids[i] = uint64(first + i)
			}
			begin := time.Now()
			err := index.AddBatch(ctx, ids[:len(vectors)], vectors)
			addTime += time.Since(begin)
			return err
		}
		if data.train != nil {
			for start := 0; start < len(data.train); start += batch {
				require.NoError(t, add(start, data.train[start:min(start+batch, len(data.train))]))
			}
		} else {
			// The index copies what it is given, so a batch is garbage as
			// soon as it is added. Unit length first, as loadBenchData
			// leaves the train set: the index then holds the same bits
			// either way.
			require.NoError(t, testinghelpers.ReadRawTrainChunks(data.dir, batch, func(first int, chunk [][]float32) error {
				for _, vec := range chunk {
					distancer.NormalizeInPlace(vec)
				}
				return add(first, chunk)
			}))
		}
		begin := time.Now()
		require.NoError(t, index.Build(ctx))
		total = addTime + time.Since(begin)
		if share != nil {
			result.Config["partitionFromCache"] = taken
			if !taken {
				require.NoError(t, saveBuildShare(cacheDir, train, share))
				t.Logf("partition and assignment kept in %s", cacheDir)
			}
			// The index is built: nothing reads what was shared any more,
			// and it must be gone before the memory of the index is read.
			index.mu.Lock()
			index.shared = nil
			index.mu.Unlock()
		}
		return index, addTime, total
	}

	// The edges of another code width: an index built with those codes over
	// the same partition lends its edges to the one that is measured.
	var lent *postingStore
	if *benchEdgeCodeBits >= 0 {
		donorConfig := uc
		donorConfig.CodeBits, donorConfig.RescueLimit = *benchEdgeCodeBits, *benchEdgeRescue
		donor, _, _ := buildIndex(donorConfig)
		lent = donor.state.Load().postings
		require.NoError(t, donor.Shutdown(ctx))
		result.Config["edgeCodeBits"], result.Config["edgeRescueLimit"] = *benchEdgeCodeBits, *benchEdgeRescue
		runtime.GC()
		result.Baseline = takeMemSnapshot(t)
	}

	index, addTime, buildTime := buildIndex(uc)
	defer index.Shutdown(ctx)
	result.BuildSec = buildTime.Seconds()
	result.PeakRSSMB = peakRSSMB()
	if lent != nil {
		// Slots are positions in the partition, so the edges of one index
		// mean the same in the other only if both laid the vectors out alike.
		store := index.state.Load().postings
		require.Equal(t, lent.start, store.start, "the two builds partitioned the vectors differently")
		require.Equal(t, lent.docIDs, store.docIDs, "the two builds laid the vectors out differently")
		store.edgeStart, store.edges = lent.edgeStart, lent.edges
		lent = nil
	}

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
	if data.train == nil {
		// Now that the index has let go of its copy: -bench.streamtrain.
		data.loadTrain(t)
	}
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
		"f32":           float64(data.rows) * float64(data.dims) * 4 / (1 << 20),
	}
	if memory.ScanCodeBytes != 0 {
		// on top of "codes", which are then those of the sidecar alone
		result.Components["scanCodes"] = float64(memory.ScanCodeBytes) / (1 << 20)
	}
	t.Logf("memory: index heap %.1f MB, analytic %.1f MB, components %v, rss %.0f MB, peak rss %.0f MB",
		result.IndexHeapMB, analytic, result.Components, result.AfterBuild.RSSMB, result.PeakRSSMB)

	search := func(ctx context.Context, query []float32) ([]uint64, error) {
		ids, _, err := index.SearchByVector(ctx, query, *benchK, nil)
		return ids, err
	}
	plan := benchSweepPlan(t)
	uc.EF, uc.LoadBatch, uc.RescoreLimit = plan[0].efs[0], plan[0].batches[0], plan[0].rescores[0]
	uc.SourceMargin = float32(max(plan[0].margins[0], 0))
	require.NoError(t, index.UpdateUserConfig(uc, func() {}))
	settle(t, data, search, result)

	var truth *edgeTruth
	result.Diagnostics, truth = postingEdgeDiagnostics(t, index, data, build, *benchSample)
	{
		st := index.state.Load()
		redundancy := edgeRedundancyOf(st.postings.edgeStart, st.postings.edges, st.side.postingOf)
		result.EdgeRedundancy = &redundancy
		maps.Copy(result.Diagnostics, redundancy.diagnostics())
		t.Logf("edge redundancy: %s", redundancy.report())
	}
	if *benchReach > 0 {
		for key, value := range edgeReachDiagnostics(t, index, data, *benchReach) {
			result.Diagnostics[key] = value
		}
	}
	t.Logf("diagnostics: %v", result.Diagnostics)
	writeBenchResult(t, result)
	if recall := result.Diagnostics["edgeRecall"]; recall < *benchMinEdgeRecall {
		t.Fatalf("edge recall %.4f is below the required %.4f, not running the sweeps", recall, *benchMinEdgeRecall)
	}

	if *benchDegreeCaps == "" {
		runPostingEdgePlan(t, index, data, uc, plan, result, *benchOut)
		if *benchCentroidEFs != "" {
			benchCentroidEFSweep(t, index, data, uc, plan, result)
		}
		return
	}
	benchCappedDegrees(t, index, data, uc, plan, result, truth)
}

// benchCentroidEFSweep measures the centroid graph search at the efs of
// -bench.centroidefs, below the default centroidEF, on the index the plan was
// just measured on. Every ef, the default first, gets the points of
// -bench.centroidpoints untimed: recall, work, and how often the entry is the
// exact nearest centroid. The walk goes down the list and stops at the first
// ef that loses more than -bench.centroidmaxdrop of recall at any point
// against the default. The last ef that did not gets the plan, timed. The
// default is put back at the end.
func benchCentroidEFSweep(t *testing.T, index *Index, data *benchData, uc UserConfig, plan []sweep, result *benchResult) {
	require.NotEmpty(t, *benchOut, "-bench.centroidefs writes a result per centroid ef next to -bench.out")
	defer func() { require.NoError(t, index.setCentroidEF(centroidEF)) }()
	first := plan[0]
	rescore, batch, margin := first.rescores[0], first.batches[0], first.margins[0]
	require.NotEqual(t, autoMargin, margin, "-bench.centroidefs needs a first sweep with a margin given")
	points := parseInts(t, *benchCentroidPoints)
	st := index.state.Load()
	exact := exactEntries(st, data.queries)

	outOf := func(ef int) string {
		return strings.TrimSuffix(*benchOut, ".json") + fmt.Sprintf("-centroidef%d.json", ef)
	}
	untimed := func(ef int) *benchResult {
		require.NoError(t, index.setCentroidEF(ef))
		cell := *result
		cell.Points, cell.Calibration, cell.AutoMargins, cell.AutoMargin = nil, nil, nil, nil
		cell.Config = maps.Clone(result.Config)
		cell.Config["centroidEF"] = ef
		cell.Diagnostics = maps.Clone(result.Diagnostics)
		cell.Diagnostics["entryIsNearestCentroid"] = entryAccuracy(st, data.queries, exact)
		for _, searchEF := range points {
			uc.EF, uc.LoadBatch, uc.RescoreLimit, uc.SourceMargin = searchEF, batch, rescore, float32(margin)
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
			// One thread, so that the phase times of the pass hold.
			stats, recall := collectSearchStats(t, index, data, 1)
			cell.Points = append(cell.Points, benchPoint{
				EF: searchEF, Threads: 1, RescoreLimit: rescore, LoadBatch: batch, SourceMargin: margin,
				Recall: recall, Stats: stats, Untimed: true,
			})
			t.Logf("centroid ef %d: ef=%d recall=%.4f | loads %.1f rounds %.1f endpoints %.0f scanned %.0f | "+
				"entry is the nearest centroid %.4f | us, untimed pass: centroid %.0f total %.0f",
				ef, searchEF, recall, stats.LoadsMean, stats.RoundsMean, stats.EndpointsMean, stats.ScannedMean,
				cell.Diagnostics["entryIsNearestCentroid"], stats.CentroidUs, stats.TotalUs)
		}
		writeBenchResultTo(t, &cell, outOf(ef))
		return &cell
	}

	base := untimed(centroidEF)
	var kept *benchResult
	keptEF := 0
	for _, ef := range parseInts(t, *benchCentroidEFs) {
		require.Less(t, ef, centroidEF, "-bench.centroidefs are efs below the default")
		cell := untimed(ef)
		worst := 0.0
		for i, p := range cell.Points {
			worst = max(worst, base.Points[i].Recall-p.Recall)
		}
		if worst > *benchCentroidMaxDrop {
			t.Logf("centroid ef %d loses %.4f of recall against %d, more than %.4f: the walk stops", ef, worst, centroidEF, *benchCentroidMaxDrop)
			break
		}
		t.Logf("centroid ef %d keeps the recall of %d (worst loss %.4f)", ef, centroidEF, worst)
		kept, keptEF = cell, ef
	}
	if kept == nil {
		t.Logf("no centroid ef below %d keeps its recall: nothing timed", centroidEF)
		return
	}
	require.NoError(t, index.setCentroidEF(keptEF))
	t.Logf("centroid ef %d: timing the plan", keptEF)
	runPostingEdgePlan(t, index, data, uc, plan, kept, outOf(keptEF))
}

// benchCappedDegrees measures the plan once per cap of -bench.degreecaps on the
// one build: the full edges of the build are capped to the k nearest of each
// vector, published, and measured, in the order of the list. Every cap gets
// a result of its own, with the build of the given one and the edge
// diagnostics, memory and points of the cap.
func benchCappedDegrees(t *testing.T, index *Index, data *benchData, uc UserConfig, plan []sweep,
	result *benchResult, truth *edgeTruth,
) {
	require.NotEmpty(t, *benchOut, "-bench.degreecaps writes a result per cap next to -bench.out")
	full := index.state.Load().postings
	fullStart, fullEdges := full.edgeStart, full.edges
	fullRecall := result.Diagnostics["edgeRecall"]
	fullEdgeMB := result.Components["edges"]
	n := full.vectors()

	named := map[int]int{}
	for _, k := range parseInts(t, *benchDegreeCaps) {
		named[k]++
		out := strings.TrimSuffix(*benchOut, ".json") + fmt.Sprintf("-cap%d", k)
		if named[k] > 1 {
			out += fmt.Sprintf("-%d", named[k])
		}
		out += ".json"

		start, edges := capDegree(fullStart, fullEdges, k)
		require.NoError(t, index.replaceEdges(start, edges))
		edgesOf := index.state.Load().postings.edgesOf

		cell := *result
		cell.Points, cell.Calibration, cell.AutoMargins, cell.AutoMargin = nil, nil, nil, nil
		cell.Config = maps.Clone(result.Config)
		cell.Config["degreeCap"] = k
		cell.Components = maps.Clone(result.Components)
		cell.Diagnostics = maps.Clone(result.Diagnostics)
		redundancy := edgeRedundancyOf(start, edges, index.state.Load().side.postingOf)
		cell.EdgeRedundancy = &redundancy
		maps.Copy(cell.Diagnostics, redundancy.diagnostics())
		memory, _ := index.MemoryStats()
		cell.Components["edges"] = float64(memory.EdgeBytes) / (1 << 20)
		// The heap is read once, after the build: a cap frees what it drops.
		cell.IndexHeapMB = result.IndexHeapMB - fullEdgeMB + cell.Components["edges"]
		cut := 0
		for slot := 0; slot < n; slot++ {
			if k > 0 && fullStart[slot+1]-fullStart[slot] > uint32(k) {
				cut++
			}
		}
		recall := truth.recallOfNearest(edgesOf, k)
		cell.Diagnostics["edgesPerVector"] = float64(len(edges)) / float64(n)
		cell.Diagnostics["edgeRecall"] = recall
		cell.Diagnostics["edgeRecallOfAllEdges"] = truth.recallOfNearest(edgesOf, 0)
		cell.Diagnostics["vectorsCapped"] = float64(cut) / float64(n)
		cell.Diagnostics["edgesDropped"] = 1 - float64(len(edges))/float64(len(fullEdges))
		// The search settings of the diagnostics of the build, not those of
		// the last point the cap before measured.
		require.NoError(t, index.UpdateUserConfig(uc, func() {}))
		if sample, found := selfRetrieval(t, index, data); sample > 0 {
			cell.Diagnostics["selfSample"], cell.Diagnostics["selfFound"] = float64(sample), found
		}
		kept := "all the true edges"
		if k > 0 {
			kept = fmt.Sprintf("the %d nearest true edges", k)
		}
		t.Logf("cap %d: %.2f edges per vector (%.1f%% of the vectors capped, %.1f%% of the edges dropped), "+
			"edge recall %.4f against %s and %.4f against all of them (full edges: %.4f), "+
			"edges %.1f MB, self found %.4f",
			k, cell.Diagnostics["edgesPerVector"], 100*cell.Diagnostics["vectorsCapped"], 100*cell.Diagnostics["edgesDropped"],
			recall, kept, cell.Diagnostics["edgeRecallOfAllEdges"], fullRecall, cell.Components["edges"], cell.Diagnostics["selfFound"])
		writeBenchResultTo(t, &cell, out)
		if recall < fullRecall-*benchCapMaxDrop {
			t.Logf("cap %d: edge recall %.4f is more than %.3f below the %.4f of the full edges, not measured",
				k, recall, *benchCapMaxDrop, fullRecall)
			continue
		}
		runPostingEdgePlan(t, index, data, uc, plan, &cell, out)
	}
	require.NoError(t, index.replaceEdges(fullStart, fullEdges))
}

// runPostingEdgePlan measures the plan on the index as it stands and writes
// the result to out, the second round of -bench.repeat next to it.
func runPostingEdgePlan(t *testing.T, index *Index, data *benchData, uc UserConfig, plan []sweep,
	result *benchResult, out string,
) {
	search := func(ctx context.Context, query []float32) ([]uint64, error) {
		ids, _, err := index.SearchByVector(ctx, query, *benchK, nil)
		return ids, err
	}

	// margin=auto is calibrated once per rescore limit: how much recall a
	// margin costs depends on how many of the candidates it thins out reach
	// the final rescore.
	auto := map[int]float64{}
	for _, sw := range plan {
		if !slices.Contains(sw.margins, autoMargin) {
			continue
		}
		for _, rescore := range sw.rescores {
			if _, done := auto[rescore]; !done {
				auto[rescore] = calibrateMargin(t, index, data, uc, sw, rescore, result, out)
			}
		}
	}

	// The work of a search does not depend on timing or on the thread count,
	// so it is collected once per configuration.
	type workKey struct {
		ef, rescore, batch int
		margin             float64
	}
	type work struct {
		stats    *statsSummary
		recall   float64
		parallel bool // collected on several threads: no phase times
	}
	collected := map[workKey]work{}
	measure := func(ef, rescore, loadBatch, threads int, margin float64, untimed bool) benchPoint {
		uc.EF, uc.LoadBatch, uc.RescoreLimit = ef, loadBatch, rescore
		uc.SourceMargin = float32(margin)
		require.NoError(t, index.UpdateUserConfig(uc, func() {}))

		point := benchPoint{Threads: threads, Untimed: untimed}
		if !untimed {
			point = measurePoint(t, data, search, threads, *benchRuns)
		}
		point.EF, point.LoadBatch, point.RescoreLimit = ef, loadBatch, rescore
		point.SourceMargin = margin
		key := workKey{ef: ef, rescore: rescore, batch: loadBatch, margin: margin}
		// A timed point takes its phase times from a pass that ran alone.
		w, found := collected[key]
		if !found || (w.parallel && !untimed) {
			workers := 1
			if untimed {
				workers = *benchWorkThreads
			}
			w.stats, w.recall = collectSearchStats(t, index, data, workers)
			w.parallel = workers > 1
			collected[key] = w
		}
		point.Stats = w.stats
		if untimed {
			point.Recall = w.recall
		}
		result.Points = append(result.Points, point)
		s := point.Stats
		t.Logf("ef=%d rescore=%d batch=%d margin=%.2f threads=%d recall=%.4f qps=%.0f p50=%.3fms p95=%.3fms | rounds %.1f (p95 %.0f) loads %.1f/%.0f/%.0f scanned %.0f (again %.0f, sources cut %.0f) edges %.0f (in loaded %.0f, repeated %.0f) endpoints %.0f rescored %.0f (in %.1f postings, p95 %.0f, elsewhere %.0f) | us: centroid %.0f scan %.0f expand %.0f select %.0f rescore %.0f total %.0f",
			ef, rescore, loadBatch, margin, threads, point.Recall, point.QPS, point.P50Ms, point.P95Ms,
			s.RoundsMean, s.RoundsP95, s.LoadsMean, s.LoadsP50, s.LoadsP95, s.ScannedMean, s.AgainMean, s.SourcesCut,
			s.EdgesMean, s.InLoadedMean, s.RepeatedMean, s.EndpointsMean, s.RescoredMean,
			s.RescoredPostingsMean, s.RescoredPostingsP95, s.RescoredElsewhereMax,
			s.CentroidUs, s.ScanUs, s.ExpandUs, s.SelectUs, s.RescoreUs, s.TotalUs)
		writeBenchResultTo(t, result, out)
		return point
	}

	// A point that two sweeps share is measured once: margin=auto may come
	// out as a margin that another sweep names.
	type pointKey struct {
		work    workKey
		threads int
		untimed bool
	}
	measured := map[pointKey]bool{}
	for _, sw := range plan {
		untimed := sw.untimed || *benchMode == "work"
		for _, named := range sw.margins {
			for _, rescore := range sw.rescores {
				margin := named
				if named == autoMargin {
					margin = auto[rescore]
				}
				for _, loadBatch := range sw.batches {
					for _, threads := range sw.threads {
						for _, ef := range sw.efs {
							key := pointKey{
								work:    workKey{ef: ef, rescore: rescore, batch: loadBatch, margin: margin},
								threads: threads, untimed: untimed,
							}
							if measured[key] {
								continue
							}
							measured[key] = true
							measure(ef, rescore, loadBatch, threads, margin, untimed)
						}
						if untimed {
							break
						}
					}
				}
			}
		}
	}

	if *benchCeiling > 0 {
		findCeilings(t, result, *benchCeiling, func(ef, rescore, loadBatch int, margin float64) float64 {
			return measure(ef, rescore, loadBatch, 1, margin, true).Recall
		})
	}

	repeatTimedPoints(t, result, out, func(first benchPoint) benchPoint {
		uc.EF, uc.LoadBatch, uc.RescoreLimit = first.EF, first.LoadBatch, first.RescoreLimit
		uc.SourceMargin = float32(first.SourceMargin)
		require.NoError(t, index.UpdateUserConfig(uc, func() {}))
		return measurePoint(t, data, search, first.Threads, *benchRuns)
	})
}

// repeatTimedPoints times every timed point of the result once more, in the
// order they were first taken, and writes the second round next to out, as
// <name>-repeat.json. again measures a point with the settings of the given
// one. It does nothing without -bench.repeat, or without out.
func repeatTimedPoints(t *testing.T, result *benchResult, out string, again func(first benchPoint) benchPoint) {
	if !*benchRepeat || out == "" {
		return
	}
	second := *result
	second.Points = nil
	path := strings.TrimSuffix(out, ".json") + "-repeat.json"
	for _, first := range result.Points {
		if first.Untimed {
			continue
		}
		point := again(first)
		point.EF, point.LoadBatch, point.RescoreLimit = first.EF, first.LoadBatch, first.RescoreLimit
		point.SourceMargin, point.Stats = first.SourceMargin, first.Stats
		second.Points = append(second.Points, point)
		t.Logf("again ef=%d rescore=%d batch=%d margin=%.2f threads=%d recall=%.4f qps=%.0f, %+.1f%% against the first round",
			point.EF, point.RescoreLimit, point.LoadBatch, point.SourceMargin, point.Threads, point.Recall, point.QPS,
			100*(point.QPS/first.QPS-1))
		raw, err := json.MarshalIndent(&second, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, raw, 0o644))
	}
}

// ceilingMaxEF is where the search for the recall ceiling of a configuration
// gives up, and ceilingMinGain the recall a doubling of ef has to add for the
// search to go on.
const (
	ceilingMaxEF   = 8192
	ceilingMinGain = 0.0005
)

// findCeilings looks for the recall every timed configuration can reach by
// raising ef alone. A configuration that stays below the target over its ef
// sweep gets untimed passes at twice its largest ef, then twice that, until it
// reaches the target, a doubling adds less than ceilingMinGain, or ef passes
// ceilingMaxEF. Where it stops it is measured once more without source margin,
// which tells whether the margin is what holds the recall down.
func findCeilings(t *testing.T, result *benchResult, target float64,
	recallAt func(ef, rescore, loadBatch int, margin float64) float64,
) {
	type config struct {
		rescore, batch int
		margin         float64
	}
	type reach struct {
		ef     int
		recall float64 // at the largest ef
		best   float64
	}
	var order []config
	reached := map[config]*reach{}
	for _, point := range result.Points {
		if point.Untimed {
			continue
		}
		key := config{rescore: point.RescoreLimit, batch: point.LoadBatch, margin: point.SourceMargin}
		r, found := reached[key]
		if !found {
			r = &reach{}
			reached[key] = r
			order = append(order, key)
		}
		if point.EF > r.ef {
			r.ef, r.recall = point.EF, point.Recall
		}
		r.best = max(r.best, point.Recall)
	}

	for _, key := range order {
		r := reached[key]
		if r.best >= target {
			continue
		}
		ef, recall := r.ef, r.recall
		for ef*2 <= ceilingMaxEF {
			next := recallAt(ef*2, key.rescore, key.batch, key.margin)
			gain := next - recall
			ef, recall = ef*2, next
			if recall >= target || gain < ceilingMinGain {
				break
			}
		}
		t.Logf("ceiling: rescore=%d batch=%d margin=%.2f reaches recall %.4f at ef=%d (target %.2f)",
			key.rescore, key.batch, key.margin, recall, ef, target)
		if key.margin != 0 {
			without := recallAt(ef, key.rescore, key.batch, 0)
			t.Logf("ceiling: rescore=%d batch=%d without margin reaches recall %.4f at ef=%d",
				key.rescore, key.batch, without, ef)
		}
	}
}

// calibrateMargin picks the source margin of a margin=auto sweep at one
// rescore limit: the smallest margin of -bench.automargins that costs at most
// -bench.autotolerance of recall at every ef of -bench.autoefs, or of the
// sweep, against no margin at the same ef. It runs untimed passes, which is
// enough for recall, and falls back to no margin if none qualifies.
func calibrateMargin(t *testing.T, index *Index, data *benchData, uc UserConfig, sw sweep, rescore int,
	result *benchResult, out string,
) float64 {
	efs := sw.efs
	if *benchAutoEFs != "" {
		efs = parseInts(t, *benchAutoEFs)
	}
	measure := func(margin float64) []float64 {
		recalls := make([]float64, len(efs))
		for i, ef := range efs {
			uc.EF, uc.LoadBatch, uc.RescoreLimit = ef, sw.batches[0], rescore
			uc.SourceMargin = float32(margin)
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
			stats, recall := collectSearchStats(t, index, data, *benchWorkThreads)
			recalls[i] = recall
			result.Calibration = append(result.Calibration, benchPoint{
				EF: ef, Threads: 1, RescoreLimit: uc.RescoreLimit, LoadBatch: uc.LoadBatch,
				SourceMargin: margin, Recall: recall, Stats: stats, Untimed: true,
			})
		}
		writeBenchResultTo(t, result, out)
		return recalls
	}

	baseline := measure(0)
	chosen := 0.0
	for _, margin := range parseFloats(t, *benchAutoMargins) {
		worst := 0.0
		for i, recall := range measure(margin) {
			worst = min(worst, recall-baseline[i])
		}
		t.Logf("calibration: rescore=%d margin %.2f costs at most %.4f of recall", rescore, margin, -worst)
		if -worst <= *benchAutoTolerance {
			chosen = margin
			break
		}
	}
	t.Logf("calibration: rescore=%d margin=auto is %.2f", rescore, chosen)
	if result.AutoMargins == nil {
		result.AutoMargins = map[int]float64{}
	}
	result.AutoMargins[rescore] = chosen
	writeBenchResultTo(t, result, out)
	return chosen
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

	repeatTimedPoints(t, result, *benchOut, func(first benchPoint) benchPoint {
		uc.EF, uc.RQ.RescoreLimit = first.EF, first.RescoreLimit
		updated := make(chan struct{})
		require.NoError(t, index.UpdateUserConfig(uc, func() { close(updated) }))
		<-updated
		return measurePoint(t, data, search, first.Threads, *benchRuns)
	})
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

// collectSearchStats runs the query set once more with the statistics of
// every search on, and returns their summary and the recall. It is kept out
// of the timed passes because collecting the statistics reads the clock and
// counts. The counts and the recall are those of the searches, whatever the
// number of threads. The phase times only mean something when every query
// runs alone, so with more than one thread they are left at zero.
func collectSearchStats(t *testing.T, index *Index, data *benchData, threads int) (*statsSummary, float64) {
	ctx := concurrency.CtxWithBudget(context.Background(), 1)
	type answer struct {
		stats SearchStats
		took  time.Duration
		hits  int
	}
	answers := make([]answer, len(data.queries))

	var next atomic.Int64
	var failed atomic.Pointer[error]
	var wg sync.WaitGroup
	for range max(1, threads) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(data.queries) {
					return
				}
				begin := time.Now()
				ids, _, stats, err := index.SearchByVectorWithStats(ctx, data.queries[i], *benchK, nil)
				if err != nil {
					failed.Store(&err)
					return
				}
				a := answer{stats: stats, took: time.Since(begin)}
				for _, want := range data.truth[i] {
					if slices.Contains(ids, want) {
						a.hits++
					}
				}
				answers[i] = a
			}
		}()
	}
	wg.Wait()
	if err := failed.Load(); err != nil {
		require.NoError(t, *err)
	}

	n := float64(len(data.queries))
	rounds := make([]float64, 0, len(data.queries))
	loads := make([]float64, 0, len(data.queries))
	rescoredPostings := make([]float64, 0, len(data.queries))
	var sum statsSummary
	hits, total := 0, 0
	for i, a := range answers {
		stats := a.stats
		hits += a.hits
		total += len(data.truth[i])

		rounds = append(rounds, float64(stats.Rounds))
		loads = append(loads, float64(stats.PostingsLoaded))
		sum.RoundsMean += float64(stats.Rounds) / n
		sum.LoadsMean += float64(stats.PostingsLoaded) / n
		sum.ScannedMean += float64(stats.VectorsScanned) / n
		sum.AgainMean += float64(stats.ScannedAgain) / n
		sum.SourcesCut += float64(stats.SourcesCut) / n
		sum.EdgesCut += float64(stats.EdgesCut) / n
		sum.EdgesMean += float64(stats.EdgesFollowed) / n
		sum.InLoadedMean += float64(stats.EndpointsInLoaded) / n
		sum.RepeatedMean += float64(stats.EndpointsRepeated()) / n
		sum.EndpointsMean += float64(stats.EndpointsScored) / n
		sum.RescoredMean += float64(stats.Rescored) / n
		rescoredPostings = append(rescoredPostings, float64(stats.RescoredPostings))
		sum.RescoredPostingsMean += float64(stats.RescoredPostings) / n
		sum.RescoredElsewhereMean += float64(stats.RescoredElsewhere) / n
		sum.RescoredElsewhereMax = max(sum.RescoredElsewhereMax, float64(stats.RescoredElsewhere))
		if threads > 1 {
			continue
		}
		sum.CentroidUs += float64(stats.CentroidTime.Nanoseconds()) / 1e3 / n
		sum.ScanUs += float64(stats.ScanTime.Nanoseconds()) / 1e3 / n
		sum.ExpandUs += float64(stats.ExpandTime.Nanoseconds()) / 1e3 / n
		sum.SelectUs += float64(stats.SelectTime.Nanoseconds()) / 1e3 / n
		sum.RescoreUs += float64(stats.RescoreTime.Nanoseconds()) / 1e3 / n
		sum.TotalUs += float64((a.took - stats.SpreadTime).Nanoseconds()) / 1e3 / n
	}
	slices.Sort(rounds)
	slices.Sort(loads)
	sum.RoundsP50 = percentileOf(rounds, 0.50)
	sum.RoundsP95 = percentileOf(rounds, 0.95)
	sum.RoundsMax = rounds[len(rounds)-1]
	sum.LoadsP50 = percentileOf(loads, 0.50)
	sum.LoadsP95 = percentileOf(loads, 0.95)
	slices.Sort(rescoredPostings)
	sum.RescoredPostingsP95 = percentileOf(rescoredPostings, 0.95)
	return &sum, float64(hits) / float64(total)
}

// postingEdgeDiagnostics measures what the search cost depends on but no
// operating point shows: how the postings turned out, how good the edges
// are, and how often the centroid graph finds the true nearest centroid. It
// returns the true edges of the sample it measured the edges against, for the
// edges a cap leaves (benchCappedDegrees).
func postingEdgeDiagnostics(t *testing.T, index *Index, data *benchData, build BuildStats, sample int,
) (map[string]float64, *edgeTruth) {
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

	// Coverage: every vector that was added is in exactly one posting. The
	// assignment is exact and nothing is replicated, so a vector that is in
	// no posting, or in two, is a bug of the build and not a number to report
	// next to the others.
	cover := coverageOf(store, data.rows)
	out["vectorsAdded"], out["vectorsInPostings"] = float64(data.rows), float64(cover.inPostings)
	out["vectorsMissing"], out["vectorsRepeated"] = float64(cover.missing), float64(cover.repeated)
	out["vectorsNeverAdded"] = float64(cover.unknown)
	t.Logf("coverage: %d vectors added, %d in postings: %d missing, %d repeated, %d that were never added",
		data.rows, cover.inPostings, cover.missing, cover.repeated, cover.unknown)
	require.Truef(t, cover.complete(data.rows), "the postings do not hold every added vector exactly once: %+v", cover)

	// Edge recall: of the true nearest neighbors of a vector that live in
	// another posting, how many is it linked to.
	slotOf := make([]uint32, n)
	for slot, docID := range store.docIDs {
		slotOf[docID] = uint32(slot)
	}
	truth := sampleEdgeTruth(data, sample, index.uc.Edges, slotOf, side.postingOf)
	out["edgeRecall"] = truth.recall(store.edgesOf)
	out["neighborsInOwnPosting"] = truth.ownPosting

	// Entry accuracy: how often the centroid graph returns the exact
	// nearest centroid of a query.
	out["entryIsNearestCentroid"] = entryAccuracy(st, data.queries, exactEntries(st, data.queries))

	if selfSample, found := selfRetrieval(t, index, data); selfSample > 0 {
		out["selfSample"], out["selfFound"] = float64(selfSample), found
	}
	return out, truth
}

// selfRetrieval searches for an evenly spread sample of -bench.selfsample
// vectors with themselves as the query: each has to come back. It is in the
// posting of its nearest centroid, so it is found whenever the centroid
// graph enters there, and through an edge otherwise: the coverage of the
// postings as a search sees it, with the search settings and the edges the
// index has now. found is the share that came back.
func selfRetrieval(t *testing.T, index *Index, data *benchData) (sample int, found float64) {
	t.Helper()
	n := len(data.train)
	sample = min(*benchSelf, n)
	if sample == 0 {
		return 0, 0
	}
	var hits atomic.Int64
	parallelFor(sample, func(i int) {
		docID := uint64(i) * uint64(n) / uint64(sample)
		ids, _, err := index.SearchByVector(context.Background(), data.train[docID], *benchK, nil)
		if err != nil {
			panic(err)
		}
		if slices.Contains(ids, docID) {
			hits.Add(1)
		}
	})
	return sample, float64(hits.Load()) / float64(sample)
}

// coverage counts how the vectors added to an index under the ids 0 to
// added-1 show up in its postings.
type coverage struct {
	inPostings int // slots the postings own: what their sizes add up to
	missing    int // added vectors that are in no posting
	repeated   int // slots holding a vector that an earlier slot holds too
	unknown    int // slots holding an id that was never added
}

// complete is every added vector in exactly one posting, and nothing else.
func (c coverage) complete(added int) bool {
	return c.inPostings == added && c.missing == 0 && c.repeated == 0 && c.unknown == 0
}

// coverageOf walks the postings the way a search that loaded all of them
// would, posting by posting over the slots each one owns.
func coverageOf(store *postingStore, added int) coverage {
	var c coverage
	seen := make([]bool, added)
	for p := 0; p < store.postings(); p++ {
		for slot := store.start[p]; slot < store.start[p+1]; slot++ {
			c.inPostings++
			switch docID := store.docIDs[slot]; {
			case docID >= uint64(added):
				c.unknown++
			case seen[docID]:
				c.repeated++
			default:
				seen[docID] = true
			}
		}
	}
	for _, in := range seen {
		if !in {
			c.missing++
		}
	}
	return c
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
		estimator := st.quantizer.NewEstimator(query)
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

// edgeTruth is what the edges of a sample of vectors should be: for each
// sampled slot, the slots of its exact nearest neighbors that live in another
// posting.
type edgeTruth struct {
	slots []uint32
	// wanted are the true edges of each sampled slot: its exact nearest
	// neighbors that live in another posting, nearest first.
	wanted [][]uint32
	// ownPosting is the share of the exact nearest neighbors of the sample
	// that live in the vector's own posting and so need no edge.
	ownPosting float64
}

// sampleEdgeTruth brute-forces the exact nearest neighbors of a random
// sample of vectors. The train position of a vector is its doc id.
func sampleEdgeTruth(data *benchData, sample, neighbors int, slotOf, postingOf []uint32) *edgeTruth {
	n := len(data.train)
	provider := distancer.NewCosineDistanceProvider()
	rng := rand.New(rand.NewPCG(7, 7))
	truth := &edgeTruth{
		slots:  make([]uint32, min(sample, n)),
		wanted: make([][]uint32, min(sample, n)),
	}
	docs := make([]int, len(truth.slots))
	for i := range docs {
		docs[i] = rng.IntN(n)
		truth.slots[i] = slotOf[docs[i]]
	}

	var same, other atomic.Int64
	parallelFor(len(docs), func(i int) {
		doc := docs[i]
		var nearest topK
		nearest.reset(neighbors)
		for candidate, vec := range data.train {
			if candidate == doc {
				continue
			}
			dist, err := provider.SingleDist(data.train[doc], vec)
			if err != nil {
				panic(err)
			}
			nearest.push(uint64(candidate), dist)
		}
		for _, nb := range nearest.sorted() {
			if postingOf[slotOf[nb.id]] == postingOf[slotOf[doc]] {
				same.Add(1)
				continue
			}
			other.Add(1)
			truth.wanted[i] = append(truth.wanted[i], slotOf[nb.id])
		}
	})
	truth.ownPosting = float64(same.Load()) / float64(same.Load()+other.Load())
	return truth
}

// recall is the share of the wanted edges that the given edges hold.
func (e *edgeTruth) recall(edgesOf func(slot uint32) []uint32) float64 {
	return e.recallOfNearest(edgesOf, 0)
}

// recallOfNearest is recall against the k nearest wanted edges of every
// sampled vector, all of them with k <= 0: the true edges of a cap on the
// degree (capDegree).
func (e *edgeTruth) recallOfNearest(edgesOf func(slot uint32) []uint32, k int) float64 {
	captured, wanted := 0, 0
	for i, slot := range e.slots {
		edges := edgesOf(slot)
		want := e.wanted[i]
		if k > 0 {
			want = want[:min(len(want), k)]
		}
		for _, want := range want {
			wanted++
			if slices.Contains(edges, want) {
				captured++
			}
		}
	}
	return float64(captured) / float64(max(wanted, 1))
}

// probePoint is the edge builder at one probe width.
type probePoint struct {
	Probe               int     `json:"probe"`
	EdgeSec             float64 `json:"edgeSec"`
	CandidatesPerVector float64 `json:"candidatesPerVector"`
	EdgesPerVector      float64 `json:"edgesPerVector"`
	EdgeRecall          float64 `json:"edgeRecall"`
}

// benchProbeSweep runs the edge builder at several probe widths over one
// partition of the corpus: the stages of Index.build up to the edges, once,
// and then the edges once per width. It reports what each width costs and
// how many of the true edges it finds.
func benchProbeSweep(t *testing.T, ctx context.Context, data *benchData, result *benchResult) {
	logger := logrus.New()
	_, tempVectorForID := benchThunks(data)
	cfg := Config{
		ID:                           "bench",
		RootPath:                     t.TempDir(),
		Logger:                       logger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		TempVectorForIDWithViewThunk: tempVectorForID,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
		Seed:                         1,
	}
	require.NoError(t, cfg.validate())
	uc := NewDefaultUserConfig()
	uc.TargetPostingSize = *benchPosting
	uc.CodeBits = *benchCodeBits
	uc.TrainingLimit = *benchTraining
	probes := parseInts(t, *benchProbes)
	vectors, n := asTheIndexStoresThem(data), len(data.train)

	quantizer, err := trainQuantizer(vectors, data.dims, uc.TrainingLimit, uc.CodeBits, &cfg)
	require.NoError(t, err)
	target := targetPostingSize(uc.TargetPostingSize, 8*len(quantizer.Encode(vectors[0])))
	stage := time.Now()
	centroids, err := partition(ctx, logger, vectors, data.dims, int(math.Ceil(float64(n)/float64(target))), cfg.Seed)
	require.NoError(t, err)
	partitionTime := time.Since(stage)
	stage = time.Now()
	assigned, err := assign(ctx, logger, vectors, centroids, slices.Max(probes))
	require.NoError(t, err)
	centroids = dropEmpty(centroids, assigned)
	layout := newSlotLayout(assigned.primary, len(centroids))
	assignTime := time.Since(stage)
	codes, codeWords, err := encode(ctx, logger, vectors, layout, quantizer)
	require.NoError(t, err)
	side := &sidecar{codeWords: codeWords, codes: codes, postingOf: postingOfSlots(layout.start)}
	result.BuildStage = map[string]float64{"partition": partitionTime.Seconds(), "assign": assignTime.Seconds()}
	result.Config = map[string]any{
		"targetPostingSize": target, "codeBits": uc.CodeBits, "edges": uc.Edges, "rescueLimit": uc.RescueLimit,
		"probes": probes, "postings": len(centroids),
	}
	t.Logf("partition %.1fs, assign %.1fs, %d postings", partitionTime.Seconds(), assignTime.Seconds(), len(centroids))

	truth := sampleEdgeTruth(data, *benchSample, uc.Edges, layout.slotOf, side.postingOf)
	for _, probe := range probes {
		begin := time.Now()
		edgeStart, edges, scored, err := buildEdges(ctx, logger, edgeInput{
			vectors:   vectors,
			layout:    layout,
			assign:    assigned,
			side:      side,
			quantizer: quantizer,
			provider:  cfg.DistanceProvider,
			probe:     probe,
			neighbors: uc.Edges,
			rescue:    uc.RescueLimit,
		})
		require.NoError(t, err)
		point := probePoint{
			Probe:               probe,
			EdgeSec:             time.Since(begin).Seconds(),
			CandidatesPerVector: float64(scored) / float64(n),
			EdgesPerVector:      float64(len(edges)) / float64(n),
			EdgeRecall: truth.recall(func(slot uint32) []uint32 {
				return edges[edgeStart[slot]:edgeStart[slot+1]]
			}),
		}
		result.ProbeSweep = append(result.ProbeSweep, point)
		t.Logf("probe=%d edges %.1fs, %.0f candidates and %.2f edges per vector, edge recall %.4f",
			probe, point.EdgeSec, point.CandidatesPerVector, point.EdgesPerVector, point.EdgeRecall)
		writeBenchResult(t, result)
	}
}

// edgeRecallPoint is the edge recall of one way of building the edges.
type edgeRecallPoint struct {
	CodeBits            int     `json:"codeBits"`
	Probe               int     `json:"probe"`
	Rescue              int     `json:"rescue"`
	CandidatesPerVector float64 `json:"candidatesPerVector"`
	// MicrosPerVector is the CPU time of the neighbor search of one
	// sampled vector. The sample is scattered over the postings, so its
	// candidate codes are colder than those of a build, which walks the
	// vectors posting by posting: an upper bound of the build cost.
	MicrosPerVector float64 `json:"microsPerVector"`
	EdgeRecall      float64 `json:"edgeRecall"`
}

// benchEdgeRecallGrid measures the edge recall over code widths, probe widths
// and rescue limits without building any edges but those of the sample the
// recall is measured on: the neighbor search of the edge builder, run for the
// sampled vectors only. One partition and assignment serve the whole grid,
// since neither depends on the codes.
func benchEdgeRecallGrid(t *testing.T, ctx context.Context, data *benchData, result *benchResult) {
	logger := logrus.New()
	_, tempVectorForID := benchThunks(data)
	cfg := Config{
		ID:                           "bench",
		RootPath:                     t.TempDir(),
		Logger:                       logger,
		DistanceProvider:             distancer.NewCosineDistanceProvider(),
		TempVectorForIDWithViewThunk: tempVectorForID,
		GetViewThunk:                 func() common.BucketView { return noopBucketView{} },
		Seed:                         1,
	}
	require.NoError(t, cfg.validate())
	uc := NewDefaultUserConfig()
	uc.TrainingLimit = *benchTraining
	probes, rescues := parseInts(t, *benchProbes), parseInts(t, *benchRescues)
	vectors, n := asTheIndexStoresThem(data), len(data.train)

	// The posting size is taken as given, or from the full code: it must
	// not move with the code width, or the grid would compare partitions.
	full, err := trainQuantizer(vectors, data.dims, uc.TrainingLimit, 0, &cfg)
	require.NoError(t, err)
	target := targetPostingSize(*benchPosting, 8*len(full.Encode(vectors[0])))
	postings, widest := int(math.Ceil(float64(n)/float64(target))), slices.Max(probes)

	// The partition and the assignment of an index built with the widest
	// probe of the grid, taken from -bench.buildcache where a build left
	// them there.
	share := &buildShare{}
	var cacheDir, train string
	if *benchBuildCache != "" {
		cacheDir = buildCacheDir(data, *benchPosting, widest)
		train, err = trainFingerprint(data.dir)
		require.NoError(t, err)
		share, err = loadBuildShare(cacheDir, train)
		require.NoError(t, err)
	}
	if share.filled() {
		require.NoError(t, share.fits(n, postings, widest))
		t.Logf("partition and assignment taken from %s", cacheDir)
	} else {
		centroids, err := partition(ctx, logger, vectors, data.dims, postings, cfg.Seed)
		require.NoError(t, err)
		assigned, err := assign(ctx, logger, vectors, centroids, widest)
		require.NoError(t, err)
		*share = buildShare{vectors: n, postings: postings, probe: widest, centroids: dropEmpty(centroids, assigned), assigned: assigned}
		if cacheDir != "" {
			require.NoError(t, saveBuildShare(cacheDir, train, share))
			t.Logf("partition and assignment kept in %s", cacheDir)
		}
	}
	centroids, assigned := share.centroids, share.assigned
	layout := newSlotLayout(assigned.primary, len(centroids))
	postingOf := postingOfSlots(layout.start)
	result.Config = map[string]any{
		"targetPostingSize": target, "edges": uc.Edges, "postings": len(centroids),
		"probes": probes, "rescues": rescues, "sample": *benchSample,
	}
	t.Logf("%d postings of %d vectors on average", len(centroids), target)

	truth := sampleEdgeTruth(data, *benchSample, uc.Edges, layout.slotOf, postingOf)

	// Probe widths the assignment does not have, for the sample alone: the
	// neighbor search of a vector reads its own probes and nothing else of
	// the assignment but the primary postings.
	type probeWidth struct {
		probe  int
		assign *assignment
	}
	var widths []probeWidth
	for _, probe := range probes {
		widths = append(widths, probeWidth{probe: probe, assign: assigned})
	}
	if *benchSampleProbes != "" {
		wider := parseInts(t, *benchSampleProbes)
		wide := sampleAssignment(t, ctx, logger, vectors, centroids, assigned, layout, truth.slots, slices.Max(wider))
		for _, probe := range wider {
			widths = append(widths, probeWidth{probe: probe, assign: wide})
		}
		result.Config["sampleProbes"] = wider
	}

	for _, codeBits := range parseInts(t, *benchCodeList) {
		quantizer, err := trainQuantizer(vectors, data.dims, uc.TrainingLimit, codeBits, &cfg)
		require.NoError(t, err)
		codes, codeWords, err := encode(ctx, logger, vectors, layout, quantizer)
		require.NoError(t, err)
		side := &sidecar{codeWords: codeWords, codes: codes, postingOf: postingOf}

		for _, width := range widths {
			probe := width.probe
			for _, rescue := range rescues {
				in := edgeInput{
					vectors:   vectors,
					layout:    layout,
					assign:    width.assign,
					side:      side,
					quantizer: quantizer,
					provider:  cfg.DistanceProvider,
					probe:     probe,
					neighbors: uc.Edges,
					rescue:    rescue,
				}
				found := make([][]uint32, len(truth.slots))
				var ranked, spent atomic.Int64
				parallelFor(len(truth.slots), func(i int) {
					var sc edgeScratch
					begin := time.Now()
					nearest, count, err := in.nearestNeighbors(truth.slots[i], &sc)
					if err != nil {
						panic(err)
					}
					spent.Add(int64(time.Since(begin)))
					ranked.Add(count)
					for _, nb := range nearest {
						found[i] = append(found[i], uint32(nb.id))
					}
				})
				bySlot := map[uint32][]uint32{}
				for i, slot := range truth.slots {
					bySlot[slot] = found[i]
				}
				point := edgeRecallPoint{
					CodeBits:            codeBits,
					Probe:               probe,
					Rescue:              rescue,
					CandidatesPerVector: float64(ranked.Load()) / float64(len(truth.slots)),
					MicrosPerVector:     float64(spent.Load()) / 1e3 / float64(len(truth.slots)),
					EdgeRecall:          truth.recall(func(slot uint32) []uint32 { return bySlot[slot] }),
				}
				result.EdgeRecall = append(result.EdgeRecall, point)
				t.Logf("code bits %d, probe %d, rescue %d: %.0f candidates and %.0f us per vector, edge recall %.4f",
					codeBits, probe, rescue, point.CandidatesPerVector, point.MicrosPerVector, point.EdgeRecall)
				writeBenchResult(t, result)
			}
		}
	}
}

// sampleAssignment is an assignment with probes of the given width for the
// vectors in the sampled slots and for no other: the exact nearest centroids
// of each, computed as a build computes them. The probes of the whole train
// set at that width would take the full assignment again, hours of it at ten
// million vectors, and as much memory as the width is wide.
//
// The probes array still spans the train set, since a vector is found in it
// by its position. Only the rows of the sample are written, and memory that
// is never touched is never resident.
func sampleAssignment(t *testing.T, ctx context.Context, logger logrus.FieldLogger, vectors, centroids [][]float32,
	assigned *assignment, layout slotLayout, slots []uint32, probe int,
) *assignment {
	sample := make([][]float32, len(slots))
	for i, slot := range slots {
		sample[i] = vectors[layout.order[slot]]
	}
	own, err := assign(ctx, logger, sample, centroids, probe)
	require.NoError(t, err)

	wide := &assignment{primary: assigned.primary, probes: make([]uint32, len(vectors)*own.width), width: own.width}
	shared := min(assigned.width, own.width)
	for i, slot := range slots {
		input := int(layout.order[slot])
		require.Equalf(t, assigned.primary[input], own.primary[i], "vector %d: the sample and the build disagree on its posting", input)
		// The probes of a vector are its nearest centroids, nearest first,
		// so the narrower list is the head of the wider one.
		require.Equalf(t, assigned.probesOf(input)[:shared], own.probesOf(i)[:shared],
			"vector %d: the probes computed for the sample are not those of the build", input)
		copy(wide.probesOf(input), own.probesOf(i))
	}
	return wide
}

// asTheIndexStoresThem returns the train vectors bit for bit as an index
// holds them while it builds. The index normalizes what it is given, and
// normalizing a vector that is already of unit length still moves its last
// bits, which is enough for the partition to come out different. The modes
// that run the build stages by hand use this so that their partition is the
// one an index builds. It normalizes the train set in place.
func asTheIndexStoresThem(data *benchData) [][]float32 {
	parallelFor(len(data.train), func(i int) { distancer.NormalizeInPlace(data.train[i]) })
	return data.train
}

// loadBenchData reads the dataset in dir. With deferTrain it leaves the train
// set in its file, for the build to stream and loadTrain to read afterwards.
func loadBenchData(t *testing.T, dir string, limit, queries, k int, deferTrain bool) *benchData {
	var raw *testinghelpers.RawDataset
	var err error
	if deferTrain {
		raw, err = testinghelpers.ReadRawQueries(dir, queries)
	} else {
		raw, err = testinghelpers.ReadRawDataset(dir, limit, queries)
	}
	require.NoError(t, err)
	data := &benchData{name: raw.Name, dims: raw.Dims, dir: dir, rows: raw.TrainRows, train: raw.Train, queries: raw.Queries}
	if !deferTrain {
		data.rows = len(data.train)
	}
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
	require.False(t, deferTrain, "the dataset ships no ground truth for %d neighbors, and the train set is not in memory to compute it", k)
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

// loadTrain reads the train set that loadBenchData left in its file, and
// leaves it as loadBenchData would have.
func (d *benchData) loadTrain(t *testing.T) {
	raw, err := testinghelpers.ReadRawDataset(d.dir, 0, 1)
	require.NoError(t, err)
	d.train = raw.Train
	parallelFor(len(d.train), func(i int) { distancer.NormalizeInPlace(d.train[i]) })
	t.Logf("train set loaded after the build: %d vectors", len(d.train))
}

func writeBenchResult(t *testing.T, result *benchResult) {
	writeBenchResultTo(t, result, *benchOut)
}

// writeBenchResultTo writes the result to path, unless path is empty.
func writeBenchResultTo(t *testing.T, result *benchResult, path string) {
	if path == "" {
		return
	}
	raw, err := json.MarshalIndent(result, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o644))
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

func parseFloats(t *testing.T, list string) []float64 {
	var out []float64
	for _, field := range strings.Split(list, ",") {
		value, err := strconv.ParseFloat(strings.TrimSpace(field), 64)
		require.NoErrorf(t, err, "not a list of numbers: %q", list)
		out = append(out, value)
	}
	return out
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

// median is the value in the middle, and the mean of the two in the middle
// where the count is even.
func median(values []float64) float64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[middle-1] + sorted[middle]) / 2
	}
	return sorted[middle]
}

func ms(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}
