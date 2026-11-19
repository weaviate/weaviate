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

package hfresh

import (
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// The real HFresh of the disk comparison of the posting-edge index, v4.0
// (adapters/repos/db/vector/postingedge/bench-report.md, "v4.0: primer
// prototipo que lee de disco"): the index of this package, with its default
// configuration, built over a dataset on a store on disk, at the level of the
// index, with no server around it. Three tests, one process each:
// TestHFreshDiskBuild builds it and finds the searchProbe of every recall;
// TestHFreshDiskCount counts what the store reads for a query;
// TestHFreshDiskTimed times it warm and cold. They mirror TestDiskWrite and
// TestDiskTimed of the posting-edge package, and share its dataset format.
//
// The exact vectors a search rescores with are read from a bucket named
// "objects", as in production: strategy Replace, the doc id as the secondary
// key. The value is the vector alone, 4 bytes per dimension; a real object
// carries its properties besides, so this is the least a rescore reads.

var (
	diskDataset = flag.String("bench.hfresh.dataset", "", "dataset directory (meta.txt, train.f32, test.f32, neighbors.i32)")
	diskDir     = flag.String("bench.hfresh.dir", "", "the directory of the index: its store and its centroid graph. For "+
		"cold measures it lives on a volume whose cache -bench.hfresh.drop drops")
	diskManifest = flag.String("bench.hfresh.manifest", "", "what TestHFreshDiskBuild leaves for the other two: the "+
		"index, its operating points and the ground truth. It must not live under -bench.hfresh.dir")
	diskOut     = flag.String("bench.hfresh.out", "", "path of the JSON result file of TestHFreshDiskCount and TestHFreshDiskTimed")
	diskLimit   = flag.Int("bench.hfresh.limit", 0, "use only the first n train vectors (0 = all); ground truth is then brute-forced")
	diskQueries = flag.Int("bench.hfresh.queries", 5000, "number of test queries")
	diskK       = flag.Int("bench.hfresh.k", 10, "top k")
	diskDrop    = flag.String("bench.hfresh.drop", "", "with TestHFreshDiskTimed: the shell command that drops the "+
		"system's cache of the files under -bench.hfresh.dir, run with the index closed before every cold batch")
	diskCold     = flag.Int("bench.hfresh.cold", 500, "with TestHFreshDiskTimed: the cold queries of every point")
	diskBudget   = flag.Int("bench.hfresh.budget", 8, "the concurrency budget of one query, which the rescore reads its vectors within")
	diskPasses   = flag.Int("bench.hfresh.passes", 3, "with TestHFreshDiskTimed: the timed warm passes of every point")
	diskRescores = flag.String("bench.hfresh.rescores", "64,100,200,350", "with TestHFreshDiskBuild: the rescore limits "+
		"searchProbe is swept with, to find for every recall the configuration that reads the least")
	diskReads = flag.String("bench.hfresh.reads", "", "what TestHFreshDiskCount left, for TestHFreshDiskTimed: the reads "+
		"of every configuration and the one that reads the least for every recall")
	diskReaders = flag.Int("bench.hfresh.readers", 1, "with TestHFreshDiskTimed: the postings of a search that are read "+
		"at once. 1 is the index as it is, which reads them one after another")
	diskPoints = flag.String("bench.hfresh.points", "", "with TestHFreshDiskTimed: the configurations to time, "+
		"comma-separated: default, and 0.95 or 0.99 for the one that reads the least for that recall (empty = all)")
)

var diskTargets = []float64{0.95, 0.99}

// diskFullProbe is the largest searchProbe whose recall is measured over all
// the queries. Above it a search reads thousands of postings, and the recall
// is measured over the first diskFewQueries queries.
const (
	diskFullProbe  = 4096
	diskFewQueries = 1000
)

const (
	diskStoreDir = "store"
	// diskCopyNodeOp is the operation the store counts the read of a node
	// under when it reads with pread.
	diskCopyNodeOp = "ReadFromSegmentcopyNode"
)

// diskBench is an HFresh index opened over a directory, and what it reads
// its exact vectors from.
type diskBench struct {
	t         *testing.T
	logger    *logrus.Logger
	dir       string
	store     *lsmkv.Store
	index     *HFresh
	scheduler *queue.Scheduler
	cycles    []cyclemanager.CycleManager
	compact   cyclemanager.CycleCallbackGroup
	dims      int
	// rescoreReads counts the vectors read from the objects bucket.
	rescoreReads atomic.Int64
}

func diskObjectKey(id uint64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[8:], id)
	return key
}

func diskDocIDKey(id uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, id)
	return key
}

// diskQuiet turns off the mutex profile the tests of this package run with
// (TestMain samples every contended unlock, with a walk of the stack each):
// it is not part of the index, and a timed search would pay for it.
func diskQuiet(t *testing.T) {
	previous := runtime.SetMutexProfileFraction(0)
	t.Cleanup(func() { runtime.SetMutexProfileFraction(previous) })
}

// openDiskBench opens the index under dir, creating it if the directory is
// empty. With train the index reads its exact vectors from memory, which is
// how it is built: a split reads the vector of every member of a posting.
// Without it, it reads them from the objects bucket, as a search in
// production does. background runs the flush and compaction cycles of the
// store, as a server does while it imports. pread makes the store read every
// node with one positional read, which is the mode it counts reads in.
func openDiskBench(t *testing.T, dir string, dims int, train [][]float32, background, pread bool, metrics *lsmkv.Metrics,
) *diskBench {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	b := &diskBench{t: t, logger: logger, dir: dir, dims: dims}

	// The compactions are always there to be run: by their cycle while the
	// index is built, and by hand afterwards, for what the build leaves.
	flush := cyclemanager.NewCallbackGroupNoop()
	compaction := cyclemanager.NewCallbackGroup("compaction", logger, 1)
	if background {
		flush = cyclemanager.NewCallbackGroup("flush", logger, 1)
		for _, cycle := range []cyclemanager.CycleManager{
			cyclemanager.NewManager("flush", cyclemanager.MemtableFlushCycleTicker(false), flush.CycleCallback, logger),
			cyclemanager.NewManager("compaction", cyclemanager.CompactionCycleTicker(false), compaction.CycleCallback, logger),
		} {
			cycle.Start()
			b.cycles = append(b.cycles, cycle)
		}
	}
	b.compact = compaction
	root := filepath.Join(dir, diskStoreDir)
	store, err := lsmkv.New(root, root, logger, metrics, nil, compaction, cyclemanager.NewCallbackGroupNoop(), flush)
	require.NoError(t, err)
	b.store = store
	bucketOptions := func(strategy string, custom ...lsmkv.BucketOption) []lsmkv.BucketOption {
		return append(lsmkv.MakeRegularBucketOptions(strategy, custom...), lsmkv.WithPread(pread))
	}
	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		bucketOptions(lsmkv.StrategyReplace, lsmkv.WithSecondaryIndices(1))...))

	cfg := DefaultConfig()
	cfg.Logger = logger
	cfg.RootPath = dir
	cfg.ID = "hfresh"
	cfg.DistanceProvider = distancer.NewCosineDistanceProvider()
	cfg.Store = StoreConfig{MakeBucketOptions: bucketOptions}
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	b.scheduler = queue.NewScheduler(queue.SchedulerOptions{Logger: logger})
	b.scheduler.Start()
	cfg.Scheduler = b.scheduler
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		Logger:     logger,
		RootPath:   dir,
		ID:         "centroids",
		HFreshMode: true,
		MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(dir, "centroids", logger, cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		DistanceProvider:  distancer.NewCosineDistanceProvider(),
		MakeBucketOptions: bucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
		GetViewThunk:      getViewThunk,
	}
	if train != nil {
		cfg.VectorForIDThunk = func(_ context.Context, id uint64) ([]float32, error) {
			if id >= uint64(len(train)) {
				return nil, storobj.NewErrNotFoundf(id, "no vector")
			}
			return train[id], nil
		}
		setDelegatingTempThunk(cfg)
	} else {
		cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
			bucket := store.Bucket(helpers.ObjectsBucketLSM)
			raw, err := bucket.GetBySecondary(ctx, 0, diskDocIDKey(id))
			if err != nil {
				return nil, err
			}
			return b.decode(id, raw, nil)
		}
		// As Shard.readVectorByIndexIDIntoSliceWithView reads it.
		cfg.TempVectorForIDWithViewThunk = func(ctx context.Context, id uint64, container *common.VectorSlice,
			view common.BucketView,
		) ([]float32, error) {
			binary.LittleEndian.PutUint64(container.Buff8, id)
			bucketView, ok := view.(lsmkv.BucketConsistentView)
			if !ok || bucketView.Bucket == nil {
				return nil, errors.Errorf("the view of the objects bucket is a %T", view)
			}
			raw, buf, err := bucketView.Bucket.GetBySecondaryWithBufferAndView(ctx, 0, container.Buff8, container.Buff, bucketView)
			if err != nil {
				return nil, err
			}
			container.Buff = buf
			b.rescoreReads.Add(1)
			vec, err := b.decode(id, raw, container.Slice)
			if err != nil {
				return nil, err
			}
			container.Slice = vec
			return vec, nil
		}
	}

	index, err := New(cfg, ent.NewDefaultUserConfig(), store)
	require.NoError(t, err)
	// PostStartup, with the version map warmed before anything is searched:
	// while it warms in the background, a search reads the versions it
	// misses from the store.
	index.Centroids.hnsw.PostStartup(ctx)
	index.warmVersionMap()
	// The index reads the postings of a search one after another. With
	// -bench.hfresh.readers the benchmark has it read several at once.
	index.PostingStore.readConcurrency, index.PostingStore.readLogger = *diskReaders, logger
	b.index = index
	return b
}

// decode turns the value of the objects bucket into the vector of a doc.
func (b *diskBench) decode(id uint64, raw []byte, into []float32) ([]float32, error) {
	if raw == nil {
		return nil, storobj.NewErrNotFoundf(id, "no object for doc id")
	}
	if len(raw) != 4*b.dims {
		return nil, errors.Errorf("the object of doc %d takes %d bytes, expected %d", id, len(raw), 4*b.dims)
	}
	if cap(into) < b.dims {
		into = make([]float32, b.dims)
	}
	into = into[:b.dims]
	for i := range into {
		into[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[4*i:]))
	}
	return into, nil
}

// close shuts the index and its store down. The store flushes what it holds.
func (b *diskBench) close() {
	ctx := context.Background()
	for _, cycle := range b.cycles {
		<-cycle.Stop(ctx)
	}
	require.NoError(b.t, b.index.Shutdown(ctx))
	require.NoError(b.t, b.scheduler.Close(ctx))
	require.NoError(b.t, b.store.Shutdown(ctx))
}

// compactAll runs the compaction of the store until it finds nothing to do.
func (b *diskBench) compactAll() int {
	rounds := 0
	for b.compact.CycleCallback(func() bool { return false }) {
		rounds++
	}
	return rounds
}

// search runs one query with the budget of the benchmark, and tells where its
// time went.
func (b *diskBench) search(ctx context.Context, query []float32, k int) ([]uint64, map[string]any, error) {
	ctx = helpers.InitSlowQueryDetails(concurrency.CtxWithBudget(ctx, *diskBudget))
	ids, _, err := b.index.SearchByVector(ctx, query, k, nil)
	return ids, helpers.ExtractSlowQueryDetails(ctx), err
}

// setSearch sets the two search tunables of the index: the postings a search
// reads and the candidates it rescores.
func (b *diskBench) setSearch(probe, rescore int) {
	uc := ent.NewDefaultUserConfig()
	uc.SearchProbe = uint32(probe)
	uc.RQ.RescoreLimit = rescore
	require.NoError(b.t, b.index.UpdateUserConfig(uc, func() {}))
}

// recall searches every query with the given searchProbe and rescore limit
// and tells the recall.
func (b *diskBench) recall(queries [][]float32, truth [][]uint64, k, probe, rescore int) float64 {
	b.setSearch(probe, rescore)
	var hits atomic.Int64
	err := compressionhelpers.ConcurrentlyWithError(b.logger, uint64(len(queries)), func(i uint64) error {
		ids, _, err := b.search(context.Background(), queries[i], k)
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
		return nil
	})
	require.NoError(b.t, err)
	return float64(hits.Load()) / float64(len(queries)*k)
}

// diskFiles sums what the index takes on disk, by what it is.
func diskFiles(dir string) (map[string]int64, map[string]int, error) {
	files := map[string]int64{}
	segments := map[string]int{}
	store := filepath.Join(dir, diskStoreDir)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		// A file of the store goes to its bucket, the first directory
		// under the store; any other is of the centroid graph.
		group := "centroids"
		if rel, err := filepath.Rel(store, path); err == nil && filepath.IsLocal(rel) {
			bucket, _, _ := strings.Cut(filepath.ToSlash(rel), "/")
			group = "bucket " + bucket
			if filepath.Ext(path) == ".db" {
				segments[group]++
			}
		}
		files[group] += info.Size()
		return nil
	})
	return files, segments, err
}

// diskStoreReads tells how many node reads the stores of the process have
// counted so far, and their bytes.
func diskStoreReads() (reads, bytes uint64, err error) {
	ch := make(chan prometheus.Metric, 4096)
	monitoring.GetMetrics().FileIOReads.Collect(ch)
	close(ch)
	for m := range ch {
		var out dto.Metric
		if err := m.Write(&out); err != nil {
			return 0, 0, err
		}
		for _, label := range out.GetLabel() {
			if label.GetName() == "operation" && label.GetValue() == diskCopyNodeOp {
				reads += out.GetSummary().GetSampleCount()
				bytes += uint64(out.GetSummary().GetSampleSum())
			}
		}
	}
	return reads, bytes, nil
}

// warmSegmentIndexes reads the index of every segment under dir, so that the
// system has it cached, as warmSegmentIndexes of the posting-edge package.
func warmSegmentIndexes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".db" {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		header := make([]byte, 16)
		if _, err := file.ReadAt(header, 0); err != nil {
			return errors.Wrapf(err, "read the header of %s", filepath.Base(path))
		}
		start := int64(binary.LittleEndian.Uint64(header[8:]))
		if start < int64(len(header)) || start > info.Size() {
			return errors.Errorf("%s: the index starts at %d, the segment has %d bytes", filepath.Base(path), start, info.Size())
		}
		buf := make([]byte, info.Size()-start)
		if _, err := file.ReadAt(buf, start); err != nil && len(buf) > 0 {
			return errors.Wrapf(err, "read the index of %s", filepath.Base(path))
		}
		total += int64(len(buf))
		return nil
	})
	return total, err
}

// diskPoint is one search configuration of the index: the default one, or
// the candidate of a rescore limit for a recall, which is the smallest
// searchProbe that reaches the recall with that rescore limit.
type diskPoint struct {
	// Name is "default", or "r<rescore limit>@<recall>".
	Name string `json:"name"`
	// Target is the recall the candidate is for, 0 for the default.
	Target       float64 `json:"target"`
	Reached      bool    `json:"reached"`
	Probe        int     `json:"searchProbe"`
	RescoreLimit int     `json:"rescoreLimit"`
	Recall       float64 `json:"recall"`
	// Queries is the queries the recall was measured over: all of them, or
	// the first diskFewQueries above diskFullProbe.
	Queries int `json:"queries"`
}

type diskProbe struct {
	Probe        int     `json:"searchProbe"`
	RescoreLimit int     `json:"rescoreLimit"`
	Recall       float64 `json:"recall"`
	Queries      int     `json:"queries"`
}

type diskManifestFile struct {
	Dataset string `json:"dataset"`
	Vectors int    `json:"vectors"`
	Queries int    `json:"queries"`
	K       int    `json:"k"`
	Dims    int    `json:"dims"`
	// Config is the user config the index was built with, the default one.
	Config ent.UserConfig `json:"config"`
	// MaxPostingSize and MinPostingSize are the bounds the index derives
	// from the config and the dimensions, in entries.
	MaxPostingSize uint32 `json:"maxPostingSize"`
	MinPostingSize uint32 `json:"minPostingSize"`
	// EntryBytes is one entry of a posting: id, version and code.
	EntryBytes int `json:"entryBytes"`
	// Postings and Entries are what the build leaves, and Replication the
	// entries per vector.
	Postings    uint64  `json:"postings"`
	Entries     uint64  `json:"entries"`
	Replication float64 `json:"replication"`
	// BuildSeconds, MaintenanceSeconds and CompactionRounds are the inserts,
	// the wait for the background work they leave, and the compactions run
	// after it.
	BuildSeconds       float64 `json:"buildSeconds"`
	MaintenanceSeconds float64 `json:"maintenanceSeconds"`
	CompactionRounds   int     `json:"compactionRounds"`
	// Sweep is the recall of every searchProbe tried, with the default
	// rescore limit.
	Sweep     []diskProbe `json:"sweep"`
	MaxRecall float64     `json:"maxRecall"`
	// Default is the index searched as it comes, and Candidates one point
	// per rescore limit and recall.
	Default    diskPoint   `json:"default"`
	Candidates []diskPoint `json:"candidates"`
	// Files and Segments are what the index takes on disk and the segments
	// of every bucket.
	Files    map[string]int64 `json:"files"`
	Segments map[string]int   `json:"segments"`
	Truth    [][]uint64       `json:"truth"`
}

func diskLoad(t *testing.T, withTrain bool) (*testinghelpers.RawDataset, [][]uint64) {
	var raw *testinghelpers.RawDataset
	var err error
	if withTrain {
		raw, err = testinghelpers.ReadRawDataset(*diskDataset, *diskLimit, *diskQueries)
	} else {
		raw, err = testinghelpers.ReadRawQueries(*diskDataset, *diskQueries)
	}
	require.NoError(t, err)
	logger := logrus.New()
	// Unit vectors up front: the index normalizes what it is given anyway.
	compressionhelpers.Concurrently(logger, uint64(len(raw.Train)), func(i uint64) { distancer.NormalizeInPlace(raw.Train[i]) })
	compressionhelpers.Concurrently(logger, uint64(len(raw.Queries)), func(i uint64) { distancer.NormalizeInPlace(raw.Queries[i]) })
	if !withTrain {
		return raw, nil
	}
	truth := make([][]uint64, len(raw.Queries))
	if raw.Neighbors != nil && len(raw.Neighbors[0]) >= *diskK {
		for i := range truth {
			truth[i] = raw.Neighbors[i][:*diskK]
		}
		return raw, truth
	}
	t.Logf("brute-forcing the ground truth over %d vectors", len(raw.Train))
	provider := distancer.NewCosineDistanceProvider()
	compressionhelpers.Concurrently(logger, uint64(len(raw.Queries)), func(i uint64) {
		truth[i], _ = testinghelpers.BruteForce(logger, raw.Train, raw.Queries[i], *diskK, func(a, b []float32) float32 {
			dist, _ := provider.SingleDist(a, b)
			return dist
		})
	})
	return raw, truth
}

// TestHFreshDiskBuild builds the index with its default configuration over
// the dataset, on a store under -bench.hfresh.dir, as a server imports it:
// parallel inserts with the flush and compaction cycles of the store
// running, then the wait for the background work of the index, a last flush
// and the compactions left. It then sweeps searchProbe for the recall it
// reaches, and leaves the operating points in the manifest.
func TestHFreshDiskBuild(t *testing.T) {
	if *diskDataset == "" || *diskDir == "" {
		t.Skip("no -bench.hfresh.dataset and -bench.hfresh.dir given")
	}
	require.NotEmpty(t, *diskManifest)
	diskQuiet(t)
	ctx := context.Background()
	raw, truth := diskLoad(t, true)
	n, dims := len(raw.Train), raw.Dims
	require.NoError(t, os.RemoveAll(*diskDir))
	require.NoError(t, os.MkdirAll(*diskDir, 0o755))
	b := openDiskBench(t, *diskDir, dims, raw.Train, true, false, nil)
	uc := ent.NewDefaultUserConfig()

	// The exact vectors, where a search reads them.
	objects := b.store.Bucket(helpers.ObjectsBucketLSM)
	begin := time.Now()
	for id, vector := range raw.Train {
		value := make([]byte, 4*dims)
		for i, x := range vector {
			binary.LittleEndian.PutUint32(value[4*i:], math.Float32bits(x))
		}
		require.NoError(t, objects.Put(diskObjectKey(uint64(id)), value, lsmkv.WithSecondaryKey(0, diskDocIDKey(uint64(id)))))
	}
	require.NoError(t, objects.FlushAndSwitch())
	t.Logf("objects: %d vectors written in %s", n, time.Since(begin).Round(time.Second))

	begin = time.Now()
	err := compressionhelpers.ConcurrentlyWithError(b.logger, uint64(n), func(i uint64) error {
		return b.index.Add(ctx, i, raw.Train[i])
	})
	require.NoError(t, err)
	build := time.Since(begin)
	begin = time.Now()
	b.index.waitForMaintenance(t)
	// A search enqueues a merge for every posting it finds under the
	// minimum size: searched until none is left to merge, so that no timed
	// search rewrites a posting.
	for round := 0; ; round++ {
		require.Less(t, round, 20, "the searches keep enqueuing merges")
		b.recall(raw.Queries, truth, *diskK, int(uc.SearchProbe), uc.RQ.RescoreLimit)
		if b.index.taskQueue.Size() == 0 {
			break
		}
		b.index.waitForMaintenance(t)
	}
	maintenance := time.Since(begin)
	require.NoError(t, b.store.FlushMemtables(ctx))
	for _, cycle := range b.cycles {
		<-cycle.Stop(ctx)
	}
	b.cycles = nil
	compactions := b.compactAll()

	manifest := &diskManifestFile{
		Dataset: raw.Name, Vectors: n, Queries: len(raw.Queries), K: *diskK, Dims: dims, Config: uc,
		MaxPostingSize: b.index.maxPostingSize, MinPostingSize: b.index.minPostingSize,
		Postings: b.index.PostingSizes.Count(), Entries: b.index.PostingSizes.totalSize.Load(),
		BuildSeconds: build.Seconds(), MaintenanceSeconds: maintenance.Seconds(), CompactionRounds: compactions,
		Truth: truth,
	}
	manifest.Replication = float64(manifest.Entries) / float64(n)
	sample := b.index.quantizer.CompressedBytes(b.index.quantizer.Encode(raw.Train[0]))
	manifest.EntryBytes = 8 + 1 + len(sample)
	t.Logf("built: %d vectors in %s, maintenance %s, %d compactions | %d postings, %d entries (%.2f per vector), "+
		"posting size %d to %d entries of %d bytes | replicas %d, maxPostingSizeKB %d, searchProbe %d, rescoreLimit %d, rq bits %d",
		n, build.Round(time.Second), maintenance.Round(time.Second), compactions, manifest.Postings, manifest.Entries,
		manifest.Replication, manifest.MinPostingSize, manifest.MaxPostingSize, manifest.EntryBytes,
		uc.Replicas, uc.MaxPostingSizeKB, uc.SearchProbe, uc.RQ.RescoreLimit, uc.RQ.Bits)

	// The recall of one configuration, measured once.
	type config struct{ probe, rescore int }
	recalls := map[config]float64{}
	queriesOf := func(probe int) int {
		if probe > diskFullProbe {
			return min(diskFewQueries, len(raw.Queries))
		}
		return len(raw.Queries)
	}
	recall := func(probe, rescore int) float64 {
		at := config{probe, rescore}
		if _, done := recalls[at]; !done {
			n := queriesOf(probe)
			recalls[at] = b.recall(raw.Queries[:n], truth[:n], *diskK, probe, rescore)
			t.Logf("rescoreLimit %d, searchProbe %d: recall %.4f over %d queries", rescore, probe, recalls[at], n)
			manifest.Sweep = append(manifest.Sweep, diskProbe{Probe: probe, RescoreLimit: rescore, Recall: recalls[at], Queries: n})
		}
		return recalls[at]
	}
	defaultProbe, defaultRescore := int(uc.SearchProbe), uc.RQ.RescoreLimit
	manifest.Default = diskPoint{
		Name: "default", Reached: true, Probe: defaultProbe, RescoreLimit: defaultRescore,
		Recall: recall(defaultProbe, defaultRescore), Queries: queriesOf(defaultProbe),
	}
	t.Logf("default (searchProbe %d, rescoreLimit %d): recall %.4f", defaultProbe, defaultRescore, manifest.Default.Recall)

	// searchProbe doubled from 16 to all the postings, for every rescore
	// limit. A candidate is the smallest searchProbe that reaches a recall,
	// looked for between the two values of the grid around it. With the
	// default rescore limit the whole grid is measured: it is up to which
	// recall the index gets.
	var grid []int
	for probe := 16; probe < int(manifest.Postings); probe *= 2 {
		grid = append(grid, probe)
	}
	grid = append(grid, int(manifest.Postings))
	var rescores []int
	for _, field := range strings.Split(*diskRescores, ",") {
		rescore, err := strconv.Atoi(strings.TrimSpace(field))
		require.NoError(t, err, "-bench.hfresh.rescores")
		rescores = append(rescores, rescore)
	}
	for _, rescore := range rescores {
		for _, target := range diskTargets {
			point := diskPoint{Name: fmt.Sprintf("r%d@%.2f", rescore, target), Target: target, RescoreLimit: rescore}
			low := *diskK
			for _, probe := range grid {
				if recall(probe, rescore) >= target {
					high := probe
					for high-low > 1 {
						if middle := (low + high) / 2; recall(middle, rescore) < target {
							low = middle
						} else {
							high = middle
						}
					}
					point.Reached, point.Probe, point.Recall, point.Queries = true, high, recall(high, rescore), queriesOf(high)
					break
				}
				low = probe
			}
			if point.Reached {
				t.Logf("rescoreLimit %d, recall %.2f: searchProbe %d, recall %.4f", rescore, target, point.Probe, point.Recall)
			} else {
				t.Logf("rescoreLimit %d does not reach recall %.2f", rescore, target)
			}
			manifest.Candidates = append(manifest.Candidates, point)
		}
		if rescore == defaultRescore {
			for _, probe := range grid {
				manifest.MaxRecall = max(manifest.MaxRecall, recall(probe, rescore))
			}
			t.Logf("rescoreLimit %d: the best recall over the grid is %.4f", rescore, manifest.MaxRecall)
		}
	}
	slices.SortFunc(manifest.Sweep, func(a, b diskProbe) int {
		return cmp.Or(a.RescoreLimit-b.RescoreLimit, a.Probe-b.Probe)
	})
	require.Zero(t, b.index.taskQueue.Size(), "the sweep left background work")
	b.close()

	manifest.Files, manifest.Segments, err = diskFiles(*diskDir)
	require.NoError(t, err)
	t.Logf("on disk: %v | segments: %v", manifest.Files, manifest.Segments)
	out, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(*diskManifest, out, 0o644))
}

func diskReadManifest(t *testing.T) *diskManifestFile {
	raw, err := os.ReadFile(*diskManifest)
	require.NoError(t, err)
	var manifest diskManifestFile
	require.NoError(t, json.Unmarshal(raw, &manifest))
	return &manifest
}

// diskCount is what the store reads for one query with one configuration.
type diskCount struct {
	diskPoint
	// PostingReads and PostingBytes are the reads of the postings, per
	// query, and RescoreReads and RescoreBytes those of the exact vectors.
	PostingReads float64 `json:"postingReads"`
	PostingBytes float64 `json:"postingBytes"`
	RescoreReads float64 `json:"rescoreReads"`
	RescoreBytes float64 `json:"rescoreBytes"`
	// Reads is all the reads of a query, which is what a configuration is
	// chosen by.
	Reads float64 `json:"reads"`
}

// diskCountFile is what TestHFreshDiskCount leaves: the reads of the default
// configuration and of every candidate, and for every recall the name of the
// candidate that reads the least.
type diskCountFile struct {
	Counts []diskCount       `json:"counts"`
	Best   map[string]string `json:"best"`
}

func diskTargetKey(target float64) string { return fmt.Sprintf("%.2f", target) }

// TestHFreshDiskCount counts what the store reads for a query with the
// default configuration and with every candidate, with the store reading
// with pread, the only mode it counts reads in, and picks for every recall
// the candidate that reads the least: fewest reads, then fewest bytes. The count of the store is of the whole process, so the
// reads of the rescore, which the test counts where they are asked for, are
// taken out of it.
func TestHFreshDiskCount(t *testing.T) {
	if *diskDataset == "" || *diskDir == "" {
		t.Skip("no -bench.hfresh.dataset and -bench.hfresh.dir given")
	}
	require.NotEmpty(t, *diskOut)
	diskQuiet(t)
	ctx := context.Background()
	manifest := diskReadManifest(t)
	raw, _ := diskLoad(t, false)
	require.Len(t, raw.Queries, len(manifest.Truth))
	prom := *monitoring.GetMetrics()
	prom.Registerer = monitoring.NoopRegisterer
	metrics, err := lsmkv.NewMetrics(&prom, "hfresh", "disk")
	require.NoError(t, err)
	b := openDiskBench(t, *diskDir, manifest.Dims, nil, false, true, metrics)

	// What one read of an exact vector takes, as the store counts it.
	readsBefore, bytesBefore, err := diskStoreReads()
	require.NoError(t, err)
	_, err = b.index.config.VectorForIDThunk(ctx, 0)
	require.NoError(t, err)
	readsAfter, bytesAfter, err := diskStoreReads()
	require.NoError(t, err)
	require.Equal(t, uint64(1), readsAfter-readsBefore, "one read per exact vector")
	vectorBytes := bytesAfter - bytesBefore

	file := &diskCountFile{Best: map[string]string{}}
	for _, point := range append([]diskPoint{manifest.Default}, manifest.Candidates...) {
		if !point.Reached {
			continue
		}
		queries, truth := raw.Queries[:point.Queries], manifest.Truth[:point.Queries]
		// Once unmeasured: the versions of the postings are read from the
		// store the first time and cached from then on.
		require.InDelta(t, point.Recall, b.recall(queries, truth, manifest.K, point.Probe, point.RescoreLimit), 1e-9)
		readsBefore, bytesBefore, err := diskStoreReads()
		require.NoError(t, err)
		rescoreBefore := b.rescoreReads.Load()
		recall := b.recall(queries, truth, manifest.K, point.Probe, point.RescoreLimit)
		readsAfter, bytesAfter, err := diskStoreReads()
		require.NoError(t, err)
		require.InDelta(t, point.Recall, recall, 1e-9)
		q := float64(len(queries))
		rescored := uint64(b.rescoreReads.Load() - rescoreBefore)
		count := diskCount{
			diskPoint:    point,
			PostingReads: float64(readsAfter-readsBefore-rescored) / q,
			PostingBytes: float64(bytesAfter-bytesBefore-rescored*vectorBytes) / q,
			RescoreReads: float64(rescored) / q, RescoreBytes: float64(rescored*vectorBytes) / q,
			Reads: float64(readsAfter-readsBefore) / q,
		}
		t.Logf("%s (searchProbe %d, rescoreLimit %d, recall %.4f): %.1f reads per query | postings: %.1f reads and "+
			"%.3f MB | rescore: %.1f reads and %.3f MB", point.Name, point.Probe, point.RescoreLimit, recall, count.Reads,
			count.PostingReads, count.PostingBytes/1e6, count.RescoreReads, count.RescoreBytes/1e6)
		file.Counts = append(file.Counts, count)
	}
	for _, target := range diskTargets {
		var best *diskCount
		for i := range file.Counts {
			count := &file.Counts[i]
			if count.Target != target {
				continue
			}
			if best == nil || count.Reads < best.Reads ||
				(count.Reads == best.Reads && count.PostingBytes+count.RescoreBytes < best.PostingBytes+best.RescoreBytes) {
				best = count
			}
		}
		if best == nil {
			t.Logf("recall %.2f: no rescore limit reaches it", target)
			continue
		}
		file.Best[diskTargetKey(target)] = best.Name
		t.Logf("recall %.2f: the fewest reads are those of %s, %.1f per query (searchProbe %d, rescoreLimit %d)",
			target, best.Name, best.Reads, best.Probe, best.RescoreLimit)
	}
	require.Zero(t, b.index.taskQueue.Size(), "the searches left background work")
	b.close()
	out, err := json.MarshalIndent(file, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(*diskOut, out, 0o644))
}

// diskLatencies summarizes the latencies of a set of queries.
type diskLatencies struct {
	Queries int     `json:"queries"`
	P50     float64 `json:"p50ms"`
	P95     float64 `json:"p95ms"`
	P99     float64 `json:"p99ms"`
	Mean    float64 `json:"meanms"`
	QPS     float64 `json:"qps"`
	// The parts of the mean latency the index reports: the centroid graph,
	// the read of the postings, their scan, and the rescore.
	CentroidMs float64 `json:"centroidms"`
	ReadMs     float64 `json:"readms"`
	ScanMs     float64 `json:"scanms"`
	RescoreMs  float64 `json:"rescorems"`
}

func diskMs(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }

func diskLatenciesOf(latencies []time.Duration, details []map[string]any, wall time.Duration) diskLatencies {
	out := diskLatencies{Queries: len(latencies)}
	sorted := make([]float64, len(latencies))
	n := float64(len(latencies))
	part := func(d map[string]any, key string) float64 {
		took, _ := d[key].(time.Duration)
		return diskMs(took) / n
	}
	for i, l := range latencies {
		sorted[i] = diskMs(l)
		out.Mean += diskMs(l) / n
		out.CentroidMs += part(details[i], "hfresh_centroid_search_took")
		out.ReadMs += part(details[i], "hfresh_posting_read_took")
		out.ScanMs += part(details[i], "hfresh_posting_scan_took")
		out.RescoreMs += part(details[i], "hfresh_rescore_took")
	}
	slices.Sort(sorted)
	at := func(q float64) float64 {
		return sorted[max(0, min(len(sorted)-1, int(math.Ceil(q*float64(len(sorted))))-1))]
	}
	out.P50, out.P95, out.P99 = at(0.50), at(0.95), at(0.99)
	out.QPS = n / wall.Seconds()
	return out
}

type diskTimedPoint struct {
	// Name is the configuration: "default" or the candidate's.
	Name         string          `json:"name"`
	Target       float64         `json:"target"`
	Probe        int             `json:"searchProbe"`
	RescoreLimit int             `json:"rescoreLimit"`
	Threads      int             `json:"threads"`
	Recall       float64         `json:"recall"`
	Warm         []diskLatencies `json:"warm"`
	Cold         *diskLatencies  `json:"cold,omitempty"`
	ColdHalves   []diskLatencies `json:"coldHalves,omitempty"`
	// ColdFirst is the first query of every cold batch, the one that finds
	// the least in the cache, and ColdRest the others. With one thread.
	ColdFirst    *diskLatencies `json:"coldFirst,omitempty"`
	ColdRest     *diskLatencies `json:"coldRest,omitempty"`
	ColdBatches  int            `json:"coldBatches,omitempty"`
	ColdBatch    int            `json:"coldBatch,omitempty"`
	ColdRecall   float64        `json:"coldRecall,omitempty"`
	ReopenMs     float64        `json:"reopenms,omitempty"`
	IndexBytes   int64          `json:"indexBytes,omitempty"`
	ColdRescored float64        `json:"coldRescored,omitempty"`
}

type diskTimedResult struct {
	Dataset string `json:"dataset"`
	Queries int    `json:"queries"`
	K       int    `json:"k"`
	Budget  int    `json:"budget"`
	Procs   int    `json:"gomaxprocs"`
	Access  string `json:"access"`
	Drop    string `json:"drop"`
	// Readers is the postings of a search that are read at once: 1 in the
	// index as it is.
	Readers        int              `json:"readers"`
	HeapBytes      uint64           `json:"heapBytes"`
	FootprintBytes int64            `json:"footprintBytes"`
	Files          map[string]int64 `json:"files"`
	Segments       map[string]int   `json:"segments"`
	Points         []diskTimedPoint `json:"points"`
}

// TestHFreshDiskTimed times the index TestHFreshDiskBuild built: with its
// default configuration and, for every recall, with the configuration
// TestHFreshDiskCount found to read the least, with 1 and with 8 threads,
// warm and, when
// -bench.hfresh.drop is given, cold, as TestDiskTimed of the posting-edge
// package times a cell. The store reads through its mapping, the default.
// -bench.hfresh.points times only some of the configurations.
func TestHFreshDiskTimed(t *testing.T) {
	if *diskDataset == "" || *diskDir == "" {
		t.Skip("no -bench.hfresh.dataset and -bench.hfresh.dir given")
	}
	require.NotEmpty(t, *diskOut)
	diskQuiet(t)
	ctx := context.Background()
	manifest := diskReadManifest(t)
	raw, _ := diskLoad(t, false)
	queries, truth, k := raw.Queries, manifest.Truth, manifest.K
	require.Len(t, queries, len(truth))
	// What to time: the default, and the best of every recall.
	require.NotEmpty(t, *diskReads, "TestHFreshDiskTimed reads the best configurations from -bench.hfresh.reads")
	rawCounts, err := os.ReadFile(*diskReads)
	require.NoError(t, err)
	var counts diskCountFile
	require.NoError(t, json.Unmarshal(rawCounts, &counts))
	// -bench.hfresh.points keeps some of them, so that a configuration can
	// be timed in a process of its own.
	var only []string
	if *diskPoints != "" {
		only = strings.Split(*diskPoints, ",")
		for _, which := range only {
			require.Containsf(t, []string{"default", diskTargetKey(0.95), diskTargetKey(0.99)}, which,
				"-bench.hfresh.points: %q is not default, 0.95 or 0.99", which)
		}
	}
	var points []diskPoint
	if only == nil || slices.Contains(only, "default") {
		points = append(points, manifest.Default)
	}
	for _, target := range diskTargets {
		if only != nil && !slices.Contains(only, diskTargetKey(target)) {
			continue
		}
		name, found := counts.Best[diskTargetKey(target)]
		if !found {
			t.Logf("recall %.2f is not reached with any rescore limit: nothing to time there", target)
			continue
		}
		at := slices.IndexFunc(manifest.Candidates, func(p diskPoint) bool { return p.Name == name })
		require.GreaterOrEqualf(t, at, 0, "the manifest has no candidate %q", name)
		points = append(points, manifest.Candidates[at])
	}

	b := openDiskBench(t, *diskDir, manifest.Dims, nil, false, false, nil)
	runtime.GC()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var usage syscall.Rusage
	require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &usage))
	footprint := int64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		footprint *= 1024
	}
	files, segments, err := diskFiles(*diskDir)
	require.NoError(t, err)
	result := &diskTimedResult{
		Dataset: manifest.Dataset, Queries: len(queries), K: k, Budget: *diskBudget, Procs: runtime.GOMAXPROCS(0),
		Access: "mmap", Drop: *diskDrop, Readers: *diskReaders, HeapBytes: mem.HeapInuse, FootprintBytes: footprint,
		Files: files, Segments: segments,
	}
	write := func() {
		out, err := json.MarshalIndent(result, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(*diskOut, out, 0o644))
	}
	write()
	t.Logf("heap in use %.1f MB after opening, footprint %.1f MB | on disk: %v | segments: %v",
		float64(mem.HeapInuse)/1e6, float64(footprint)/1e6, files, segments)

	run := func(which []int, threads int) ([]time.Duration, []map[string]any, int, time.Duration) {
		latencies := make([]time.Duration, len(which))
		details := make([]map[string]any, len(which))
		var next, hits atomic.Int64
		var wg sync.WaitGroup
		errs := make([]error, threads)
		begin := time.Now()
		for worker := 0; worker < threads; worker++ {
			wg.Add(1)
			enterrors.GoWrapper(func() {
				defer wg.Done()
				for {
					i := int(next.Add(1)) - 1
					if i >= len(which) {
						return
					}
					q := which[i]
					start := time.Now()
					ids, took, err := b.search(ctx, queries[q], k)
					latencies[i] = time.Since(start)
					if err != nil {
						errs[worker] = err
						return
					}
					details[i] = took
					found := 0
					for _, want := range truth[q] {
						if slices.Contains(ids, want) {
							found++
						}
					}
					hits.Add(int64(found))
				}
			}, b.logger)
		}
		wg.Wait()
		wall := time.Since(begin)
		for _, err := range errs {
			require.NoError(t, err)
		}
		return latencies, details, int(hits.Load()), wall
	}
	all := make([]int, len(queries))
	for i := range all {
		all[i] = i
	}

	for _, point := range points {
		for _, threads := range []int{1, 8} {
			b.setSearch(point.Probe, point.RescoreLimit)
			timed := diskTimedPoint{
				Name: point.Name, Target: point.Target, Probe: point.Probe, RescoreLimit: point.RescoreLimit, Threads: threads,
			}
			_, _, hits, _ := run(all, 8)
			timed.Recall = float64(hits) / float64(len(queries)*k)
			if point.Queries == len(queries) {
				require.InDelta(t, point.Recall, timed.Recall, 1e-9, "the recall of the manifest")
			}
			for pass := 0; pass < *diskPasses; pass++ {
				latencies, details, _, wall := run(all, threads)
				timed.Warm = append(timed.Warm, diskLatenciesOf(latencies, details, wall))
			}
			warm := timed.Warm[len(timed.Warm)/2]
			t.Logf("%s (searchProbe %d, rescoreLimit %d, recall %.4f), %d threads, warm: p50 %.3f ms, p95 %.3f, p99 %.3f | %.0f QPS | "+
				"read %.3f ms, scan %.3f, rescore %.3f, centroids %.3f", point.Name, point.Probe, point.RescoreLimit, timed.Recall, threads,
				warm.P50, warm.P95, warm.P99, warm.QPS, warm.ReadMs, warm.ScanMs, warm.RescoreMs, warm.CentroidMs)

			if *diskDrop != "" {
				batch := 10
				if threads > 1 {
					batch = 2 * threads
				}
				var latencies []time.Duration
				var details []map[string]any
				var halves [2]struct {
					latencies []time.Duration
					details   []map[string]any
					wall      time.Duration
				}
				var wall, reopen time.Duration
				var firstLatencies, restLatencies []time.Duration
				var firstDetails, restDetails []map[string]any
				hits := 0
				rescored := int64(0)
				for from := 0; from < *diskCold; from += batch {
					which := all[from:min(from+batch, *diskCold)]
					b.close()
					out, err := exec.CommandContext(ctx, "sh", "-c", *diskDrop).CombinedOutput()
					require.NoErrorf(t, err, "drop the cache: %s", out)
					begin := time.Now()
					b = openDiskBench(t, *diskDir, manifest.Dims, nil, false, false, nil)
					b.setSearch(point.Probe, point.RescoreLimit)
					timed.IndexBytes, err = warmSegmentIndexes(filepath.Join(*diskDir, diskStoreDir))
					require.NoError(t, err)
					reopen += time.Since(begin)

					batchLatencies, batchDetails, batchHits, batchWall := run(which, threads)
					rescored += b.rescoreReads.Load()
					half := &halves[timed.ColdBatches%2]
					half.latencies, half.details = append(half.latencies, batchLatencies...), append(half.details, batchDetails...)
					half.wall += batchWall
					latencies, details = append(latencies, batchLatencies...), append(details, batchDetails...)
					firstLatencies, firstDetails = append(firstLatencies, batchLatencies[0]), append(firstDetails, batchDetails[0])
					restLatencies, restDetails = append(restLatencies, batchLatencies[1:]...), append(restDetails, batchDetails[1:]...)
					wall += batchWall
					hits += batchHits
					timed.ColdBatches++
				}
				cold := diskLatenciesOf(latencies, details, wall)
				timed.Cold, timed.ColdBatch = &cold, batch
				if threads == 1 {
					// Their time is that of the queries themselves: one at a time.
					sum := func(latencies []time.Duration) (total time.Duration) {
						for _, l := range latencies {
							total += l
						}
						return total
					}
					first := diskLatenciesOf(firstLatencies, firstDetails, sum(firstLatencies))
					rest := diskLatenciesOf(restLatencies, restDetails, sum(restLatencies))
					timed.ColdFirst, timed.ColdRest = &first, &rest
				}
				for _, half := range halves {
					timed.ColdHalves = append(timed.ColdHalves, diskLatenciesOf(half.latencies, half.details, half.wall))
				}
				timed.ColdRecall = float64(hits) / float64(len(latencies)*k)
				timed.ReopenMs = diskMs(reopen) / float64(timed.ColdBatches)
				timed.ColdRescored = float64(rescored) / float64(len(latencies))
				t.Logf("%s (searchProbe %d, rescoreLimit %d), %d threads, cold: p50 %.3f ms, p95 %.3f, p99 %.3f | %.0f QPS | "+
					"read %.3f ms, scan %.3f, rescore %.3f | %d batches of %d, halves at p50 %.3f and %.3f ms | reopened in %.0f ms",
					point.Name, point.Probe, point.RescoreLimit, threads, cold.P50, cold.P95, cold.P99, cold.QPS, cold.ReadMs, cold.ScanMs,
					cold.RescoreMs, timed.ColdBatches, batch, timed.ColdHalves[0].P50, timed.ColdHalves[1].P50, timed.ReopenMs)
			}
			result.Points = append(result.Points, timed)
			write()
		}
	}
	require.Zero(t, b.index.taskQueue.Size(), "the searches left background work")
	b.close()
}
