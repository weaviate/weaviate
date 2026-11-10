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

//go:build nfcorpus

package hfresh

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestNFCorpusMuvera measures HFresh+Muvera on the MVC1 cache, the baseline
// for the comparison with TachiomFresh (TestNFCorpusFrontier in
// tachiomfresh/nfcorpus_test.go). The setup follows TestLOTTESweep: default
// Muvera config, an AddMulti loop, a wait until the task queue has drained,
// then a sweep of the routing (searchProbe) and rerank (rescoreLimit)
// budgets. Distance is dot, as on the TachiomFresh side and as the ground
// truth (TestLOTTESweep uses L2).
//
// Measurement definitions shared with TestNFCorpusFrontier: build time runs
// from the first AddMulti until the index is complete (here: task queue
// drained); RAM is the live-heap delta around the build with memtables
// flushed; disk is every file under the test's temp root; QPS is
// single-threaded, queries / summed search time, k=10.
//
// Builds run in parallel and searches on one core (see parallelInsert).
//
//	TACHIOMFRESH_MVC1=/path/nfcorpus-colbertv2.bin go test -tags=nfcorpus -run 'TestNFCorpusMuvera$' -v -timeout 900m ./adapters/repos/db/vector/hfresh/...
func TestNFCorpusMuvera(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	t.Logf("command: TACHIOMFRESH_MVC1=%s go test -tags=nfcorpus -run 'TestNFCorpusMuvera$' -v -timeout 900m ./adapters/repos/db/vector/hfresh/...\n", path)

	corpus, err := loadMVC1(path)
	require.NoError(t, err)
	t.Logf("corpus: %d docs, %d queries\n", corpus.numDocs(), len(corpus.queries))
	// searches run under GOMAXPROCS=1, so HFresh's background workers and
	// the GC share the query's one core
	workers := buildWorkers()
	t.Logf("build: GOMAXPROCS=%d workers=%d; search: GOMAXPROCS=1\n", runtime.GOMAXPROCS(0), workers)

	// the documents come from the memory-mapped corpus, or from an objects
	// bucket read as a shard reads it (see objectStore); read on demand, as
	// tachiomfresh's corpus thunk does: a map of stored views would add
	// slice headers to the RAM delta
	thunk := func(ctx context.Context, id uint64) ([][]float32, error) {
		return corpus.doc(int(id)), nil
	}
	// the exact scoring reads through the buffered thunk, as tachiomfresh
	// does; over the memory-mapped corpus it hands out views, the container
	// has nothing to hold
	tempThunk := func(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
		return corpus.doc(int(id)), nil
	}
	documents := "mmap"
	if objects := openObjectStore(t, path, corpus.numDocs(), corpus.doc); objects != nil {
		defer objects.close(t)
		thunk = objects.thunk
		tempThunk = objects.tempThunk
		documents = "lsm"
	}
	t.Logf("documents: %s\n", documents)

	// The build axis mirrors TachiomFresh's: vectors per posting before a
	// split, at the default Muvera config. maxPostingSizeKB cannot reach
	// below 192 vectors here: computeMaxPostingSize is floored at
	// maxPostingSizeFloor, and at 2,560 FDE dims the 48 KB default already
	// computes to 143. So maxPostingSize is set directly (see below); 192 is
	// the default the KB budget yields. Search axis: searchProbe matters,
	// since small postings mean many of them.
	for _, maxPostingSize := range envList(t, "TACHIOMFRESH_MPS", []uint32{16, 32, 64, 128, 192, 384}) {
		// a subtest per build gives each index its own temp root, measured
		// for disk
		t.Run(fmt.Sprintf("mps=%d", maxPostingSize), func(t *testing.T) {
			before := heapLive()

			uc := muveraUserConfig()
			index, store, dir, saved := benchMuveraIndex(t, uc, maxPostingSize, thunk, tempThunk)
			defer func() {
				index.Shutdown(context.Background())
				if dir != "" {
					require.NoError(t, store.Shutdown(context.Background()))
				}
			}()

			if saved != "" {
				// the build line of the run that built it
				logLines(t, saved)
				t.Logf("loaded index=hfresh-muvera maxPostingSize=%d dir=%s centroids=%d\n", maxPostingSize, dir, index.PostingSizes.Count())
				// restore derived the sizes from maxPostingSizeKB; set them
				// as the build did (see below)
				index.maxPostingSize = maxPostingSize
				index.minPostingSize = max(maxPostingSize/3, minPostingSizeFloor)
			} else {
				start := time.Now()
				require.NoError(t, index.AddMulti(t.Context(), 0, corpus.doc(0)))
				// the first insert ran initDimensions, which computed the sizes
				// from maxPostingSizeKB; override them the way setMaxPostingSize
				// derives them, minus the floor. Safe unsynchronized: one
				// posting with one vector exists, so no split or merge can be
				// reading these yet, and the parallel inserts start after.
				index.maxPostingSize = maxPostingSize
				index.minPostingSize = max(maxPostingSize/3, minPostingSizeFloor)
				require.NoError(t, parallelInsert(corpus.numDocs()-1, workers, func(i int) error {
					return index.AddMulti(t.Context(), uint64(i+1), corpus.doc(i+1))
				}))
				inserted := time.Since(start)
				index.waitForMaintenance(t)
				build := time.Since(start)

				require.NoError(t, index.store.FlushMemtables(t.Context()))
				ram := heapLive() - before
				root := dir
				if root == "" {
					root = filepath.Dir(t.TempDir())
				}
				disk := dirSize(t, root)
				line := fmt.Sprintf("build index=hfresh-muvera maxPostingSize=%d build_s=%.3f insert_s=%.3f ram_bytes=%d disk_bytes=%d centroids=%d fde_dims=%d documents=%s\n",
					maxPostingSize, build.Seconds(), inserted.Seconds(), ram, disk,
					index.Centroids.counter.Load(), atomic.LoadUint32(&index.dims), documents)
				logLines(t, line)
				if dir != "" {
					require.NoError(t, os.WriteFile(filepath.Join(dir, "build.txt"), []byte(line), 0o644))
				}
			}

			// one query at a time on one core
			prev := runtime.GOMAXPROCS(1)
			defer runtime.GOMAXPROCS(prev)

			// 10 is the floor: muveraSearchBudgets raises a probe below k=10
			// to 10. Unlike TachiomFresh's kCentroids, which is per query
			// token, searchProbe is per query (one FDE).
			for _, searchProbe := range envList(t, "TACHIOMFRESH_PROBE", []uint32{10, 16, 32, 64, 128, 256, 512}) {
				for _, rescoreLimit := range envList(t, "TACHIOMFRESH_RESCORE", []int{100, 250, 500, 1000, 2000}) {
					uc.SearchProbe = searchProbe
					uc.RQ.RescoreLimit = rescoreLimit
					require.NoError(t, index.UpdateUserConfig(uc, func() {}))

					var recall10 float64
					var took time.Duration
					for q, query := range corpus.queries {
						before := time.Now()
						ids, _, err := index.SearchByMultiVector(t.Context(), query, 10, nil)
						took += time.Since(before)
						require.NoError(t, err)
						recall10 += recallAt10(ids, corpus.groundTruth(q))
					}
					nq := float64(len(corpus.queries))
					t.Logf("cell index=hfresh-muvera maxPostingSize=%d searchProbe=%d rescoreLimit=%d recall@10=%.4f qps=%.2f mean_ms=%.3f\n",
						maxPostingSize, searchProbe, rescoreLimit, recall10/nq, nq/took.Seconds(), took.Seconds()*1000/nq)
				}
			}
			// searches enqueue merges for undersized postings (searchByFDE),
			// so the index may have changed during the sweep
			// PostingSizes counts the postings with entries; the centroid
			// counter is not restored on a reload
			t.Logf("after sweep maxPostingSize=%d: centroids %d\n", maxPostingSize, index.PostingSizes.Count())
		})
	}
}

// benchMuveraIndex opens the index of one test build. With
// TACHIOMFRESH_INDEX_DIR set, the index lives in <dir>/hfresh-muvera-mps<k>
// and survives the run, so later runs reuse it without rebuilding: the LSM
// store in store/, the task queues and the centroid
// HNSW's commit log in root/, wired as makeHFreshConfig does for hfresh's own
// restart test. saved then holds the build line of the run that built it and
// is empty when the index still has to be built. Unset, everything is in
// temp dirs through newTestIndex.
func benchMuveraIndex(t *testing.T, uc ent.UserConfig, maxPostingSize uint32, thunk func(ctx context.Context, id uint64) ([][]float32, error), tempThunk common.TempVectorForID[[]float32]) (index *HFresh, store *lsmkv.Store, dir string, saved string) {
	withThunk := func(cfg *Config) {
		cfg.MultiVectorForIDThunk = thunk
		cfg.TempMultiVectorForIDThunk = tempThunk
	}
	root := os.Getenv("TACHIOMFRESH_INDEX_DIR")
	if root == "" {
		tf := newTestIndex(t, uc, nil, withDistanceProvider(distancer.NewDotProductProvider()), withThunk)
		return tf.Index, nil, "", ""
	}

	dir = filepath.Join(root, fmt.Sprintf("hfresh-muvera-mps%d", maxPostingSize))
	if b, err := os.ReadFile(filepath.Join(dir, "build.txt")); err == nil {
		saved = string(b)
	} else {
		// a build that did not finish leaves no build.txt: start clean
		require.NoError(t, os.RemoveAll(dir))
	}
	rootPath := filepath.Join(dir, "root")
	require.NoError(t, os.MkdirAll(rootPath, 0o755))

	logger, _ := test.NewNullLogger()
	cfg := DefaultConfig()
	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger})
	cfg.Scheduler = scheduler
	cfg.RootPath = rootPath
	cfg.ID = "hfresh"
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath: rootPath,
		ID:       "centroids",
		MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(rootPath, "centroids", logger, cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		DistanceProvider:  distancer.NewDotProductProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
	}
	cfg.DistanceProvider = distancer.NewDotProductProvider()
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger
	withThunk(cfg)

	scheduler.Start()
	t.Cleanup(func() {
		scheduler.Close(t.Context())
	})

	setDelegatingTempThunk(cfg)
	cfg.VectorForIDThunk = func(context.Context, uint64) ([]float32, error) {
		return nil, fmt.Errorf("no single-vector store in this benchmark")
	}

	store = testinghelpers.NewDummyStoreFromFolder(filepath.Join(dir, "store"), t)
	createObjectsBucket(t, store)

	index, err := New(cfg, uc, store)
	require.NoError(t, err)
	index.multiVectorForID = cfg.MultiVectorForIDThunk
	return index, store, dir, saved
}

// recallAt10 returns |ids[:10] ∩ gt[:10]| / 10.
func recallAt10(ids []uint64, gt []uint32) float64 {
	want := make(map[uint64]struct{}, 10)
	for _, id := range gt[:10] {
		want[uint64(id)] = struct{}{}
	}
	hits := 0
	for _, id := range ids[:min(10, len(ids))] {
		if _, ok := want[id]; ok {
			hits++
		}
	}
	return float64(hits) / 10
}

// buildWorkers and parallelInsert are copied from
// tachiomfresh/nfcorpus_test.go: GOMAXPROCS-1 inserting goroutines, doc i
// going to worker i % workers, as Weaviate's index queue scheduler feeds a
// vector index (queue/scheduler.go).
func buildWorkers() int {
	return max(1, runtime.GOMAXPROCS(0)-1)
}

func parallelInsert(n, workers int, add func(i int) error) error {
	errs := make([]error, workers)
	done := make([]chan error, workers)
	for w := range workers {
		// a panic in a worker comes back as its error, so that a build
		// that lost documents fails instead of being measured
		done[w] = enterrors.GoWrapperWithErrorCh(func() {
			for i := w; i < n; i += workers {
				if err := add(i); err != nil {
					errs[w] = err
					return
				}
			}
		}, logrus.StandardLogger())
	}
	for w, ch := range done {
		if err := <-ch; err != nil {
			errs[w] = err
		}
	}
	return errors.Join(errs...)
}

// envList returns the comma-separated list in environment variable name,
// or def when it is unset. The test grids read it, so each corpus's builds
// and search budgets can be picked without code edits: TACHIOMFRESH_MPS for
// the builds, TACHIOMFRESH_KC and TACHIOMFRESH_KDOCS for TachiomFresh's
// search, TACHIOMFRESH_PROBE and TACHIOMFRESH_RESCORE for HFresh's. Unset,
// every test runs its default grid. Same function in
// tachiomfresh/nfcorpus_test.go.
func envList[T ~int | ~uint32](t *testing.T, name string, def []T) []T {
	env := os.Getenv(name)
	if env == "" {
		return def
	}
	var out []T
	for _, f := range strings.Split(env, ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(f), 10, 32)
		require.NoError(t, err, "%s=%q", name, env)
		out = append(out, T(v))
	}
	return out
}

// heapLive returns the live Go heap after two collections; its delta around
// a build is the index's RAM. RSS would also count freed-but-unreturned
// pages. Same function in tachiomfresh/nfcorpus_test.go.
func heapLive() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// dirSize sums the regular files under root. Called on the parent of
// t.TempDir(), which holds every temp dir of the (sub)test: the LSM store,
// the centroid HNSW root and cfg.RootPath with the task queue files.
func dirSize(t *testing.T, root string) int64 {
	var total int64
	require.NoError(t, filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	}))
	return total
}

// mvc1Corpus and loadMVC1 are trimmed from tachiomfresh/nfcorpus_test.go
// (Corpus, LoadCorpus, cacheReader): the fields this test needs, same file
// layout.
type mvc1Corpus struct {
	dims       int
	gtDepth    int
	docOffsets []uint32
	docTokens  []float32
	queries    [][][]float32
	gt         []uint32
}

func (c *mvc1Corpus) numDocs() int { return len(c.docOffsets) - 1 }

func (c *mvc1Corpus) doc(i int) [][]float32 {
	flat := c.docTokens[int(c.docOffsets[i])*c.dims : int(c.docOffsets[i+1])*c.dims]
	out := make([][]float32, 0, len(flat)/c.dims)
	for off := 0; off < len(flat); off += c.dims {
		out = append(out, flat[off:off+c.dims:off+c.dims])
	}
	return out
}

func (c *mvc1Corpus) groundTruth(q int) []uint32 {
	return c.gt[q*c.gtDepth : (q+1)*c.gtDepth]
}

// mapFloat32s maps n float32s at byte offset off of f read-only and views
// them in place. Reading a large corpus's tokens into the Go heap let the GC
// grow the process past the machine's memory, and the swapping invalidated
// the timings; mapped file pages stay out of the heap and the OS drops and
// rereads them instead of swapping. The cache is little-endian, as the host
// (arm64) is. The mapping lives as long as the test process. Same function
// in tachiomfresh/nfcorpus_test.go.
func mapFloat32s(f *os.File, off int64, n int) ([]float32, error) {
	page := int64(os.Getpagesize())
	start := off &^ (page - 1)
	b, err := syscall.Mmap(int(f.Fd()), start, int(off-start)+4*n, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	out := unsafe.Slice((*float32)(unsafe.Pointer(&b[off-start])), n)
	// read every page once, so the build does not start on a cold cache
	var sink float32
	for i := 0; i < n; i += int(page) / 4 {
		sink += out[i]
	}
	_ = sink
	return out, nil
}

func loadMVC1(path string) (*mvc1Corpus, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)

	head := make([]byte, 40)
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, fmt.Errorf("header: %w", err)
	}
	if string(head[0:4]) != "MVC1" {
		return nil, fmt.Errorf("%s is not an MVC1 cache", path)
	}
	dims := int(binary.LittleEndian.Uint32(head[4:]))
	nDocs := int(binary.LittleEndian.Uint32(head[8:]))
	nQueries := int(binary.LittleEndian.Uint32(head[12:]))
	depth := int(binary.LittleEndian.Uint32(head[16:]))
	docTokens := int(binary.LittleEndian.Uint32(head[20:]))
	queryTokens := int(binary.LittleEndian.Uint32(head[24:]))

	readU32 := func(n int) ([]uint32, error) {
		buf := make([]byte, 4*n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out := make([]uint32, n)
		for i := range out {
			out[i] = binary.LittleEndian.Uint32(buf[4*i:])
		}
		return out, nil
	}
	readF32 := func(n int) ([]float32, error) {
		u, err := readU32(n)
		if err != nil {
			return nil, err
		}
		out := make([]float32, n)
		for i, v := range u {
			out[i] = math.Float32frombits(v)
		}
		return out, nil
	}

	if _, err := readU32(nDocs); err != nil { // doc ids, unused
		return nil, fmt.Errorf("doc ids: %w", err)
	}
	docCounts, err := readU32(nDocs)
	if err != nil {
		return nil, fmt.Errorf("doc counts: %w", err)
	}
	queryCounts, err := readU32(nQueries)
	if err != nil {
		return nil, fmt.Errorf("query counts: %w", err)
	}
	if _, err := readF32(dims); err != nil { // mean, unused
		return nil, fmt.Errorf("mean: %w", err)
	}
	gt, err := readU32(nQueries * depth)
	if err != nil {
		return nil, fmt.Errorf("ground truth: %w", err)
	}
	offsets := make([]uint32, nDocs+1)
	for i, c := range docCounts {
		offsets[i+1] = offsets[i] + c
	}
	if int(offsets[nDocs]) != docTokens {
		return nil, fmt.Errorf("doc counts sum to %d tokens, header says %d", offsets[nDocs], docTokens)
	}
	// the token sections are mapped, not read: see mapFloat32s
	docsOff := int64(40 + 4*(2*nDocs+nQueries+dims+nQueries*depth))
	flatDocs, err := mapFloat32s(f, docsOff, docTokens*dims)
	if err != nil {
		return nil, fmt.Errorf("document tokens: %w", err)
	}
	flatQueries, err := mapFloat32s(f, docsOff+int64(4*docTokens*dims), queryTokens*dims)
	if err != nil {
		return nil, fmt.Errorf("query tokens: %w", err)
	}

	queries := make([][][]float32, 0, nQueries)
	off := 0
	for _, n := range queryCounts {
		q := make([][]float32, n)
		for i := range q {
			q[i] = flatQueries[off : off+dims : off+dims]
			off += dims
		}
		queries = append(queries, q)
	}

	return &mvc1Corpus{
		dims:       dims,
		gtDepth:    depth,
		docOffsets: offsets,
		docTokens:  flatDocs,
		queries:    queries,
		gt:         gt,
	}, nil
}

// logLines logs s one line at a time, so that every line of the run's
// output carries the test log prefix.
func logLines(t testing.TB, s string) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		t.Log(line)
	}
}
