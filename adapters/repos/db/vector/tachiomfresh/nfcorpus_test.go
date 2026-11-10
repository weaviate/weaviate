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

package tachiomfresh

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// TestNFCorpusRecall builds the index on the MVC1 cache written by
// bench/convert_beir_npy.py and reports recall@10 and recall@100 against
// the cache's exact MaxSim ground truth for a sweep of build and search
// parameters. Skipped unless TACHIOMFRESH_MVC1 names the cache.
//
//	TACHIOMFRESH_MVC1=/path/nfcorpus-colbertv2.bin go test -tags=nfcorpus -run TestNFCorpus -v -timeout 60m ./adapters/repos/db/vector/tachiomfresh/...
func TestNFCorpusRecall(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	t.Logf("command: TACHIOMFRESH_MVC1=%s go test -tags=nfcorpus -run TestNFCorpus -v -timeout 60m ./adapters/repos/db/vector/tachiomfresh/...\n", path)

	corpus, err := LoadCorpus(path, 0)
	require.NoError(t, err)
	corpus.loadTokenIDs(t)
	t.Logf("corpus: %d docs, %d doc tokens, %d queries, dims %d, ground truth depth %d\n",
		corpus.NumDocs(), corpus.TotalDocTokens(), corpus.NumQueries(), corpus.Dims, corpus.GTDepth)
	require.GreaterOrEqual(t, corpus.GTDepth, 100)

	// The ground truth is a dot-product MaxSim ranking and the tokens are
	// unit norm, so the dot provider reproduces the ground truth's own
	// scoring exactly and recall measures the index alone.
	corpusThunk := func(cfg *hfresh.Config) {
		cfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
			return corpus.Doc(int(id), nil), nil
		}
		cfg.TokenForIDThunk = corpus.token
		cfg.TempMultiVectorForIDThunk = corpus.tempThunk
	}

	for _, maxPostingSize := range []uint32{16, 32, 64, 128, 256} {
		// kCentroids and kDocsToScore are overwritten per sweep cell below;
		// they do not affect the build
		tf := newTestIndex(t, maxPostingSize, 1, 1, hfreshEFConstruction,
			withDiscardLogger(),
			withDistanceProvider(distancer.NewDotProductProvider()), corpusThunk)
		centerOnCorpus(t, &tf, "")

		start := time.Now()
		for i := range corpus.NumDocs() {
			require.NoError(t, corpus.add(t.Context(), tf.Index, uint64(i), i))
		}
		tf.Index.waitForMaintenance(t)
		insertTook := time.Since(start)

		var centroids, tokens int
		for _, refs := range tf.Index.postings.lists {
			if n := refs.len(); n > 0 {
				centroids++
				tokens += n
			}
		}
		require.Equal(t, corpus.TotalDocTokens(), tokens, "every token must be in exactly one posting")
		// every successful split nets one centroid; no merge has run yet
		// (searches trigger them)
		t.Logf("build maxPostingSize=%d: insert %s, splits %d, centroids %d, mean posting size %.1f\n",
			maxPostingSize, insertTook.Round(time.Millisecond), centroids-1, centroids, float64(tokens)/float64(centroids))

		for _, kCentroids := range []int{8, 16, 32, 64} {
			for _, kDocsToScore := range []int{100, 200, 500, 1000} {
				tf.Index.setSearchBudgets(uint32(kCentroids), uint32(kDocsToScore))

				var recall10, recall100 float64
				latencies := make([]time.Duration, 0, corpus.NumQueries())
				for q, query := range corpus.Queries {
					// one k=100 search per query: its top-10 equals a
					// k=10 search's, since the candidate set is
					// max(k, kDocsToScore) and kDocsToScore >= 100 here
					before := time.Now()
					ids, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 100, nil)
					latencies = append(latencies, time.Since(before))
					require.NoError(t, err)

					gt := corpus.GroundTruth(q)
					recall10 += overlap(ids, gt, 10)
					recall100 += overlap(ids, gt, 100)
				}
				var total time.Duration
				for _, l := range latencies {
					total += l
				}
				sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
				nq := float64(corpus.NumQueries())
				t.Logf("maxPostingSize=%d kCentroids=%d kDocsToScore=%d recall@10=%.4f recall@100=%.4f mean=%s p50=%s\n",
					maxPostingSize, kCentroids, kDocsToScore, recall10/nq, recall100/nq,
					(total / time.Duration(len(latencies))).Round(time.Microsecond),
					latencies[len(latencies)/2].Round(time.Microsecond))
			}
		}
	}
}

// overlap returns |ids[:k] ∩ gt[:k]| / k.
func overlap(ids []uint64, gt []uint32, k int) float64 {
	want := make(map[uint64]struct{}, k)
	for _, id := range gt[:k] {
		want[uint64(id)] = struct{}{}
	}
	hits := 0
	for _, id := range ids[:min(k, len(ids))] {
		if _, ok := want[id]; ok {
			hits++
		}
	}
	return float64(hits) / float64(k)
}

// Below: the corpus cache reader (Corpus, LoadCorpus/openCorpus,
// cacheReader). It loads the whole cache: the thunk needs the float32
// tokens resident anyway.

// Corpus is the cache written by convert_lotte.py / convert_beir_npy.py:
// documents and queries as flat float32 with per-document token offsets,
// the token mean, and each query's exact ranking of the documents.
//
// Flat rather than [][]float32 because a corpus has millions of tokens and
// one slice header per token would cost a lot of memory before a single
// coordinate. Documents are viewed as [][]float32 on demand.
type Corpus struct {
	Dims       int
	Seed       uint64
	CorpusDocs int
	GTDepth    int

	DocIDs     []uint32 // ids in the source file, for reproducibility
	DocOffsets []uint32 // len NumDocs()+1, in tokens
	DocTokens  []float32

	Queries [][][]float32

	// Mean is the token mean over the kept documents.
	Mean []float32

	// GT holds each query's exact ranking of the documents, GTDepth deep.
	GT []uint32

	// TokenIDs holds the token id of every document token, in the order of
	// DocTokens: see loadTokenIDs.
	TokenIDs []uint32
}

// loadTokenIDs reads the side file named by TACHIOMFRESH_TOKENIDS (written
// by bench/export_token_ids.py: one little-endian uint32 per document
// token). The index needs the token id of every token. The ids are a side
// file because the MVC1 cache does not carry them and its format is shared
// with another reader.
func (c *Corpus) loadTokenIDs(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_TOKENIDS")
	if path == "" {
		t.Fatal("TACHIOMFRESH_TOKENIDS is not set: the index needs the token id of every token (bench/export_token_ids.py writes them)")
	}
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, 4*c.TotalDocTokens(), len(b), "%s must hold one uint32 per document token", path)
	c.TokenIDs = make([]uint32, c.TotalDocTokens())
	for i := range c.TokenIDs {
		c.TokenIDs[i] = binary.LittleEndian.Uint32(b[4*i:])
	}
	t.Logf("token ids: %s, typed builds\n", path)
}

// add indexes corpus document doc under index doc id id, with its token
// ids (loadTokenIDs).
func (c *Corpus) add(ctx context.Context, index *TachiomFresh, id uint64, doc int) error {
	return index.AddMultiWithTypes(ctx, id, c.Doc(doc, nil), c.TokenIDs[c.DocOffsets[doc]:c.DocOffsets[doc+1]])
}

func (c *Corpus) NumDocs() int    { return len(c.DocOffsets) - 1 }
func (c *Corpus) NumQueries() int { return len(c.Queries) }

// Doc views document i as [][]float32, appending into dst[:0] so the slice
// headers are reused across candidates rather than reallocated per document.
func (c *Corpus) Doc(i int, dst [][]float32) [][]float32 {
	dst = dst[:0]
	flat := c.DocTokens[int(c.DocOffsets[i])*c.Dims : int(c.DocOffsets[i+1])*c.Dims]
	for off := 0; off < len(flat); off += c.Dims {
		dst = append(dst, flat[off:off+c.Dims:off+c.Dims])
	}
	return dst
}

// token is the token thunk over the memory-mapped corpus: token pos of
// document id, a copy, as a store's token read hands out memory of its own.
func (c *Corpus) token(ctx context.Context, id uint64, pos int, _ int) ([]float32, error) {
	if int(id) >= c.NumDocs() {
		return nil, fmt.Errorf("no doc %d", id)
	}
	n := int(c.DocOffsets[id+1] - c.DocOffsets[id])
	if pos < 0 || pos >= n {
		return nil, fmt.Errorf("doc %d has %d tokens but token %d is asked", id, n, pos)
	}
	off := (int(c.DocOffsets[id]) + pos) * c.Dims
	return slices.Clone(c.DocTokens[off : off+c.Dims]), nil
}

// tempThunk is the buffered thunk over the memory-mapped corpus: views of
// the corpus, as thunk gives; the container has nothing to hold.
func (c *Corpus) tempThunk(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
	return c.Doc(int(id), nil), nil
}

// GroundTruth returns query q's exact ranking of the documents.
func (c *Corpus) GroundTruth(q int) []uint32 {
	return c.GT[q*c.GTDepth : (q+1)*c.GTDepth]
}

// TotalDocTokens is the corpus size in tokens.
func (c *Corpus) TotalDocTokens() int { return int(c.DocOffsets[len(c.DocOffsets)-1]) }

const cacheMagic = "MVC1"

// LoadCorpus reads an MVC1 cache. It streams through a buffered reader:
// reading a large cache whole before decoding would double its footprint.
func LoadCorpus(path string, maxQueries int) (*Corpus, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rd := &cacheReader{r: bufio.NewReaderSize(f, 1<<20)}

	head := make([]byte, 40)
	if _, err := io.ReadFull(rd.r, head); err != nil {
		return nil, fmt.Errorf("header: %w", err)
	}
	if string(head[0:4]) != cacheMagic {
		return nil, fmt.Errorf("%s is not a %s cache", path, cacheMagic)
	}
	dims := int(binary.LittleEndian.Uint32(head[4:]))
	nDocs := int(binary.LittleEndian.Uint32(head[8:]))
	nQueries := int(binary.LittleEndian.Uint32(head[12:]))
	depth := int(binary.LittleEndian.Uint32(head[16:]))
	docTokens := int(binary.LittleEndian.Uint32(head[20:]))
	queryTokens := int(binary.LittleEndian.Uint32(head[24:]))
	seed := binary.LittleEndian.Uint64(head[28:])
	corpusDocs := int(binary.LittleEndian.Uint32(head[36:]))

	docIDs, err := rd.uint32s(nDocs)
	if err != nil {
		return nil, fmt.Errorf("doc ids: %w", err)
	}
	docCounts, err := rd.uint32s(nDocs)
	if err != nil {
		return nil, fmt.Errorf("doc counts: %w", err)
	}
	queryCounts, err := rd.uint32s(nQueries)
	if err != nil {
		return nil, fmt.Errorf("query counts: %w", err)
	}
	mean, err := rd.float32s(dims)
	if err != nil {
		return nil, fmt.Errorf("mean: %w", err)
	}
	gt, err := rd.uint32s(nQueries * depth)
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
		for t := range q {
			q[t] = flatQueries[off : off+dims : off+dims]
			off += dims
		}
		queries = append(queries, q)
	}
	if maxQueries > 0 && maxQueries < len(queries) {
		queries = queries[:maxQueries]
	}

	return &Corpus{
		Dims:       dims,
		Seed:       seed,
		CorpusDocs: corpusDocs,
		GTDepth:    depth,
		DocIDs:     docIDs,
		DocOffsets: offsets,
		DocTokens:  flatDocs,
		Queries:    queries,
		Mean:       mean,
		GT:         gt,
	}, nil
}

// mapFloat32s maps n float32s at byte offset off of f read-only and views
// them in place. Reading a large corpus's tokens into the Go heap let the GC
// grow the process past the machine's memory, and the swapping invalidated
// the timings; mapped file pages stay out of the heap and the OS drops and
// rereads them instead of swapping. The cache is little-endian, as the host
// (arm64) is. The mapping lives as long as the test process. Same function
// in hfresh/nfcorpus_muvera_test.go.
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

type cacheReader struct {
	r   *bufio.Reader
	buf []byte
}

// chunk is how many bytes the decoder converts at a time. Large enough that the
// read syscalls are not the cost, small enough that it never doubles a large
// section's footprint.
const chunk = 1 << 23

func (c *cacheReader) fill(n int) error {
	if cap(c.buf) < n {
		c.buf = make([]byte, n)
	}
	c.buf = c.buf[:n]
	_, err := io.ReadFull(c.r, c.buf)
	return err
}

func (c *cacheReader) uint32s(n int) ([]uint32, error) {
	out := make([]uint32, n)
	for done := 0; done < n; {
		batch := min(n-done, chunk/4)
		if err := c.fill(4 * batch); err != nil {
			return nil, err
		}
		for i := range batch {
			out[done+i] = binary.LittleEndian.Uint32(c.buf[4*i:])
		}
		done += batch
	}
	return out, nil
}

func (c *cacheReader) float32s(n int) ([]float32, error) {
	out := make([]float32, n)
	for done := 0; done < n; {
		batch := min(n-done, chunk/4)
		if err := c.fill(4 * batch); err != nil {
			return nil, err
		}
		for i := range batch {
			out[done+i] = math.Float32frombits(binary.LittleEndian.Uint32(c.buf[4*i:]))
		}
		done += batch
	}
	return out, nil
}

// TestNFCorpusFrontier measures TachiomFresh against HFresh+Muvera
// (TestNFCorpusMuvera in hfresh/nfcorpus_muvera_test.go): recall@10 and
// single-thread QPS for every cell, build time, RAM and disk for every
// build. The measurement definitions are the ones documented on
// TestNFCorpusMuvera. The grid extends TestNFCorpusRecall's with
// kDocsToScore 25 and 50, since the frontier needs points at the fast end,
// where HFresh's rescoreLimit goes down to 10, and searches with k=10 as the
// HFresh side does.
//
// Builds run in parallel and searches on one core (see parallelInsert).
//
//	TACHIOMFRESH_MVC1=/path/nfcorpus-colbertv2.bin go test -tags=nfcorpus -run 'TestNFCorpusFrontier$' -v -timeout 900m ./adapters/repos/db/vector/tachiomfresh/...
func TestNFCorpusFrontier(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	t.Logf("command: TACHIOMFRESH_MVC1=%s go test -tags=nfcorpus -run 'TestNFCorpusFrontier$' -v -timeout 900m ./adapters/repos/db/vector/tachiomfresh/...\n", path)

	corpus, err := LoadCorpus(path, 0)
	require.NoError(t, err)
	t.Logf("corpus: %d docs, %d queries\n", corpus.NumDocs(), corpus.NumQueries())
	corpus.loadTokenIDs(t)
	workers := buildWorkers()
	t.Logf("build: GOMAXPROCS=%d workers=%d; search: GOMAXPROCS=1\n", runtime.GOMAXPROCS(0), workers)

	// the documents come from the memory-mapped corpus, or from an objects
	// bucket read as a shard reads it (see objectStore)
	thunk := func(ctx context.Context, id uint64) ([][]float32, error) {
		return corpus.Doc(int(id), nil), nil
	}
	// the token thunk and the buffered document thunk over the same source
	tokenThunk := common.TokenForID(corpus.token)
	tempThunk := common.TempVectorForID[[]float32](corpus.tempThunk)
	documents := "mmap"
	// TACHIOMFRESH_INSERT_PATH: "direct" (the default) hands the index each
	// document's token ids with the tokens (AddMultiWithTypes); "reader"
	// inserts through AddMulti, which reads them back from the stored
	// object as the shard's index does. Set to either, the documents come
	// from the object store whose objects hold their token ids, so that
	// both paths read the same objects.
	insertPath := os.Getenv("TACHIOMFRESH_INSERT_PATH")
	withIDs := insertPath != ""
	if insertPath == "" {
		insertPath = "direct"
	}
	require.Contains(t, []string{"direct", "reader"}, insertPath, "TACHIOMFRESH_INSERT_PATH")
	docOf := func(i int) [][]float32 { return corpus.Doc(i, nil) }
	var objects *objectStore
	if withIDs {
		objects = openObjectStoreWithTokenIDs(t, path, corpus.NumDocs(), docOf, func(i int) []uint32 {
			return corpus.TokenIDs[corpus.DocOffsets[i]:corpus.DocOffsets[i+1]]
		})
		require.NotNil(t, objects, "TACHIOMFRESH_INSERT_PATH needs TACHIOMFRESH_OBJECT_STORE")
	} else {
		objects = openObjectStore(t, path, corpus.NumDocs(), docOf)
	}
	if objects != nil {
		defer objects.close(t)
		thunk = objects.thunk
		tokenThunk = objects.token
		tempThunk = objects.tempThunk
		documents = "lsm"
	}
	t.Logf("documents: %s\n", documents)
	t.Logf("insert path: %s\n", insertPath)
	// reads reports the object store's reads since the last call (documents,
	// tokens, bytes), zeros without one
	reads := func() string {
		if objects == nil {
			return noReads
		}
		if objects.sim != nil {
			logLines(t, objects.sim.report())
		}
		return objects.counters()
	}

	// 256 is left out as too large: no frontier point used it
	for _, maxPostingSize := range envList(t, "TACHIOMFRESH_MPS", []uint32{16, 32, 64, 128}) {
		// a subtest per build gives each index its own temp root, measured
		// for disk
		t.Run(fmt.Sprintf("mps=%d", maxPostingSize), func(t *testing.T) {
			before := heapLive()

			tf, dir, saved := benchIndexWithTokens(t, maxPostingSize, thunk, tokenThunk, tempThunk)
			centerOnCorpus(t, &tf, saved)

			if saved != "" {
				// the build and memory lines of the run that built it
				logLines(t, saved)
				t.Logf("loaded index=tachiomfresh maxPostingSize=%d dir=%s\n", maxPostingSize, dir)
			} else {
				add := func(i int) error { return corpus.add(t.Context(), tf.Index, uint64(i), i) }
				if insertPath == "reader" {
					tf.Index.tokenIDsForID = objects.tokenIDs
					add = func(i int) error { return tf.Index.AddMulti(t.Context(), uint64(i), corpus.Doc(i, nil)) }
				}
				start := time.Now()
				require.NoError(t, parallelInsert(corpus.NumDocs(), workers, add))
				inserted := time.Since(start)
				// complete when split, reassign and merge have drained, as
				// TestNFCorpusMuvera measures HFresh
				tf.Index.waitForMaintenance(t)
				build := time.Since(start)
				logLines(t, centerLine(&tf))

				require.NoError(t, tf.Index.store.FlushMemtables(t.Context()))
				ram := heapLive() - before
				root := dir
				if root == "" {
					root = filepath.Dir(t.TempDir())
				}
				disk := dirSize(t, root)
				centroids, postings, empty, entries := tf.postingStats()
				line := fmt.Sprintf("build index=tachiomfresh maxPostingSize=%d build_s=%.3f insert_s=%.3f ram_bytes=%d disk_bytes=%d centroids=%d postings=%d empty_postings=%d entries=%d reassign=%s center=%d documents=%s %s\n",
					maxPostingSize, build.Seconds(), inserted.Seconds(), ram, disk, centroids, postings, empty, entries, reassignSetting(), tf.centeredOn.Load(), documents, reads())
				logLines(t, line)
				saveBuildLine(t, dir, line)
				heapProfile(t, fmt.Sprintf("mps%d-built", maxPostingSize))
				if line := typeStats(t, &tf, corpus, maxPostingSize, dir); line != "" {
					logLines(t, line)
					saveBuildLine(t, dir, line)
				}
			}

			// one query at a time on one core
			prev := runtime.GOMAXPROCS(1)
			defer runtime.GOMAXPROCS(prev)
			for _, kCentroids := range envList(t, "TACHIOMFRESH_KC", []int{8, 16, 32, 64}) {
				for _, kDocsToScore := range envList(t, "TACHIOMFRESH_KDOCS", []int{25, 50, 100, 200, 500, 1000}) {
					tf.Index.setSearchBudgets(uint32(kCentroids), uint32(kDocsToScore))

					var recall10 float64
					var took time.Duration
					for q, query := range corpus.Queries {
						before := time.Now()
						ids, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, nil)
						took += time.Since(before)
						require.NoError(t, err)
						recall10 += overlap(ids, corpus.GroundTruth(q), 10)
					}
					nq := float64(corpus.NumQueries())
					t.Logf("cell index=tachiomfresh maxPostingSize=%d kCentroids=%d kDocsToScore=%d recall@10=%.4f qps=%.2f mean_ms=%.3f\n",
						maxPostingSize, kCentroids, kDocsToScore, recall10/nq, nq/took.Seconds(), took.Seconds()*1000/nq)
				}
			}

			// searches enqueue merges for undersized postings (hfresh's
			// trigger), so the index may have changed during the
			// sweep, as HFresh's does
			runtime.GOMAXPROCS(prev)
			tf.Index.waitForMaintenance(t)
			centroids, postings, empty, entries := tf.postingStats()
			t.Logf("after sweep maxPostingSize=%d: centroids %d postings %d empty_postings %d entries %d %s\n",
				maxPostingSize, centroids, postings, empty, entries, reads())
			heapProfile(t, fmt.Sprintf("mps%d-swept", maxPostingSize))

			// last: the breakdown tears the index down. Only for the run
			// that built it: a reloaded index's heap is not the build's.
			if saved == "" {
				saveBuildLine(t, dir, memoryBreakdown(t, &tf, maxPostingSize, before))
			} else {
				require.NoError(t, tf.Index.Shutdown(t.Context()))
			}
			if dir != "" {
				require.NoError(t, tf.store.Shutdown(t.Context()))
			}
		})
	}
}

// withDiscardLogger gives the index a logger that writes nowhere and keeps
// nothing (here and not in helper_for_test.go because only this build tag
// uses it). The default test logger records a copy of every entry for
// hasLog, at Debug level, and the merges a search sweep triggers log one
// line each (doMerge, hfresh's line): those entries would count as RAM the
// index did not account for.
func withDiscardLogger() testIndexOption {
	return func(cfg *hfresh.Config) {
		l := logrus.New()
		l.Out = io.Discard
		cfg.Logger = l
	}
}

// benchIndex opens the index of one test build. With
// TACHIOMFRESH_INDEX_DIR set, the index lives in
// <dir>/tachiomfresh-mps<k>[-reassign-<v>] and survives the run, so later
// runs reuse it without rebuilding; saved then holds the build and memory
// lines of the run that built it, and is empty when the index still has to
// be built. Unset, everything is in temp dirs.
// TACHIOMFRESH_REASSIGN picks the reassign variant: unset is hfresh's
// default of 8 neighbours, a number sets ReassignNeighbors (0: the two new
// postings only), "off" disables reassign after splits and merges.
func benchIndex(t *testing.T, maxPostingSize uint32, thunk func(ctx context.Context, id uint64) ([][]float32, error)) (tf testIndex, dir string, saved string) {
	return benchIndexWithTokens(t, maxPostingSize, thunk, nil, nil)
}

// benchIndexWithTokens is benchIndex with the token thunk and the buffered
// document thunk (nil: the document thunk serves every read).
func benchIndexWithTokens(t *testing.T, maxPostingSize uint32, thunk func(ctx context.Context, id uint64) ([][]float32, error), tokenThunk common.TokenForID, tempThunk common.TempVectorForID[[]float32]) (tf testIndex, dir string, saved string) {
	name := fmt.Sprintf("tachiomfresh-mps%d", maxPostingSize)
	reassign := os.Getenv("TACHIOMFRESH_REASSIGN")
	if reassign != "" {
		name += "-reassign-" + reassign
	}
	if root := os.Getenv("TACHIOMFRESH_INDEX_DIR"); root != "" {
		dir = filepath.Join(root, name)
		if b, err := os.ReadFile(filepath.Join(dir, "build.txt")); err == nil {
			saved = string(b)
		} else {
			// a build that did not finish leaves no build.txt: start clean
			require.NoError(t, os.RemoveAll(dir))
		}
	}
	tf = newTestIndexAt(t, dir, maxPostingSize, 1, 1, hfreshEFConstruction,
		withDiscardLogger(),
		withDistanceProvider(distancer.NewDotProductProvider()),
		func(cfg *hfresh.Config) {
			cfg.MultiVectorForIDThunk = thunk
			cfg.TokenForIDThunk = tokenThunk
			cfg.TempMultiVectorForIDThunk = tempThunk
			if reassign != "" && reassign != "off" {
				n, err := strconv.Atoi(reassign)
				require.NoError(t, err, "TACHIOMFRESH_REASSIGN=%q", reassign)
				cfg.ReassignNeighbors = n
			}
		})
	tf.Index.opts.disableReassign = reassign == "off"
	return tf, dir, saved
}

func reassignSetting() string {
	if v := os.Getenv("TACHIOMFRESH_REASSIGN"); v != "" {
		return v
	}
	return strconv.Itoa(hfresh.DefaultReassignNeighbors)
}

// centerOnCorpus gives a fresh index the production threshold: it stages
// the first defaultCenterTokens document tokens and builds itself on their
// mean (staging.go). The test helper sets the threshold to 0 for the
// package tests. A saved index restored its own phase and mean and gets
// nothing.
func centerOnCorpus(t *testing.T, tf *testIndex, saved string) {
	t.Helper()
	if saved != "" {
		return
	}
	tf.Index.setCenterTokens(defaultCenterTokens)
}

// centerLine reports the mean a built index centers on, for the output:
// how many tokens it was computed from (the staged tokens when the
// threshold was reached, at least defaultCenterTokens, the whole of a
// corpus under it) and its norm.
func centerLine(tf *testIndex) string {
	var norm float64
	for _, x := range tf.Index.center {
		norm += float64(x) * float64(x)
	}
	return fmt.Sprintf("center: the mean of the first %d document tokens, norm %.4f\n", tf.centeredOn.Load(), math.Sqrt(norm))
}

// saveBuildLine appends a build or memory line to the saved index's
// build.txt, which a later run prints instead of rebuilding.
func saveBuildLine(t *testing.T, dir, line string) {
	if dir == "" {
		return
	}
	f, err := os.OpenFile(filepath.Join(dir, "build.txt"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteString(line)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// postingStats counts the centroids, the postings with entries, the
// postings whose centroid exists but that reassign emptied, and the entries.
// Call it with the queues drained.
func (tf *testIndex) postingStats() (centroids, postings, empty, entries int) {
	for id, refs := range tf.Index.postings.lists {
		exists := tf.Index.Centroids.Exists(uint64(id))
		if exists {
			centroids++
		}
		switch {
		case refs.len() > 0:
			postings++
			entries += refs.len()
		case exists:
			empty++
		}
	}
	return centroids, postings, empty, entries
}

// TestNFCorpusMemoryBreakdown splits each build's RAM into its parts: the
// builds of TestNFCorpusFrontier, without the search grid.
//
//	TACHIOMFRESH_MVC1=/path/nfcorpus-colbertv2.bin go test -tags=nfcorpus -run 'TestNFCorpusMemoryBreakdown$' -v -timeout 60m ./adapters/repos/db/vector/tachiomfresh/...
func TestNFCorpusMemoryBreakdown(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	t.Logf("command: TACHIOMFRESH_MVC1=%s go test -tags=nfcorpus -run 'TestNFCorpusMemoryBreakdown$' -v -timeout 60m ./adapters/repos/db/vector/tachiomfresh/...\n", path)

	corpus, err := LoadCorpus(path, 0)
	require.NoError(t, err)
	t.Logf("corpus: %d docs, %d queries\n", corpus.NumDocs(), corpus.NumQueries())
	corpus.loadTokenIDs(t)
	workers := buildWorkers()
	t.Logf("build: GOMAXPROCS=%d workers=%d\n", runtime.GOMAXPROCS(0), workers)

	// same builds as TestNFCorpusFrontier
	for _, maxPostingSize := range envList(t, "TACHIOMFRESH_MPS", []uint32{16, 32, 64, 128}) {
		t.Run(fmt.Sprintf("mps=%d", maxPostingSize), func(t *testing.T) {
			before := heapLive()

			tf := newTestIndex(t, maxPostingSize, 1, 1, hfreshEFConstruction,
				withDiscardLogger(),
				withDistanceProvider(distancer.NewDotProductProvider()),
				func(cfg *hfresh.Config) {
					cfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
						return corpus.Doc(int(id), nil), nil
					}
					cfg.TokenForIDThunk = corpus.token
					cfg.TempMultiVectorForIDThunk = corpus.tempThunk
				})
			centerOnCorpus(t, &tf, "")
			require.NoError(t, parallelInsert(corpus.NumDocs(), workers, func(i int) error {
				return corpus.add(t.Context(), tf.Index, uint64(i), i)
			}))
			tf.Index.waitForMaintenance(t)
			require.NoError(t, tf.Index.store.FlushMemtables(t.Context()))

			memoryBreakdown(t, &tf, maxPostingSize, before)
		})
	}
}

// memoryBreakdown frees the index one part at a time and prints the live
// heap each step gives back (a measured drop needs no model of Go's or
// hnsw's internals). before is heapLive() taken before the index was
// created, as for ram_bytes. It leaves the index unusable.
//
//   - types: the posting types and the centroid ids of each token type,
//     printed last as types_bytes;
//   - postings: the posting table;
//   - vector_cache: hnsw Shutdown, which releases the RQ-8 centroid codes
//     (releaseVectors) and nothing else held in memory;
//   - graph: dropping the HNSW itself: nodes, links and the rest of it,
//     including every centroid a split replaced (the test harness never
//     cleans tombstones: TombstoneCallbacks is a noop group);
//   - memtable: what the LSM store's memtables give back when flushed,
//     measured first. After a search grid they hold the writes of the
//     merges and reassigns the searches triggered. In production the
//     shard's flush cycle empties a memtable at 10 MB of logical size or
//     after 60 s dirty; the harness store has no cycle callbacks
//     (testinghelpers.NewDummyStore), so only an explicit flush does, the
//     one ram_bytes is measured after;
//   - other: what is left above before: the version map, the task queues,
//     the locks, the id sequence, the HNSW commit logger (present after
//     the build too).
//
// The posting drop is split further by counting the table (exact byte
// counts, not measured): the payload, the entries at their posting's widths
// (a posting is a packed byte array); the padding, always 0; the capacity
// the arrays leave unused, 0 as the arrays are exactly sized; the headers,
// 24 B per slot of lists plus each posting's width byte; and the rest of the
// drop as allocator rounding.
func memoryBreakdown(t *testing.T, tf *testIndex, maxPostingSize uint32, before int64) string {
	total := heapLive() - before

	var entries, payload, slack, live, cleared int64
	for _, refs := range tf.Index.postings.lists {
		n := int64(refs.len())
		entries += n
		slack += int64(cap(refs) - len(refs))
		if n > 0 {
			docBytes, posBytes, _ := refs.widths()
			payload += n * int64(docBytes+posBytes)
			live++
		} else {
			cleared++
		}
	}
	headerSize := int64(unsafe.Sizeof(packedPosting(nil)))
	slots := int64(len(tf.Index.postings.lists))

	h0 := heapLive()
	require.NoError(t, tf.Index.store.FlushMemtables(t.Context()))
	hf := heapLive()
	// the posting types and the groups. Their spread sums are persisted
	// first: the empty table Shutdown finds after the drop has none, and
	// the saved index must keep them.
	require.NoError(t, tf.Index.types.persistSpread())
	tf.Index.types = &typeTable{
		store:  tf.Index.types.store,
		bucket: tf.Index.types.bucket,
		groups: make(map[uint32]*typeGroup),
	}
	h1 := heapLive()
	tf.Index.postings = nil
	h2 := heapLive()
	// the index shuts down as a whole (task queues, id sequence, then the
	// hnsw), so a saved index is left consistent; the
	// queues hold nothing at this point (the caller drained them)
	require.NoError(t, tf.Index.Shutdown(t.Context()))
	h3 := heapLive()
	tf.Index.Centroids = nil
	h4 := heapLive()

	postings := h1 - h2
	var padding int64
	headers := slots*headerSize + live
	line := fmt.Sprintf("memory index=tachiomfresh maxPostingSize=%d total_bytes=%d postings_bytes=%d vector_cache_bytes=%d graph_bytes=%d other_bytes=%d "+
		"posting_payload_bytes=%d posting_padding_bytes=%d posting_slack_bytes=%d posting_headers_bytes=%d posting_rounding_bytes=%d "+
		"entries=%d live_postings=%d cleared_postings=%d types_bytes=%d memtable_bytes=%d\n",
		maxPostingSize, total, postings, h2-h3, h3-h4, h4-before,
		payload, padding, slack, headers, postings-payload-padding-slack-headers,
		entries, live, cleared, hf-h1, h0-hf)
	logLines(t, line)
	return line
}

// buildWorkers is the number of inserting goroutines: GOMAXPROCS-1, the
// default of Weaviate's index queue scheduler (queue/scheduler.go), which
// is how production feeds a vector index. Same function in
// hfresh/nfcorpus_muvera_test.go.
func buildWorkers() int {
	return max(1, runtime.GOMAXPROCS(0)-1)
}

// parallelInsert calls add for every doc in [0, n) from workers goroutines,
// doc i going to worker i % workers as the scheduler partitions tasks by
// key (sequential builds took most of the test time). A worker stops at
// its first error; all are returned. Same function in
// hfresh/nfcorpus_muvera_test.go.
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
// hfresh/nfcorpus_muvera_test.go.
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

// heapProfile writes the live heap to $TACHIOMFRESH_HEAP_PROFILE-<tag>.pprof
// when the variable is set: after a search sweep the process can hold
// memory that memoryBreakdown attributes to no part of the index, and the
// profile names the owner. Two collections first, as heapLive.
func heapProfile(t *testing.T, tag string) {
	prefix := os.Getenv("TACHIOMFRESH_HEAP_PROFILE")
	if prefix == "" {
		return
	}
	runtime.GC()
	runtime.GC()
	f, err := os.Create(fmt.Sprintf("%s-%s.pprof", prefix, tag))
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, pprof.WriteHeapProfile(f))
	t.Logf("heap profile %s\n", f.Name())
}

// heapLive returns the live Go heap after two collections; its delta around
// a build is the index's RAM. RSS would also count freed-but-unreturned
// pages. Same function in hfresh/nfcorpus_muvera_test.go.
func heapLive() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// dirSize sums the regular files under root. Called on the parent of
// t.TempDir(), which holds every temp dir of the subtest: the LSM store,
// the centroid HNSW root and cfg.RootPath.
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

// TestNFCorpusEFConstruction checks whether the centroid HNSW's
// efConstruction (64 in hfresh) caps recall: maxPostingSize 128,
// efConstruction 64/200/500, the four corner cells of the sweep.
//
//	TACHIOMFRESH_MVC1=/path/nfcorpus-colbertv2.bin go test -tags=nfcorpus -run TestNFCorpusEFConstruction -v -timeout 30m ./adapters/repos/db/vector/tachiomfresh/...
func TestNFCorpusEFConstruction(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	t.Logf("command: TACHIOMFRESH_MVC1=%s go test -tags=nfcorpus -run TestNFCorpusEFConstruction -v -timeout 30m ./adapters/repos/db/vector/tachiomfresh/...\n", path)
	corpus, err := LoadCorpus(path, 0)
	require.NoError(t, err)
	corpus.loadTokenIDs(t)

	for _, efConstruction := range []int{64, 200, 500} {
		tf := newTestIndex(t, 128, 1, 1, efConstruction,
			withDiscardLogger(),
			withDistanceProvider(distancer.NewDotProductProvider()),
			func(cfg *hfresh.Config) {
				cfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
					return corpus.Doc(int(id), nil), nil
				}
				cfg.TokenForIDThunk = corpus.token
				cfg.TempMultiVectorForIDThunk = corpus.tempThunk
			})
		centerOnCorpus(t, &tf, "")
		start := time.Now()
		for i := range corpus.NumDocs() {
			require.NoError(t, corpus.add(t.Context(), tf.Index, uint64(i), i))
		}
		tf.Index.waitForMaintenance(t)
		var centroids int
		for _, refs := range tf.Index.postings.lists {
			if refs.len() > 0 {
				centroids++
			}
		}
		t.Logf("build maxPostingSize=128 efConstruction=%d: insert %s, centroids %d\n",
			efConstruction, time.Since(start).Round(time.Millisecond), centroids)

		for _, kCentroids := range []int{8, 64} {
			for _, kDocsToScore := range []int{100, 1000} {
				tf.Index.setSearchBudgets(uint32(kCentroids), uint32(kDocsToScore))
				var recall10, recall100 float64
				for q, query := range corpus.Queries {
					ids, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 100, nil)
					require.NoError(t, err)
					gt := corpus.GroundTruth(q)
					recall10 += overlap(ids, gt, 10)
					recall100 += overlap(ids, gt, 100)
				}
				nq := float64(corpus.NumQueries())
				t.Logf("efConstruction=%d kCentroids=%d kDocsToScore=%d recall@10=%.4f recall@100=%.4f\n",
					efConstruction, kCentroids, kDocsToScore, recall10/nq, recall100/nq)
			}
		}
	}
}

// TestNFCorpusRoutingQuality is a diagnostic: a token placed before its
// posting was split may sit far from the posting's centroid. It reports the
// fraction of tokens whose posting is the nearest centroid at the end of the
// build, which bounds how well the stage 1 centroid similarity stands in for
// the token similarity.
//
//	TACHIOMFRESH_MVC1=/path/nfcorpus-colbertv2.bin go test -tags=nfcorpus -run TestNFCorpusRoutingQuality -v -timeout 30m ./adapters/repos/db/vector/tachiomfresh/...
func TestNFCorpusRoutingQuality(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	corpus, err := LoadCorpus(path, 0)
	require.NoError(t, err)
	corpus.loadTokenIDs(t)
	workers := buildWorkers()
	t.Logf("build: GOMAXPROCS=%d workers=%d\n", runtime.GOMAXPROCS(0), workers)

	// the posting sizes come from TACHIOMFRESH_MPS and the build is
	// parallel, as TestNFCorpusFrontier's, so the diagnostic describes the
	// builds the frontier measures
	for _, maxPostingSize := range envList(t, "TACHIOMFRESH_MPS", []uint32{128}) {
		routingQuality(t, corpus, maxPostingSize, workers)
	}
}

func routingQuality(t *testing.T, corpus *Corpus, maxPostingSize uint32, workers int) {
	tf, dir, saved := benchIndex(t, maxPostingSize, func(ctx context.Context, id uint64) ([][]float32, error) {
		return corpus.Doc(int(id), nil), nil
	})
	centerOnCorpus(t, &tf, saved)
	if saved == "" {
		require.NoError(t, parallelInsert(corpus.NumDocs(), workers, func(i int) error {
			return corpus.add(t.Context(), tf.Index, uint64(i), i)
		}))
		tf.Index.waitForMaintenance(t)
	}
	defer func() {
		require.NoError(t, tf.Index.Shutdown(t.Context()))
		if dir != "" {
			require.NoError(t, tf.store.Shutdown(t.Context()))
		}
	}()

	// posting of every token, from the posting table
	postingOf := make(map[tokenRef]uint64, corpus.TotalDocTokens())
	for id, refs := range tf.Index.postings.lists {
		for _, ref := range refs.decode() {
			postingOf[ref] = uint64(id)
		}
	}

	// A token can only sit with a centroid of its type, so two fractions
	// are reported: the token is with the nearest centroid of its type
	// (what the placement aims at) and with the nearest centroid overall
	// (what stage 1 of search would like).
	var checked, nearest, nearestOfType int
	var sumDistOwn, sumDistNearest float64
	for i := range corpus.NumDocs() {
		for pos, token := range corpus.Doc(i, nil) {
			res, err := tf.Index.Centroids.Search(t.Context(), token, 1, nil)
			require.NoError(t, err)
			owner := postingOf[tokenRef{DocID: uint32(i), Pos: uint16(pos)}]
			inType, err := tf.Index.nearestCentroids(token, corpus.TokenIDs[int(corpus.DocOffsets[i])+pos], 1)
			require.NoError(t, err)
			for id := range inType.Iter() {
				if id == owner {
					nearestOfType++
				}
			}
			for id, dist := range res.Iter() {
				if id == owner {
					nearest++
				}
				sumDistNearest += float64(dist)
			}
			centroid, err := tf.Index.Centroids.Get(owner)
			require.NoError(t, err)
			dist, err := tf.Index.config.DistanceProvider.SingleDist(token, centroid.Uncompressed)
			require.NoError(t, err)
			sumDistOwn += float64(dist)
			checked++
		}
	}
	t.Logf("routing quality maxPostingSize=%d reassign=%s: %d/%d tokens (%.4f) sit in the posting of their nearest centroid; "+
		"mean dot(token, own centroid)=%.4f, mean dot(token, nearest centroid)=%.4f\n",
		maxPostingSize, reassignSetting(), nearest, checked, float64(nearest)/float64(checked), -sumDistOwn/float64(checked), -sumDistNearest/float64(checked))
	t.Logf("routing quality within type maxPostingSize=%d: %d/%d tokens (%.4f) sit in the posting of the nearest centroid of their type\n",
		maxPostingSize, nearestOfType, checked, float64(nearestOfType)/float64(checked))
}

// typeStats describes the groups of the index after its build: how many
// token types there are, how the centroids spread over them, and how many
// centroids an insert of each token would scan now (the size of its type's
// group; during the build the groups were smaller). With dir set it also
// writes <dir>/types.tsv, one line per type: type, tokens, centroids, the
// type's split threshold (see postingLimits) and its spread, so the centroid
// budget per type can be studied later without rebuilding.
func typeStats(t *testing.T, tf *testIndex, corpus *Corpus, maxPostingSize uint32, dir string) string {
	tokens := make(map[uint32]int)
	for _, typ := range corpus.TokenIDs {
		tokens[typ]++
	}
	types := make([]uint32, 0, len(tokens))
	for typ := range tokens {
		types = append(types, typ)
	}
	sort.Slice(types, func(i, j int) bool { return types[i] < types[j] })

	var tsv strings.Builder
	var centroids, largest, single, scanned int
	for _, typ := range types {
		n := tf.Index.types.groupSize(typ)
		centroids += n
		largest = max(largest, n)
		if n == 1 {
			single++
		}
		scanned += n * tokens[typ]
		threshold := maxPostingSize
		// -1: no spread, the type never reached spreadObserveEntries
		spread := -1.0
		if g := tf.Index.types.group(typ); g != nil && len(g.ids) > 0 {
			_, threshold = tf.Index.postingLimits(g.ids[0])
			g.spreadMu.Lock()
			if v, ok := g.variance(); ok {
				spread = v
			}
			g.spreadMu.Unlock()
		}
		fmt.Fprintf(&tsv, "%d\t%d\t%d\t%d\t%.4f\n", typ, tokens[typ], n, threshold, spread)
	}
	if dir != "" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "types.tsv"), []byte(tsv.String()), 0o644))
	}
	return fmt.Sprintf("types index=tachiomfresh maxPostingSize=%d types=%d centroids=%d single_centroid_types=%d largest_group=%d mean_scan=%.1f\n",
		maxPostingSize, len(types), centroids, single, largest, float64(scanned)/float64(len(corpus.TokenIDs)))
}

// TestNFCorpusDynamics measures the index under updates: build it, then in
// each round delete a seeded random 20 % of the live documents and reinsert
// them under new doc ids, as a Weaviate update does. After the build, after
// each delete and after each reinsert it prints the posting statistics and
// recall@10 and QPS on a few cells.
//
// Ground truth: the cache holds each query's exact MaxSim top 100 over the
// whole corpus, so the exact top 10 over the live documents is the first 10
// live documents of that list (a query with fewer than 10 of them left is
// brute-forced instead). With TACHIOMFRESH_VERIFY_GT=1 every delete phase
// also brute-forces all queries over the live documents and requires the
// same scores, so the shortcut is checked, not assumed.
//
//	TACHIOMFRESH_MVC1=/path/nfcorpus-colbertv2.bin go test -tags=nfcorpus -run 'TestNFCorpusDynamics$' -v -timeout 900m ./adapters/repos/db/vector/tachiomfresh/...
func TestNFCorpusDynamics(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	corpus, err := LoadCorpus(path, 0)
	require.NoError(t, err)
	corpus.loadTokenIDs(t)
	workers := buildWorkers()
	const (
		rounds   = 3
		fraction = 0.2
		seed     = 1
	)
	n := corpus.NumDocs()
	perRound := int(fraction * float64(n))
	t.Logf("corpus: %d docs, %d queries; rounds=%d fraction=%.2f (%d docs per round) seed=%d; build: workers=%d; search: GOMAXPROCS=1\n",
		n, corpus.NumQueries(), rounds, fraction, perRound, seed, workers)

	// docOf[indexDocID] is the corpus document behind an index doc id, -1
	// once its record is dropped; current[corpusDoc] is its live index doc
	// id. Sized for every reinsert up front, and an entry is written before
	// its id is first used, so background tasks can read it without a lock.
	docOf := make([]atomic.Int32, n+rounds*perRound)
	current := make([]uint64, n)
	for i := range n {
		docOf[i].Store(int32(i))
		current[i] = uint64(i)
	}
	live := make([]bool, n) // by corpus doc
	for i := range live {
		live[i] = true
	}
	thunk := func(ctx context.Context, id uint64) ([][]float32, error) {
		doc := docOf[id].Load()
		if doc < 0 {
			return nil, fmt.Errorf("doc %d was deleted", id)
		}
		return corpus.Doc(int(doc), nil), nil
	}
	tokenThunk := func(ctx context.Context, id uint64, pos int, dims int) ([]float32, error) {
		doc := docOf[id].Load()
		if doc < 0 {
			return nil, fmt.Errorf("doc %d was deleted", id)
		}
		return corpus.token(ctx, uint64(doc), pos, dims)
	}
	tempThunk := func(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
		doc := docOf[id].Load()
		if doc < 0 {
			return nil, fmt.Errorf("doc %d was deleted", id)
		}
		return corpus.Doc(int(doc), nil), nil
	}

	for _, maxPostingSize := range envList(t, "TACHIOMFRESH_MPS", []uint32{16}) {
		// the documents come from the memory-mapped corpus, or from an
		// objects bucket read as a shard reads it (see objectStore), written
		// and dropped as the rounds reinsert and delete. The rounds change
		// the store, so each build gets a fresh copy of its own, removed
		// after.
		var objects *objectStore
		documents := "mmap"
		if root := os.Getenv("TACHIOMFRESH_OBJECT_STORE"); root != "" {
			dir := objectStoreDir(root, path) + "-dynamics"
			objects = openObjectStoreAt(t, dir, true, n, func(i int) [][]float32 { return corpus.Doc(i, nil) }, nil)
			thunk = objects.thunk
			tokenThunk = objects.token
			tempThunk = objects.tempThunk
			documents = "lsm"
		}
		t.Logf("documents: %s\n", documents)
		// reads reports the object store's reads since the last call
		// (documents, tokens, bytes), zeros without one
		reads := func() string {
			if objects == nil {
				return noReads
			}
			return objects.counters()
		}

		tf := newTestIndex(t, maxPostingSize, 1, 1, hfreshEFConstruction,
			withDiscardLogger(),
			withDistanceProvider(distancer.NewDotProductProvider()),
			func(cfg *hfresh.Config) {
				cfg.MultiVectorForIDThunk = thunk
				cfg.TokenForIDThunk = tokenThunk
				cfg.TempMultiVectorForIDThunk = tempThunk
			})
		centerOnCorpus(t, &tf, "")

		// exact top 10 of query q over the live documents, as index doc ids
		groundTruth := func(q int) map[uint64]struct{} {
			want := make(map[uint64]struct{}, 10)
			for _, doc := range corpus.GroundTruth(q) {
				if live[doc] {
					want[current[doc]] = struct{}{}
					if len(want) == 10 {
						return want
					}
				}
			}
			want = make(map[uint64]struct{}, 10)
			for _, r := range bruteForceLive(corpus, live, q) {
				want[current[r.doc]] = struct{}{}
			}
			return want
		}

		// deadEntries counts the posting entries of deleted documents and
		// the postings where they are more than a third of the entries,
		// the share at which a scan enqueues a cleanup (scanPostings)
		deadEntries := func() (dead, overThird int) {
			for _, refs := range tf.Index.postings.lists {
				var n, d int
				for _, ref := range refs.decode() {
					n++
					if docOf[ref.DocID].Load() < 0 {
						d++
					}
				}
				dead += d
				if 3*d > n {
					overThird++
				}
			}
			return dead, overThird
		}

		measure := func(round int, phase string) {
			tf.Index.waitForMaintenance(t)
			centroids, postings, empty, entries := tf.postingStats()
			dead, overThird := deadEntries()
			t.Logf("dynamics maxPostingSize=%d round=%d phase=%s centroids=%d postings=%d empty_postings=%d entries=%d dead_entries=%d postings_over_third=%d mean_posting=%.2f heap_bytes=%d\n",
				maxPostingSize, round, phase, centroids, postings, empty, entries, dead, overThird, float64(entries)/float64(max(1, postings)), heapLive())

			prev := runtime.GOMAXPROCS(1)
			for _, kCentroids := range envList(t, "TACHIOMFRESH_KC", []int{16, 32, 64}) {
				for _, kDocsToScore := range envList(t, "TACHIOMFRESH_KDOCS", []int{100, 500}) {
					tf.Index.setSearchBudgets(uint32(kCentroids), uint32(kDocsToScore))
					var recall10 float64
					var took time.Duration
					for q, query := range corpus.Queries {
						before := time.Now()
						ids, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, nil)
						took += time.Since(before)
						require.NoError(t, err)
						want := groundTruth(q)
						hits := 0
						for _, id := range ids {
							require.GreaterOrEqual(t, docOf[id].Load(), int32(0), "deleted doc %d returned", id)
							if _, ok := want[id]; ok {
								hits++
							}
						}
						recall10 += float64(hits) / 10
					}
					nq := float64(corpus.NumQueries())
					t.Logf("cell index=tachiomfresh maxPostingSize=%d round=%d phase=%s kCentroids=%d kDocsToScore=%d recall@10=%.4f qps=%.2f mean_ms=%.3f\n",
						maxPostingSize, round, phase, kCentroids, kDocsToScore, recall10/nq, nq/took.Seconds(), took.Seconds()*1000/nq)
				}
			}
			runtime.GOMAXPROCS(prev)
			// the searches enqueued merges and cleanups of the postings
			// they found over a third dead (cleanup.go): the dead entries
			// are counted again, with the cleanups run so far
			tf.Index.waitForMaintenance(t)
			centroids, postings, empty, entries = tf.postingStats()
			dead, overThird = deadEntries()
			t.Logf("dynamics maxPostingSize=%d round=%d phase=%s-after-search centroids=%d postings=%d empty_postings=%d entries=%d dead_entries=%d postings_over_third=%d cleanups=%d %s\n",
				maxPostingSize, round, phase, centroids, postings, empty, entries, dead, overThird, tf.cleanupCount(t), reads())
		}

		start := time.Now()
		require.NoError(t, parallelInsert(n, workers, func(i int) error {
			return corpus.add(t.Context(), tf.Index, uint64(i), i)
		}))
		tf.Index.waitForMaintenance(t)
		t.Logf("dynamics maxPostingSize=%d build_s=%.3f documents=%s %s\n", maxPostingSize, time.Since(start).Seconds(), documents, reads())
		measure(0, "built")

		rng := rand.New(rand.NewSource(seed))
		nextID := uint64(n)
		for round := 1; round <= rounds; round++ {
			victims := rng.Perm(n)[:perRound]

			// With the object store, the objects are dropped before the
			// index's deletes and written before its reinserts, as a shard
			// orders them (shard_write_delete.go, shard_write_put.go), and
			// outside the timed sections: delete_s and reinsert_s stay the
			// index's own cost, the store's is printed on its own line.
			var objectsDelete, objectsPut time.Duration
			if objects != nil {
				before := time.Now()
				for _, doc := range victims {
					require.NoError(t, objects.delete(current[doc]))
				}
				require.NoError(t, objects.flush())
				objectsDelete = time.Since(before)
			}
			start = time.Now()
			for _, doc := range victims {
				id := current[doc]
				require.NoError(t, tf.Index.DeleteMulti(id))
				docOf[id].Store(-1) // the doc record is dropped
				live[doc] = false
			}
			t.Logf("dynamics maxPostingSize=%d round=%d delete_s=%.3f\n", maxPostingSize, round, time.Since(start).Seconds())
			if os.Getenv("TACHIOMFRESH_VERIFY_GT") != "" {
				verifyGroundTruth(t, corpus, live)
			}
			measure(round, "deleted")

			ids := make([]uint64, len(victims))
			for i, doc := range victims {
				ids[i] = nextID
				docOf[nextID].Store(int32(doc))
				nextID++
			}
			if objects != nil {
				before := time.Now()
				for i, doc := range victims {
					require.NoError(t, objects.put(ids[i], corpus.Doc(doc, nil)))
				}
				require.NoError(t, objects.flush())
				objectsPut = time.Since(before)
				t.Logf("dynamics maxPostingSize=%d round=%d objects_delete_s=%.3f objects_put_s=%.3f\n",
					maxPostingSize, round, objectsDelete.Seconds(), objectsPut.Seconds())
			}
			start = time.Now()
			require.NoError(t, parallelInsert(len(victims), workers, func(i int) error {
				return corpus.add(t.Context(), tf.Index, ids[i], victims[i])
			}))
			tf.Index.waitForMaintenance(t)
			for i, doc := range victims {
				current[doc] = ids[i]
				live[doc] = true
			}
			t.Logf("dynamics maxPostingSize=%d round=%d reinsert_s=%.3f %s\n", maxPostingSize, round, time.Since(start).Seconds(), reads())
			measure(round, "reinserted")
		}
		require.NoError(t, tf.Index.Shutdown(t.Context()))
		if objects != nil {
			// the working copy of the store goes with the build it served
			objects.close(t)
			require.NoError(t, os.RemoveAll(objectStoreDir(os.Getenv("TACHIOMFRESH_OBJECT_STORE"), path)+"-dynamics"))
		}
	}
}

type scoredDoc struct {
	doc   int
	score float32
}

// bruteForceLive returns query q's exact MaxSim (dot) top 10 over the live
// corpus documents, best first.
func bruteForceLive(corpus *Corpus, live []bool, q int) []scoredDoc {
	dot := distancer.NewDotProductProvider()
	var top []scoredDoc
	var doc [][]float32
	for d := range corpus.NumDocs() {
		if !live[d] {
			continue
		}
		doc = corpus.Doc(d, doc)
		var score float32
		for _, qt := range corpus.Queries[q] {
			best := float32(math.Inf(-1))
			for _, dt := range doc {
				negDot, _ := dot.SingleDist(qt, dt)
				best = max(best, -negDot)
			}
			score += best
		}
		top = append(top, scoredDoc{doc: d, score: score})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].score != top[j].score {
			return top[i].score > top[j].score
		}
		return top[i].doc < top[j].doc
	})
	return top[:min(10, len(top))]
}

// verifyGroundTruth brute-forces every query over the live documents and
// requires the ten best scores to equal those of the first ten live
// documents of the cache's ranking (scores, not ids: exact ties may order
// differently).
func verifyGroundTruth(t *testing.T, corpus *Corpus, live []bool) {
	start := time.Now()
	tops := make([][]scoredDoc, corpus.NumQueries())
	require.NoError(t, parallelInsert(corpus.NumQueries(), runtime.GOMAXPROCS(0), func(q int) error {
		tops[q] = bruteForceLive(corpus, live, q)
		return nil
	}))
	for q, top := range tops {
		scores := make(map[int]float32, len(top))
		for _, r := range top {
			scores[r.doc] = r.score
		}
		var fromCache []int
		for _, doc := range corpus.GroundTruth(q) {
			if live[doc] {
				fromCache = append(fromCache, int(doc))
				if len(fromCache) == 10 {
					break
				}
			}
		}
		require.Len(t, fromCache, 10, "query %d has fewer than 10 live documents in its cached top %d", q, corpus.GTDepth)
		for _, doc := range fromCache {
			if _, ok := scores[doc]; !ok {
				// not in the brute-force top 10 by id: must tie its last score
				single := bruteForceLive(corpus, liveOnly(len(live), doc), q)
				require.InDelta(t, top[len(top)-1].score, single[0].score, 1e-4, "query %d doc %d is in the filtered ranking but not in the exact top 10", q, doc)
			}
		}
	}
	t.Logf("ground truth verified by brute force over the live documents: %d queries, %.1fs\n", corpus.NumQueries(), time.Since(start).Seconds())
}

// liveOnly marks a single document live.
func liveOnly(n, doc int) []bool {
	live := make([]bool, n)
	live[doc] = true
	return live
}

// TestNFCorpusFiltered measures the filtered search on a saved index: seeded
// random allow lists of a fraction of the documents, recall@10 against the
// brute force over the allowed documents (the cache's ground truth is
// unfiltered, so bruteForceLive per query, in parallel before the timed
// searches), QPS on one core as the frontier's cells. Each filter is
// measured under the modes of TACHIOMFRESH_FILTER_MODES: "flat" is the
// index as configured (the flat path at or below flatSearchFactor x
// kDocsToScore allowed documents, the widening above it), "widen" the
// stages with the flat path disabled, which shows the widening. The cell
// line reports the probe doublings over the queries.
//
//	TACHIOMFRESH_MVC1=<cache> TACHIOMFRESH_INDEX_DIR=<dir> TACHIOMFRESH_OBJECT_STORE=<dir> go test -tags=nfcorpus -run 'TestNFCorpusFiltered$' -v -timeout 900m ./adapters/repos/db/vector/tachiomfresh/...
//
// TACHIOMFRESH_FILTERS lists the fractions in permille (default
// 500,100,10,1), the other variables are the frontier's.
func TestNFCorpusFiltered(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	t.Logf("command: TACHIOMFRESH_MVC1=%s TACHIOMFRESH_INDEX_DIR=%s TACHIOMFRESH_OBJECT_STORE=%s go test -tags=nfcorpus -run 'TestNFCorpusFiltered$' -v -timeout 900m ./adapters/repos/db/vector/tachiomfresh/...\n",
		path, os.Getenv("TACHIOMFRESH_INDEX_DIR"), os.Getenv("TACHIOMFRESH_OBJECT_STORE"))

	corpus, err := LoadCorpus(path, 0)
	require.NoError(t, err)
	t.Logf("corpus: %d docs, %d queries\n", corpus.NumDocs(), corpus.NumQueries())
	corpus.loadTokenIDs(t)

	thunk := func(ctx context.Context, id uint64) ([][]float32, error) {
		return corpus.Doc(int(id), nil), nil
	}
	tokenThunk := common.TokenForID(corpus.token)
	tempThunk := common.TempVectorForID[[]float32](corpus.tempThunk)
	documents := "mmap"
	objects := openObjectStore(t, path, corpus.NumDocs(), func(i int) [][]float32 { return corpus.Doc(i, nil) })
	if objects != nil {
		defer objects.close(t)
		thunk = objects.thunk
		tokenThunk = objects.token
		tempThunk = objects.tempThunk
		documents = "lsm"
	}
	t.Logf("documents: %s\n", documents)

	// the allow lists and their ground truth, shared by every build
	type filter struct {
		permille int
		allow    helpers.AllowList
		live     []bool
		allowed  int
		top      [][]scoredDoc // per query, the brute force over the allowed documents
	}
	var filters []filter
	rng := rand.New(rand.NewSource(1))
	for _, permille := range envList(t, "TACHIOMFRESH_FILTERS", []int{500, 100, 10, 1}) {
		f := filter{permille: permille, live: make([]bool, corpus.NumDocs())}
		var ids []uint64
		for d := range corpus.NumDocs() {
			if rng.Intn(1000) < permille {
				f.live[d] = true
				ids = append(ids, uint64(d))
			}
		}
		f.allow = helpers.NewAllowList(ids...)
		f.allowed = len(ids)
		start := time.Now()
		f.top = make([][]scoredDoc, corpus.NumQueries())
		require.NoError(t, parallelInsert(corpus.NumQueries(), runtime.GOMAXPROCS(0), func(q int) error {
			f.top[q] = bruteForceLive(corpus, f.live, q)
			return nil
		}))
		t.Logf("filter=%.3f allowed=%d ground truth by brute force: %.1fs\n", float64(permille)/1000, f.allowed, time.Since(start).Seconds())
		filters = append(filters, f)
	}
	gtIDs := func(top []scoredDoc) []uint32 {
		ids := make([]uint32, len(top))
		for i, r := range top {
			ids[i] = uint32(r.doc)
		}
		return ids
	}

	for _, maxPostingSize := range envList(t, "TACHIOMFRESH_MPS", []uint32{64}) {
		t.Run(fmt.Sprintf("mps=%d", maxPostingSize), func(t *testing.T) {
			tf, dir, saved := benchIndexWithTokens(t, maxPostingSize, thunk, tokenThunk, tempThunk)
			require.NotEmpty(t, saved, "TestNFCorpusFiltered runs on a saved index (TACHIOMFRESH_INDEX_DIR)")
			logLines(t, saved)
			t.Logf("loaded index=tachiomfresh maxPostingSize=%d dir=%s\n", maxPostingSize, dir)

			type filterMode struct {
				name       string
				maxWiden   int
				flatFactor int
			}
			modes := map[string]filterMode{
				"widen": {"widen", maxWidenings, 0},
				"flat":  {"flat", maxWidenings, flatSearchFactor},
			}
			modeNames := []string{"flat", "widen"}
			if env := os.Getenv("TACHIOMFRESH_FILTER_MODES"); env != "" {
				modeNames = strings.Split(env, ",")
			}

			prev := runtime.GOMAXPROCS(1)
			defer runtime.GOMAXPROCS(prev)
			for _, f := range filters {
				for _, name := range modeNames {
					mode, ok := modes[name]
					require.True(t, ok, "unknown filter mode %q", name)
					tf.Index.opts.maxWiden = mode.maxWiden
					tf.Index.opts.flatFactor = mode.flatFactor
					for _, kCentroids := range envList(t, "TACHIOMFRESH_KC", []int{16, 32}) {
						for _, kDocsToScore := range envList(t, "TACHIOMFRESH_KDOCS", []int{100, 500}) {
							if name == "flat" && f.allowed > flatSearchFactor*kDocsToScore {
								continue // the widen mode's cell
							}
							tf.Index.setSearchBudgets(uint32(kCentroids), uint32(kDocsToScore))
							widenings := tf.widenings.Load()
							var recall10 float64
							var took time.Duration
							for q, query := range corpus.Queries {
								before := time.Now()
								ids, _, err := tf.Index.SearchByMultiVector(t.Context(), query, 10, f.allow)
								took += time.Since(before)
								require.NoError(t, err)
								recall10 += overlap(ids, gtIDs(f.top[q]), 10)
							}
							nq := float64(corpus.NumQueries())
							t.Logf("cell index=tachiomfresh maxPostingSize=%d filter=%.3f allowed=%d mode=%s kCentroids=%d kDocsToScore=%d recall@10=%.4f qps=%.2f mean_ms=%.3f widenings=%d\n",
								maxPostingSize, float64(f.permille)/1000, f.allowed, mode.name, kCentroids, kDocsToScore, recall10/nq, nq/took.Seconds(), took.Seconds()*1000/nq,
								tf.widenings.Load()-widenings)
						}
					}
				}
			}
			tf.Index.opts.maxWiden = maxWidenings
			tf.Index.opts.flatFactor = flatSearchFactor

			runtime.GOMAXPROCS(prev)
			tf.Index.waitForMaintenance(t)
			require.NoError(t, tf.Index.Shutdown(t.Context()))
			if dir != "" {
				require.NoError(t, tf.store.Shutdown(t.Context()))
			}
		})
	}
}

// TestNFCorpusFilterPassRate reports, for seeded filters of the same
// fractions as TestNFCorpusFiltered, the share of live postings that hold
// an allowed document, by posting size, and the share of posting ids whose
// centroid is gone (a split's tombstone: an empty posting the centroid HNSW
// still visits until its tombstone cleanup runs, which the harness never
// does).
//
//	TACHIOMFRESH_MVC1=<cache> TACHIOMFRESH_INDEX_DIR=<dir> TACHIOMFRESH_OBJECT_STORE=<dir> go test -tags=nfcorpus -run 'TestNFCorpusFilterPassRate$' -v ./adapters/repos/db/vector/tachiomfresh/...
func TestNFCorpusFilterPassRate(t *testing.T) {
	path := os.Getenv("TACHIOMFRESH_MVC1")
	if path == "" {
		t.Skip("TACHIOMFRESH_MVC1 not set")
	}
	corpus, err := LoadCorpus(path, 0)
	require.NoError(t, err)
	corpus.loadTokenIDs(t)
	thunk := func(ctx context.Context, id uint64) ([][]float32, error) {
		return corpus.Doc(int(id), nil), nil
	}
	for _, maxPostingSize := range envList(t, "TACHIOMFRESH_MPS", []uint32{64}) {
		tf, dir, saved := benchIndexWithTokens(t, maxPostingSize, thunk, corpus.token, corpus.tempThunk)
		require.NotEmpty(t, saved)
		n := tf.Index.postings.count()
		var live, dead int
		sizes := map[int]int{}
		for id := range n {
			if !tf.Index.Centroids.Exists(uint64(id)) {
				dead++
				continue
			}
			live++
			sizes[sizeBucket(tf.Index.postings.get(uint64(id)).len())]++
		}
		t.Logf("passrate maxPostingSize=%d ids=%d live=%d dead_ids=%d dead_share=%.3f\n", maxPostingSize, n, live, dead, float64(dead)/float64(n))
		for _, b := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
			t.Logf("passrate maxPostingSize=%d size<=%d postings=%d\n", maxPostingSize, b, sizes[b])
		}
		rng := rand.New(rand.NewSource(1))
		for _, permille := range envList(t, "TACHIOMFRESH_FILTERS", []int{500, 100, 10, 1}) {
			allowed := make([]bool, corpus.NumDocs())
			for d := range corpus.NumDocs() {
				allowed[d] = rng.Intn(1000) < permille
			}
			var pass int
			passBySize := map[int]int{}
			for id := range n {
				if !tf.Index.Centroids.Exists(uint64(id)) {
					continue
				}
				refs := tf.Index.postings.get(uint64(id))
				for _, doc := range refs.docIDs(nil) {
					if allowed[doc] {
						pass++
						passBySize[sizeBucket(refs.len())]++
						break
					}
				}
			}
			line := fmt.Sprintf("passrate maxPostingSize=%d filter=%.3f live_pass=%d live_pass_share=%.3f", maxPostingSize, float64(permille)/1000, pass, float64(pass)/float64(live))
			for _, b := range []int{1, 2, 4, 8, 16, 32, 64, 128} {
				if sizes[b] > 0 {
					line += fmt.Sprintf(" pass<=%d=%.3f", b, float64(passBySize[b])/float64(sizes[b]))
				}
			}
			t.Log(line)
		}
		require.NoError(t, tf.Index.Shutdown(t.Context()))
		if dir != "" {
			require.NoError(t, tf.store.Shutdown(t.Context()))
		}
	}
}

// count is the number of posting ids the table addresses.
func (p *postingTable) count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.lists)
}

// sizeBucket is the smallest power of two at or above n, capped at 128.
func sizeBucket(n int) int {
	for _, b := range []int{1, 2, 4, 8, 16, 32, 64} {
		if n <= b {
			return b
		}
	}
	return 128
}

// logLines logs s one line at a time, so that every line of the run's
// output carries the test log prefix.
func logLines(t testing.TB, s string) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		t.Log(line)
	}
}
