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

//go:build integrationTest
// +build integrationTest

package lsmkv

// BlockMax WAND profiling harness.
//
// Points the real query path (CreateDiskTerm -> DoBlockMaxWand/DoBlockMaxAnd) at
// an on-disk searchable (inverted) bucket and runs many multi-term queries built
// from terms that actually exist in those segments. It captures CPU + allocation
// profiles and parses them in-process to print a ranked list of the hottest
// BlockMax functions, so you can see what to optimize first without leaving the
// test.
//
// Usage:
//
//	# A folder holding ONE searchable property's segment files (*.db etc).
//	# Copy a property dir out of a Weaviate data dir, e.g.
//	#   <data>/<class>/<shard>/lsm/property_<name>_searchable
//	BMW_SEGMENTS_DIR=/path/to/property_searchable \
//	  go test -tags integrationTest -run TestProfileBlockMaxWand -v -timeout 30m \
//	  ./adapters/repos/db/lsmkv/
//
//	# Standard Go benchmark (works with the usual pprof flags):
//	BMW_SEGMENTS_DIR=/path/to/property_searchable \
//	  go test -tags integrationTest -run x -bench BenchmarkBlockMaxWand -benchmem \
//	  -cpuprofile cpu.prof -memprofile mem.prof ./adapters/repos/db/lsmkv/
//	go tool pprof -http=: cpu.prof
//
// Tunables (env vars):
//
//	BMW_SEGMENTS_DIR     required; folder with the bucket's segments
//	BMW_QUERY_SIZE       terms per query; CSV for the benchmark (default "1,2,3,5"), single int for the test (default 3)
//	BMW_NUM_QUERIES      distinct query combinations to generate (default 500)
//	BMW_PROFILE_SECONDS  wall-clock to drive the profiler in the test (default 15); ignored if BMW_PROFILE_QUERIES is set
//	BMW_PROFILE_QUERIES  fixed query-execution cap under the profiler (overrides BMW_PROFILE_SECONDS)
//	BMW_LIMIT            top-K limit (default 10)
//	BMW_SAMPLE           max terms reservoir-sampled from the bucket (default 4000)
//	BMW_TERMS            explicit query groups, ';'-separated groups of ','-separated terms (overrides sampling)
//	BMW_OPERATOR         "or" (BlockMaxWand, default) or "and" (BlockMaxAnd)
//	BMW_FILTER           filter selectivity in (0,1): builds an AllowList passing ~that fraction of docIDs, exercising the filtered-search path (default 0 = no filter)
//	BMW_CONCURRENCY      goroutines running queries during the profiled run (default 1); >1 also captures mutex/block profiles
//	BMW_SCALING          CSV concurrency levels for a contention sweep, e.g. "1,2,4,8,16" (put 1 first); prints throughput + scaling efficiency
//	BMW_SCALING_SECONDS  duration per scaling level (default 4)
//	BMW_BLOCK_RATE_NS    block-profile rate in ns when concurrent (default 10000)
//	BMW_EXPLAIN          "true" to request additionalExplanations (heavier scoring path)
//	BMW_PREAD            "true" to force pread instead of the default mmap path
//	BMW_COPY             "false" to open the dir in place instead of copying to temp (default copies)
//	BMW_PROFILE_OUT      dir for the written profiles (default <tmp>/bmw-profiles)
//	BMW_TOPN             hotspot rows to print (default 40)
//	BMW_SEED             rng seed for sampling/combinations (default 42)
//	BMW_K1, BMW_B        BM25 params (default 1.2 / 0.75)

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// bmwQuietLogger silences the per-100k-iteration timeout warnings; with a
// background context that path is never hit anyway.
var bmwQuietLogger = func() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.PanicLevel)
	return l
}()

// packages whose functions are worth ranking; everything else (runtime, GC,
// syscall) is shown only in the unfiltered "context" view.
var bmwPackages = []string{
	"adapters/repos/db/lsmkv",
	"adapters/repos/db/inverted",
	"adapters/repos/db/priorityqueue",
	"usecases/byteops",
	"weaviate/sroar",
	"lsmkv/varenc",
	"lsmkv/gobenc",
}

type queryStats struct {
	queries      int
	emptyQueries int
	results      int
	docsScored   uint64
	blocksDocIds uint64
	blocksFreqs  uint64
	docsDecoded  uint64
}

func (s *queryStats) add(o queryStats) {
	s.queries += o.queries
	s.emptyQueries += o.emptyQueries
	s.results += o.results
	s.docsScored += o.docsScored
	s.blocksDocIds += o.blocksDocIds
	s.blocksFreqs += o.blocksFreqs
	s.docsDecoded += o.docsDecoded
}

func bm25ConfigFromEnv() schema.BM25Config {
	return schema.BM25Config{
		K1: envFloat("BMW_K1", 1.2),
		B:  envFloat("BMW_B", 0.75),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(key string) bool {
	return strings.EqualFold(os.Getenv(key), "true")
}

// openBMWBucket loads an inverted bucket from dir. By default the dir is copied
// into a temp location first so the source segments are never mutated (NewBucket
// may recover a WAL or compact in place).
func openBMWBucket(tb testing.TB, dir string) (*Bucket, func()) {
	tb.Helper()

	openDir := dir
	if !strings.EqualFold(envStr("BMW_COPY", "true"), "false") {
		tmp := tb.TempDir()
		require.NoError(tb, bmwCopyDir(dir, tmp), "copy segments dir")
		openDir = tmp
		tb.Logf("opened a copy of the segments at %s (set BMW_COPY=false to open in place)", openDir)
	}

	opts := []BucketOption{WithStrategy(StrategyInverted)}
	if envBool("BMW_PREAD") {
		opts = append(opts, WithPread(true))
	}

	b, err := NewBucketCreator().NewBucket(context.Background(), openDir, openDir, bmwQuietLogger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(tb, err, "open inverted bucket")
	require.Equal(tb, StrategyInverted, b.Strategy(),
		"BMW_SEGMENTS_DIR must point at a searchable/inverted bucket directory")

	cleanup := func() { _ = b.Shutdown(context.Background()) }
	return b, cleanup
}

func bmwCopyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, e := range entries {
		s := filepath.Join(src, e.Name())
		d := filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := bmwCopyDir(s, d); err != nil {
				return err
			}
			continue
		}
		if !e.Type().IsRegular() {
			continue
		}
		if err := bmwCopyFile(s, d); err != nil {
			return err
		}
	}
	return nil
}

func bmwCopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

// sampleTermKeys collects a spread of term keys across the keyspace using the
// segment index's quantile keys, which walk the tree without decoding any
// posting lists (cheap even on huge buckets). Keys are gathered from every disk
// segment, deduped, and trimmed to max. NOTE: MapCursorKeyOnly cannot be used
// here — its keyOnly path drops every key because it merges empty value lists.
func sampleTermKeys(tb testing.TB, b *Bucket, max int, rng *rand.Rand) []string {
	tb.Helper()
	view := b.GetConsistentView()
	defer view.ReleaseView()

	q := max
	if q < 1 {
		q = 1
	}
	set := make(map[string]struct{}, max)
	for _, seg := range view.Disk {
		for _, k := range seg.quantileKeys(q) {
			set[string(k)] = struct{}{}
		}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	if max > 0 && len(keys) > max {
		rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		keys = keys[:max]
	}
	tb.Logf("sampled %d terms across %d disk segments", len(keys), len(view.Disk))
	return keys
}

// termFrequencies returns the document frequency (postings count) per term,
// read cheaply from CreateDiskTerm's idf counts. Doubles as a warmup that loads
// the per-segment property lengths.
func termFrequencies(tb testing.TB, ctx context.Context, b *Bucket, terms []string, N float64, cfg schema.BM25Config) map[string]uint64 {
	tb.Helper()
	out := make(map[string]uint64, len(terms))
	const batch = 200
	for i := 0; i < len(terms); i += batch {
		end := i + batch
		if end > len(terms) {
			end = len(terms)
		}
		grp := terms[i:end]
		boosts := ones(len(grp))
		_, idfCounts, release, err := b.CreateDiskTerm(N, nil, grp, "", 1, boosts, cfg, ctx)
		require.NoError(tb, err)
		for t, n := range idfCounts {
			out[t] = n
		}
		release()
	}
	return out
}

func ones(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = 1
	}
	return s
}

// buildQueries forms `count` query groups of the requested size. WAND cost is
// dominated by the term-frequency mix, so queries deliberately span tiers:
// some all-frequent (heavy postings, lots of block skipping), most a frequent
// term plus rarer ones (the common case), some all-rare (cheap).
func buildQueries(tb testing.TB, present []string, freqs map[string]uint64, size, count int, rng *rand.Rand) [][]string {
	tb.Helper()
	if size < 1 {
		size = 1
	}
	sorted := make([]string, len(present))
	copy(sorted, present)
	sort.Slice(sorted, func(i, j int) bool { return freqs[sorted[i]] > freqs[sorted[j]] })

	nHigh := len(sorted) / 10
	if nHigh < 1 {
		nHigh = 1
	}
	nMid := len(sorted) * 4 / 10
	high := sorted[:nHigh]
	mid := sorted[nHigh : nHigh+nMid]
	low := sorted[nHigh+nMid:]
	rest := append(append([]string{}, mid...), low...)
	if len(rest) == 0 {
		rest = high
	}

	queries := make([][]string, 0, count)
	for i := 0; i < count; i++ {
		var q []string
		switch i % 4 {
		case 0: // stress: all frequent
			q = sampleDistinct(rng, high, size)
		case 3: // light: all rare/mid
			q = sampleDistinct(rng, rest, size)
		default: // typical: one frequent + rest mixed
			q = sampleDistinct(rng, high, 1)
			q = append(q, sampleDistinct(rng, rest, size-1)...)
		}
		q = dedupe(q)
		if len(q) > 0 {
			queries = append(queries, q)
		}
	}
	return queries
}

func sampleDistinct(rng *rand.Rand, pool []string, k int) []string {
	if k <= 0 || len(pool) == 0 {
		return nil
	}
	if k >= len(pool) {
		out := append([]string{}, pool...)
		rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
		return out
	}
	idx := rng.Perm(len(pool))[:k]
	out := make([]string, 0, k)
	for _, i := range idx {
		out = append(out, pool[i])
	}
	return out
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// runQuery executes one query exactly like the production block path: build the
// per-segment term iterators, then run WAND/AND once per segment. Safe to call
// concurrently — each call builds its own iterators and accumulates into its own
// returned queryStats; only the (read-only) filter/bucket are shared.
func runQuery(ctx context.Context, s *bmwSetup, query []string) queryStats {
	b, N, avgPropLen, limit, cfg := s.bucket, s.N, s.avgPropLen, s.limit, s.cfg
	st := queryStats{queries: 1}
	boosts := ones(len(query))
	diskTerms, _, release, err := b.CreateDiskTerm(N, s.filter, query, "", 1, boosts, cfg, ctx)
	if err != nil || diskTerms == nil {
		st.emptyQueries = 1
		if release != nil {
			release()
		}
		return st
	}
	defer release()

	matched := false
	for _, seg := range diskTerms {
		if len(seg) == 0 {
			continue
		}
		matched = true
		var heap *priorityqueue.Queue[[]*terms.DocPointerWithScore]
		if s.andOp {
			heap = DoBlockMaxAnd(ctx, limit, seg, avgPropLen, s.explain, len(query), len(query), bmwQuietLogger)
		} else {
			heap, _ = DoBlockMaxWand(ctx, limit, seg, avgPropLen, s.explain, len(query), 1, bmwQuietLogger)
		}
		if heap != nil {
			st.results += heap.Len()
		}
		for _, t := range seg {
			if t == nil {
				continue
			}
			st.docsScored += t.Metrics.DocCountScored
			st.blocksDocIds += t.Metrics.BlockCountDecodedDocIds
			st.blocksFreqs += t.Metrics.BlockCountDecodedFreqs
			st.docsDecoded += t.Metrics.DocCountDecodedDocIds
		}
	}
	if !matched {
		st.emptyQueries = 1
	}
	return st
}

// runConcurrent drives queries across `workers` goroutines until `dur` elapses
// (when maxExec<=0) or `maxExec` total executions are reached. Each worker
// accumulates into its own queryStats so the harness adds no shared-write
// contention of its own — only the bucket and filter (read-only) are shared,
// exactly as in production. Returns total executions, wall time, merged stats.
func runConcurrent(ctx context.Context, s *bmwSetup, queries [][]string, workers int, dur time.Duration, maxExec int) (int, time.Duration, queryStats) {
	if workers < 1 {
		workers = 1
	}
	stats := make([]queryStats, workers)
	useDeadline := maxExec <= 0
	var counter int64
	start := time.Now()
	deadline := start.Add(dur)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := &stats[w]
			i := w*7919 + 1 // distinct phase per worker so they don't run in lockstep
			for {
				if useDeadline {
					if time.Now().After(deadline) {
						return
					}
				} else if atomic.AddInt64(&counter, 1) > int64(maxExec) {
					return
				}
				local.add(runQuery(ctx, s, queries[i%len(queries)]))
				i++
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var agg queryStats
	for w := range stats {
		agg.add(stats[w])
	}
	return agg.queries, elapsed, agg
}

// bmwSetup loads the bucket and prepares the query set shared by the benchmark
// and the profiling test.
type bmwSetup struct {
	bucket     *Bucket
	cleanup    func()
	terms      []string
	freqs      map[string]uint64
	present    []string
	N          float64
	avgPropLen float64
	cfg        schema.BM25Config
	andOp      bool
	explain    bool
	limit      int
	filter     helpers.AllowList
	rng        *rand.Rand
}

func setupBMW(tb testing.TB) *bmwSetup {
	tb.Helper()
	dir := os.Getenv("BMW_SEGMENTS_DIR")
	if dir == "" {
		tb.Skip("set BMW_SEGMENTS_DIR to a searchable/inverted bucket directory to run")
	}
	ctx := context.Background()
	cfg := bm25ConfigFromEnv()
	rng := rand.New(rand.NewSource(int64(envInt("BMW_SEED", 42))))

	// the BlockMetrics counters are gated off in production; turn them on so the
	// harness can report per-query docsScored/blocksDecoded.
	collectBlockMetrics = true
	applyDeferTombstoneEnv()

	bucket, cleanup := openBMWBucket(tb, dir)

	avgPropLen, docCount := bucket.GetAveragePropertyLength()
	if avgPropLen == 0 {
		avgPropLen = 1
	}
	N := float64(docCount)

	explicit := os.Getenv("BMW_TERMS")
	var sampled []string
	if explicit == "" {
		sampled = sampleTermKeys(tb, bucket, envInt("BMW_SAMPLE", 4000), rng)
		require.NotEmpty(tb, sampled, "no terms found in bucket")
	}

	freqs := map[string]uint64{}
	if explicit == "" {
		freqs = termFrequencies(tb, ctx, bucket, sampled, maxF(N, 1), cfg)
	}

	// derive N from the data if the bucket didn't report a doc count
	if N <= 0 {
		var mx uint64
		for _, n := range freqs {
			if n > mx {
				mx = n
			}
		}
		N = float64(mx) + 1
	}

	injectSyntheticTombstones(tb, bucket, envFloat("BMW_TOMBSTONE_PCT", 0), N)

	present := make([]string, 0, len(sampled))
	for _, t := range sampled {
		if freqs[t] > 0 {
			present = append(present, t)
		}
	}

	return &bmwSetup{
		bucket: bucket, cleanup: cleanup,
		terms: sampled, freqs: freqs, present: present,
		N: N, avgPropLen: avgPropLen, cfg: cfg,
		andOp:   strings.EqualFold(envStr("BMW_OPERATOR", "or"), "and"),
		explain: envBool("BMW_EXPLAIN"),
		limit:   envInt("BMW_LIMIT", 10),
		filter:  buildFilter(tb, envFloat("BMW_FILTER", 0), N),
		rng:     rng,
	}
}

// buildFilter constructs a filter AllowList that passes ~selectivity of the
// docID space, so the filtered-search path (SegmentBlockMax.advanceOnTombstoneOrFilter
// -> AllowList.Contains -> sroar) is exercised. selectivity<=0 means no filter.
func buildFilter(tb testing.TB, selectivity, N float64) helpers.AllowList {
	if selectivity <= 0 || selectivity >= 1 {
		return nil
	}
	upper := uint64(N)
	if upper == 0 {
		upper = 1 << 20
	}
	step := uint64(1 / selectivity)
	if step < 1 {
		step = 1
	}
	bm := sroar.NewBitmap()
	count := 0
	for id := uint64(0); id < upper; id += step {
		bm.Set(id)
		count++
	}
	tb.Logf("filter: selectivity=%.4f (~%d of %d docIDs allowed; every %dth)", selectivity, count, upper, step)
	return helpers.NewAllowListFromBitmap(bm)
}

// applyDeferTombstoneEnv leaves the shipping default in place unless
// BMW_DEFER_TOMBSTONE is explicitly set (=false forces advance-time for A/B).
func applyDeferTombstoneEnv() {
	if v, ok := os.LookupEnv("BMW_DEFER_TOMBSTONE"); ok {
		deferTombstoneToScore = strings.EqualFold(v, "true")
	}
}

// injectSyntheticTombstones marks ~pct of [0,N) deleted on the active memtable,
// whose tombstones createDiskTermFromCV ORs into memTombstones for every disk
// segment — simulating deletes without rewriting any segment. Strided so separate
// dump/compare processes build the identical set the bit-identical check needs.
func injectSyntheticTombstones(tb testing.TB, b *Bucket, pct, N float64) int {
	if pct <= 0 || N <= 0 {
		return 0
	}
	if pct >= 1 {
		pct = 0.99
	}
	upper := uint64(N)
	step := uint64(1 / pct)
	if step < 1 {
		step = 1
	}
	n := 0
	for id := uint64(0); id < upper; id += step {
		require.NoError(tb, b.active.SetTombstone(id))
		n++
	}
	tb.Logf("synthetic tombstones: pct=%.4f (~%d of %d docIDs deleted; every %dth)", pct, n, upper, step)
	return n
}

func (s *bmwSetup) queriesOfSize(tb testing.TB, size, count int) [][]string {
	if explicit := os.Getenv("BMW_TERMS"); explicit != "" {
		var qs [][]string
		for _, g := range strings.Split(explicit, ";") {
			var q []string
			for _, t := range strings.Split(g, ",") {
				if t = strings.TrimSpace(t); t != "" {
					q = append(q, t)
				}
			}
			if len(q) > 0 {
				qs = append(qs, q)
			}
		}
		require.NotEmpty(tb, qs, "BMW_TERMS produced no queries")
		return qs
	}
	qs := buildQueries(tb, s.present, s.freqs, size, count, s.rng)
	require.NotEmpty(tb, qs, "could not build queries; not enough terms")
	return qs
}

func maxF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func parseSizes(csv string) []int {
	var out []int
	for _, p := range strings.Split(csv, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		out = []int{3}
	}
	return out
}

// BenchmarkBlockMaxWand drives the search path under the standard Go benchmark
// flags. Use -cpuprofile/-memprofile to capture profiles for `go tool pprof`.
func BenchmarkBlockMaxWand(b *testing.B) {
	s := setupBMW(b)
	defer s.cleanup()
	ctx := context.Background()
	numQueries := envInt("BMW_NUM_QUERIES", 500)

	for _, size := range parseSizes(envStr("BMW_QUERY_SIZE", "1,2,3,5")) {
		queries := s.queriesOfSize(b, size, numQueries)
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			var agg queryStats
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				agg.add(runQuery(ctx, s, queries[i%len(queries)]))
			}
			b.StopTimer()
			if b.N > 0 {
				b.ReportMetric(float64(agg.docsScored)/float64(b.N), "docsScored/op")
				b.ReportMetric(float64(agg.blocksDocIds)/float64(b.N), "blocksDecoded/op")
				b.ReportMetric(float64(agg.results)/float64(b.N), "results/op")
			}
		})
	}
}

// TestBMWSlowQueries searches for the consistently-slowest queries on the given
// segments. It builds many candidate queries from the highest-document-frequency
// terms (the worst case for WAND: little block skipping, almost everything gets
// scored), times each over several repeats (median, to be robust), and reports
// the slowest set with their terms, doc-frequencies and per-query work. The
// slowest queries are also emitted as a BMW_TERMS string for replay.
//
// TestBMWTopTerms lists the BMW_TOP_TERMS most frequent sampled terms, plus a
// ready-to-paste BMW_TERMS string — for building common-term stress queries.
func TestBMWTopTerms(t *testing.T) {
	n := envInt("BMW_TOP_TERMS", 0)
	if n <= 0 {
		t.Skip("set BMW_TOP_TERMS=N to list the N most frequent sampled terms")
	}
	s := setupBMW(t)
	defer s.cleanup()
	top := append([]string(nil), s.present...)
	sort.Slice(top, func(i, j int) bool { return s.freqs[top[i]] > s.freqs[top[j]] })
	if len(top) > n {
		top = top[:n]
	}
	for _, term := range top {
		t.Logf("%9d  %s", s.freqs[term], term)
	}
	t.Logf("BMW_TERMS=%s", strings.Join(top, ","))
}

//	BMW_SEGMENTS_DIR=… BMW_SLOW=true BMW_QUERY_SIZE=15 \
//	  go test -tags integrationTest -run TestBMWSlowQueries -v ./adapters/repos/db/lsmkv/
func TestBMWSlowQueries(t *testing.T) {
	if os.Getenv("BMW_SLOW") == "" {
		t.Skip("set BMW_SLOW=true to search for high-latency queries")
	}
	s := setupBMW(t)
	defer s.cleanup()
	ctx := context.Background()

	size := envInt("BMW_QUERY_SIZE", 15)
	candidates := envInt("BMW_SLOW_CANDIDATES", 400)
	repeats := envInt("BMW_SLOW_REPEATS", 9)
	topK := envInt("BMW_SLOW_TOPK", 20)

	// candidate pool = the most frequent terms (≈ top 5%, at least 4× the query
	// size), where WAND prunes least.
	present := append([]string(nil), s.present...)
	sort.Slice(present, func(i, j int) bool { return s.freqs[present[i]] > s.freqs[present[j]] })
	poolSize := envInt("BMW_SLOW_POOL", 0)
	if poolSize <= 0 {
		poolSize = len(present) / 20
		if poolSize < size*4 {
			poolSize = size * 4
		}
	}
	if poolSize > len(present) {
		poolSize = len(present)
	}
	pool := present[:poolSize]
	require.GreaterOrEqual(t, len(pool), size, "not enough terms for the requested query size")
	t.Logf("pool: top %d terms by freq (%d..%d docs); building %d candidate %d-term queries",
		len(pool), s.freqs[pool[len(pool)-1]], s.freqs[pool[0]], candidates, size)

	type qstat struct {
		q          []string
		medianUs   float64
		spreadUs   float64
		docsScored uint64
		blocks     uint64
		results    int
		minFreq    uint64
		maxFreq    uint64
	}

	built := make([][]string, 0, candidates)
	seen := map[string]struct{}{}
	for len(built) < candidates {
		q := dedupe(sampleDistinct(s.rng, pool, size))
		if len(q) < size {
			continue
		}
		key := strings.Join(q, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		built = append(built, q)
	}

	// warm the page cache for every term involved
	for _, q := range built {
		runQuery(ctx, s, q)
	}

	stats := make([]qstat, 0, len(built))
	for _, q := range built {
		lats := make([]float64, repeats)
		var st queryStats
		for r := 0; r < repeats; r++ {
			t0 := time.Now()
			st = runQuery(ctx, s, q)
			lats[r] = float64(time.Since(t0).Microseconds())
		}
		sort.Float64s(lats)
		var minF, maxF uint64 = ^uint64(0), 0
		for _, term := range q {
			f := s.freqs[term]
			if f < minF {
				minF = f
			}
			if f > maxF {
				maxF = f
			}
		}
		stats = append(stats, qstat{
			q: q, medianUs: lats[len(lats)/2], spreadUs: lats[len(lats)-1] - lats[0],
			docsScored: st.docsScored, blocks: st.blocksDocIds, results: st.results, minFreq: minF, maxFreq: maxF,
		})
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].medianUs > stats[j].medianUs })

	var allMed float64
	for _, st := range stats {
		allMed += st.medianUs
	}
	t.Logf("\n=== %d candidates: median-latency distribution ===", len(stats))
	t.Logf("slowest=%.0fµs  median-of-candidates=%.0fµs  fastest=%.0fµs  mean=%.0fµs",
		stats[0].medianUs, stats[len(stats)/2].medianUs, stats[len(stats)-1].medianUs, allMed/float64(len(stats)))

	t.Logf("\n=== top %d slowest queries (median over %d repeats) ===", topK, repeats)
	var replay []string
	for i := 0; i < topK && i < len(stats); i++ {
		st := stats[i]
		t.Logf("#%2d  median=%6.0fµs (spread %.0fµs)  docsScored=%d  blocksDecoded=%d  results=%d  termFreq=%d..%d\n      terms: %s",
			i+1, st.medianUs, st.spreadUs, st.docsScored, st.blocks, st.results, st.minFreq, st.maxFreq, strings.Join(st.q, " "))
		replay = append(replay, strings.Join(st.q, ","))
	}
	t.Logf("\nreplay the slow set with:\n  BMW_TERMS='%s'", strings.Join(replay, ";"))
}

// TestProfileBlockMaxWand runs a fixed batch of queries under the CPU profiler,
// writes CPU + allocation profiles, then parses them and prints a ranked list of
// the hottest BlockMax functions — i.e. what to optimize first.
func TestProfileBlockMaxWand(t *testing.T) {
	s := setupBMW(t)
	defer s.cleanup()
	ctx := context.Background()

	size := envInt("BMW_QUERY_SIZE", 3)
	numQueries := envInt("BMW_NUM_QUERIES", 500)
	queries := s.queriesOfSize(t, size, numQueries)

	outDir := envStr("BMW_PROFILE_OUT", filepath.Join(os.TempDir(), "bmw-profiles"))
	require.NoError(t, os.MkdirAll(outDir, 0o755))
	cpuPath := filepath.Join(outDir, "bmw_cpu.prof")
	memPath := filepath.Join(outDir, "bmw_mem.prof")

	// warm caches (property lengths, OS page cache) so the profile reflects steady state
	for i := 0; i < min(len(queries), 500); i++ {
		runQuery(ctx, s, queries[i])
	}

	// Drive the profiler for a target wall-clock duration by default so CPU
	// sampling is meaningful regardless of dataset size/speed. An explicit
	// BMW_PROFILE_QUERIES caps by query count instead.
	maxExec := envInt("BMW_PROFILE_QUERIES", 0)
	targetDur := time.Duration(envFloat("BMW_PROFILE_SECONDS", 15) * float64(time.Second))
	conc := envInt("BMW_CONCURRENCY", 1)

	// Optional scaling sweep: run the same workload at several concurrency levels
	// and report how per-worker throughput degrades as queries contend. efficiency
	// = per-worker q/s at this level / per-worker q/s at the first level (stays at
	// 100% if the search scales perfectly; drops as contention bites). Put the
	// lowest level (e.g. 1) first to anchor the baseline.
	if scaling := os.Getenv("BMW_SCALING"); scaling != "" {
		levels := parseSizes(scaling)
		phaseDur := time.Duration(envFloat("BMW_SCALING_SECONDS", 4) * float64(time.Second))
		t.Logf("\n=== concurrency scaling (each level ~%.0fs, GOMAXPROCS=%d) ===", phaseDur.Seconds(), runtime.GOMAXPROCS(0))
		t.Logf("%8s  %12s  %16s  %11s", "workers", "queries/s", "per-worker q/s", "efficiency")
		base := 0.0
		for _, lvl := range levels {
			n, el, _ := runConcurrent(ctx, s, queries, lvl, phaseDur, 0)
			qps := float64(n) / el.Seconds()
			per := qps / float64(lvl)
			if base == 0 {
				base = per
			}
			t.Logf("%8d  %12.0f  %16.0f  %10.0f%%", lvl, qps, per, 100*per/base)
		}
		conc = levels[len(levels)-1]
	}

	// mutex + block profiling exposes lock contention when running concurrently
	if conc > 1 {
		runtime.SetMutexProfileFraction(1)
		runtime.SetBlockProfileRate(envInt("BMW_BLOCK_RATE_NS", 10000))
		defer runtime.SetMutexProfileFraction(0)
		defer runtime.SetBlockProfileRate(0)
	}

	cpuFile, err := os.Create(cpuPath)
	require.NoError(t, err)
	require.NoError(t, pprof.StartCPUProfile(cpuFile))

	exec, elapsed, agg := runConcurrent(ctx, s, queries, conc, targetDur, maxExec)

	pprof.StopCPUProfile()
	require.NoError(t, cpuFile.Close())

	runtime.GC()
	memFile, err := os.Create(memPath)
	require.NoError(t, err)
	require.NoError(t, pprof.Lookup("allocs").WriteTo(memFile, 0))
	require.NoError(t, memFile.Close())

	op := "OR/BlockMaxWand"
	if s.andOp {
		op = "AND/BlockMaxAnd"
	}
	t.Logf("\n=== BlockMax run summary (%s) ===", op)
	t.Logf("terms/query=%d  distinct queries=%d  executions=%d  concurrency=%d  limit=%d  N=%.0f  avgPropLen=%.2f  filter=%v",
		size, len(queries), exec, conc, s.limit, s.N, s.avgPropLen, s.filter != nil)
	t.Logf("wall=%s  throughput=%.0f queries/s  empty=%d",
		elapsed.Round(time.Millisecond), float64(exec)/elapsed.Seconds(), agg.emptyQueries)
	t.Logf("per-query: docsScored=%.1f  blocksDecoded(docIds)=%.1f  blocksDecoded(freqs)=%.1f  docsDecoded=%.1f  results=%.1f",
		perQ(agg.docsScored, exec), perQ(agg.blocksDocIds, exec), perQ(agg.blocksFreqs, exec),
		perQ(agg.docsDecoded, exec), perQ(uint64(agg.results), exec))

	reportHotspots(t, cpuPath, "cpu", "ms", 1e6, envInt("BMW_TOPN", 40))
	reportHotspots(t, memPath, "alloc_space", "MB", 1e6, envInt("BMW_TOPN", 40))

	written := fmt.Sprintf("  go tool pprof -http=: %s\n  go tool pprof -http=: %s", cpuPath, memPath)
	if conc > 1 {
		mutexPath := filepath.Join(outDir, "bmw_mutex.prof")
		blockPath := filepath.Join(outDir, "bmw_block.prof")
		writeLookup(t, "mutex", mutexPath)
		writeLookup(t, "block", blockPath)
		// "delay" = nanoseconds blocked; the CUM view filtered to BMW packages
		// shows which lock-protected sections serialize concurrent queries.
		reportHotspots(t, mutexPath, "delay", "ms", 1e6, envInt("BMW_TOPN", 40))
		reportHotspots(t, blockPath, "delay", "ms", 1e6, envInt("BMW_TOPN", 40))
		written += fmt.Sprintf("\n  go tool pprof -http=: %s\n  go tool pprof -http=: %s", mutexPath, blockPath)
	}
	t.Logf("\nprofiles written:\n%s", written)
}

func writeLookup(t *testing.T, name, path string) {
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	if p := pprof.Lookup(name); p != nil {
		require.NoError(t, p.WriteTo(f, 0))
	}
}

// TestGenerateBMWFixture builds a synthetic on-disk inverted bucket with a
// Zipf-skewed vocabulary spread over several segments, so the profiling harness
// can be exercised without a real Weaviate data dir. Point BMW_SEGMENTS_DIR at
// BMW_GEN_DIR afterwards.
//
//	BMW_GEN_DIR=/tmp/bmwfix go test -tags integrationTest -run TestGenerateBMWFixture ./adapters/repos/db/lsmkv/
func TestGenerateBMWFixture(t *testing.T) {
	dir := os.Getenv("BMW_GEN_DIR")
	if dir == "" {
		t.Skip("set BMW_GEN_DIR to generate a synthetic inverted bucket")
	}
	docs := envInt("BMW_GEN_DOCS", 50000)
	vocab := envInt("BMW_GEN_VOCAB", 2000)
	segments := envInt("BMW_GEN_SEGMENTS", 4)
	rng := rand.New(rand.NewSource(int64(envInt("BMW_GEN_SEED", 7))))
	zipf := rand.NewZipf(rng, 1.2, 1, uint64(vocab-1))

	require.NoError(t, os.MkdirAll(dir, 0o755))
	b, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, bmwQuietLogger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
	require.NoError(t, err)
	b.SetMemtableThreshold(1 << 30)
	defer func() { require.NoError(t, b.Shutdown(context.Background())) }()

	segBoundary := docs / maxI(segments, 1)
	for docID := 0; docID < docs; docID++ {
		nTerms := 5 + rng.Intn(45)
		used := make(map[uint64]struct{}, nTerms)
		for j := 0; j < nTerms; j++ {
			tid := zipf.Uint64()
			if _, ok := used[tid]; ok {
				continue
			}
			used[tid] = struct{}{}
			pair := NewMapPairFromDocIdAndTf(uint64(docID), float32(1+rng.Intn(8)), float32(nTerms), false)
			require.NoError(t, b.MapSet([]byte(fmt.Sprintf("term%06d", tid)), pair))
		}
		if segBoundary > 0 && docID > 0 && docID%segBoundary == 0 {
			require.NoError(t, b.FlushAndSwitch())
		}
	}
	require.NoError(t, b.FlushAndSwitch())
	t.Logf("generated synthetic inverted bucket at %s (docs=%d vocab=%d segments~=%d)", dir, docs, vocab, segments)
}

func maxI(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func perQ(v uint64, n int) float64 {
	if n == 0 {
		return 0
	}
	return float64(v) / float64(n)
}

// reportHotspots parses a pprof profile and prints flat (self) and cumulative
// rankings, filtered to the BlockMax-relevant packages, then a short unfiltered
// view for context (GC, syscalls, etc.).
func reportHotspots(t *testing.T, path, valueType, unit string, divisor float64, topN int) {
	f, err := os.Open(path)
	if err != nil {
		t.Logf("cannot open profile %s: %v", path, err)
		return
	}
	defer f.Close()
	p, err := profile.Parse(f)
	if err != nil {
		t.Logf("cannot parse profile %s: %v", path, err)
		return
	}

	flat, cum, total := aggregateProfile(p, valueType)
	if total == 0 {
		t.Logf("profile %s has no %s samples (run longer or raise BMW_PROFILE_QUERIES)", path, valueType)
		return
	}

	if valueType == "cpu" && total < 5_000_000_000 { // < ~5s of CPU samples
		t.Logf("WARNING: only %.0fms of CPU samples — low confidence. Raise BMW_PROFILE_QUERIES and/or point at a larger BMW_SEGMENTS_DIR for a meaningful CPU profile (the alloc profile below is exact regardless).", float64(total)/1e6)
	}
	t.Logf("\n========== HOTSPOTS: %s (total %.1f %s) ==========", valueType, float64(total)/divisor, unit)
	printTable(t, fmt.Sprintf("BlockMax FLAT (self %s) — optimize these first", unit), flat, total, unit, divisor, topN, isBMWFunc)
	printTable(t, fmt.Sprintf("BlockMax CUMULATIVE (%s incl. callees)", unit), cum, total, unit, divisor, topN, isBMWFunc)
	printTable(t, "All packages FLAT (context: GC/syscall/runtime)", flat, total, unit, divisor, 15, nil)
}

func isBMWFunc(name string) bool {
	for _, p := range bmwPackages {
		if strings.Contains(name, p) {
			return true
		}
	}
	return false
}

func aggregateProfile(p *profile.Profile, valueType string) (flat, cum map[string]int64, total int64) {
	idx := -1
	for i, st := range p.SampleType {
		if st.Type == valueType {
			idx = i
			break
		}
	}
	if idx == -1 {
		idx = len(p.SampleType) - 1
	}

	flat = map[string]int64{}
	cum = map[string]int64{}
	for _, s := range p.Sample {
		if idx >= len(s.Value) {
			continue
		}
		v := s.Value[idx]
		if v <= 0 {
			continue
		}
		total += v

		// leaf (self): innermost frame of the top location
		if len(s.Location) > 0 && len(s.Location[0].Line) > 0 {
			if fn := s.Location[0].Line[0].Function; fn != nil {
				flat[fn.Name] += v
			}
		}

		// cumulative: each distinct function appearing anywhere in the stack
		seen := map[string]struct{}{}
		for _, loc := range s.Location {
			for _, ln := range loc.Line {
				if ln.Function == nil {
					continue
				}
				if _, ok := seen[ln.Function.Name]; ok {
					continue
				}
				seen[ln.Function.Name] = struct{}{}
				cum[ln.Function.Name] += v
			}
		}
	}
	return flat, cum, total
}

func printTable(t *testing.T, title string, m map[string]int64, total int64, unit string, divisor float64, topN int, filter func(string) bool) {
	type row struct {
		name string
		v    int64
	}
	rows := make([]row, 0, len(m))
	for name, v := range m {
		if filter != nil && !filter(name) {
			continue
		}
		rows = append(rows, row{name, v})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].v > rows[j].v })
	if len(rows) > topN {
		rows = rows[:topN]
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "\n--- %s ---\n", title)
	fmt.Fprintf(&sb, "%8s  %7s  %s\n", unit, "%", "function")
	for _, r := range rows {
		fmt.Fprintf(&sb, "%8.1f  %6.2f%%  %s\n", float64(r.v)/divisor, 100*float64(r.v)/float64(total), shortFunc(r.name))
	}
	t.Log(sb.String())
}

func shortFunc(name string) string {
	name = strings.TrimPrefix(name, "github.com/weaviate/weaviate/")
	name = strings.TrimPrefix(name, "github.com/weaviate/")
	return name
}
