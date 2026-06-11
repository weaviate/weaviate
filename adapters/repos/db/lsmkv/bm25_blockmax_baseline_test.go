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

// BlockMax WAND scores/rankings baseline harness.
//
// Runs a deterministic set of queries against an on-disk inverted bucket and
// records, per query, the ranked top-K (docID, score). In `dump` mode it writes
// that to a file; in `compare` mode it loads a baseline and diffs it against the
// current code, reporting ranking / set / score changes. Used to prove an
// optimization branch returns the same results as main.
//
// This file is intentionally self-contained (no dependency on the profiling
// harness) and uses only APIs present on main, so the SAME file can be dropped
// into a `main` worktree to generate the baseline.
//
//	# 1) baseline from main (in a clean worktree of main):
//	BMW_SEGMENTS_DIR=/…/property_text_searchable BMW_BASELINE=/tmp/bmw_base.json \
//	  BMW_BASELINE_MODE=dump go test -tags integrationTest -run TestBMWBaseline -v ./adapters/repos/db/lsmkv/
//
//	# 2) compare current branch against it:
//	BMW_SEGMENTS_DIR=/…/property_text_searchable BMW_BASELINE=/tmp/bmw_base.json \
//	  BMW_BASELINE_MODE=compare go test -tags integrationTest -run TestBMWBaseline -v ./adapters/repos/db/lsmkv/
//
// Reuses the BMW_QUERY_SIZE / BMW_NUM_QUERIES / BMW_SEED / BMW_LIMIT / BMW_SAMPLE
// / BMW_FILTER / BMW_OPERATOR / BMW_K1 / BMW_B knobs (must match between the two
// runs). BMW_SCORE_EPS (default 1e-6) is the score-drift tolerance;
// BMW_BASELINE_STRICT=false turns the comparison into a report instead of a
// test failure.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

type bmwDocScore struct {
	DocID uint64  `json:"d"`
	Score float64 `json:"s"`
}

type bmwBaselineFile struct {
	Params  map[string]string        `json:"params"`
	Results map[string][]bmwDocScore `json:"results"`
}

func bEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func bEnvI(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func bEnvF(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return def
}

func TestBMWBaseline(t *testing.T) {
	mode := os.Getenv("BMW_BASELINE_MODE")
	if mode == "" {
		t.Skip("set BMW_BASELINE_MODE=dump|compare (and BMW_SEGMENTS_DIR, BMW_BASELINE)")
	}
	dir := os.Getenv("BMW_SEGMENTS_DIR")
	require.NotEmpty(t, dir, "BMW_SEGMENTS_DIR required")
	basePath := os.Getenv("BMW_BASELINE")
	require.NotEmpty(t, basePath, "BMW_BASELINE (file path) required")

	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	// open a copy so the source segments are never mutated
	openDir := dir
	if !strings.EqualFold(bEnv("BMW_COPY", "true"), "false") {
		tmp := t.TempDir()
		require.NoError(t, bmwBaseCopyDir(dir, tmp))
		openDir = tmp
	}
	bucket, err := NewBucketCreator().NewBucket(ctx, openDir, openDir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)
	require.Equal(t, StrategyInverted, bucket.Strategy())

	// dump and compare may run with different BMW_DEFER_TOMBSTONE values; both must
	// yield the same top-K, which is what this baseline proves.
	applyDeferTombstoneEnv()

	cfg := schema.BM25Config{K1: bEnvF("BMW_K1", 1.2), B: bEnvF("BMW_B", 0.75)}
	limit := bEnvI("BMW_LIMIT", 10)
	andOp := strings.EqualFold(bEnv("BMW_OPERATOR", "or"), "and")
	avgPropLen, docCount := bucket.GetAveragePropertyLength()
	if avgPropLen == 0 {
		avgPropLen = 1
	}
	N := float64(docCount)

	queries := bmwBaseQueries(t, ctx, bucket, cfg, N)
	require.NotEmpty(t, queries, "no queries built")
	if N <= 0 {
		N = 1
	}
	tombPct := bEnvF("BMW_TOMBSTONE_PCT", 0)
	injectSyntheticTombstones(t, bucket, tombPct, N)
	filter := bmwBaseFilter(t, bEnvF("BMW_FILTER", 0), N)

	results := make(map[string][]bmwDocScore, len(queries))
	for _, q := range queries {
		results[strings.Join(q, " ")] = bmwBaseRunQuery(ctx, bucket, q, N, avgPropLen, limit, cfg, filter, andOp, logger)
	}

	params := map[string]string{
		"querySize": bEnv("BMW_QUERY_SIZE", "3"), "numQueries": strconv.Itoa(len(queries)),
		"seed": bEnv("BMW_SEED", "42"), "limit": strconv.Itoa(limit),
		"sample": bEnv("BMW_SAMPLE", "4000"), "filter": bEnv("BMW_FILTER", "0"),
		"operator": bEnv("BMW_OPERATOR", "or"), "k1": fmt.Sprintf("%g", cfg.K1), "b": fmt.Sprintf("%g", cfg.B),
		"N": fmt.Sprintf("%.0f", N), "avgPropLen": fmt.Sprintf("%.4f", avgPropLen),
		"tombstonePct": fmt.Sprintf("%g", tombPct),
	}

	switch mode {
	case "dump":
		f, err := os.Create(basePath)
		require.NoError(t, err)
		enc := json.NewEncoder(f)
		enc.SetEscapeHTML(false)
		require.NoError(t, enc.Encode(bmwBaselineFile{Params: params, Results: results}))
		require.NoError(t, f.Close())
		t.Logf("wrote baseline: %d queries -> %s (params: %v)", len(results), basePath, params)
	case "compare":
		raw, err := os.ReadFile(basePath)
		require.NoError(t, err)
		var base bmwBaselineFile
		require.NoError(t, json.Unmarshal(raw, &base))
		bmwBaseCompare(t, base, results, params)
	default:
		t.Fatalf("unknown BMW_BASELINE_MODE %q (want dump|compare)", mode)
	}
}

func bmwBaseCompare(t *testing.T, base bmwBaselineFile, cur map[string][]bmwDocScore, curParams map[string]string) {
	eps := bEnvF("BMW_SCORE_EPS", 1e-6)
	strict := !strings.EqualFold(bEnv("BMW_BASELINE_STRICT", "true"), "false")

	t.Logf("\n=== baseline compare ===")
	t.Logf("baseline params: %v", base.Params)
	t.Logf("current  params: %v", curParams)
	if !bmwParamsComparable(base.Params, curParams) {
		t.Fatalf("baseline and current params differ on query generation — set identical BMW_QUERY_SIZE/NUM_QUERIES/SEED/LIMIT/SAMPLE/FILTER/OPERATOR/K1/B")
	}

	var nQueries, identical, rankingChanged, setChanged, scoreDrift, missingQuery int
	var maxAbs, maxRel float64
	var samples []string

	keys := make([]string, 0, len(base.Results))
	for k := range base.Results {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		nQueries++
		b := base.Results[k]
		c, ok := cur[k]
		if !ok {
			missingQuery++
			continue
		}
		setEq, orderEq, absDiff, relDiff := bmwDiffRanked(b, c)
		if absDiff > maxAbs {
			maxAbs = absDiff
		}
		if relDiff > maxRel {
			maxRel = relDiff
		}
		switch {
		case !setEq:
			setChanged++
			if len(samples) < 10 {
				samples = append(samples, fmt.Sprintf("[%q] doc-set changed\n    base=%s\n    cur =%s", k, bmwFmt(b), bmwFmt(c)))
			}
		case !orderEq:
			rankingChanged++
			if len(samples) < 10 {
				samples = append(samples, fmt.Sprintf("[%q] ranking changed (same docs)\n    base=%s\n    cur =%s", k, bmwFmt(b), bmwFmt(c)))
			}
		case absDiff > eps:
			scoreDrift++
			if len(samples) < 10 {
				samples = append(samples, fmt.Sprintf("[%q] score drift |Δ|=%.3g", k, absDiff))
			}
		default:
			identical++
		}
	}

	t.Logf("queries compared:     %d", nQueries)
	t.Logf("identical:            %d", identical)
	t.Logf("ranking changed:      %d", rankingChanged)
	t.Logf("doc-set changed:      %d", setChanged)
	t.Logf("score drift > %.0e:    %d", eps, scoreDrift)
	t.Logf("missing in current:   %d", missingQuery)
	t.Logf("max |Δscore|=%.3g  max relΔ=%.3g", maxAbs, maxRel)
	for _, s := range samples {
		t.Logf("  %s", s)
	}

	regressions := rankingChanged + setChanged + missingQuery
	if strict {
		require.Zerof(t, regressions, "result regressions vs baseline (ranking/set/missing); see log")
		require.Zerof(t, scoreDrift, "score drift beyond %g vs baseline; see log", eps)
	} else if regressions > 0 || scoreDrift > 0 {
		t.Logf("NOTE: differences found but BMW_BASELINE_STRICT=false, not failing")
	}
}

// bmwDiffRanked compares two ranked result lists. Returns whether the doc set is
// equal, whether the order is identical, and the max absolute/relative score
// delta over docs present in both.
func bmwDiffRanked(b, c []bmwDocScore) (setEq, orderEq bool, maxAbs, maxRel float64) {
	orderEq = len(b) == len(c)
	if orderEq {
		for i := range b {
			if b[i].DocID != c[i].DocID {
				orderEq = false
				break
			}
		}
	}
	bset := make(map[uint64]float64, len(b))
	for _, d := range b {
		bset[d.DocID] = d.Score
	}
	cset := make(map[uint64]float64, len(c))
	for _, d := range c {
		cset[d.DocID] = d.Score
	}
	setEq = len(bset) == len(cset)
	if setEq {
		for id := range bset {
			if _, ok := cset[id]; !ok {
				setEq = false
				break
			}
		}
	}
	for id, bs := range bset {
		if cs, ok := cset[id]; ok {
			abs := math.Abs(bs - cs)
			if abs > maxAbs {
				maxAbs = abs
			}
			if bs != 0 {
				if rel := abs / math.Abs(bs); rel > maxRel {
					maxRel = rel
				}
			}
		}
	}
	return setEq, orderEq, maxAbs, maxRel
}

func bmwFmt(ds []bmwDocScore) string {
	var sb strings.Builder
	n := len(ds)
	if n > 8 {
		n = 8
	}
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "%d:%.4g ", ds[i].DocID, ds[i].Score)
	}
	if len(ds) > n {
		sb.WriteString("…")
	}
	return sb.String()
}

func bmwParamsComparable(a, b map[string]string) bool {
	for _, k := range []string{"querySize", "seed", "limit", "sample", "filter", "operator", "k1", "b"} {
		if a[k] != b[k] {
			return false
		}
	}
	return true
}

// bmwBaseRunQuery runs one query and returns the merged top-K ranked by score
// desc then docID asc (deterministic), taking the max score per doc across
// segments. Score precision is float32 (as stored in the heap).
func bmwBaseRunQuery(ctx context.Context, b *Bucket, query []string, N, avgPropLen float64, limit int, cfg schema.BM25Config, filter helpers.AllowList, andOp bool, logger logrus.FieldLogger) []bmwDocScore {
	boosts := make([]int, len(query))
	for i := range boosts {
		boosts[i] = 1
	}
	diskTerms, _, release, err := b.CreateDiskTerm(N, filter, query, "", 1, boosts, cfg, ctx)
	if err != nil || diskTerms == nil {
		if release != nil {
			release()
		}
		return nil
	}
	defer release()
	for _, seg := range diskTerms {
		for _, t := range seg {
			if t != nil {
				t.SetIdf(t.Idf())
			}
		}
	}

	best := map[uint64]float64{}
	for _, seg := range diskTerms {
		if len(seg) == 0 {
			continue
		}
		var heap *priorityqueue.Queue[[]*terms.DocPointerWithScore]
		if andOp {
			heap = DoBlockMaxAnd(ctx, limit, seg, avgPropLen, false, len(query), len(query), logger)
		} else {
			heap, _ = DoBlockMaxWand(ctx, limit, seg, avgPropLen, false, len(query), 1, logger)
		}
		for heap != nil && heap.Len() > 0 {
			item := heap.Pop()
			s := float64(item.Dist)
			if cur, ok := best[item.ID]; !ok || s > cur {
				best[item.ID] = s
			}
		}
	}
	out := make([]bmwDocScore, 0, len(best))
	for id, s := range best {
		out = append(out, bmwDocScore{DocID: id, Score: s})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].DocID < out[j].DocID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// BenchmarkBMWCompare times the search path (CreateDiskTerm + DoBlockMax* per
// segment, draining the heap) over the deterministic query set. It is
// self-contained and main-compatible, so the identical benchmark can be run in a
// `main` worktree and on the branch for a whole-branch-vs-main comparison. Pin
// BMW_SEGMENTS_DIR / BMW_QUERY_SIZE / BMW_NUM_QUERIES / BMW_SEED / BMW_LIMIT /
// BMW_FILTER / BMW_OPERATOR identically across the two runs.
func BenchmarkBMWCompare(b *testing.B) {
	dir := os.Getenv("BMW_SEGMENTS_DIR")
	if dir == "" {
		b.Skip("set BMW_SEGMENTS_DIR to a searchable/inverted bucket directory")
	}
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	openDir := dir
	if !strings.EqualFold(bEnv("BMW_COPY", "true"), "false") {
		tmp := b.TempDir()
		require.NoError(b, bmwBaseCopyDir(dir, tmp))
		openDir = tmp
	}
	bucket, err := NewBucketCreator().NewBucket(ctx, openDir, openDir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
	require.NoError(b, err)
	defer bucket.Shutdown(ctx)

	cfg := schema.BM25Config{K1: bEnvF("BMW_K1", 1.2), B: bEnvF("BMW_B", 0.75)}
	limit := bEnvI("BMW_LIMIT", 10)
	andOp := strings.EqualFold(bEnv("BMW_OPERATOR", "or"), "and")
	avgPropLen, docCount := bucket.GetAveragePropertyLength()
	if avgPropLen == 0 {
		avgPropLen = 1
	}
	N := float64(docCount)
	queries := bmwBaseQueries(b, ctx, bucket, cfg, N)
	require.NotEmpty(b, queries)
	if N <= 0 {
		N = 1
	}
	applyDeferTombstoneEnv()
	injectSyntheticTombstones(b, bucket, bEnvF("BMW_TOMBSTONE_PCT", 0), N)
	filter := bmwBaseFilter(b, bEnvF("BMW_FILTER", 0), N)

	// warm caches
	for i := 0; i < len(queries) && i < 500; i++ {
		bmwBaseRunLean(ctx, bucket, queries[i], N, avgPropLen, limit, cfg, filter, andOp, logger)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var sink int
	for i := 0; i < b.N; i++ {
		sink += bmwBaseRunLean(ctx, bucket, queries[i%len(queries)], N, avgPropLen, limit, cfg, filter, andOp, logger)
	}
	_ = sink
}

// TestBMWConcurrentLoad models N concurrent clients (default 100) hammering one
// shared read-only bucket for a fixed duration, with GOMAXPROCS left at the
// machine default — i.e. heavy oversubscription, the regime where lock/GC
// contention and tail latency surface. Reports throughput and latency
// percentiles. Self-contained, so the same test runs in a main worktree and on
// the branch. Gate with BMW_LOAD=true; tune with BMW_WORKERS / BMW_LOAD_SECONDS.
func TestBMWConcurrentLoad(t *testing.T) {
	if os.Getenv("BMW_LOAD") == "" {
		t.Skip("set BMW_LOAD=true (and BMW_SEGMENTS_DIR) to run the concurrent load test")
	}
	dir := os.Getenv("BMW_SEGMENTS_DIR")
	require.NotEmpty(t, dir, "BMW_SEGMENTS_DIR required")
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	openDir := dir
	if !strings.EqualFold(bEnv("BMW_COPY", "true"), "false") {
		tmp := t.TempDir()
		require.NoError(t, bmwBaseCopyDir(dir, tmp))
		openDir = tmp
	}
	bucket, err := NewBucketCreator().NewBucket(ctx, openDir, openDir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	cfg := schema.BM25Config{K1: bEnvF("BMW_K1", 1.2), B: bEnvF("BMW_B", 0.75)}
	limit := bEnvI("BMW_LIMIT", 10)
	andOp := strings.EqualFold(bEnv("BMW_OPERATOR", "or"), "and")
	avgPropLen, docCount := bucket.GetAveragePropertyLength()
	if avgPropLen == 0 {
		avgPropLen = 1
	}
	N := float64(docCount)
	queries := bmwBaseQueries(t, ctx, bucket, cfg, N)
	require.NotEmpty(t, queries)
	if N <= 0 {
		N = 1
	}
	filter := bmwBaseFilter(t, bEnvF("BMW_FILTER", 0), N)

	for i := 0; i < len(queries) && i < 500; i++ { // warm caches
		bmwBaseRunLean(ctx, bucket, queries[i], N, avgPropLen, limit, cfg, filter, andOp, logger)
	}

	workers := bEnvI("BMW_WORKERS", 100)
	dur := time.Duration(bEnvF("BMW_LOAD_SECONDS", 8) * float64(time.Second))
	lats := make([][]time.Duration, workers)
	counts := make([]int64, workers)

	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(dur)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := w*7919 + 1
			local := make([]time.Duration, 0, 8192)
			for time.Now().Before(deadline) {
				t0 := time.Now()
				bmwBaseRunLean(ctx, bucket, queries[i%len(queries)], N, avgPropLen, limit, cfg, filter, andOp, logger)
				local = append(local, time.Since(t0))
				i++
			}
			lats[w] = local
			counts[w] = int64(len(local))
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	var total int64
	all := make([]time.Duration, 0, 1<<16)
	for w := 0; w < workers; w++ {
		total += counts[w]
		all = append(all, lats[w]...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(q float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		idx := int(float64(len(all)) * q)
		if idx >= len(all) {
			idx = len(all) - 1
		}
		return all[idx]
	}
	var sum time.Duration
	for _, d := range all {
		sum += d
	}
	avg := time.Duration(0)
	if len(all) > 0 {
		avg = sum / time.Duration(len(all))
	}

	t.Logf("\n=== concurrent load: workers=%d GOMAXPROCS=%d limit=%d filter=%v dur=%s ===",
		workers, runtime.GOMAXPROCS(0), limit, filter != nil, elapsed.Round(time.Millisecond))
	t.Logf("total queries=%d  throughput=%.0f q/s", total, float64(total)/elapsed.Seconds())
	t.Logf("latency  avg=%s  p50=%s  p90=%s  p95=%s  p99=%s  p99.9=%s  max=%s",
		avg.Round(time.Microsecond), pct(0.50).Round(time.Microsecond), pct(0.90).Round(time.Microsecond),
		pct(0.95).Round(time.Microsecond), pct(0.99).Round(time.Microsecond), pct(0.999).Round(time.Microsecond),
		all[len(all)-1].Round(time.Microsecond))
}

// BenchmarkBMWCompareParallel runs the search across GOMAXPROCS goroutines
// (vary with -cpu=1,4,8,…) sharing one read-only bucket+filter, exactly as
// concurrent queries do in production. ns/op falls as parallelism scales; compare
// main vs branch at each -cpu level to see both absolute throughput and how well
// each scales (the atomic refcount + lower allocations help the branch here).
func BenchmarkBMWCompareParallel(b *testing.B) {
	dir := os.Getenv("BMW_SEGMENTS_DIR")
	if dir == "" {
		b.Skip("set BMW_SEGMENTS_DIR to a searchable/inverted bucket directory")
	}
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	openDir := dir
	if !strings.EqualFold(bEnv("BMW_COPY", "true"), "false") {
		tmp := b.TempDir()
		require.NoError(b, bmwBaseCopyDir(dir, tmp))
		openDir = tmp
	}
	bucket, err := NewBucketCreator().NewBucket(ctx, openDir, openDir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
	require.NoError(b, err)
	defer bucket.Shutdown(ctx)

	cfg := schema.BM25Config{K1: bEnvF("BMW_K1", 1.2), B: bEnvF("BMW_B", 0.75)}
	limit := bEnvI("BMW_LIMIT", 10)
	andOp := strings.EqualFold(bEnv("BMW_OPERATOR", "or"), "and")
	avgPropLen, docCount := bucket.GetAveragePropertyLength()
	if avgPropLen == 0 {
		avgPropLen = 1
	}
	N := float64(docCount)
	queries := bmwBaseQueries(b, ctx, bucket, cfg, N)
	require.NotEmpty(b, queries)
	if N <= 0 {
		N = 1
	}
	filter := bmwBaseFilter(b, bEnvF("BMW_FILTER", 0), N)

	for i := 0; i < len(queries) && i < 500; i++ {
		bmwBaseRunLean(ctx, bucket, queries[i], N, avgPropLen, limit, cfg, filter, andOp, logger)
	}

	b.ReportAllocs()
	b.ResetTimer()
	var ctr int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(atomic.AddInt64(&ctr, 1)) * 7919 // distinct phase per worker
		for pb.Next() {
			bmwBaseRunLean(ctx, bucket, queries[i%len(queries)], N, avgPropLen, limit, cfg, filter, andOp, logger)
			i++
		}
	})
}

// bmwBaseRunLean runs one query and returns the result count, without building a
// result map — the leanest representation of the search hot path.
func bmwBaseRunLean(ctx context.Context, b *Bucket, query []string, N, avgPropLen float64, limit int, cfg schema.BM25Config, filter helpers.AllowList, andOp bool, logger logrus.FieldLogger) int {
	boosts := make([]int, len(query))
	for i := range boosts {
		boosts[i] = 1
	}
	diskTerms, _, release, err := b.CreateDiskTerm(N, filter, query, "", 1, boosts, cfg, ctx)
	if err != nil || diskTerms == nil {
		if release != nil {
			release()
		}
		return 0
	}
	defer release()
	n := 0
	for _, seg := range diskTerms {
		if len(seg) == 0 {
			continue
		}
		var heap *priorityqueue.Queue[[]*terms.DocPointerWithScore]
		if andOp {
			heap = DoBlockMaxAnd(ctx, limit, seg, avgPropLen, false, len(query), len(query), logger)
		} else {
			heap, _ = DoBlockMaxWand(ctx, limit, seg, avgPropLen, false, len(query), 1, logger)
		}
		if heap != nil {
			n += heap.Len()
		}
	}
	return n
}

// bmwBaseQueries deterministically builds the query set. Terms are sampled from
// the segment quantile keys then SORTED before any rng use, so the set is
// identical across processes (a Go map's iteration order is not).
func bmwBaseQueries(t testing.TB, ctx context.Context, bucket *Bucket, cfg schema.BM25Config, N float64) [][]string {
	if explicit := os.Getenv("BMW_TERMS"); explicit != "" {
		var qs [][]string
		for _, g := range strings.Split(explicit, ";") {
			var q []string
			for _, tk := range strings.Split(g, ",") {
				if tk = strings.TrimSpace(tk); tk != "" {
					q = append(q, tk)
				}
			}
			if len(q) > 0 {
				qs = append(qs, q)
			}
		}
		return qs
	}

	maxSample := bEnvI("BMW_SAMPLE", 4000)
	seed := int64(bEnvI("BMW_SEED", 42))
	size := bEnvI("BMW_QUERY_SIZE", 3)
	count := bEnvI("BMW_NUM_QUERIES", 500)

	view := bucket.GetConsistentView()
	set := map[string]struct{}{}
	for _, seg := range view.Disk {
		for _, k := range seg.quantileKeys(maxSample) {
			set[string(k)] = struct{}{}
		}
	}
	view.ReleaseView()
	sampled := make([]string, 0, len(set))
	for k := range set {
		sampled = append(sampled, k)
	}
	sort.Strings(sampled) // determinism across processes
	rng := rand.New(rand.NewSource(seed))
	if maxSample > 0 && len(sampled) > maxSample {
		rng.Shuffle(len(sampled), func(i, j int) { sampled[i], sampled[j] = sampled[j], sampled[i] })
		sampled = sampled[:maxSample]
		sort.Strings(sampled)
	}

	// document frequencies (deterministic: data-derived)
	freqs := map[string]uint64{}
	const batch = 200
	for i := 0; i < len(sampled); i += batch {
		end := i + batch
		if end > len(sampled) {
			end = len(sampled)
		}
		grp := sampled[i:end]
		boosts := make([]int, len(grp))
		for j := range boosts {
			boosts[j] = 1
		}
		_, idf, release, err := bucket.CreateDiskTerm(maxF64(N, 1), nil, grp, "", 1, boosts, cfg, ctx)
		require.NoError(t, err)
		for term, n := range idf {
			freqs[term] = n
		}
		release()
	}

	present := make([]string, 0, len(sampled))
	for _, term := range sampled {
		if freqs[term] > 0 {
			present = append(present, term)
		}
	}
	// stable tiering: by freq desc, term asc
	sort.Slice(present, func(i, j int) bool {
		if freqs[present[i]] != freqs[present[j]] {
			return freqs[present[i]] > freqs[present[j]]
		}
		return present[i] < present[j]
	})
	if len(present) == 0 {
		return nil
	}

	nHigh := len(present) / 10
	if nHigh < 1 {
		nHigh = 1
	}
	high := present[:nHigh]
	rest := present[nHigh:]
	if len(rest) == 0 {
		rest = high
	}

	queries := make([][]string, 0, count)
	for i := 0; i < count; i++ {
		var q []string
		switch i % 4 {
		case 0:
			q = bmwPick(rng, high, size)
		case 3:
			q = bmwPick(rng, rest, size)
		default:
			q = bmwPick(rng, high, 1)
			q = append(q, bmwPick(rng, rest, size-1)...)
		}
		q = bmwDedupe(q)
		if len(q) > 0 {
			queries = append(queries, q)
		}
	}
	return queries
}

func bmwPick(rng *rand.Rand, pool []string, k int) []string {
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

func bmwDedupe(in []string) []string {
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

func bmwBaseFilter(t testing.TB, selectivity, N float64) helpers.AllowList {
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
	for id := uint64(0); id < upper; id += step {
		bm.Set(id)
	}
	return helpers.NewAllowListFromBitmap(bm)
}

func maxF64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func bmwBaseCopyDir(src, dst string) error {
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
			if err := bmwBaseCopyDir(s, d); err != nil {
				return err
			}
			continue
		}
		if !e.Type().IsRegular() {
			continue
		}
		in, err := os.Open(s)
		if err != nil {
			return err
		}
		out, err := os.Create(d)
		if err != nil {
			in.Close()
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			in.Close()
			out.Close()
			return err
		}
		in.Close()
		out.Close()
	}
	return nil
}
