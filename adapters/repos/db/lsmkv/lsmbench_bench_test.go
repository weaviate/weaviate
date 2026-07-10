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

package lsmkv

// "Speed of light" floor for HFresh's search-path disk work. These benchmarks
// hit an existing shard's lsm buckets directly, with no HFresh machinery, and
// measure only the two operations HFresh performs: a SetRawList over posting
// keys, and a GetBySecondaryWithBuffer by docID. Compare the resulting
// latencies against HFresh's profiler phase times to see how much of the cost
// is the store itself versus HFresh on top of it.
//
// The server that owns LSMBENCH_DIR MUST be stopped: these open the buckets
// read-write and would otherwise fight the running process for the WAL/locks.
//
// Run against a real shard (LSMBENCH_DIR is the directory containing the
// `objects` and `hfresh_postings_main` subdirectories):
//
//	LSMBENCH_DIR=/data/<Class>/<shard>/lsm \
//	  go test -run xxx -bench BenchmarkLSMPostingRead \
//	  -cpu 1,8,16,32 ./adapters/repos/db/lsmkv/
//
//	LSMBENCH_DIR=/data/<Class>/<shard>/lsm LSMBENCH_MAX_DOCID=5000000 \
//	  go test -run xxx -bench 'BenchmarkLSMObjectBySecondary|BenchmarkLSMQueryShaped' \
//	  -cpu 1,8,16,32 ./adapters/repos/db/lsmkv/

import (
	"context"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

const (
	envDir      = "LSMBENCH_DIR"       // shard lsm dir; benchmarks skip if unset
	envMaxDocID = "LSMBENCH_MAX_DOCID" // exclusive upper bound for sampled docIDs
	envProbe    = "LSMBENCH_PROBE"     // posting reads per simulated query
	envRescore  = "LSMBENCH_RESCORE"   // secondary gets per simulated query

	defaultProbe   = 50
	defaultRescore = 350

	// rescore fetches inside one query run through this many workers, matching
	// HFresh's bounded parallelism for the rescore phase.
	queryWorkers = 16

	// cap on real keys collected from the postings bucket; enough to defeat
	// per-key caching without holding the whole keyspace in memory.
	maxPostingKeys = 100_000

	benchSeed = 42 // fixed so a run's sampled keys/docIDs are reproducible
)

func benchLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

// docIDKey is the 8-byte little-endian secondary key HFresh's rescore uses to
// look objects up by docID (see readVectorByIndexIDIntoSliceWithView).
func docIDKey(docID uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, docID)
	return b
}

// openPostingsBucket opens <lsmDir>/hfresh_postings_main with the strategy
// production uses for the posting store (see NewPostingStore). Only the read
// path is mirrored; compaction-only options are intentionally omitted.
func openPostingsBucket(tb testing.TB, lsmDir string) *Bucket {
	tb.Helper()
	dir := filepath.Join(lsmDir, "hfresh_postings_main")
	b, err := NewBucketCreator().NewBucket(context.Background(), dir, "", benchLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategySetCollection))
	require.NoError(tb, err)
	return b
}

// openObjectsBucket opens <lsmDir>/objects with the options production uses for
// the object store (see initObjectBucket). The secondary index and tombstone
// options must match on-disk layout for GetBySecondaryWithBuffer to work.
func openObjectsBucket(tb testing.TB, lsmDir string) *Bucket {
	tb.Helper()
	dir := filepath.Join(lsmDir, "objects")
	b, err := NewBucketCreator().NewBucket(context.Background(), dir, "", benchLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1), WithKeepTombstones(true))
	require.NoError(tb, err)
	return b
}

// samplePostingKeys cursors the postings bucket and copies up to limit real
// keys. These are the exact keys SetRawList takes.
func samplePostingKeys(tb testing.TB, b *Bucket, limit int) [][]byte {
	tb.Helper()
	c := b.SetCursor()
	defer c.Close()

	keys := make([][]byte, 0, limit)
	for k, _ := c.First(); k != nil && len(keys) < limit; k, _ = c.Next() {
		kc := make([]byte, len(k))
		copy(kc, k)
		keys = append(keys, kc)
	}
	return keys
}

// intEnv reads an int env var, falling back to def when unset/empty.
func intEnv(tb testing.TB, name string, def int) int {
	tb.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	require.NoErrorf(tb, err, "parse %s", name)
	return n
}

// requireMaxDocID returns LSMBENCH_MAX_DOCID or skips: the objects benchmarks
// sample uniform random docIDs in [0, max) and cannot proceed without a bound.
func requireMaxDocID(tb testing.TB) uint64 {
	tb.Helper()
	v := os.Getenv(envMaxDocID)
	if v == "" {
		tb.Skipf("%s not set", envMaxDocID)
	}
	n, err := strconv.ParseUint(v, 10, 64)
	require.NoErrorf(tb, err, "parse %s", envMaxDocID)
	require.Positivef(tb, n, "%s must be > 0", envMaxDocID)
	return n
}

// requireDir returns LSMBENCH_DIR or skips the benchmark.
func requireDir(tb testing.TB) string {
	tb.Helper()
	dir := os.Getenv(envDir)
	if dir == "" {
		tb.Skipf("%s not set", envDir)
	}
	return dir
}

// rescoreOnce runs R secondary gets for random docIDs through a bounded worker
// pool, reusing one buffer per worker, and returns the number of misses (nil
// results). Misses are expected for deleted/sparse docIDs and are not failures.
func rescoreOnce(objects *Bucket, rescore int, maxDocID uint64, rnd *rand.Rand) int64 {
	jobs := make(chan uint64, rescore)
	for i := 0; i < rescore; i++ {
		jobs <- uint64(rnd.Int63n(int64(maxDocID)))
	}
	close(jobs)

	var misses int64
	var wg sync.WaitGroup
	for w := 0; w < queryWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 0, 1024)
			for docID := range jobs {
				v, nb, err := objects.GetBySecondaryWithBuffer(context.Background(), 0, docIDKey(docID), buf)
				if err == nil {
					buf = nb
				}
				if v == nil {
					atomic.AddInt64(&misses, 1)
				}
			}
		}()
	}
	wg.Wait()
	return misses
}

// BenchmarkLSMPostingRead measures one SetRawList per iteration over sampled
// real posting keys (round-robin). This is HFresh's probe-phase disk read.
func BenchmarkLSMPostingRead(b *testing.B) {
	dir := requireDir(b)
	bucket := openPostingsBucket(b, dir)
	defer bucket.Shutdown(context.Background())

	keys := samplePostingKeys(b, bucket, maxPostingKeys)
	require.NotEmpty(b, keys, "postings bucket yielded no keys")

	var goroutineID int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// spread starting offsets so goroutines don't march in lockstep
		i := int(atomic.AddInt64(&goroutineID, 1)) * 997
		for pb.Next() {
			_, err := bucket.SetRawList(keys[i%len(keys)])
			if err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/sec")
}

// BenchmarkLSMObjectBySecondary measures one GetBySecondaryWithBuffer per
// iteration for a uniform random docID, with a reused buffer per goroutine.
// This is HFresh's rescore-phase disk read.
func BenchmarkLSMObjectBySecondary(b *testing.B) {
	dir := requireDir(b)
	maxDocID := requireMaxDocID(b)
	bucket := openObjectsBucket(b, dir)
	defer bucket.Shutdown(context.Background())

	var goroutineID, misses int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rnd := rand.New(rand.NewSource(benchSeed + atomic.AddInt64(&goroutineID, 1)))
		buf := make([]byte, 0, 1024)
		for pb.Next() {
			v, nb, err := bucket.GetBySecondaryWithBuffer(context.Background(), 0,
				docIDKey(uint64(rnd.Int63n(int64(maxDocID)))), buf)
			if err != nil {
				b.Error(err)
				return
			}
			buf = nb
			if v == nil {
				atomic.AddInt64(&misses, 1)
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/sec")
	b.ReportMetric(float64(misses)/float64(b.N), "miss_rate")
}

// BenchmarkLSMQueryShaped simulates one HFresh query's disk work per iteration:
// P sequential posting reads (probe) followed by R secondary gets (rescore)
// issued through a bounded worker pool. It reports whole-query throughput.
func BenchmarkLSMQueryShaped(b *testing.B) {
	dir := requireDir(b)
	maxDocID := requireMaxDocID(b)
	probe := intEnv(b, envProbe, defaultProbe)
	rescore := intEnv(b, envRescore, defaultRescore)

	postings := openPostingsBucket(b, dir)
	defer postings.Shutdown(context.Background())
	objects := openObjectsBucket(b, dir)
	defer objects.Shutdown(context.Background())

	keys := samplePostingKeys(b, postings, maxPostingKeys)
	require.NotEmpty(b, keys, "postings bucket yielded no keys")

	var goroutineID, misses int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rnd := rand.New(rand.NewSource(benchSeed + atomic.AddInt64(&goroutineID, 1)))
		keyStart := int(rnd.Int63())
		for pb.Next() {
			for i := 0; i < probe; i++ {
				if _, err := postings.SetRawList(keys[(keyStart+i)%len(keys)]); err != nil {
					b.Error(err)
					return
				}
			}
			keyStart += probe
			atomic.AddInt64(&misses, rescoreOnce(objects, rescore, maxDocID, rnd))
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
	b.ReportMetric(float64(b.N)*float64(probe+rescore)/b.Elapsed().Seconds(), "ops/sec")
	b.ReportMetric(float64(misses)/float64(b.N*rescore), "miss_rate")
}

// buildTinyDataset writes a minimal postings + objects bucket pair under lsmDir,
// flushed to disk, so the harness exercises the same helpers the benchmarks use
// without a real shard.
func buildTinyDataset(t *testing.T, lsmDir string) {
	t.Helper()
	ctx := context.Background()

	postings := openPostingsBucket(t, lsmDir)
	for id := uint64(0); id < 5; id++ {
		key := docIDKey(id) // any 8-byte key works for the set strategy
		require.NoError(t, postings.SetAdd(key, [][]byte{{byte(id)}, {byte(id + 1)}}))
	}
	require.NoError(t, postings.FlushMemtable())
	require.NoError(t, postings.Shutdown(ctx))

	objects := openObjectsBucket(t, lsmDir)
	for id := uint64(0); id < 5; id++ {
		require.NoError(t, objects.Put([]byte("obj-"+strconv.FormatUint(id, 10)),
			[]byte("value-"+strconv.FormatUint(id, 10)), WithSecondaryKey(0, docIDKey(id))))
	}
	require.NoError(t, objects.FlushMemtable())
	require.NoError(t, objects.Shutdown(ctx))
}

// TestLSMBenchHarness runs the sampling and one read of each kind through the
// benchmark helpers against a tiny temp dataset, so the file is exercised in a
// normal `go test` run with no env vars.
func TestLSMBenchHarness(t *testing.T) {
	lsmDir := t.TempDir()
	buildTinyDataset(t, lsmDir)

	postings := openPostingsBucket(t, lsmDir)
	defer postings.Shutdown(context.Background())
	objects := openObjectsBucket(t, lsmDir)
	defer objects.Shutdown(context.Background())

	keys := samplePostingKeys(t, postings, maxPostingKeys)
	require.NotEmpty(t, keys)

	list, err := postings.SetRawList(keys[0])
	require.NoError(t, err)
	require.NotEmpty(t, list)

	v, _, err := objects.GetBySecondaryWithBuffer(context.Background(), 0, docIDKey(0), make([]byte, 0, 16))
	require.NoError(t, err)
	require.NotEmpty(t, v)

	misses := rescoreOnce(objects, 4, 5, rand.New(rand.NewSource(benchSeed)))
	require.GreaterOrEqual(t, misses, int64(0))
}
