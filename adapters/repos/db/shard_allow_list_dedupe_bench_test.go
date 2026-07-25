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

package db

// Measures the CPU/latency effect of hybrid filter dedupe: the paired
// allow-list builds one filtered hybrid query runs per shard.
//
//	go test -tags integrationTest -run TestFilterDedupeMeasurement -v \
//	  -timeout 60m ./adapters/repos/db/
//
// Set DEDUPE_BENCH=1; tune via DEDUPE_BENCH_DOCS/ITERS/CONCURRENCY/REPEATS.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// setupRangeFilterShard builds a shard whose integer property carries a range
// index, so the filter resolves through the bit-sliced cascade that dominates
// the production profile rather than a single roaring-set lookup.
func setupRangeFilterShard(t *testing.T, docs int) *Shard {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retry bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockFSM.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}).Maybe()
	mockFSM.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}, nil).Maybe()
	mockNodes := cluster.NewMockNodeSelector(t)
	mockNodes.EXPECT().LocalName().Return("node1").Maybe()
	mockNodes.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000000,
		MaxImportGoroutinesFactor: 1,
		QueryLimit:                20,
	}, &FakeRemoteClient{}, mockNodes, &FakeRemoteNodeClient{}, nil, nil, nil,
		mockNodes, mockSchemaReader, mockFSM)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	vTrue := true
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
		Class:               "RangeClass",
		Vectorizer:          "none",
		Properties: []*models.Property{
			{
				Name:              "score",
				DataType:          schema.DataTypeInt.PropString(),
				IndexRangeFilters: &vTrue,
				IndexFilterable:   &vTrue,
			},
			{
				Name:            "available",
				DataType:        schema.DataTypeBoolean.PropString(),
				IndexFilterable: &vTrue,
			},
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexSearchable: &vTrue,
			},
		},
	}
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
	require.NoError(t, NewMigrator(repo, logger, "node1").AddClass(context.Background(), class))

	idx := repo.GetIndex("RangeClass")
	require.NotNil(t, idx)

	rnd := rand.New(rand.NewSource(42))
	for i := 0; i < docs; i++ {
		require.NoError(t, repo.PutObject(context.Background(), &models.Object{
			Class: "RangeClass",
			ID:    strfmt.UUID(fmt.Sprintf("%08x-0000-0000-0000-000000000000", i)),
			Properties: map[string]interface{}{
				// Mirrors the production predicate: a wide integer range that
				// admits nearly everything, so the cascade does real work.
				"score":     float64(rnd.Intn(1_000_000)),
				"available": true,
				"title":     fmt.Sprintf("product %d alpha beta gamma", i%1000),
			},
		}, []float32{float32(i%97) / 97, 0.5, 0.25}, nil, nil, nil, 0))
	}

	require.NoError(t, idx.ForEachShard(func(_ string, sl ShardLike) error {
		if s, ok := sl.(*Shard); ok {
			return s.store.FlushMemtables(context.Background())
		}
		return nil
	}))

	return firstShard(t, idx)
}

func rangeFilter() *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorOr,
		Operands: []filters.Clause{
			{
				Operator: filters.OperatorGreaterThan,
				On:       &filters.Path{Class: "RangeClass", Property: "score"},
				Value:    &filters.Value{Value: 100, Type: schema.DataTypeInt},
			},
			{
				Operator: filters.OperatorEqual,
				On:       &filters.Path{Class: "RangeClass", Property: "available"},
				Value:    &filters.Value{Value: true, Type: schema.DataTypeBoolean},
			},
		},
	}}
}

type latencyStats struct {
	p50, p95, max time.Duration
	cpu           time.Duration
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	tv := func(t syscall.Timeval) time.Duration {
		return time.Duration(t.Sec)*time.Second + time.Duration(t.Usec)*time.Microsecond
	}
	return tv(ru.Utime) + tv(ru.Stime)
}

// runLegPairs fires `legs` concurrent allow-list builds per iteration, sharing
// a dedupe token across them when dedupe is set, and reports latency/CPU.
func runLegPairs(t *testing.T, shard *Shard, filter *filters.LocalFilter,
	iters, legs, concurrency int, dedupe bool,
) latencyStats {
	t.Helper()

	took := make([]time.Duration, iters)
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	cpuBefore := processCPU()
	for i := 0; i < iters; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			ctx := context.Background()
			if dedupe {
				ctx = helpers.CtxWithQueryDedupeToken(ctx, strconv.Itoa(i))
			}

			start := time.Now()
			var legWg sync.WaitGroup
			for l := 0; l < legs; l++ {
				legWg.Add(1)
				go func() {
					defer legWg.Done()
					list, err := shard.buildAllowList(ctx, filter, additional.Properties{})
					if err != nil {
						t.Error(err)
						return
					}
					// Touch the result so a build that was optimised away would
					// show up as a wrong answer rather than a free win.
					_ = list.Len()
					list.Close()
				}()
			}
			legWg.Wait()
			took[i] = time.Since(start)
		}(i)
	}
	wg.Wait()
	cpu := processCPU() - cpuBefore

	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	return latencyStats{
		p50: percentile(took, 0.50),
		p95: percentile(took, 0.95),
		max: took[len(took)-1],
		cpu: cpu,
	}
}

// runHybridLegs races the real dense and sparse legs as Explorer.Hybrid does,
// so latency includes work the dedupe doesn't touch and isn't directly
// comparable to runLegPairs' phase-level numbers.
func runHybridLegs(t *testing.T, shard *Shard, filter *filters.LocalFilter,
	iters, concurrency int, dedupe bool,
) latencyStats {
	t.Helper()

	took := make([]time.Duration, iters)
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	keyword := &searchparams.KeywordRanking{
		Query:      "alpha beta",
		Type:       "bm25",
		Properties: []string{"title"},
	}

	cpuBefore := processCPU()
	for i := 0; i < iters; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			ctx := context.Background()
			if dedupe {
				ctx = helpers.CtxWithQueryDedupeToken(ctx, strconv.Itoa(i))
			}

			start := time.Now()
			var legs sync.WaitGroup
			legs.Add(2)
			go func() {
				defer legs.Done()
				_, _, err := shard.ObjectVectorSearch(ctx, []models.Vector{[]float32{0.5, 0.5, 0.25}},
					[]string{""}, 0, 20, filter, nil, nil, additional.Properties{}, nil, nil, nil)
				if err != nil {
					t.Error(err)
				}
			}()
			go func() {
				defer legs.Done()
				_, _, err := shard.ObjectSearch(ctx, 20, filter, keyword, nil, nil,
					additional.Properties{}, nil)
				if err != nil {
					t.Error(err)
				}
			}()
			legs.Wait()
			took[i] = time.Since(start)
		}(i)
	}
	wg.Wait()
	cpu := processCPU() - cpuBefore

	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	return latencyStats{
		p50: percentile(took, 0.50),
		p95: percentile(took, 0.95),
		max: took[len(took)-1],
		cpu: cpu,
	}
}

func TestFilterDedupeMeasurement(t *testing.T) {
	if os.Getenv("DEDUPE_BENCH") == "" {
		t.Skip("set DEDUPE_BENCH=1 to run the measurement harness")
	}

	docs := envInt("DEDUPE_BENCH_DOCS", 200_000)
	iters := envInt("DEDUPE_BENCH_ITERS", 200)

	shard := setupRangeFilterShard(t, docs)
	filter := rangeFilter()

	// Warm the segment caches so the first arm does not pay for both.
	for i := 0; i < 5; i++ {
		list, err := shard.buildAllowListDirect(context.Background(), filter, additional.Properties{})
		require.NoError(t, err)
		require.NotZero(t, list.Len())
		list.Close()
	}

	matched, err := shard.buildAllowListDirect(context.Background(), filter, additional.Properties{})
	require.NoError(t, err)
	t.Logf("docs=%d matched=%d (selectivity %.2f%%)",
		docs, matched.Len(), 100*float64(matched.Len())/float64(docs))
	matched.Close()

	repeats := envInt("DEDUPE_BENCH_REPEATS", 5)

	arms := []struct {
		name string
		run  func(t *testing.T, conc int, dedupe bool) latencyStats
	}{
		{
			name: "filter-build-phase",
			run: func(t *testing.T, conc int, dedupe bool) latencyStats {
				return runLegPairs(t, shard, filter, iters, 2, conc, dedupe)
			},
		},
		{
			name: "hybrid-legs-end-to-end",
			run: func(t *testing.T, conc int, dedupe bool) latencyStats {
				return runHybridLegs(t, shard, filter, iters, conc, dedupe)
			},
		},
	}

	pct := func(a, b time.Duration) float64 {
		if a == 0 {
			return 0
		}
		return 100 * (float64(b) - float64(a)) / float64(a)
	}

	for _, arm := range arms {
		for _, concurrency := range []int{1, 4, 8} {
			conc := concurrency
			t.Run(fmt.Sprintf("%s/concurrency-%d", arm.name, conc), func(t *testing.T) {
				var off, on latencyStats
				// Interleave the arms so drift in machine state hits both
				// equally, and keep the minimum of each metric.
				for r := 0; r < repeats; r++ {
					off = minStats(off, arm.run(t, conc, false))
					on = minStats(on, arm.run(t, conc, true))
				}

				t.Logf("%s concurrency=%d iters=%d min-of-%d", arm.name, conc, iters, repeats)
				t.Logf("  cpu  off=%v on=%v  delta=%+.1f%%", off.cpu, on.cpu, pct(off.cpu, on.cpu))
				t.Logf("  p50  off=%v on=%v  delta=%+.1f%%", off.p50, on.p50, pct(off.p50, on.p50))
				t.Logf("  p95  off=%v on=%v  delta=%+.1f%%", off.p95, on.p95, pct(off.p95, on.p95))
				t.Logf("  max  off=%v on=%v  delta=%+.1f%%", off.max, on.max, pct(off.max, on.max))
			})
		}
	}
}

func minStats(a, b latencyStats) latencyStats {
	if a.cpu == 0 {
		return b
	}
	minD := func(x, y time.Duration) time.Duration {
		if x < y {
			return x
		}
		return y
	}
	return latencyStats{
		p50: minD(a.p50, b.p50),
		p95: minD(a.p95, b.p95),
		max: minD(a.max, b.max),
		cpu: minD(a.cpu, b.cpu),
	}
}

var _ = storobj.Object{}
