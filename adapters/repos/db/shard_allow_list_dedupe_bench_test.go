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

// Measures the hybrid filter dedupe. CPU is the claim rather than wall clock:
// the two legs run concurrently, so removing one build need not shorten the
// query, which is why cpu-ns/op is reported alongside ns/op.
//
// Compare the dedupe-off and dedupe-on arms with benchstat:
//
//	go test -tags integrationTest -run '^$' -bench BenchmarkFilterDedupe \
//	  -benchtime 60x -cpu 1,4,8 -timeout 60m ./adapters/repos/db/

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
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
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Large enough that the bit-sliced cascade dominates the profile, and the size
// the measurements in this PR's description were taken at.
const benchDedupeDocs = 50_000

// setupRangeFilterShard builds a shard whose range-indexed property resolves
// through the bit-sliced cascade that dominates the production profile.
func setupRangeFilterShard(tb testing.TB, docs int) *Shard {
	tb.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(tb)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retry bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockFSM := replicationTypes.NewMockReplicationFSMReader(tb)
	mockFSM.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}).Maybe()
	mockFSM.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}, nil).Maybe()
	mockNodes := cluster.NewMockNodeSelector(tb)
	mockNodes.EXPECT().LocalName().Return("node1").Maybe()
	mockNodes.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  tb.TempDir(),
		QueryMaximumResults:       10000000,
		MaxImportGoroutinesFactor: 1,
		QueryLimit:                20,
	}, &FakeRemoteClient{}, mockNodes, &FakeRemoteNodeClient{}, nil, nil, nil,
		mockNodes, mockSchemaReader, mockFSM)
	require.Nil(tb, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(tb, repo.WaitForStartup(context.TODO()))
	tb.Cleanup(func() { repo.Shutdown(context.Background()) })

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
	require.NoError(tb, NewMigrator(repo, logger, "node1").AddClass(context.Background(), class))

	idx := repo.GetIndex("RangeClass")
	require.NotNil(tb, idx)

	rnd := rand.New(rand.NewSource(42))
	for i := 0; i < docs; i++ {
		require.NoError(tb, repo.PutObject(context.Background(), &models.Object{
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

	require.NoError(tb, idx.ForEachShard(func(_ string, sl ShardLike) error {
		if s, ok := sl.(*Shard); ok {
			return s.store.FlushMemtables(context.Background())
		}
		return nil
	}))

	return firstShard(tb, idx)
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

func BenchmarkFilterDedupe(b *testing.B) {
	shard := setupRangeFilterShard(b, benchDedupeDocs)
	filter := rangeFilter()
	addl := additional.Properties{}

	// Warm the segment caches so the first arm does not pay for both.
	for i := 0; i < 5; i++ {
		list, err := shard.buildAllowListDirect(context.Background(), filter, addl)
		require.NoError(b, err)
		require.NotZero(b, list.Len())
		list.Close()
	}

	keyword := &searchparams.KeywordRanking{
		Query:      "alpha beta",
		Type:       "bm25",
		Properties: []string{"title"},
	}

	arms := []struct {
		name string
		// run performs one query's worth of work: the two legs a filtered
		// hybrid query fans out into, racing each other as they do in
		// production.
		run func(ctx context.Context) error
	}{
		{
			// Isolates the filter build, so the saving is not diluted by the
			// rest of each leg's work.
			name: "filter-build-phase",
			run: func(ctx context.Context) error {
				var wg sync.WaitGroup
				errs := make([]error, 2)
				wg.Add(2)
				for leg := 0; leg < 2; leg++ {
					go func(leg int) {
						defer wg.Done()
						list, err := shard.buildAllowList(ctx, filter, addl)
						if err != nil {
							errs[leg] = err
							return
						}
						// Touch the result so a build that was optimised away
						// shows up as a wrong answer rather than a free win.
						if list.Len() == 0 {
							errs[leg] = fmt.Errorf("leg %d: empty allow list", leg)
						}
						list.Close()
					}(leg)
				}
				wg.Wait()
				return firstErr(errs)
			},
		},
		{
			name: "hybrid-legs-end-to-end",
			run: func(ctx context.Context) error {
				var wg sync.WaitGroup
				errs := make([]error, 2)
				wg.Add(2)
				go func() {
					defer wg.Done()
					_, _, errs[0] = shard.ObjectVectorSearch(ctx,
						[]models.Vector{[]float32{0.5, 0.5, 0.25}},
						[]string{""}, 0, 20, filter, nil, nil, addl, nil, nil, nil)
				}()
				go func() {
					defer wg.Done()
					_, _, errs[1] = shard.ObjectSearch(ctx, 20, filter, keyword, nil, nil,
						addl, nil)
				}()
				wg.Wait()
				return firstErr(errs)
			},
		},
	}

	for _, arm := range arms {
		for _, dedupe := range []bool{false, true} {
			state := "off"
			if dedupe {
				state = "on"
			}
			b.Run(fmt.Sprintf("%s/dedupe-%s", arm.name, state), func(b *testing.B) {
				// Tokens coalesce only within one query, so each iteration
				// needs its own or the arms would not be comparable.
				var query atomic.Int64

				cpuBefore := processCPU()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						ctx := context.Background()
						if dedupe {
							ctx = helpers.CtxWithQueryDedupeToken(ctx,
								strconv.FormatInt(query.Add(1), 10))
						}
						if err := arm.run(ctx); err != nil {
							b.Error(err)
							return
						}
					}
				})
				b.StopTimer()

				b.ReportMetric(float64(processCPU()-cpuBefore)/float64(b.N), "cpu-ns/op")
			})
		}
	}
}

func firstErr(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
