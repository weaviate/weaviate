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

import (
	"context"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/modules"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/traverser"
)

func setupDedupeRepo(t *testing.T) (*DB, *fakeSchemaGetter, *models.Class, logrus.FieldLogger) {
	t.Helper()

	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		QueryLimit:                20,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, nil, nil,
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	class := SetupFusionClass(t, repo, schemaGetter, logger, 1.2, 0.75)
	return repo, schemaGetter, class, logger
}

func descriptionEquals(term string) filters.Clause {
	return filters.Clause{
		Operator: filters.OperatorEqual,
		On: &filters.Path{
			Class:    schema.ClassName("MyClass"),
			Property: schema.PropertyName("description"),
		},
		Value: &filters.Value{Value: term, Type: schema.DataTypeText},
	}
}

// dedupeTestFilter matches a subset of the fixture.
func dedupeTestFilter() *filters.LocalFilter {
	c := descriptionEquals("BM25F")
	return &filters.LocalFilter{Root: &c}
}

// dedupeBroadFilter matches every fixture doc through an Or root, which is the
// shape the production workload uses (Or root, near-zero selectivity).
func dedupeBroadFilter() *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorOr,
		Operands: []filters.Clause{
			descriptionEquals("BM25F"),
			descriptionEquals("elephants"),
		},
	}}
}

// TestShardAllowListDedupeSharesRealBitmap runs coalescing against the real
// inverted index and pool, where a broken ownership model would corrupt a
// buffer instead of just miscounting.
func TestShardAllowListDedupeSharesRealBitmap(t *testing.T) {
	repo, _, _, _ := setupDedupeRepo(t)
	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)
	shard := firstShard(t, idx)

	filter := dedupeTestFilter()
	addl := additional.Properties{}
	ctx := context.Background()

	// Reference result from the undeduplicated path.
	want, err := shard.buildAllowListDirect(ctx, filter, addl)
	require.NoError(t, err)
	wantIDs := want.Slice()
	want.Close()
	require.NotEmpty(t, wantIDs, "fixture must match at least one doc, or the test proves nothing")

	// Hold the leader inside the build so the follower provably overlaps it.
	gate := make(chan struct{})
	gatedBuild := func(ctx context.Context) (helpers.AllowList, error) {
		<-gate
		return shard.buildAllowListDirect(ctx, filter, addl)
	}

	var (
		leaderList, followList helpers.AllowList
		leaderErr, followErr   error
		followOutcome          string
		wg                     sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		leaderList, _, leaderErr = shard.allowListDedupe.do(ctx, "tok", filter, gatedBuild)
	}()
	waitForParticipants(t, &shard.allowListDedupe, "tok", 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		followList, followOutcome, followErr = shard.allowListDedupe.do(ctx, "tok", filter, gatedBuild)
	}()
	waitForParticipants(t, &shard.allowListDedupe, "tok", 2)

	close(gate)
	wg.Wait()

	require.NoError(t, leaderErr)
	require.NoError(t, followErr)
	assert.Equal(t, helpers.AllowListDedupeShared, followOutcome)
	assert.Same(t,
		leaderList.(*helpers.BitmapAllowList).Bm,
		followList.(*helpers.BitmapAllowList).Bm)
	assert.Equal(t, wantIDs, leaderList.Slice())
	assert.Equal(t, wantIDs, followList.Slice())

	leaderList.Close()
	// The follower still owns the buffer here; reading it must stay correct.
	assert.Equal(t, wantIDs, followList.Slice())
	followList.Close()

	// A double-freed pooled buffer would resurface here as corrupted or
	// aliased data.
	after, err := shard.buildAllowListDirect(ctx, filter, addl)
	require.NoError(t, err)
	assert.Equal(t, wantIDs, after.Slice())
	after.Close()

	assert.Empty(t, shard.allowListDedupe.inFlight)
}

// TestShardAllowListDedupeSequentialDoesNotCache pins that a finished build is
// never reused by a later, sequential caller.
func TestShardAllowListDedupeSequentialDoesNotCache(t *testing.T) {
	repo, _, _, _ := setupDedupeRepo(t)
	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)
	shard := firstShard(t, idx)

	filter := dedupeTestFilter()
	ctx := helpers.CtxWithQueryDedupeToken(context.Background(), "tok")

	first, err := shard.buildAllowList(ctx, filter, additional.Properties{})
	require.NoError(t, err)
	second, err := shard.buildAllowList(ctx, filter, additional.Properties{})
	require.NoError(t, err)

	assert.NotSame(t,
		first.(*helpers.BitmapAllowList).Bm,
		second.(*helpers.BitmapAllowList).Bm,
		"a finished build must not be handed to a later caller")
	assert.Equal(t, first.Slice(), second.Slice())

	first.Close()
	second.Close()
}

// TestHybridFilteredResultsUnchangedByDedupe pins that dedupe never changes
// hybrid query results.
func TestHybridFilteredResultsUnchangedByDedupe(t *testing.T) {
	repo, schemaGetter, class, logger := setupDedupeRepo(t)

	newExplorer := func(dedupeDisabled bool) *traverser.Explorer {
		prov := modules.NewProvider(logger, config.Config{})
		prov.SetClassDefaults(class)
		prov.SetSchemaGetter(schemaGetter)
		testerModule := &TesterModule{}
		testerModule.AddVector("elephant", elephantVector())
		testerModule.AddVector("journey", JourneyVector())
		prov.Register(testerModule)

		conf := defaultConfig
		conf.HybridFilterDedupeDisabled = runtime.NewDynamicValue(dedupeDisabled)

		log, _ := test.NewNullLogger()
		explorer := traverser.NewExplorer(repo, log, prov, nil, conf)
		explorer.SetSchemaGetter(schemaGetter)
		return explorer
	}

	params := func(filter *filters.LocalFilter, alpha float64) dto.GetParams {
		return dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "elephant",
				Vector: elephantVector(),
				Alpha:  alpha,
			},
			Pagination: &filters.Pagination{Offset: 0, Limit: -1},
			Filters:    filter,
			Properties: search.SelectProperties{
				search.SelectProperty{Name: "title"},
				search.SelectProperty{Name: "description"},
			},
		}
	}

	tests := []struct {
		name   string
		filter *filters.LocalFilter
		alpha  float64
	}{
		{name: "broad Or filter, both legs", filter: dedupeBroadFilter(), alpha: 0.5},
		{name: "narrow filter, both legs", filter: dedupeTestFilter(), alpha: 0.5},
		{name: "unfiltered, both legs", filter: nil, alpha: 0.5},
		{name: "filtered, dense only", filter: dedupeBroadFilter(), alpha: 1},
		{name: "filtered, sparse only", filter: dedupeBroadFilter(), alpha: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseline, err := newExplorer(true).Hybrid(context.Background(), params(tt.filter, tt.alpha))
			require.NoError(t, err)
			require.NotEmpty(t, baseline, "an empty baseline would make the comparison vacuous")

			// Repeat so a result that only matches by luck of a single race shows up.
			for i := 0; i < 5; i++ {
				got, err := newExplorer(false).Hybrid(context.Background(), params(tt.filter, tt.alpha))
				require.NoError(t, err)
				require.Equal(t, len(baseline), len(got))
				for j := range baseline {
					assert.Equal(t, baseline[j].ID, got[j].ID)
					assert.InDelta(t, baseline[j].Score, got[j].Score, 1e-6)
				}
			}
		})
	}
}

// TestHybridFilteredConcurrentQueries is the race gate for many concurrent
// hybrid queries racing on one shard-level allow list.
func TestHybridFilteredConcurrentQueries(t *testing.T) {
	repo, schemaGetter, class, logger := setupDedupeRepo(t)

	prov := modules.NewProvider(logger, config.Config{})
	prov.SetClassDefaults(class)
	prov.SetSchemaGetter(schemaGetter)
	testerModule := &TesterModule{}
	testerModule.AddVector("elephant", elephantVector())
	testerModule.AddVector("journey", JourneyVector())
	prov.Register(testerModule)

	conf := defaultConfig
	conf.HybridFilterDedupeDisabled = runtime.NewDynamicValue(false)
	log, _ := test.NewNullLogger()
	explorer := traverser.NewExplorer(repo, log, prov, nil, conf)
	explorer.SetSchemaGetter(schemaGetter)

	params := dto.GetParams{
		ClassName: "MyClass",
		HybridSearch: &searchparams.HybridSearch{
			Query:  "elephant",
			Vector: elephantVector(),
			Alpha:  0.5,
		},
		Pagination: &filters.Pagination{Offset: 0, Limit: -1},
		Filters:    dedupeBroadFilter(),
		Properties: search.SelectProperties{search.SelectProperty{Name: "title"}},
	}

	want, err := explorer.Hybrid(context.Background(), params)
	require.NoError(t, err)
	require.NotEmpty(t, want)
	wantIDs := make([]strfmt.UUID, len(want))
	for i := range want {
		wantIDs[i] = want[i].ID
	}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := explorer.Hybrid(context.Background(), params)
			if !assert.NoError(t, err) {
				return
			}
			gotIDs := make([]strfmt.UUID, len(got))
			for j := range got {
				gotIDs[j] = got[j].ID
			}
			// A shared bitmap freed or mutated under a peer shows up as a
			// short or reordered result set.
			assert.Equal(t, wantIDs, gotIDs)
		}()
	}
	wg.Wait()
}
