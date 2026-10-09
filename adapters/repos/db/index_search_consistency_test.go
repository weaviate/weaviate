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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router/types"
	localschema "github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// searchConsistencyClient answers DigestObjects per host: hosts present in
// downHosts return an error, every other host mirrors the caller's objects
// back with matching update times so the digest vote can agree.
type searchConsistencyClient struct {
	FakeReplicationClient
	updateTime int64
	downHosts  map[string]bool
}

func (c *searchConsistencyClient) DigestObjects(_ context.Context, host, _, _ string,
	ids []strfmt.UUID, _ int,
) ([]types.RepairResponse, error) {
	if c.downHosts[host] {
		return nil, fmt.Errorf("host %s unreachable", host)
	}
	out := make([]types.RepairResponse, len(ids))
	for i, id := range ids {
		out[i] = types.RepairResponse{ID: id.String(), UpdateTime: c.updateTime}
	}
	return out, nil
}

// swapSearchConsistencyReplicator replaces the index's replicator with one
// whose router resolves three replicas per shard and computes the consistency
// level against all three, including unreachable ones. Digest reads go to
// client.
func swapSearchConsistencyReplicator(t *testing.T, idx *Index, client replica.Client) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	mockRouter := types.NewMockRouter(t)
	mockRouter.EXPECT().
		BuildRoutingPlanOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(tenant, shard string, cl types.ConsistencyLevel, direct string) types.RoutingPlanBuildOptions {
			return types.RoutingPlanBuildOptions{
				Shard: shard, Tenant: tenant, ConsistencyLevel: cl, DirectCandidateNode: direct,
			}
		}).Maybe()
	mockRouter.EXPECT().
		BuildReadRoutingPlan(mock.Anything).
		RunAndReturn(func(opts types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
			replicas := []types.Replica{
				{NodeName: "node1", ShardName: opts.Shard, HostAddr: "127.0.0.1"},
				{NodeName: "node2", ShardName: opts.Shard, HostAddr: "127.0.0.2"},
				{NodeName: "node3", ShardName: opts.Shard, HostAddr: "127.0.0.3"},
			}
			return types.ReadRoutingPlan{
				LocalHostname:       "127.0.0.1",
				Shard:               opts.Shard,
				Tenant:              opts.Tenant,
				ReplicaSet:          types.ReadReplicaSet{Replicas: replicas},
				ConsistencyLevel:    opts.ConsistencyLevel,
				IntConsistencyLevel: opts.ConsistencyLevel.ToInt(len(replicas)),
			}, nil
		}).Maybe()
	nodeResolver := cluster.NewMockNodeResolver(t)
	nodeResolver.EXPECT().NodeHostname(mock.Anything).RunAndReturn(func(node string) (string, bool) {
		hosts := map[string]string{"node1": "127.0.0.1", "node2": "127.0.0.2", "node3": "127.0.0.3"}
		h, ok := hosts[node]
		return h, ok
	}).Maybe()
	rep, err := replica.NewReplicator(
		idx.Config.ClassName.String(),
		mockRouter,
		nodeResolver,
		"node1",
		func() string { return models.ReplicationConfigDeletionStrategyNoAutomatedResolution },
		client,
		monitoring.GetMetrics(),
		logger,
	)
	require.NoError(t, err)
	idx.replicator = rep
}

const searchConsistencyUpdateTime = int64(999)

func setupSearchConsistencyRepo(t *testing.T, client replica.Client) (*DB, *Index) {
	return setupSearchConsistencyRepoWithState(t, client, singleShardState())
}

func setupSearchConsistencyRepoWithState(t *testing.T, client replica.Client, shardState *sharding.State) (*DB, *Index) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := localschema.NewMockSchemaReader(t)
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)

	// Schema metadata lookups made by the search paths under test.
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

	// Replication routing state consulted when reads fan out to replicas.
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().HasActiveTargetReplicationForShard(mock.Anything, mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()

	cfg := Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}
	repo, err := New(logger, "node1", cfg, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(context.TODO()))
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	class := &models.Class{
		Class:               "SearchConsistencyTestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord},
		},
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
	}
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
	migrator := NewMigrator(repo, logger, "node1")
	require.NoError(t, migrator.AddClass(context.Background(), class))

	idx := repo.GetIndex("SearchConsistencyTestClass")
	require.NotNil(t, idx)
	return repo, idx
}

func putSearchConsistencyObj(t *testing.T, repo *DB, id strfmt.UUID) {
	t.Helper()
	obj := &models.Object{
		ID:                 id,
		Class:              "SearchConsistencyTestClass",
		LastUpdateTimeUnix: searchConsistencyUpdateTime,
		Properties:         map[string]interface{}{"title": "hello"},
	}
	require.NoError(t, repo.PutObject(context.Background(), obj, []float32{0.1, 0.2, 0.3, 0.4}, nil, nil, nil, 0))
}

// Searches that ask for QUORUM or ALL must fail when the level cannot be
// verified instead of silently returning results. One of three replicas is
// down for the whole test.
func TestObjectSearchConsistencyLevelEnforcement(t *testing.T) {
	ctx := context.Background()
	client := &searchConsistencyClient{
		updateTime: searchConsistencyUpdateTime,
		downHosts:  map[string]bool{"127.0.0.3": true},
	}
	repo, idx := setupSearchConsistencyRepo(t, client)
	// write with the default (single-node) replicator, then swap in the
	// three-replica one that drives the consistency checks
	putSearchConsistencyObj(t, repo, "00000000-0000-0000-0000-0000000000a1")
	swapSearchConsistencyReplicator(t, idx, client)

	t.Run("ALL fails when a replica is down", func(t *testing.T) {
		res, _, err := idx.objectSearch(ctx, 10, nil, nil, nil, nil, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "ALL"}, "", 0, nil)
		require.Error(t, err)
		require.Nil(t, res)
		require.Contains(t, err.Error(), `cannot achieve consistency level "ALL"`)
	})

	t.Run("QUORUM passes on the remaining replicas", func(t *testing.T) {
		res, _, err := idx.objectSearch(ctx, 10, nil, nil, nil, nil, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "QUORUM"}, "", 0, nil)
		require.NoError(t, err)
		require.Len(t, res, 1)
	})

	t.Run("ONE skips the check", func(t *testing.T) {
		res, _, err := idx.objectSearch(ctx, 10, nil, nil, nil, nil, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "ONE"}, "", 0, nil)
		require.NoError(t, err)
		require.Len(t, res, 1)
	})

	t.Run("vector search at ALL fails when a replica is down", func(t *testing.T) {
		probe, _, probeErr := idx.objectVectorSearch(ctx,
			[]models.Vector{[]float32{0.1, 0.2, 0.3, 0.4}}, []string{""}, 0, 10,
			nil, nil, nil, additional.Properties{}, nil, "", nil, nil)
		t.Logf("probe at default level: res=%d err=%v", len(probe), probeErr)
		res, _, err := idx.objectVectorSearch(ctx,
			[]models.Vector{[]float32{0.1, 0.2, 0.3, 0.4}}, []string{""}, 0, 10,
			nil, nil, nil, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "ALL"}, "", nil, nil)
		require.Error(t, err)
		require.Nil(t, res)
		require.Contains(t, err.Error(), `cannot achieve consistency level "ALL"`)
	})

	// A search that retains no hits never enters the digest vote, so the level
	// must still be validated against replica availability for the shard.
	zeroHitFilter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			On: &filters.Path{
				Class:    "SearchConsistencyTestClass",
				Property: "title",
			},
			Value: &filters.Value{
				Value: "zzqqxneverpresent",
				Type:  schema.DataTypeText,
			},
		},
	}

	t.Run("ALL fails on a zero-hit search when a replica is down", func(t *testing.T) {
		res, _, err := idx.objectSearch(ctx, 10, zeroHitFilter, nil, nil, nil, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "ALL"}, "", 0, nil)
		require.Error(t, err)
		require.Nil(t, res)
		require.Contains(t, err.Error(), `cannot achieve consistency level "ALL"`)
	})

	t.Run("QUORUM passes on a zero-hit search with two replicas up", func(t *testing.T) {
		res, _, err := idx.objectSearch(ctx, 10, zeroHitFilter, nil, nil, nil, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "QUORUM"}, "", 0, nil)
		require.NoError(t, err)
		require.Empty(t, res)
	})
}

// Objects read from single-replica shards carry no ownership stamp. They must
// be excluded from the consistency check instead of failing it with a
// "missing node or shard" contract error.
func TestCheckSearchConsistencySkipsUnownedObjects(t *testing.T) {
	ctx := context.Background()
	client := &searchConsistencyClient{updateTime: searchConsistencyUpdateTime}
	_, idx := setupSearchConsistencyRepo(t, client)
	swapSearchConsistencyReplicator(t, idx, client)

	owned := &storobj.Object{
		Object:        models.Object{ID: "00000000-0000-0000-0000-0000000000b1", LastUpdateTimeUnix: searchConsistencyUpdateTime},
		BelongsToNode: "node1", BelongsToShard: "shard1",
	}
	unowned := &storobj.Object{
		Object: models.Object{ID: "00000000-0000-0000-0000-0000000000b2", LastUpdateTimeUnix: searchConsistencyUpdateTime},
	}

	t.Run("mixed results only check owned objects", func(t *testing.T) {
		err := idx.checkSearchConsistency(ctx, "QUORUM", "", []string{"shard1"}, []*storobj.Object{owned, unowned})
		require.NoError(t, err)
	})

	t.Run("all-unowned results skip the check entirely", func(t *testing.T) {
		err := idx.checkSearchConsistency(ctx, "ALL", "", []string{"shard1"}, []*storobj.Object{unowned})
		require.NoError(t, err)
	})
}

// Multi-shard vector searches fan out and merge coordinator-side; every merge
// shape (groupBy, explicit sort, default distance order) funnels into the same
// consistency enforcement. One of three replicas per shard is down, so ALL
// must fail on every shape while QUORUM still passes.
func TestObjectVectorSearchConsistencyMultiShard(t *testing.T) {
	ctx := context.Background()
	client := &searchConsistencyClient{
		updateTime: searchConsistencyUpdateTime,
		downHosts:  map[string]bool{"127.0.0.3": true},
	}
	repo, idx := setupSearchConsistencyRepoWithState(t, client, multiShardState())
	// write with the default (single-node) replicator, then swap in the
	// three-replica one that drives the consistency checks
	putSearchConsistencyObj(t, repo, "00000000-0000-0000-0000-0000000000c1")
	putSearchConsistencyObj(t, repo, "00000000-0000-0000-0000-0000000000c2")
	putSearchConsistencyObj(t, repo, "00000000-0000-0000-0000-0000000000c3")
	swapSearchConsistencyReplicator(t, idx, client)

	vec := []models.Vector{[]float32{0.1, 0.2, 0.3, 0.4}}

	t.Run("default distance merging", func(t *testing.T) {
		res, _, err := idx.objectVectorSearch(ctx, vec, []string{""}, 0, 10,
			nil, nil, nil, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "ALL"}, "", nil, nil)
		require.Error(t, err)
		require.Nil(t, res)
		require.Contains(t, err.Error(), `cannot achieve consistency level "ALL"`)
	})

	t.Run("explicit sort", func(t *testing.T) {
		sort := []filters.Sort{{Path: []string{"title"}, Order: "asc"}}
		res, _, err := idx.objectVectorSearch(ctx, vec, []string{""}, 0, 10,
			nil, sort, nil, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "ALL"}, "", nil, nil)
		require.Error(t, err)
		require.Nil(t, res)
		require.Contains(t, err.Error(), `cannot achieve consistency level "ALL"`)
	})

	t.Run("groupBy", func(t *testing.T) {
		groupBy := &searchparams.GroupBy{Property: "title", Groups: 1, ObjectsPerGroup: 1}
		res, _, err := idx.objectVectorSearch(ctx, vec, []string{""}, 0, 10,
			nil, nil, groupBy, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "ALL"}, "", nil, nil)
		require.Error(t, err)
		require.Nil(t, res)
		require.Contains(t, err.Error(), `cannot achieve consistency level "ALL"`)
	})

	t.Run("QUORUM passes on the remaining replicas", func(t *testing.T) {
		res, _, err := idx.objectVectorSearch(ctx, vec, []string{""}, 0, 10,
			nil, nil, nil, additional.Properties{},
			&additional.ReplicationProperties{ConsistencyLevel: "QUORUM"}, "", nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, res)
	})
}
