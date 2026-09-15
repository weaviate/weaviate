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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	batchDeleteClassName = "ThingForDeleteLimit"
	batchDeleteLimit     = int64(10)
)

func TestBatchDeleteObjects_MatchesCappedAtLimit(t *testing.T) {
	tests := []struct {
		name        string
		objectCount int
		limit       int64
		dryRun      bool
		// wantMatches is the Matches field of the first reply. Anything above the limit means
		// more objects match than this call deletes.
		wantMatches int64
		// wantHandled is how many objects the first reply reports on, deleted or dry-run listed.
		wantHandled int
	}{
		{
			name:        "no match",
			objectCount: 0,
			limit:       batchDeleteLimit,
			wantMatches: 0,
			wantHandled: 0,
		},
		{
			name:        "fewer matches than the limit",
			objectCount: 5,
			limit:       batchDeleteLimit,
			wantMatches: 5,
			wantHandled: 5,
		},
		{
			name:        "exactly as many matches as the limit",
			objectCount: 10,
			limit:       batchDeleteLimit,
			wantMatches: 10,
			wantHandled: 10,
		},
		{
			name:        "one match more than the limit",
			objectCount: 11,
			limit:       batchDeleteLimit,
			wantMatches: 11,
			wantHandled: 10,
		},
		{
			name:        "many more matches than the limit",
			objectCount: 50,
			limit:       batchDeleteLimit,
			wantMatches: 11,
			wantHandled: 10,
		},
		{
			name:        "dry run with many more matches than the limit",
			objectCount: 50,
			limit:       batchDeleteLimit,
			dryRun:      true,
			wantMatches: 11,
			wantHandled: 10,
		},
		{
			// A limit of zero or less deletes nothing, so there is nothing to cap and the
			// reply counts every match.
			name:        "limit of zero",
			objectCount: 5,
			limit:       0,
			wantMatches: 5,
			wantHandled: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newBatchDeleteRepo(t, batchDeleteTestClass(false), singleShardState(), tt.limit)
			simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, "", tt.objectCount)

			res, err := repo.BatchDeleteObjects(context.Background(),
				batchDeleteAllParams(tt.dryRun), time.Now(), nil, "", 0)
			require.NoError(t, err)
			require.Equal(t, tt.wantMatches, res.Matches)
			require.Len(t, res.Objects, tt.wantHandled)
			require.Equal(t, tt.limit, res.Limit)

			if tt.limit <= 0 {
				return
			}

			deleted := 0
			if !tt.dryRun {
				deleted = tt.wantHandled
			}
			deleted += drainBatchDelete(t, repo, "")
			require.Equal(t, tt.objectCount, deleted,
				"every matching object must still be deletable in later rounds")
		})
	}
}

func TestBatchDeleteObjects_MatchesCappedAtLimitPerShard(t *testing.T) {
	shardState := multiShardState()
	repo := newBatchDeleteRepo(t, batchDeleteTestClass(false), shardState, batchDeleteLimit)
	simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, "", 50)

	res, err := repo.BatchDeleteObjects(context.Background(),
		batchDeleteAllParams(false), time.Now(), nil, "", 0)
	require.NoError(t, err)

	shards := int64(len(shardState.AllPhysicalShards()))
	require.Greater(t, res.Matches, batchDeleteLimit)
	require.LessOrEqual(t, res.Matches, shards*(batchDeleteLimit+1))
	require.Len(t, res.Objects, int(batchDeleteLimit))

	deleted := int(batchDeleteLimit) + drainBatchDelete(t, repo, "")
	require.Equal(t, 50, deleted)
}

func TestBatchDeleteObjects_MatchesCappedAtLimitForTenant(t *testing.T) {
	const tenant = "foo-tenant"

	shardState := NewMultiTenantShardingStateBuilder().
		WithNodePrefix("node").
		WithIndexName("batch-delete-limit-index").
		WithReplicationFactor(1).
		WithTenant(tenant, models.TenantActivityStatusHOT).
		Build()
	repo := newBatchDeleteRepo(t, batchDeleteTestClass(true), shardState, batchDeleteLimit)
	simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, tenant, 50)

	res, err := repo.BatchDeleteObjects(context.Background(),
		batchDeleteAllParams(false), time.Now(), nil, tenant, 0)
	require.NoError(t, err)
	require.Equal(t, batchDeleteLimit+1, res.Matches)
	require.Len(t, res.Objects, int(batchDeleteLimit))

	deleted := int(batchDeleteLimit) + drainBatchDelete(t, repo, tenant)
	require.Equal(t, 50, deleted)
}

// drainBatchDelete deletes what is left round by round and returns how many objects went away.
// It is the loop a caller writes against Matches, so it fails if the reply ever hides a match.
func drainBatchDelete(t *testing.T, repo *DB, tenant string) int {
	t.Helper()

	deleted := 0
	for round := 0; round < 100; round++ {
		res, err := repo.BatchDeleteObjects(context.Background(),
			batchDeleteAllParams(false), time.Now(), nil, tenant, 0)
		require.NoError(t, err)
		if res.Matches == 0 {
			return deleted
		}
		require.NotEmpty(t, res.Objects, "a non-zero match count must delete something")
		deleted += len(res.Objects)
	}

	t.Fatal("batch delete did not drain the matches within 100 rounds")
	return deleted
}

func batchDeleteTestClass(multiTenancy bool) *models.Class {
	class := &models.Class{
		Class:               batchDeleteClassName,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
	if multiTenancy {
		class.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
			AutoTenantCreation:   true,
		}
	}
	return class
}

func batchDeleteAllParams(dryRun bool) objects.BatchDeleteParams {
	return objects.BatchDeleteParams{
		ClassName: batchDeleteClassName,
		Filters: &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: filters.OperatorLike,
				On: &filters.Path{
					Class:    batchDeleteClassName,
					Property: schema.PropertyName("id"),
				},
				Value: &filters.Value{
					Value: "*",
					Type:  schema.DataTypeText,
				},
			},
		},
		DryRun: dryRun,
		Output: "verbose",
	}
}

func newBatchDeleteRepo(t *testing.T, class *models.Class, shardState *sharding.State,
	queryMaximumResults int64,
) *DB {
	t.Helper()

	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().LocalShards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().LocalActiveShardsCount(mock.Anything).Return(len(shardState.AllPhysicalShards()), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		return readFunc(&models.Class{Class: className}, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()

	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()

	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       queryMaximumResults,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil,
		memwatch.NewDummyMonitor(), mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	t.Cleanup(func() {
		require.NoError(t, repo.Shutdown(context.Background()))
	})

	require.NoError(t, NewMigrator(repo, logger, "node1").AddClass(context.Background(), class))
	schemaGetter.schema.Objects = &models.Schema{Classes: []*models.Class{class}}

	return repo
}
