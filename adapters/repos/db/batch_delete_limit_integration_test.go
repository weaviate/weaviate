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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/objects"
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
		// wantMatches: expected res.Matches; above limit means more matched than this call deletes.
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
			// limit <= 0 disables capping: Matches counts everything, nothing is deleted.
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
			deleted += drainBatchDelete(t, repo, batchDeleteAllParams(false), "")
			require.Equal(t, tt.objectCount, deleted,
				"every matching object must still be deletable in later calls")
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

	deleted := int(batchDeleteLimit) + drainBatchDelete(t, repo, batchDeleteAllParams(false), "")
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

	deleted := int(batchDeleteLimit) + drainBatchDelete(t, repo, batchDeleteAllParams(false), tenant)
	require.Equal(t, 50, deleted)
}

// TestBatchDeleteObjects_ResolvesPastDeadDocIDs pins that a doc id whose object is gone
// does not take a slot in the bounded resolve. A deny-list filter starts from the doc id
// universe, which is rebuilt at shard init from a counter that never decrements, so after
// a restart every doc id an earlier call deleted is back in it.
func TestBatchDeleteObjects_ResolvesPastDeadDocIDs(t *testing.T) {
	const objectCount = 30

	tests := []struct {
		name string
		// callsBeforeRestart is how many batch deletes run before the restart. Each
		// removes batchDeleteLimit objects, lowest doc id first.
		callsBeforeRestart int
		deleteOneOutOfBand bool
		wantMatches        int64
	}{
		{
			name:               "one object deleted out of band",
			deleteOneOutOfBand: true,
			wantMatches:        batchDeleteLimit + 1,
		},
		{
			name:               "the doc ids earlier calls deleted",
			callsBeforeRestart: 2,
			wantMatches:        objectCount - 2*batchDeleteLimit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			rootDir := t.TempDir()
			shardState := singleShardState()

			repo := newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, batchDeleteLimit)
			simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, "", objectCount)

			gone := 0
			for i := 0; i < tt.callsBeforeRestart; i++ {
				res, err := repo.BatchDeleteObjects(ctx, batchDeleteDenyListParams(false),
					time.Now(), nil, "", 0)
				require.NoError(t, err)
				gone += len(res.Objects)
			}
			if tt.deleteOneOutOfBand {
				require.NoError(t, repo.DeleteObject(ctx, batchDeleteClassName,
					strfmt.UUID("8d5a3aa2-3c8d-4589-9ae1-3f638f506000"), time.Now(), nil, "", 0))
				gone++
			}
			require.NoError(t, repo.Shutdown(ctx))

			repo = newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, batchDeleteLimit)
			t.Cleanup(func() {
				require.NoError(t, repo.Shutdown(context.Background()))
			})

			res, err := repo.BatchDeleteObjects(ctx, batchDeleteDenyListParams(false),
				time.Now(), nil, "", 0)
			require.NoError(t, err)
			require.Equal(t, tt.wantMatches, res.Matches)

			deleted := len(res.Objects) + drainBatchDelete(t, repo, batchDeleteDenyListParams(false), "")
			require.Equal(t, objectCount-gone, deleted,
				"every object still matching must be deleted after the restart")
		})
	}
}

// drainBatchDelete repeats batch delete until Matches hits zero, returning the total
// deleted. Fails if a reply reports matches but deletes nothing, or if the matches
// are still not drained after 100 calls.
func drainBatchDelete(t *testing.T, repo *DB, params objects.BatchDeleteParams, tenant string) int {
	t.Helper()

	deleted := 0
	for i := 0; i < 100; i++ {
		res, err := repo.BatchDeleteObjects(context.Background(),
			params, time.Now(), nil, tenant, 0)
		require.NoError(t, err)
		if res.Matches == 0 {
			return deleted
		}
		require.NotEmpty(t, res.Objects, "a non-zero match count must delete something")
		deleted += len(res.Objects)
	}

	t.Fatal("batch delete did not drain the matches within 100 calls")
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
	return batchDeleteParams(&filters.Clause{
		Operator: filters.OperatorLike,
		On: &filters.Path{
			Class:    batchDeleteClassName,
			Property: schema.PropertyName("id"),
		},
		Value: &filters.Value{
			Value: "*",
			Type:  schema.DataTypeText,
		},
	}, dryRun)
}

// batchDeleteDenyListParams matches every object through a deny list: the shard
// subtracts the rows holding the value from the doc id universe, and that universe
// holds every doc id the shard ever allocated, including deleted ones.
func batchDeleteDenyListParams(dryRun bool) objects.BatchDeleteParams {
	return batchDeleteParams(&filters.Clause{
		Operator: filters.OperatorNotEqual,
		On: &filters.Path{
			Class:    batchDeleteClassName,
			Property: schema.PropertyName("stringProp"),
		},
		Value: &filters.Value{
			Value: "absent",
			Type:  schema.DataTypeText,
		},
	}, dryRun)
}

func batchDeleteParams(root *filters.Clause, dryRun bool) objects.BatchDeleteParams {
	return objects.BatchDeleteParams{
		ClassName: batchDeleteClassName,
		Filters:   &filters.LocalFilter{Root: root},
		DryRun:    dryRun,
		Output:    "verbose",
	}
}

func newBatchDeleteRepo(t *testing.T, class *models.Class, shardState *sharding.State,
	queryMaximumResults int64,
) *DB {
	t.Helper()

	repo := newBatchDeleteRepoAt(t, t.TempDir(), class, shardState, queryMaximumResults)
	t.Cleanup(func() {
		require.NoError(t, repo.Shutdown(context.Background()))
	})

	return repo
}

// newBatchDeleteRepoAt opens a DB on rootDir. The caller owns the shutdown, so the
// same directory can be reopened within one test.
func newBatchDeleteRepoAt(t *testing.T, rootDir string, class *models.Class,
	shardState *sharding.State, queryMaximumResults int64,
) *DB {
	t.Helper()

	return setupTestDBWithShardState(t, rootDir, shardState, func(cfg *Config) {
		cfg.QueryMaximumResults = queryMaximumResults
	}, class)
}
