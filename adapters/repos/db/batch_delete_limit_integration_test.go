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
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	batchDeleteClassName = "ThingForDeleteLimit"
	batchDeleteLimit     = int64(10)
	batchDeleteTenant    = "foo-tenant"
)

func TestBatchDeleteObjects_MatchesCappedAtLimit(t *testing.T) {
	const otherTenant = "other-tenant"

	tests := []struct {
		name        string
		objectCount int
		limit       int64
		dryRun      bool
		// shardState holds a single shard when nil.
		shardState *sharding.State
		// tenant, when set, makes the class multi-tenant and runs the delete for it.
		tenant string
		// otherTenant, when set, holds objectCount objects of its own that the delete
		// for tenant must leave alone.
		otherTenant string
		// wantMatches is res.Matches, which stops at one above the limit.
		wantMatches int64
		// wantHandled is how many objects the first reply reports on, deleted or dry-run listed.
		wantHandled int
		// wantCappedShards is how many shards resolved as many matches as they were asked for.
		wantCappedShards int
		// wantCappedLogged is whether the call writes the line that tells an operator more
		// matched than it deleted. It fires on every clamped reply, not only when a shard
		// filled its own window.
		wantCappedLogged bool
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
			name:             "one match more than the limit",
			objectCount:      11,
			limit:            batchDeleteLimit,
			wantMatches:      11,
			wantHandled:      10,
			wantCappedShards: 1,
			wantCappedLogged: true,
		},
		{
			name:             "two matches more than the limit",
			objectCount:      12,
			limit:            batchDeleteLimit,
			wantMatches:      11,
			wantHandled:      10,
			wantCappedShards: 1,
			wantCappedLogged: true,
		},
		{
			name:             "many more matches than the limit",
			objectCount:      50,
			limit:            batchDeleteLimit,
			wantMatches:      11,
			wantHandled:      10,
			wantCappedShards: 1,
			wantCappedLogged: true,
		},
		{
			name:             "dry run with many more matches than the limit",
			objectCount:      50,
			limit:            batchDeleteLimit,
			dryRun:           true,
			wantMatches:      11,
			wantHandled:      10,
			wantCappedShards: 1,
			wantCappedLogged: true,
		},
		{
			name:             "many more matches than the limit spread over shards",
			objectCount:      50,
			limit:            batchDeleteLimit,
			shardState:       multiShardState(),
			wantMatches:      11,
			wantHandled:      10,
			wantCappedShards: 3,
			wantCappedLogged: true,
		},
		{
			name:             "many more matches than the limit in one tenant",
			objectCount:      50,
			limit:            batchDeleteLimit,
			shardState:       batchDeleteTenantShardState(batchDeleteTenant, otherTenant),
			tenant:           batchDeleteTenant,
			otherTenant:      otherTenant,
			wantMatches:      11,
			wantHandled:      10,
			wantCappedShards: 1,
			wantCappedLogged: true,
		},
		{
			// Every shard stays under the per-shard bound while the total crosses the
			// limit, so the reply is clamped with no capped shard to report.
			name:             "more matches than the limit spread under the per-shard bound",
			objectCount:      12,
			limit:            batchDeleteLimit,
			shardState:       multiShardState(),
			wantMatches:      11,
			wantHandled:      10,
			wantCappedShards: 0,
			wantCappedLogged: true,
		},
		{
			// A limit <= 0 turns the cap off: Matches counts everything, nothing deletes.
			name:        "limit of zero",
			objectCount: 5,
			limit:       0,
			wantMatches: 5,
			wantHandled: 0,
		},
		{
			name:        "negative limit",
			objectCount: 5,
			limit:       -1,
			wantMatches: 5,
			wantHandled: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shardState := tt.shardState
			if shardState == nil {
				shardState = singleShardState()
			}
			repo := newBatchDeleteRepo(t, batchDeleteTestClass(tt.tenant != ""), shardState, tt.limit)
			simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, tt.tenant, tt.objectCount)
			if tt.otherTenant != "" {
				simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, tt.otherTenant, tt.objectCount)
			}
			logs := batchDeleteLogs(t, repo)

			res, err := repo.BatchDeleteObjects(context.Background(),
				batchDeleteMatchAllParams(tt.dryRun), time.Now(), nil, tt.tenant, 0)
			require.NoError(t, err)
			require.Equal(t, tt.wantMatches, res.Matches)
			require.Len(t, res.Objects, tt.wantHandled)
			require.Equal(t, tt.limit, res.Limit)
			logged, cappedShards := cappedLineLogged(t, logs)
			require.Equal(t, tt.wantCappedLogged, logged)
			require.Equal(t, tt.wantCappedShards, cappedShards)

			if tt.limit <= 0 {
				// drainBatchDelete cannot drain this one: the call deletes nothing and
				// the next reports the same matches, for as long as a caller repeats it.
				res, err = repo.BatchDeleteObjects(context.Background(),
					batchDeleteMatchAllParams(tt.dryRun), time.Now(), nil, tt.tenant, 0)
				require.NoError(t, err)
				require.Equal(t, tt.wantMatches, res.Matches)
				require.Empty(t, res.Objects)
				return
			}

			deleted := 0
			if !tt.dryRun {
				deleted = tt.wantHandled
			}
			deleted += drainBatchDelete(t, repo, batchDeleteMatchAllParams(false), tt.tenant)
			require.Equal(t, tt.objectCount, deleted,
				"every matching object must still be deletable in later calls")

			if tt.otherTenant != "" {
				require.Equal(t, tt.objectCount,
					drainBatchDelete(t, repo, batchDeleteMatchAllParams(false), tt.otherTenant),
					"draining one tenant must leave the other tenant's objects in place")
			}
		})
	}
}

// TestBatchDeleteObjects_ResolvesPastDeadDocIDs pins that a dead doc id (its object
// gone) never consumes a resolve slot, including after a restart resurrects it in the
// doc id universe a deny-list filter starts from.
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
// deleted. Fails if a reply matches but deletes nothing, or after 100 calls.
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
	class := makeTestClass(batchDeleteClassName)
	if multiTenancy {
		class.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
			AutoTenantCreation:   true,
		}
	}
	return class
}

func batchDeleteTenantShardState(tenants ...string) *sharding.State {
	builder := NewMultiTenantShardingStateBuilder().
		WithNodePrefix("node").
		WithIndexName("batch-delete-limit-index").
		WithReplicationFactor(1)
	for _, tenant := range tenants {
		builder = builder.WithTenant(tenant, models.TenantActivityStatusHOT)
	}
	return builder.Build()
}

// batchDeleteLogs collects the log entries the DB writes from here on, so a test can
// read the fields of the line one batch delete call produced.
func batchDeleteLogs(t *testing.T, repo *DB) *test.Hook {
	t.Helper()

	logger, ok := repo.logger.(*logrus.Logger)
	require.True(t, ok, "the test DB logs through a *logrus.Logger")
	hook := test.NewLocal(logger)
	t.Cleanup(hook.Reset)

	return hook
}

// cappedLineLogged returns whether the hook saw the line a capped call writes, and the
// capped_shards it reported. The line fires on every clamped reply, which a shard filling
// its own window is only one case of, so the two answers are not the same question.
func cappedLineLogged(t *testing.T, hook *test.Hook) (bool, int) {
	t.Helper()

	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] != "batch_delete_objects_capped" {
			continue
		}
		capped, ok := entry.Data["capped_shards"].(int)
		require.True(t, ok, "capped_shards is logged as an int")
		return true, capped
	}
	return false, 0
}

func batchDeleteMatchAllParams(dryRun bool) objects.BatchDeleteParams {
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
