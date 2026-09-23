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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	batchDeleteClassName = "ThingForDeleteLimit"
	batchDeleteLimit     = int64(10)
	batchDeleteTenant    = "foo-tenant"

	// batchDeleteTTLClassName is a second class because the objects TTL sweep filters on
	// a date property the batch delete class has no reason to carry.
	batchDeleteTTLClassName = "ThingForDeleteLimitTTL"
	batchDeleteTTLProp      = "expiresAt"
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
		// cappedShardsFromLayout takes wantCappedShards from the objects each shard holds
		// rather than the literal above. sharding.InitState shuffles the virtual shards, so
		// the same objects do not split over the physical shards the same way twice.
		cappedShardsFromLayout bool
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
			name:                   "many more matches than the limit spread over shards",
			objectCount:            50,
			limit:                  batchDeleteLimit,
			shardState:             multiShardState(),
			wantMatches:            11,
			wantHandled:            10,
			cappedShardsFromLayout: true,
			wantCappedLogged:       true,
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
			wantCappedShards := tt.wantCappedShards
			if tt.cappedShardsFromLayout {
				wantCappedShards = shardsAtTheResolveLimit(t, repo, batchDeleteClassName, tt.limit)
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
			require.Equal(t, wantCappedShards, cappedShards)

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

// shardsAtTheResolveLimit counts the class's shards holding at least as many objects as one
// call resolves per shard, which is how many of them a call reports as capped. It counts the
// objects the shards hold rather than working out where an id routes, so it measures the
// split the call itself runs against.
func shardsAtTheResolveLimit(t *testing.T, repo *DB, className string, limit int64) int {
	t.Helper()

	perShard := perShardResolveLimit(limit)
	require.Positive(t, perShard, "an uncapped resolve caps no shard")

	index := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, index)

	atLimit := 0
	require.NoError(t, index.ForEachShard(func(_ string, shard ShardLike) error {
		count, err := shard.ObjectCount(context.Background())
		if err != nil {
			return err
		}
		if count >= perShard {
			atLimit++
		}
		return nil
	}))

	return atLimit
}

func batchDeleteMatchAllParams(dryRun bool) objects.BatchDeleteParams {
	return batchDeleteParams(batchDeleteMatchAllClause(), dryRun)
}

// batchDeleteAndRootParams matches every object through an And of two leaf clauses. A
// compound root is resolved with the limit dropped (inverted/prop_value_pairs.go:142),
// so the allow list it produces is complete and was never truncated by the bound.
func batchDeleteAndRootParams(dryRun bool) objects.BatchDeleteParams {
	return batchDeleteParams(&filters.Clause{
		Operator: filters.OperatorAnd,
		Operands: []filters.Clause{
			*batchDeleteMatchAllClause(),
			{
				Operator: filters.OperatorLike,
				On: &filters.Path{
					Class:    batchDeleteClassName,
					Property: schema.PropertyName("stringProp"),
				},
				Value: &filters.Value{
					Value: "*",
					Type:  schema.DataTypeText,
				},
			},
		},
	}, dryRun)
}

func batchDeleteMatchAllClause() *filters.Clause {
	return &filters.Clause{
		Operator: filters.OperatorLike,
		On: &filters.Path{
			Class:    batchDeleteClassName,
			Property: schema.PropertyName("id"),
		},
		Value: &filters.Value{
			Value: "*",
			Type:  schema.DataTypeText,
		},
	}
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

// TestBatchDeleteObjects_DeadDocIDDoesNotBurnASlot pins that a doc id whose object row is
// gone while the inverted postings still name it costs a read and not one of the caller's
// limit slots. Were the filter resolved capped at the limit, a dead id inside that window
// would leave the reply at exactly limit, which the published contract reads as "exact,
// everything handled".
func TestBatchDeleteObjects_DeadDocIDDoesNotBurnASlot(t *testing.T) {
	tests := []struct {
		name   string
		params func(dryRun bool) objects.BatchDeleteParams
		// objectCount is how many objects the class holds before any row is dropped.
		objectCount int
		// dropRows is how many of the first reply's objects lose their row, postings kept.
		dropRows    int
		wantMatches int64
		wantHandled int
	}{
		{
			name:        "leaf allow-list filter with one dead doc id in the window",
			params:      batchDeleteMatchAllParams,
			objectCount: 30,
			dropRows:    1,
			wantMatches: batchDeleteLimit + 1,
			wantHandled: int(batchDeleteLimit),
		},
		{
			name:        "leaf allow-list filter with several dead doc ids in the window",
			params:      batchDeleteMatchAllParams,
			objectCount: 30,
			dropRows:    3,
			wantMatches: batchDeleteLimit + 1,
			wantHandled: int(batchDeleteLimit),
		},
		{
			// A deny list resolves against the doc id universe, which still holds the
			// dead id until a walk prunes it.
			name:        "deny-list filter",
			params:      batchDeleteDenyListParams,
			objectCount: 30,
			dropRows:    1,
			wantMatches: batchDeleteLimit + 1,
			wantHandled: int(batchDeleteLimit),
		},
		{
			name:        "compound root with one dead doc id",
			params:      batchDeleteAndRootParams,
			objectCount: 30,
			dropRows:    1,
			wantMatches: batchDeleteLimit + 1,
			wantHandled: int(batchDeleteLimit),
		},
		{
			// Five matches never fill the window, so the reply is short because one
			// object is genuinely gone.
			name:        "fewer matches than the limit with one of them dead",
			params:      batchDeleteMatchAllParams,
			objectCount: 5,
			dropRows:    1,
			wantMatches: 4,
			wantHandled: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo := newBatchDeleteRepo(t, batchDeleteTestClass(false), singleShardState(), batchDeleteLimit)
			simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, "", tt.objectCount)

			before, err := repo.BatchDeleteObjects(ctx, tt.params(true), time.Now(), nil, "", 0)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(before.Objects), tt.dropRows)

			// The UUIDs a dry run lists are the lowest doc ids the filter resolved, which
			// is the window a truncated allow list holds.
			for i := 0; i < tt.dropRows; i++ {
				dropObjectRow(t, repo, batchDeleteClassName, before.Objects[i].UUID)
			}

			after, err := repo.BatchDeleteObjects(ctx, tt.params(true), time.Now(), nil, "", 0)
			require.NoError(t, err)
			require.Equal(t, tt.wantMatches, after.Matches,
				"%d objects still match", tt.objectCount-tt.dropRows)
			require.Len(t, after.Objects, tt.wantHandled)
		})
	}
}

// TestBatchDeleteObjects_UnreadableRowDoesNotBurnASlot is the dead doc id case for a row
// that is there but carries no readable id. The walk skips it like a missing row and reads
// on to the next match, so the reply still reaches limit + 1, and the skip is reported once.
func TestBatchDeleteObjects_UnreadableRowDoesNotBurnASlot(t *testing.T) {
	ctx := context.Background()
	const objectCount = 30

	repo := newBatchDeleteRepo(t, batchDeleteTestClass(false), singleShardState(), batchDeleteLimit)
	simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, "", objectCount)

	before, err := repo.BatchDeleteObjects(ctx, batchDeleteMatchAllParams(true), time.Now(), nil, "", 0)
	require.NoError(t, err)
	require.NotEmpty(t, before.Objects)
	corruptObjectRow(t, repo, batchDeleteClassName, before.Objects[0].UUID)

	hook := batchDeleteLogs(t, repo)
	after, err := repo.BatchDeleteObjects(ctx, batchDeleteMatchAllParams(true), time.Now(), nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, batchDeleteLimit+1, after.Matches, "%d readable objects still match", objectCount-1)
	require.Len(t, after.Objects, int(batchDeleteLimit))

	warns := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && entry.Data["op"] == "shard.find_uuids" {
			warns++
		}
	}
	require.Equal(t, 1, warns, "the skip is reported once per call, not once per doc id")
}

// TestFindUUIDs_ResolvesTheFilterOnce pins that a call runs the inverted resolve once,
// counted through the per-leaf entries the searcher appends to the slow query details.
// Dead doc ids inside the first limit matches are the shape that could tempt a second,
// uncapped resolve; a positive filter's postings keep naming them, since pruning only
// touches the deny-list universe, so a second resolve would be paid on every call.
func TestFindUUIDs_ResolvesTheFilterOnce(t *testing.T) {
	tests := []struct {
		name     string
		params   func(dryRun bool) objects.BatchDeleteParams
		dropRows int
	}{
		{name: "leaf allow-list filter, no dead doc id", params: batchDeleteMatchAllParams},
		{name: "leaf allow-list filter, one dead doc id in the window", params: batchDeleteMatchAllParams, dropRows: 1},
		{name: "leaf allow-list filter, several dead doc ids in the window", params: batchDeleteMatchAllParams, dropRows: 3},
		{name: "deny-list filter, one dead doc id", params: batchDeleteDenyListParams, dropRows: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo := newBatchDeleteRepo(t, batchDeleteTestClass(false), singleShardState(), batchDeleteLimit)
			simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, "", 30)

			before, err := repo.BatchDeleteObjects(ctx, tt.params(true), time.Now(), nil, "", 0)
			require.NoError(t, err)
			for i := 0; i < tt.dropRows; i++ {
				dropObjectRow(t, repo, batchDeleteClassName, before.Objects[i].UUID)
			}

			shard := loadedShard(t, repo, batchDeleteClassName)
			limit := perShardResolveLimit(batchDeleteLimit)
			for call := 0; call < 2; call++ {
				callCtx := helpers.InitSlowQueryDetails(ctx)
				uuids, err := shard.FindUUIDs(callCtx, tt.params(true).Filters, limit)
				require.NoError(t, err)
				require.Len(t, uuids, limit, "call %d fills the window past the dead ids", call)

				resolves, ok := helpers.ExtractSlowQueryDetails(callCtx)["build_allow_list_doc_bitmap"].([]map[string]any)
				require.True(t, ok, "call %d: the searcher records each leaf it resolves", call)
				require.Len(t, resolves, 1, "call %d resolves the filter once", call)
			}
		})
	}
}

// TestBatchDeleteObjects_FailsOnAReadError pins the behaviour this change leads its own
// risks with: a read error on an object row fails the whole resolve where the merge base
// skipped that doc id and walked on. Skipping bounded nothing, since only a resolved UUID
// advances the counter the walk breaks on, so a store that fails every read walked the
// shard's entire allow list before returning a short reply.
func TestBatchDeleteObjects_FailsOnAReadError(t *testing.T) {
	ctx := context.Background()
	readErr := errors.New("segment read failed")

	repo := newBatchDeleteRepo(t, batchDeleteTestClass(false), singleShardState(), batchDeleteLimit)
	simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, "", 30)

	// Load the shard through a real call, then drive the seam under it directly: the
	// bucket is a parameter, so a failing one needs no broken store.
	_, err := repo.BatchDeleteObjects(ctx, batchDeleteMatchAllParams(true), time.Now(), nil, "", 0)
	require.NoError(t, err)
	shard := loadedShard(t, repo, batchDeleteClassName)

	failing := &stubDocIDBucket{err: readErr}

	limit := int(batchDeleteLimit)
	pass, err := shard.resolveAndCollectUUIDs(ctx, batchDeleteMatchAllParams(true).Filters,
		limit, failing)
	require.ErrorContains(t, err, "resolve uuids")
	require.ErrorIs(t, err, readErr, "the store's error still reaches the caller")
	require.Empty(t, pass.uuids, "a failed resolve returns no UUIDs")
}

// TestBatchDeleteObjects_PrunesOnlyBelowTheDocIDWatermark pins which doc ids the resolve
// may drop from the doc id universe. An id allocated before shard init is written or dead
// forever; an id allocated since may belong to an insert that has taken the id and not yet
// written the row, and dropping that one hides a live object from every deny-list filter
// until the next shard init.
func TestBatchDeleteObjects_PrunesOnlyBelowTheDocIDWatermark(t *testing.T) {
	ctx := context.Background()
	const (
		beforeRestart = 10
		afterRestart  = 5
	)

	rootDir := t.TempDir()
	shardState := singleShardState()

	repo := newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, 0)
	insertBatchDeleteObjects(t, repo, 0, beforeRestart)
	require.NoError(t, repo.Shutdown(ctx))

	// Reopening sets the watermark to the doc id counter, so every id above is below it
	// and every id the inserts below take is at or above it.
	repo = newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, 0)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	insertBatchDeleteObjects(t, repo, beforeRestart, afterRestart)

	shard := loadedShard(t, repo, batchDeleteClassName)
	require.Equal(t, uint64(beforeRestart), shard.docIDPruneWatermark)

	belowWatermark := dropObjectRow(t, repo, batchDeleteClassName, batchDeleteObjectID(0))
	atWatermark := dropObjectRow(t, repo, batchDeleteClassName, batchDeleteObjectID(beforeRestart))
	require.Less(t, belowWatermark, shard.docIDPruneWatermark)
	require.GreaterOrEqual(t, atWatermark, shard.docIDPruneWatermark)

	res, err := repo.BatchDeleteObjects(ctx, batchDeleteDenyListParams(true), time.Now(), nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(beforeRestart+afterRestart-2), res.Matches,
		"both rowless doc ids are skipped, neither is reported as a match")

	universe, release := shard.bitmapFactory.GetBitmap()
	defer release()
	require.False(t, universe.Contains(belowWatermark),
		"a doc id below the watermark with no object row is dropped from the universe")
	require.True(t, universe.Contains(atWatermark),
		"a doc id at or above the watermark is kept: its object row may still be on its way")
}

// TestBatchDeleteObjects_WalksWhileObjectsAreInserted runs the walk against a shard that is
// being written to. The walk is not read-only: it subtracts the doc ids it found no object
// row for from the doc id universe, under the same BitmapFactory lock an insert's universe
// read takes, so this is the one case that exercises the two against each other. No object
// an insert completed may be missing from a resolve that follows it.
//
// What this does NOT pin is the watermark, and the reason is worth writing down so the next
// reader does not assume it does. The hazard the watermark exists for is an id an insert
// has taken (shard_write_put.go:319) whose row has not landed yet (:342) being read by a
// walk. A walk reads the universe in ascending doc id order, so the newest id is the last
// one it reads, by which point the write is long done: instrumenting this test at 500 to
// 1000 overlapping walks per run measured zero ids read without a row. The watermark's pin
// is TestBatchDeleteObjects_PrunesOnlyBelowTheDocIDWatermark, which puts an id on each side
// of it directly.
func TestBatchDeleteObjects_WalksWhileObjectsAreInserted(t *testing.T) {
	ctx := context.Background()
	const (
		beforeRestart = 20
		insertRounds  = 20
		insertBatch   = 25
		concurrent    = insertRounds * insertBatch
		// High enough that a dry run lists every object it matched, so the assertion can
		// name the UUIDs rather than only count them.
		limit = int64(4 * (beforeRestart + concurrent))
	)

	rootDir := t.TempDir()
	shardState := singleShardState()

	repo := newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, limit)
	insertBatchDeleteObjects(t, repo, 0, beforeRestart)
	require.NoError(t, repo.Shutdown(ctx))

	// The restart is what puts a dead prefix in front of the walk, so the walk below has
	// ids to prune and takes the BitmapFactory write lock while the inserts read it.
	repo = newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, limit)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })

	// The inserting goroutine reports rather than asserts: testify's failures only work on
	// the test's own goroutine.
	var (
		wg        sync.WaitGroup
		inserting = make(chan struct{})
		insertErr error
	)
	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		defer close(inserting)
		for i := 0; i < insertRounds; i++ {
			batch := newBatchDeleteObjects(beforeRestart+i*insertBatch, insertBatch)
			res, err := repo.BatchPutObjects(ctx, batch, nil, 0)
			if err != nil {
				insertErr = err
				return
			}
			for _, item := range res {
				if item.Err != nil {
					insertErr = item.Err
					return
				}
			}
		}
	}, repo.logger)

	// A deny-list dry run walks the whole doc id universe, and keeps walking for as long
	// as the inserts run.
	walks := 0
	for done := false; !done; {
		select {
		case <-inserting:
			done = true
		default:
			_, err := repo.BatchDeleteObjects(ctx, batchDeleteDenyListParams(true), time.Now(), nil, "", 0)
			require.NoError(t, err)
			walks++
		}
	}
	wg.Wait()
	require.NoError(t, insertErr)
	require.Positive(t, walks, "the walks have to have overlapped the inserts")

	res, err := repo.BatchDeleteObjects(ctx, batchDeleteDenyListParams(true), time.Now(), nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(beforeRestart+concurrent), res.Matches,
		"an object inserted while the walk pruned must still be in the universe")

	returned := make(map[strfmt.UUID]struct{}, len(res.Objects))
	for _, obj := range res.Objects {
		returned[obj.UUID] = struct{}{}
	}
	for i := 0; i < beforeRestart+concurrent; i++ {
		id := batchDeleteObjectID(i)
		require.Contains(t, returned, id, "object %d is still there and must still match", i)
	}
}

// TestObjectsTTLSweepResolvesPastADeadDocID pins Shard.FindUUIDs' other caller. The objects
// TTL sweep runs the same bounded resolve and inherits the same walk, so a doc id whose
// object row is gone while its postings still name it must cost the sweep a read and not an
// expired object left behind, and must leave the doc id universe like any other.
func TestObjectsTTLSweepResolvesPastADeadDocID(t *testing.T) {
	ctx := context.Background()
	const (
		expiredCount = 12
		aliveCount   = 3
		sweepBatch   = 5
	)

	rootDir := t.TempDir()
	shardState := singleShardState()
	class := batchDeleteTTLClass()
	newTTLRepo := func() *DB {
		return setupTestDBWithShardState(t, rootDir, shardState, func(cfg *Config) {
			cfg.ObjectsTTLBatchSize = configRuntime.NewDynamicValue(sweepBatch)
		}, class)
	}

	repo := newTTLRepo()
	insertTTLObjects(t, repo, expiredCount, aliveCount)
	// The first object is expired and loses its row, postings kept, which is the state
	// every delete passes through.
	deadDocID := dropObjectRow(t, repo, batchDeleteTTLClassName, batchDeleteObjectID(0))
	require.NoError(t, repo.Shutdown(ctx))

	// The reopen rebuilds the universe from the doc id counter, so the dead id is back in
	// it and sits below the watermark.
	repo = newTTLRepo()
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })

	index := repo.GetIndex(schema.ClassName(batchDeleteTTLClassName))
	require.NotNil(t, index)
	logger, ok := repo.logger.(*logrus.Logger)
	require.True(t, ok, "the test DB logs through a *logrus.Logger")

	var deleted atomic.Int32
	eg := enterrors.NewErrorGroupWrapper(logger)
	ec := errorcompounder.New()
	index.incomingDeleteObjectsExpired(ctx, eg, ec, batchDeleteTTLProp, time.Now(), time.Now(),
		func(n int32) { deleted.Add(n) }, 0)
	eg.Wait()
	require.NoError(t, ec.ToError())

	require.Equal(t, int32(expiredCount-1), deleted.Load(),
		"every expired object that still has a row goes; the one whose row is gone has none to delete")

	shard := loadedShard(t, repo, batchDeleteTTLClassName)
	bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)
	for i := 1; i < expiredCount; i++ {
		require.Nil(t, ttlObjectRow(t, bucket, i), "expired object %d must be gone", i)
	}
	for i := expiredCount; i < expiredCount+aliveCount; i++ {
		require.NotNil(t, ttlObjectRow(t, bucket, i), "object %d has not expired", i)
	}

	universe, release := shard.bitmapFactory.GetBitmap()
	defer release()
	require.False(t, universe.Contains(deadDocID),
		"the sweep drops a dead doc id from the universe like the batch delete path does")
}

// TestBatchDeleteObjects_PrunesMoreDeadDocIDsThanOneBatch pins the prune of a dead prefix
// longer than one batch. The ids are accumulated into a bitmap that is subtracted and
// reset every deadDocIDPruneBatch ids, so a reset that loses ids leaves part of the prefix
// in the universe for every later resolve to read again.
func TestBatchDeleteObjects_PrunesMoreDeadDocIDsThanOneBatch(t *testing.T) {
	ctx := context.Background()
	const (
		objectCount = 1100
		alive       = 5
	)
	require.Greater(t, objectCount-alive, deadDocIDPruneBatch,
		"the dead prefix must outrun one batch, or the flush inside the loop never runs")

	rootDir := t.TempDir()
	shardState := singleShardState()

	repo := newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, 0)
	insertBatchDeleteObjects(t, repo, 0, objectCount)
	for i := 0; i < objectCount-alive; i++ {
		require.NoError(t, repo.DeleteObject(ctx, batchDeleteClassName,
			batchDeleteObjectID(i), time.Now(), nil, "", 0))
	}
	require.NoError(t, repo.Shutdown(ctx))

	// The restart rebuilds the universe from the doc id counter, so every deleted id is
	// back in it and all of them are below the watermark.
	repo = newBatchDeleteRepoAt(t, rootDir, batchDeleteTestClass(false), shardState, 0)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })

	_, err := repo.BatchDeleteObjects(ctx, batchDeleteNoMatchParams(true), time.Now(), nil, "", 0)
	require.NoError(t, err)
	shard := loadedShard(t, repo, batchDeleteClassName)

	before, releaseBefore := shard.bitmapFactory.GetBitmap()
	resurrected := before.GetCardinality()
	releaseBefore()
	require.Equal(t, objectCount, resurrected,
		"the restart must put the whole dead prefix back, or there is nothing to prune")

	res, err := repo.BatchDeleteObjects(ctx, batchDeleteDenyListParams(true), time.Now(), nil, "", 0)
	require.NoError(t, err)
	require.Equal(t, int64(alive), res.Matches)

	after, releaseAfter := shard.bitmapFactory.GetBitmap()
	defer releaseAfter()
	require.Equal(t, alive, after.GetCardinality(),
		"every dead doc id must leave the universe, not only the ids in the last partial batch")
}

// batchDeleteTTLClass is the batch delete test class plus the date property the objects
// TTL sweep filters on.
func batchDeleteTTLClass() *models.Class {
	class := makeTestClass(batchDeleteTTLClassName)
	class.Properties = append(class.Properties, &models.Property{
		Name:     batchDeleteTTLProp,
		DataType: []string{string(schema.DataTypeDate)},
	})
	return class
}

// insertTTLObjects writes expired objects first, then objects that have not expired, so
// the expired ones hold the lowest doc ids.
func insertTTLObjects(t *testing.T, repo *DB, expiredCount, aliveCount int) {
	t.Helper()

	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)

	batch := make(objects.BatchObjects, expiredCount+aliveCount)
	for i := range batch {
		expiresAt := past
		if i >= expiredCount {
			expiresAt = future
		}
		id := batchDeleteObjectID(i)
		batch[i] = objects.BatchObject{
			OriginalIndex: i,
			UUID:          id,
			Object: &models.Object{
				Class: batchDeleteTTLClassName,
				ID:    id,
				Properties: map[string]interface{}{
					"stringProp":       fmt.Sprintf("element %d", i),
					batchDeleteTTLProp: expiresAt.Format(time.RFC3339),
				},
				Vector: []float32{1, 2, 3},
			},
		}
	}

	res, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
	require.NoError(t, err)
	assertAllItemsErrorFree(t, res)
}

// ttlObjectRow returns the i-th TTL object's row, or nil when it has none.
func ttlObjectRow(t *testing.T, bucket *lsmkv.Bucket, i int) []byte {
	t.Helper()

	idBytes, err := uuid.MustParse(batchDeleteObjectID(i).String()).MarshalBinary()
	require.NoError(t, err)
	row, err := bucket.Get(idBytes)
	require.NoError(t, err)

	return row
}

// batchDeleteObjectID is the id insertBatchDeleteObjects gives the i-th object. It shares
// no prefix with simpleInsertObjectsForTenant's ids, which run out past 999.
func batchDeleteObjectID(i int) strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("7c4b2aa1-2b7c-4478-8bd0-2e527e%06d", i))
}

// newBatchDeleteObjects builds count objects with ids batchDeleteObjectID(from) onward.
func newBatchDeleteObjects(from, count int) objects.BatchObjects {
	batch := make(objects.BatchObjects, count)
	for i := range batch {
		id := batchDeleteObjectID(from + i)
		batch[i] = objects.BatchObject{
			OriginalIndex: i,
			UUID:          id,
			Object: &models.Object{
				Class:      batchDeleteClassName,
				ID:         id,
				Properties: map[string]interface{}{"stringProp": fmt.Sprintf("element %d", from+i)},
				Vector:     []float32{1, 2, 3},
			},
		}
	}

	return batch
}

// insertBatchDeleteObjects writes count objects with ids batchDeleteObjectID(from) onward.
func insertBatchDeleteObjects(t *testing.T, repo *DB, from, count int) {
	t.Helper()

	res, err := repo.BatchPutObjects(context.Background(), newBatchDeleteObjects(from, count), nil, 0)
	require.NoError(t, err)
	assertAllItemsErrorFree(t, res)
}

// dropObjectRow removes an object's row from the objects bucket and leaves its inverted
// postings alone, which is the state every delete passes through between
// shard_write_delete.go:90 and :112. It returns the doc id the postings still name.
func dropObjectRow(t *testing.T, repo *DB, className string, id strfmt.UUID) uint64 {
	t.Helper()

	shard := loadedShard(t, repo, className)
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	require.NoError(t, err)

	bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	existing, err := bucket.Get(idBytes)
	require.NoError(t, err)
	require.NotNil(t, existing, "the object must exist before its row is dropped")

	docID, _, err := storobj.DocIDAndTimeFromBinary(existing)
	require.NoError(t, err)

	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	require.NoError(t, bucket.Delete(idBytes,
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))

	return docID
}

// corruptObjectRow overwrites an object's row with bytes the object decoder rejects, under
// the same doc id, and leaves its inverted postings alone.
func corruptObjectRow(t *testing.T, repo *DB, className string, id strfmt.UUID) {
	t.Helper()

	shard := loadedShard(t, repo, className)
	idBytes, err := uuid.MustParse(id.String()).MarshalBinary()
	require.NoError(t, err)

	bucket := shard.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	existing, err := bucket.Get(idBytes)
	require.NoError(t, err)
	require.NotNil(t, existing, "the object must exist before its row is corrupted")

	docID, _, err := storobj.DocIDAndTimeFromBinary(existing)
	require.NoError(t, err)

	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	require.NoError(t, bucket.Put(idBytes, []byte("garbage"),
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
}

// loadedShard returns the class's first loaded shard. The shard loads lazily, so a call
// has to have reached it already.
func loadedShard(t *testing.T, repo *DB, className string) *Shard {
	t.Helper()

	idx := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)

	var found *Shard
	require.NoError(t, idx.ForEachLoadedShard(func(_ string, s ShardLike) error {
		if found == nil {
			found, _ = s.(*Shard)
		}
		return nil
	}))
	require.NotNil(t, found, "no loaded shard: run a call against the class first")

	return found
}

// batchDeleteNoMatchParams matches nothing, so it loads the shard without walking the
// doc id universe.
func batchDeleteNoMatchParams(dryRun bool) objects.BatchDeleteParams {
	return batchDeleteParams(&filters.Clause{
		Operator: filters.OperatorEqual,
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
