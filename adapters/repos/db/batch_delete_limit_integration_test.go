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

// secondResolveLogged returns whether the hook saw the line the shard writes when it had
// to resolve the filter a second time with no cap.
func secondResolveLogged(hook *test.Hook) bool {
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == "find_uuids_second_resolve" {
			return true
		}
	}
	return false
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
// limit slots. A leaf allow-list filter is the shape the bounded resolve truncates: the
// searcher stops the row reader once the allow list reaches the limit it was given
// (inverted/searcher_doc_bitmap.go:96), so a dead id inside a window that short leaves
// the walk nothing to retry against and the reply lands on exactly limit, which the
// published contract reads as "exact, everything handled". The resolve is then run again
// with no cap, and only then.
//
// The last case is the other side of that rule: a shard that matched fewer objects than
// the limit was never truncated, so a short reply there is the whole answer and running
// the filter a second time would buy nothing.
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
		// wantSecondResolve is whether the shard had to resolve the filter a second time
		// with no cap to answer, which it reports on its own info line.
		wantSecondResolve bool
	}{
		{
			name:              "leaf allow-list filter with one dead doc id in the window",
			params:            batchDeleteMatchAllParams,
			objectCount:       30,
			dropRows:          1,
			wantMatches:       batchDeleteLimit + 1,
			wantHandled:       int(batchDeleteLimit),
			wantSecondResolve: true,
		},
		{
			// Three dead ids leave the bounded pass at 8 of 11, so the uncapped pass is
			// what carries the reply back to a full window.
			name:              "leaf allow-list filter with several dead doc ids in the window",
			params:            batchDeleteMatchAllParams,
			objectCount:       30,
			dropRows:          3,
			wantMatches:       batchDeleteLimit + 1,
			wantHandled:       int(batchDeleteLimit),
			wantSecondResolve: true,
		},
		{
			// A deny list resolves against the whole doc id universe rather than a
			// truncated allow list, so the walk reaches a full window past the dead id on
			// the first pass and the second resolve would buy nothing.
			name:              "deny-list filter",
			params:            batchDeleteDenyListParams,
			objectCount:       30,
			dropRows:          1,
			wantMatches:       batchDeleteLimit + 1,
			wantHandled:       int(batchDeleteLimit),
			wantSecondResolve: false,
		},
		{
			// An And root is the common production filter shape and takes a different
			// path: inverted/prop_value_pairs.go:142 resolves a compound root with the
			// limit dropped, so the allow list is complete and the walk reaches a full
			// window past the dead id on the first pass. Nothing was truncated, so the
			// second resolve would buy nothing, and the condition has to see that.
			name:              "compound root with one dead doc id",
			params:            batchDeleteAndRootParams,
			objectCount:       30,
			dropRows:          1,
			wantMatches:       batchDeleteLimit + 1,
			wantHandled:       int(batchDeleteLimit),
			wantSecondResolve: false,
		},
		{
			// Five matches never fill the window, so nothing was truncated and the reply
			// is short because one object is genuinely gone.
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

			hook := batchDeleteLogs(t, repo)
			after, err := repo.BatchDeleteObjects(ctx, tt.params(true), time.Now(), nil, "", 0)
			require.NoError(t, err)
			require.Equal(t, tt.wantMatches, after.Matches,
				"%d objects still match", tt.objectCount-tt.dropRows)
			require.Len(t, after.Objects, tt.wantHandled)
			require.Equal(t, tt.wantSecondResolve, secondResolveLogged(hook),
				"the uncapped resolve is what an operator pays for and the only place it is visible")
		})
	}
}

// TestBatchDeleteObjects_SamplesTheSecondResolveLine pins the rate on the line the shard
// writes when it resolves twice. The condition behind that line does not clear itself: the
// dropped row below leaves a doc id whose object is gone inside the first window of every
// later call, so without a rate the objects TTL sweep would write the line on every batch
// of every cycle until the shard is reopened.
func TestBatchDeleteObjects_SamplesTheSecondResolveLine(t *testing.T) {
	ctx := context.Background()
	const calls = 20

	repo := newBatchDeleteRepo(t, batchDeleteTestClass(false), singleShardState(), batchDeleteLimit)
	simpleInsertObjectsForTenant(t, repo, batchDeleteClassName, "", 30)

	before, err := repo.BatchDeleteObjects(ctx, batchDeleteMatchAllParams(true), time.Now(), nil, "", 0)
	require.NoError(t, err)
	require.NotEmpty(t, before.Objects)
	dropObjectRow(t, repo, batchDeleteClassName, before.Objects[0].UUID)

	hook := batchDeleteLogs(t, repo)
	for i := 0; i < calls; i++ {
		_, err := repo.BatchDeleteObjects(ctx, batchDeleteMatchAllParams(true), time.Now(), nil, "", 0)
		require.NoError(t, err)
	}

	lines := 0
	for _, entry := range hook.AllEntries() {
		if entry.Data["action"] == "find_uuids_second_resolve" {
			lines++
		}
	}
	require.Equal(t, 1, lines,
		"%d calls that all took the second resolve must collapse to one line per window", calls)
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
	// lookup is a parameter, so a failing one needs no broken store.
	_, err := repo.BatchDeleteObjects(ctx, batchDeleteMatchAllParams(true), time.Now(), nil, "", 0)
	require.NoError(t, err)
	shard := loadedShard(t, repo, batchDeleteClassName)

	failing := func(_ context.Context, _ int, _, buffer []byte) ([]byte, []byte, error) {
		return nil, buffer, readErr
	}

	limit := int(batchDeleteLimit)
	pass, err := shard.resolveAndCollect(ctx, batchDeleteMatchAllParams(true).Filters,
		limit, limit, failing)
	require.ErrorContains(t, err, "resolve doc id",
		"the error names the doc id the read failed on")
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
	go func() {
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
	}()

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
