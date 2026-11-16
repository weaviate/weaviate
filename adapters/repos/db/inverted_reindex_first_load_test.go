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

package db

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type firstLoadKind struct {
	name   string
	class  func(className string) *models.Class
	header ReindexTaskHeader
	flip   func(*models.Class)
	object func(className string) *storobj.Object
	filter func(className string) *filters.LocalFilter
	bm25   string
}

func firstLoadKinds() []firstLoadKind {
	text := func(value string) func(string) *storobj.Object {
		return func(c string) *storobj.Object { return createTestObjectWithText(c, value) }
	}
	equal := func(value string) func(string) *filters.LocalFilter {
		return func(c string) *filters.LocalFilter { return propEqualsFilter(c, "title", value) }
	}
	retokenizable := func(c string) *models.Class { return newTestClassWithProps(c, []string{"title"}) }
	toField := func(c *models.Class) { c.Properties[0].Tokenization = models.PropertyTokenizationField }
	return []firstLoadKind{
		{
			name: "enable-filterable", class: func(c string) *models.Class { return newEnableFilterableTestClass(c, "title") },
			header: ReindexTaskHeader{MigrationType: ReindexTypeEnableFilterable, Properties: []string{"title"}},
			flip:   func(c *models.Class) { c.Properties[0].IndexFilterable = boolPtr(true) },
			object: text(semanticWindowToken), filter: equal(semanticWindowToken),
		},
		{
			name: "enable-searchable", class: func(c string) *models.Class { return newEnableSearchableTestClass(c, []string{"title"}) },
			header: ReindexTaskHeader{
				MigrationType: ReindexTypeEnableSearchable, Properties: []string{"title"},
				TargetTokenization: models.PropertyTokenizationWord,
			},
			flip:   func(c *models.Class) { c.Properties[0].IndexSearchable = boolPtr(true) },
			object: text(semanticWindowToken), bm25: semanticWindowToken,
		},
		{
			name: "enable-rangeable", class: newNoLiveIndexRangeableTestClass,
			header: ReindexTaskHeader{MigrationType: ReindexTypeEnableRangeable, Properties: []string{filterableToRangeablePropName}},
			flip:   func(c *models.Class) { c.Properties[0].IndexRangeFilters = boolPtr(true) },
			object: func(c string) *storobj.Object {
				return &storobj.Object{MarshallerVersion: 1, Object: models.Object{
					ID: strfmt.UUID(uuid.NewString()), Class: c,
					Properties: map[string]any{filterableToRangeablePropName: semanticWindowScore},
				}}
			},
			filter: func(c string) *filters.LocalFilter {
				return &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorGreaterThan,
					On:       &filters.Path{Class: schema.ClassName(c), Property: filterableToRangeablePropName},
					Value:    &filters.Value{Value: int(semanticWindowScore) - 1, Type: schema.DataTypeInt},
				}}
			},
		},
		{
			name: "change-tokenization", class: retokenizable,
			header: ReindexTaskHeader{
				MigrationType: ReindexTypeChangeTokenization, Properties: []string{"title"},
				TargetTokenization: models.PropertyTokenizationField, BucketStrategy: lsmkv.StrategyMapCollection,
			},
			flip: toField, object: text("The Hobbit"), bm25: "The Hobbit",
		},
		{
			name: "change-tokenization-filterable", class: retokenizable,
			header: ReindexTaskHeader{
				MigrationType: ReindexTypeChangeTokenizationFilterable, Properties: []string{"title"},
				TargetTokenization: models.PropertyTokenizationField,
			},
			flip: toField, object: text("The Hobbit"), filter: equal("The Hobbit"),
		},
		{
			name: "change-algorithm", class: retokenizable,
			header: ReindexTaskHeader{MigrationType: ReindexTypeChangeAlgorithm, Properties: []string{"title"}},
			flip: func(c *models.Class) {
				c.Properties[0].SearchableBlockmax = boolPtr(true)
				c.InvertedIndexConfig.UsingBlockMaxWAND = true
			},
			object: text(semanticWindowToken), bm25: semanticWindowToken,
		},
	}
}

func (k firstLoadKind) assertServes(t *testing.T, ctx context.Context, s *Shard, className string, objs []*storobj.Object) {
	t.Helper()
	var kw *searchparams.KeywordRanking
	var props []string
	if k.bm25 != "" {
		kw = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: k.bm25}
		props = []string{"title"}
	}
	var filter *filters.LocalFilter
	if k.filter != nil {
		filter = k.filter(className)
	}
	found, _, err := s.ObjectSearch(ctx, 100, filter, kw, nil, nil, additional.Properties{}, props)
	require.NoError(t, err)
	for _, obj := range objs {
		assert.Contains(t, objectIDs(found), obj.ID().String(), "the target index finds the object")
	}
	if k.header.MigrationType == ReindexTypeChangeAlgorithm {
		// BM25 answers from either format, so the format is checked on the bucket.
		bucket := s.store.Bucket(helpers.BucketSearchableFromPropNameLSM("title"))
		require.NotNil(t, bucket)
		assert.Equal(t, lsmkv.StrategyInverted, bucket.Strategy())
		assert.Contains(t, s.getSearchableBlockmaxProperties(), "title")
	}
}

// decidedRunManager drives a real task manager to a run that decided shard's
// group, with the swap owed by node2 only, so node1 is a new copy.
func decidedRunManager(t *testing.T, header ReindexTaskHeader, shard string, grouped bool) *distributedtask.Manager {
	t.Helper()
	mgr := distributedtask.NewManager(distributedtask.ManagerParameters{
		HeaderDecoders: map[string]distributedtask.HeaderDecoder{ReindexNamespace: DecodeReindexTaskHeader},
	})
	const id, version = "run", uint64(7)
	cmd := func(sub any) *api.ApplyRequest {
		raw, err := json.Marshal(sub)
		require.NoError(t, err)
		return &api.ApplyRequest{SubCommand: raw}
	}
	payload, err := json.Marshal(ReindexTaskPayload{
		MigrationType: header.MigrationType, Collection: header.Class, Properties: header.Properties,
		TargetTokenization: header.TargetTokenization, BucketStrategy: header.BucketStrategy,
	})
	require.NoError(t, err)
	group, unit := "", MigrationUnitID(shard, "node2")
	if grouped {
		group = shard
	}
	now := time.Now().UnixMilli()
	require.NoError(t, mgr.AddTask(cmd(&api.AddDistributedTaskRequest{
		Namespace: ReindexNamespace, Id: id, Payload: payload, SubmittedAtUnixMillis: now,
		UnitSpecs: []*api.UnitSpec{{Id: unit, GroupId: group}}, NeedsPreparationBarrier: true,
	}), version))
	require.NoError(t, mgr.RecordUnitCompletion(cmd(&api.RecordDistributedTaskUnitCompletionRequest{
		Namespace: ReindexNamespace, Id: id, Version: version, NodeId: "node2", UnitId: unit, FinishedAtUnixMillis: now,
	})))
	require.NoError(t, mgr.RecordPreparationCompleteAck(cmd(&api.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace: ReindexNamespace, Id: id, Version: version, GroupId: &group, NodeId: "node2", Success: true,
		AckedAtUnixMillis: now,
	})))
	require.Len(t, mgr.DecidedRunsOf(header.Class, shard, "node1"), 1, "the fixture must reach a decided group")
	return mgr
}

const lateShard = "late"

func unwrappedShard(t *testing.T, ctx context.Context, idx *Index, name string) *Shard {
	t.Helper()
	s, err := unwrapShard(ctx, idx.shards.Load(name))
	require.NoError(t, err)
	return s
}

func reloadLateShard(t *testing.T, ctx context.Context, idx *Index) *Shard {
	t.Helper()
	closed, ok := idx.shards.LoadAndDelete(lateShard)
	require.True(t, ok)
	require.NoError(t, closed.Shutdown(ctx))
	require.NoError(t, idx.loadLocalShardForReload(ctx, lateShard))
	return unwrappedShard(t, ctx, idx, lateShard)
}

func firstLoadIndex(t *testing.T, ctx context.Context, class *models.Class, grouped bool) (*Index, ShardLike) {
	t.Helper()
	setup := testShardWithSettings
	if grouped {
		setup = testShardWithMultiTenantSettings
	}
	shd, idx := setup(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false, false)
	t.Cleanup(func() {
		_ = idx.ForEachShard(func(_ string, s ShardLike) error { return s.Shutdown(context.Background()) })
	})
	return idx, shd
}

// The fixture decides the group, and a new copy joins before the schema flips.
func TestShardJoiningAfterASwapServesTheTargetIndex(t *testing.T) {
	for _, k := range firstLoadKinds() {
		for _, grouped := range []bool{true, false} {
			t.Run(k.name+map[bool]string{true: "/multi-tenant", false: "/without tenants"}[grouped], func(t *testing.T) {
				ctx := testCtx()
				className := "FirstLoad_" + uuid.NewString()[:8]
				class := k.class(className)
				idx, _ := firstLoadIndex(t, ctx, class, grouped)
				k.header.Class = className
				idx.db.SetMigrationDecidedRunSource(decidedRunManager(t, k.header, lateShard, grouped).DecidedRunsOf)

				require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
				objs := []*storobj.Object{k.object(className), k.object(className), k.object(className)}
				require.NoError(t, unwrappedShard(t, ctx, idx, lateShard).PutObject(ctx, objs[0]))
				k.flip(class)
				require.NoError(t, idx.updateProperty(ctx, class.Properties[0]))
				require.NoError(t, unwrappedShard(t, ctx, idx, lateShard).PutObject(ctx, objs[1]))
				k.assertServes(t, ctx, unwrappedShard(t, ctx, idx, lateShard), className, objs[:2])

				late := reloadLateShard(t, ctx, idx)
				require.NoError(t, late.PutObject(ctx, objs[2]))
				k.assertServes(t, ctx, late, className, objs)
			})
		}
	}
}

func assertFirstLoadRecords(t *testing.T, s *Shard, want []MigrationRecord) {
	t.Helper()
	require.NotEmpty(t, want)
	for _, rec := range want {
		_, there := s.migrationRecords.Get(rec.Subject().Key)
		assert.True(t, there, "record %s", rec.Subject().Key)
	}
}

func TestFirstLoadRecordsAreWrittenOnRetry(t *testing.T) {
	kind := firstLoadKinds()[3] // change-tokenization: two records, one per index
	unit := MigrationUnitID(lateShard, "node1")
	setup := func(t *testing.T) (context.Context, *Index, []MigrationRecord) {
		ctx := testCtx()
		className := "FirstLoadRetry_" + uuid.NewString()[:8]
		idx, _ := firstLoadIndex(t, ctx, kind.class(className), false)
		kind.header.Class = className
		mgr := decidedRunManager(t, kind.header, lateShard, false)
		idx.db.SetMigrationDecidedRunSource(mgr.DecidedRunsOf)
		want, err := firstLoadRecords(idx.logger, mgr.DecidedRunsOf(className, lateShard, "node1")[0], unit,
			kind.class(className))
		require.NoError(t, err)
		require.Len(t, want, 2)
		return ctx, idx, want
	}

	t.Run("a failed write is retried by the next load", func(t *testing.T) {
		ctx, idx, want := setup(t)
		lsm := shardPathLSM(idx.path(), lateShard)
		require.NoError(t, os.MkdirAll(lsm, os.ModePerm))
		blocker := filepath.Join(lsm, migrationsDir)
		require.NoError(t, os.WriteFile(blocker, nil, 0o600))
		require.ErrorIs(t, idx.LoadLocalShardForNewReplica(ctx, lateShard), errFirstLoadRecord)

		require.NoError(t, os.Remove(blocker))
		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		assertFirstLoadRecords(t, unwrappedShard(t, ctx, idx, lateShard), want)
	})

	t.Run("records, empty folders and other files are still a first load", func(t *testing.T) {
		ctx, idx, want := setup(t)
		lsm := shardPathLSM(idx.path(), lateShard)
		require.NoError(t, os.MkdirAll(filepath.Join(lsm, helpers.ObjectsBucketLSM), os.ModePerm))
		require.NoError(t, os.WriteFile(filepath.Join(shardPath(idx.path(), lateShard), "downloaded"), []byte("x"), 0o600))
		require.NoError(t, NewMigrationRecordStoreForUnit(lsm, unit, idx.logger).Put(want[0]))

		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		assertFirstLoadRecords(t, unwrappedShard(t, ctx, idx, lateShard), want)
	})

	t.Run("control: a shard that holds objects is left as it is", func(t *testing.T) {
		ctx := testCtx()
		className := "FirstLoadControl_" + uuid.NewString()[:8]
		idx, _ := firstLoadIndex(t, ctx, kind.class(className), false)
		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		require.NoError(t, unwrappedShard(t, ctx, idx, lateShard).PutObject(ctx, kind.object(className)))

		kind.header.Class = className
		idx.db.SetMigrationDecidedRunSource(decidedRunManager(t, kind.header, lateShard, false).DecidedRunsOf)
		assert.Empty(t, reloadLateShard(t, ctx, idx).migrationRecords.Records())
	})
}

func TestEagerBootLeavesAShardUnloadedWhenItsFirstLoadRecordFails(t *testing.T) {
	ctx := testCtx()
	kind := firstLoadKinds()[3]
	className := "FirstLoadBoot_" + uuid.NewString()[:8]
	class := kind.class(className)
	idx, shd := firstLoadIndex(t, ctx, class, false)
	name := shd.Name()
	idx.shards.LoadAndDelete(name)
	require.NoError(t, shd.Shutdown(ctx))
	require.NoError(t, os.RemoveAll(shardPath(idx.path(), name)))
	lsm := shardPathLSM(idx.path(), name)
	require.NoError(t, os.MkdirAll(lsm, os.ModePerm))
	blocker := filepath.Join(lsm, migrationsDir)
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	kind.header.Class = className
	mgr := decidedRunManager(t, kind.header, name, false)
	idx.db.SetMigrationDecidedRunSource(mgr.DecidedRunsOf)

	idx.Config.EnableLazyLoadShards = false
	require.NoError(t, idx.initAndStoreShards(ctx, class, nil), "one shard's failed write must not fail the index")
	lazy, ok := idx.shards.Load(name).(*LazyLoadShard)
	require.True(t, ok, "the shard is stored unloaded")
	require.False(t, lazy.isLoaded())

	require.NoError(t, os.Remove(blocker))
	want, err := firstLoadRecords(idx.logger, mgr.DecidedRunsOf(className, name, "node1")[0],
		MigrationUnitID(name, "node1"), class)
	require.NoError(t, err)
	assertFirstLoadRecords(t, unwrappedShard(t, ctx, idx, name), want)
}
