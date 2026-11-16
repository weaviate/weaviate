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
	"fmt"
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

const lateShard = "late"

type firstLoadKind struct {
	header ReindexTaskHeader
	class  func(className string) *models.Class
	flip   func(*models.Class)
	object func(className string) *storobj.Object
	filter func(className string) *filters.LocalFilter
	bm25   string
	// BM25 answers from either searchable format, so the bucket says which one was built.
	searchableStrategy string
	// Filterable writes take the searchable overlay's tokenization, so no query sees a
	// missing filterable record; its key is checked instead.
	records []MigrationStrategyCode
}

func firstLoadText(value string) func(string) *storobj.Object {
	return func(c string) *storobj.Object { return createTestObjectWithText(c, value) }
}

func firstLoadEqual(value string) func(string) *filters.LocalFilter {
	return func(c string) *filters.LocalFilter { return propEqualsFilter(c, "title", value) }
}

func retokenizableClass(c string) *models.Class { return newTestClassWithProps(c, []string{"title"}) }

func toFieldTokenization(c *models.Class) {
	c.Properties[0].Tokenization = models.PropertyTokenizationField
}

var (
	changeTokenizationKind = firstLoadKind{
		header: ReindexTaskHeader{
			MigrationType: ReindexTypeChangeTokenization, Properties: []string{"title"},
			OriginalTokenization: models.PropertyTokenizationWord, TargetTokenization: models.PropertyTokenizationField,
			BucketStrategy: lsmkv.StrategyMapCollection,
		},
		class: retokenizableClass, flip: toFieldTokenization, object: firstLoadText("The Hobbit"),
		filter: firstLoadEqual("The Hobbit"), bm25: "The Hobbit", searchableStrategy: lsmkv.StrategyMapCollection,
		records: []MigrationStrategyCode{StrategyCodeSearchableRetokenize, StrategyCodeFilterableRetokenize},
	}
	changeAlgorithmKind = firstLoadKind{
		header: ReindexTaskHeader{MigrationType: ReindexTypeChangeAlgorithm, Properties: []string{"title"}},
		class:  retokenizableClass,
		flip: func(c *models.Class) {
			c.Properties[0].SearchableBlockmax = boolPtr(true)
			c.InvertedIndexConfig.UsingBlockMaxWAND = true
		},
		object: firstLoadText(semanticWindowToken), bm25: semanticWindowToken, searchableStrategy: lsmkv.StrategyInverted,
	}
)

func firstLoadKinds() []firstLoadKind {
	return []firstLoadKind{
		{
			header: ReindexTaskHeader{MigrationType: ReindexTypeEnableFilterable, Properties: []string{"title"}},
			class:  func(c string) *models.Class { return newEnableFilterableTestClass(c, "title") },
			flip:   func(c *models.Class) { c.Properties[0].IndexFilterable = boolPtr(true) },
			object: firstLoadText(semanticWindowToken), filter: firstLoadEqual(semanticWindowToken),
		},
		{
			header: ReindexTaskHeader{
				MigrationType: ReindexTypeEnableSearchable, Properties: []string{"title"},
				TargetTokenization: models.PropertyTokenizationWord,
			},
			class:  func(c string) *models.Class { return newEnableSearchableTestClass(c, []string{"title"}) },
			flip:   func(c *models.Class) { c.Properties[0].IndexSearchable = boolPtr(true) },
			object: firstLoadText(semanticWindowToken), bm25: semanticWindowToken, searchableStrategy: lsmkv.StrategyInverted,
		},
		{
			header: ReindexTaskHeader{MigrationType: ReindexTypeEnableRangeable, Properties: []string{filterableToRangeablePropName}},
			class:  newNoLiveIndexRangeableTestClass,
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
		changeTokenizationKind,
		{
			header: ReindexTaskHeader{
				MigrationType: ReindexTypeChangeTokenizationFilterable, Properties: []string{"title"},
				OriginalTokenization: models.PropertyTokenizationWord, TargetTokenization: models.PropertyTokenizationField,
			},
			class: retokenizableClass, flip: toFieldTokenization, object: firstLoadText("The Hobbit"),
			filter: firstLoadEqual("The Hobbit"),
		},
		changeAlgorithmKind,
	}
}

func (k firstLoadKind) assertServes(t *testing.T, ctx context.Context, s *Shard, className string, objs []*storobj.Object) {
	t.Helper()
	assertFinds(t, ctx, s, className, k.filter, k.bm25, objs)
	if k.searchableStrategy != "" {
		bucket := s.store.Bucket(helpers.BucketSearchableFromPropNameLSM("title"))
		require.NotNil(t, bucket)
		assert.Equal(t, k.searchableStrategy, bucket.Strategy())
		if k.searchableStrategy == lsmkv.StrategyInverted {
			assert.Contains(t, s.getSearchableBlockmaxProperties(), "title")
		}
	}
}

func assertFinds(t *testing.T, ctx context.Context, s *Shard, className string,
	filter func(string) *filters.LocalFilter, bm25 string, objs []*storobj.Object,
) {
	t.Helper()
	var where *filters.LocalFilter
	if filter != nil {
		where = filter(className)
	}
	var kw *searchparams.KeywordRanking
	var props []string
	if bm25 != "" {
		kw = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: bm25}
		props = []string{"title"}
	}
	found, _, err := s.ObjectSearch(ctx, 100, where, kw, nil, nil, additional.Properties{}, props)
	require.NoError(t, err)
	for _, obj := range objs {
		assert.Contains(t, objectIDs(found), obj.ID().String(), "the query finds the object")
	}
}

// lateRun is a run that decided the group of shard "late", with its one unit on node2.
func lateRun(className string, version uint64, header ReindexTaskHeader, status distributedtask.TaskStatus) restoredRun {
	header.Class = className
	return restoredRun{
		desc:    distributedtask.TaskDescriptor{ID: fmt.Sprintf("run-%d", version), Version: version},
		payload: header.payload(), status: status, barrier: true, group: lateShard, decided: true,
		units: map[string]string{MigrationUnitID(lateShard, "node2"): "node2"},
	}
}

func installRuns(t *testing.T, idx *Index, runs ...restoredRun) {
	t.Helper()
	mgr := distributedtask.NewManager(distributedtask.ManagerParameters{
		HeaderDecoders: map[string]distributedtask.HeaderDecoder{ReindexNamespace: DecodeReindexTaskHeader},
	})
	restoreReindexRuns(t, mgr, runs...)
	idx.db.SetMigrationBarrierRunSource(mgr.BarrierRunsOf)
}

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

func assertRecordKeys(t *testing.T, s *Shard, version uint64, codes ...MigrationStrategyCode) {
	t.Helper()
	for _, code := range codes {
		key := MigrationRecordKey{TaskVersion: version, StrategyCode: code, UnitID: s.migrationUnit()}
		rec, there := s.migrationRecords.Get(key)
		if assert.True(t, there, "record %s", key) {
			assert.Equal(t, MigrationStatePromoted, rec.State())
		}
	}
}

func TestShardJoiningAfterASwapServesTheTargetIndex(t *testing.T) {
	type row struct {
		kind   firstLoadKind
		status distributedtask.TaskStatus
		group  string
	}
	var rows []row
	for i, kind := range firstLoadKinds() {
		status := distributedtask.TaskStatusCancelled
		if i%2 == 1 {
			status = distributedtask.TaskStatusFailed
		}
		rows = append(rows, row{kind: kind, status: status, group: lateShard})
	}
	rows = append(rows, row{kind: changeTokenizationKind, status: distributedtask.TaskStatusSwapping})
	for _, r := range rows {
		name := fmt.Sprintf("%s/%s/group %q", r.kind.header.MigrationType, r.status, r.group)
		t.Run(name, func(t *testing.T) {
			ctx := testCtx()
			className := "FirstLoad_" + uuid.NewString()[:8]
			class := r.kind.class(className)
			idx, _ := firstLoadIndex(t, ctx, class, r.group != "")
			run := lateRun(className, 7, r.kind.header, r.status)
			run.group = r.group
			installRuns(t, idx, run)

			// The new replica joins and takes a write before the schema changes.
			require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
			if len(r.kind.records) > 0 {
				assertRecordKeys(t, unwrappedShard(t, ctx, idx, lateShard), 7, r.kind.records...)
			}
			objs := []*storobj.Object{r.kind.object(className), r.kind.object(className), r.kind.object(className)}
			require.NoError(t, unwrappedShard(t, ctx, idx, lateShard).PutObject(ctx, objs[0]))
			r.kind.flip(class)
			require.NoError(t, idx.updateProperty(ctx, class.Properties[0]))
			require.NoError(t, unwrappedShard(t, ctx, idx, lateShard).PutObject(ctx, objs[1]))
			r.kind.assertServes(t, ctx, unwrappedShard(t, ctx, idx, lateShard), className, objs[:2])

			late := reloadLateShard(t, ctx, idx)
			require.NoError(t, late.PutObject(ctx, objs[2]))
			r.kind.assertServes(t, ctx, late, className, objs)
		})
	}
}

// A run cancelled after its swap stays listed while a lost node owes that swap.
// The finished runs that moved the property on are cleaned up first, and a new
// replica must still follow the schema.
func TestFirstLoadSkipsATokenizationRunTheSchemaMovedOn(t *testing.T) {
	ctx := testCtx()
	className := "FirstLoadStale_" + uuid.NewString()[:8]
	class := retokenizableClass(className)
	idx, _ := firstLoadIndex(t, ctx, class, true)
	mgr := distributedtask.NewManager(distributedtask.ManagerParameters{
		HeaderDecoders: map[string]distributedtask.HeaderDecoder{ReindexNamespace: DecodeReindexTaskHeader},
	})
	idx.db.SetMigrationBarrierRunSource(mgr.BarrierRunsOf)
	apply := func(op func(*api.ApplyRequest) error, sub any) {
		raw, err := json.Marshal(sub)
		require.NoError(t, err)
		require.NoError(t, op(&api.ApplyRequest{SubCommand: raw}))
	}
	now := time.Now().UnixMilli()
	// run adds one change of title's tokenization with one unit per group,
	// completes and decides the given groups, and pays node2's swaps.
	run := func(version uint64, from, to string, units map[string]string, decided ...string) string {
		id := fmt.Sprintf("run-%d", version)
		payload, err := json.Marshal(ReindexTaskPayload{
			MigrationType: ReindexTypeChangeTokenization, Collection: className, Properties: []string{"title"},
			OriginalTokenization: from, TargetTokenization: to, BucketStrategy: lsmkv.StrategyMapCollection,
		})
		require.NoError(t, err)
		var specs []*api.UnitSpec
		for unit, group := range units {
			specs = append(specs, &api.UnitSpec{Id: unit, GroupId: group})
		}
		apply(func(c *api.ApplyRequest) error { return mgr.AddTask(c, version) }, &api.AddDistributedTaskRequest{
			Namespace: ReindexNamespace, Id: id, Payload: payload, SubmittedAtUnixMillis: now,
			UnitSpecs: specs, NeedsPreparationBarrier: true,
		})
		for _, group := range decided {
			var nodes []string
			for unit, unitGroup := range units {
				if unitGroup == group {
					nodes = append(nodes, unit[len(group)+2:])
					apply(mgr.RecordUnitCompletion, &api.RecordDistributedTaskUnitCompletionRequest{
						Namespace: ReindexNamespace, Id: id, Version: version, NodeId: nodes[len(nodes)-1], UnitId: unit,
						FinishedAtUnixMillis: now,
					})
				}
			}
			for _, node := range nodes {
				apply(mgr.RecordPreparationCompleteAck, &api.RecordDistributedTaskPreparationCompleteAckRequest{
					Namespace: ReindexNamespace, Id: id, Version: version, GroupId: &group, NodeId: node, Success: true,
					AckedAtUnixMillis: now,
				})
			}
			apply(mgr.RecordPostCompletionAck, &api.RecordDistributedTaskPostCompletionAckRequest{
				Namespace: ReindexNamespace, Id: id, Version: version, GroupId: group, NodeId: "node2", Success: true,
				AckedAtUnixMillis: now,
			})
		}
		return id
	}
	cancel := func(id string, version uint64) {
		apply(mgr.CancelTask, &api.CancelDistributedTaskRequest{
			Namespace: ReindexNamespace, Id: id, Version: version, CancelledAtUnixMillis: now,
		})
	}
	finish := func(id string, version uint64) {
		apply(mgr.MarkTaskFinalized, &api.MarkTaskFinalizedRequest{
			Namespace: ReindexNamespace, Id: id, Version: version, FinalizedAtUnixMillis: now,
		})
	}
	cleanUp := func(id string, version uint64) {
		apply(mgr.CleanUpTask, &api.CleanUpDistributedTaskRequest{Namespace: ReindexNamespace, Id: id, Version: version})
	}
	word, field := models.PropertyTokenizationWord, models.PropertyTokenizationField
	whitespace, lowercase := models.PropertyTokenizationWhitespace, models.PropertyTokenizationLowercase
	other := map[string]string{"other__node2": "other"}

	// nodeN's swap of the late group is never paid, so v1 stays listed.
	v1 := run(1, word, field, map[string]string{
		"late__node2": lateShard, "late__nodeN": lateShard, "busy__node2": "busy",
	}, lateShard)
	cancel(v1, 1)
	finish(run(2, word, field, other, "other"), 2)
	finish(run(3, field, whitespace, other, "other"), 3)
	cancel(run(4, whitespace, lowercase, map[string]string{"other__node2": "other", "busy__node2": "busy"}), 4)
	cleanUp("run-2", 2)
	cleanUp("run-3", 3)
	listed := mgr.BarrierRunsOf(ReindexNamespace, className, lateShard)
	require.Len(t, listed, 1, "only v1 is listed, with its group decided")
	require.Equal(t, distributedtask.TaskStatusCancelled, listed[0].Status)
	class.Properties[0].Tokenization = whitespace
	require.NoError(t, idx.updateProperty(ctx, class.Properties[0]))

	require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
	late := unwrappedShard(t, ctx, idx, lateShard)
	assert.Empty(t, late.migrationRecords.Records())
	obj := createTestObjectWithText(className, "The Hobbit")
	require.NoError(t, late.PutObject(ctx, obj))
	assertFinds(t, ctx, late, className, firstLoadEqual("Hobbit"), "Hobbit", []*storobj.Object{obj})
}

func TestFirstLoadRecords(t *testing.T) {
	retokenizeCodes := []MigrationStrategyCode{StrategyCodeSearchableRetokenize, StrategyCodeFilterableRetokenize}
	setup := func(t *testing.T, class func(string) *models.Class, runs ...func(className string) restoredRun) (context.Context, *Index) {
		ctx := testCtx()
		className := "FirstLoadRecords_" + uuid.NewString()[:8]
		idx, _ := firstLoadIndex(t, ctx, class(className), true)
		restored := make([]restoredRun, 0, len(runs))
		for _, run := range runs {
			restored = append(restored, run(className))
		}
		installRuns(t, idx, restored...)
		return ctx, idx
	}
	cancelled := func(version uint64, kind firstLoadKind) func(string) restoredRun {
		return func(c string) restoredRun {
			return lateRun(c, version, kind.header, distributedtask.TaskStatusCancelled)
		}
	}

	t.Run("a failed write is retried by the next load", func(t *testing.T) {
		ctx, idx := setup(t, retokenizableClass, cancelled(7, changeTokenizationKind))
		blocker := blockRecordStore(t, idx, lateShard)
		require.ErrorIs(t, idx.LoadLocalShardForNewReplica(ctx, lateShard), errFirstLoadRecord)

		require.NoError(t, os.Remove(blocker))
		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		assertRecordKeys(t, unwrappedShard(t, ctx, idx, lateShard), 7, retokenizeCodes...)
	})

	t.Run("records, empty folders and other files still count as an empty shard", func(t *testing.T) {
		ctx, idx := setup(t, retokenizableClass, cancelled(7, changeTokenizationKind))
		lsm := shardPathLSM(idx.path(), lateShard)
		require.NoError(t, os.MkdirAll(filepath.Join(lsm, helpers.ObjectsBucketLSM), os.ModePerm))
		require.NoError(t, os.WriteFile(filepath.Join(shardPath(idx.path(), lateShard), "downloaded"), []byte("x"), 0o600))
		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		first := unwrappedShard(t, ctx, idx, lateShard).migrationRecords.Records()
		require.NotEmpty(t, first)
		require.NoError(t, os.Remove(filepath.Join(lsm, migrationsDir, migrationRecordsDirName, first[0].Subject().Key.fileName())))

		assertRecordKeys(t, reloadLateShard(t, ctx, idx), 7, retokenizeCodes...)
	})

	t.Run("a node that still owes the swap gets the record on an empty shard", func(t *testing.T) {
		ctx, idx := setup(t, retokenizableClass, func(c string) restoredRun {
			run := lateRun(c, 7, changeTokenizationKind.header, distributedtask.TaskStatusCancelled)
			run.units[MigrationUnitID(lateShard, "node1")] = "node1"
			run.owedBy = []string{"node1"}
			return run
		})
		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		late := unwrappedShard(t, ctx, idx, lateShard)
		assertRecordKeys(t, late, 7, retokenizeCodes...)

		header := changeTokenizationKind.header
		header.Class = idx.Config.ClassName.String()
		payload := header.payload()
		tasks, err := buildMigrationTasks(idx.logger, nil, 7, &payload, func(string, string) bool { return true })
		require.NoError(t, err)
		for _, task := range tasks {
			task.setMigrationIdentity(distributedtask.TaskDescriptor{ID: "run-7", Version: 7}, late.migrationUnit(), &payload)
		}
		obj := createTestObjectWithText(header.Class, "The Hobbit")
		require.NoError(t, late.PutObject(ctx, obj))
		p := &ReindexProvider{logger: idx.logger, localNode: "node1", serverCtx: ctx}
		res := p.runShardSwapPhase(ctx, &payload, late.migrationUnit(), lateShard, late, tasks, idx.logger)
		require.Empty(t, res.Errs, "the swap the node owes completes over the record")
		assertFinds(t, ctx, late, header.Class, firstLoadEqual("The Hobbit"), "The Hobbit", []*storobj.Object{obj})
		assertRecordKeys(t, late, 7, retokenizeCodes...)
	})

	t.Run("a record the shard already holds is kept", func(t *testing.T) {
		ctx, idx := setup(t, retokenizableClass, cancelled(7, changeTokenizationKind))
		className := idx.Config.ClassName.String()
		header := changeTokenizationKind.header
		header.Class = className
		unit := MigrationUnitID(lateShard, "node1")
		built, err := firstLoadRecords(idx.logger, firstLoadRun{
			desc: distributedtask.TaskDescriptor{ID: "run-7", Version: 7}, header: header,
		}, unit, retokenizableClass(className))
		require.NoError(t, err)
		held := built[0].Subject()
		held.TaskID = "held"
		lsm := shardPathLSM(idx.path(), lateShard)
		require.NoError(t, os.MkdirAll(lsm, os.ModePerm))
		require.NoError(t, NewMigrationRecordStoreForUnit(lsm, unit, idx.logger).Put(
			NewMigrationRecordPromoted(held, held.Properties(), nil)))

		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		late := unwrappedShard(t, ctx, idx, lateShard)
		rec, there := late.migrationRecords.Get(held.Key)
		require.True(t, there)
		assert.Equal(t, "held", rec.Subject().TaskID)
		assertRecordKeys(t, late, 7, retokenizeCodes...)
	})

	t.Run("every decided run writes its records, and the newer one retires the older on a shared index", func(t *testing.T) {
		filterableToWhitespace := firstLoadKind{header: ReindexTaskHeader{
			MigrationType: ReindexTypeChangeTokenizationFilterable, Properties: []string{"title"},
			TargetTokenization: models.PropertyTokenizationWhitespace,
		}}
		ctx, idx := setup(t, retokenizableClass, cancelled(7, changeTokenizationKind), cancelled(9, filterableToWhitespace))
		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		late := unwrappedShard(t, ctx, idx, lateShard)
		assertRecordKeys(t, late, 7, StrategyCodeSearchableRetokenize)
		assertRecordKeys(t, late, 9, StrategyCodeFilterableRetokenize)
		_, kept := late.migrationRecords.Get(MigrationRecordKey{
			TaskVersion: 7, StrategyCode: StrategyCodeFilterableRetokenize, UnitID: late.migrationUnit(),
		})
		assert.False(t, kept, "the reconcile pass retires the superseded record")
	})

	t.Run("a run builds blockmax only for its own properties", func(t *testing.T) {
		twoProps := func(c string) *models.Class { return newTestClassWithProps(c, []string{"title", "body"}) }
		ctx, idx := setup(t, twoProps, cancelled(7, changeAlgorithmKind))
		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		late := unwrappedShard(t, ctx, idx, lateShard)
		assert.Equal(t, lsmkv.StrategyInverted, late.store.Bucket(helpers.BucketSearchableFromPropNameLSM("title")).Strategy())
		assert.Equal(t, lsmkv.StrategyMapCollection, late.store.Bucket(helpers.BucketSearchableFromPropNameLSM("body")).Strategy())
	})

	t.Run("control: a shard that holds objects is left as it is", func(t *testing.T) {
		ctx, idx := setup(t, retokenizableClass)
		require.NoError(t, idx.LoadLocalShardForNewReplica(ctx, lateShard))
		className := idx.Config.ClassName.String()
		require.NoError(t, unwrappedShard(t, ctx, idx, lateShard).PutObject(ctx, createTestObjectWithText(className, "x")))

		installRuns(t, idx, lateRun(className, 7, changeTokenizationKind.header, distributedtask.TaskStatusCancelled))
		assert.Empty(t, reloadLateShard(t, ctx, idx).migrationRecords.Records())
	})
}

// blockRecordStore puts a file where the record store's directory goes, so its next load fails.
func blockRecordStore(t *testing.T, idx *Index, shard string) string {
	t.Helper()
	lsm := shardPathLSM(idx.path(), shard)
	require.NoError(t, os.MkdirAll(lsm, os.ModePerm))
	blocker := filepath.Join(lsm, migrationsDir)
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	return blocker
}

func TestEagerBootLeavesAShardUnloadedWhenItsFirstLoadRecordFails(t *testing.T) {
	ctx := testCtx()
	className := "FirstLoadBoot_" + uuid.NewString()[:8]
	class := retokenizableClass(className)
	idx, shd := firstLoadIndex(t, ctx, class, false)
	name := shd.Name()
	idx.shards.LoadAndDelete(name)
	require.NoError(t, shd.Shutdown(ctx))
	require.NoError(t, os.RemoveAll(shardPath(idx.path(), name)))
	blocker := blockRecordStore(t, idx, name)
	run := lateRun(className, 7, changeTokenizationKind.header, distributedtask.TaskStatusSwapping)
	run.group = ""
	installRuns(t, idx, run)

	idx.Config.EnableLazyLoadShards = false
	require.NoError(t, idx.initAndStoreShards(ctx, class, nil), "one shard's failed write must not fail the index")
	lazy, ok := idx.shards.Load(name).(*LazyLoadShard)
	require.True(t, ok, "the shard is stored unloaded")
	require.False(t, lazy.isLoaded())

	require.NoError(t, os.Remove(blocker))
	assertRecordKeys(t, unwrappedShard(t, ctx, idx, name), 7,
		StrategyCodeSearchableRetokenize, StrategyCodeFilterableRetokenize)
}
