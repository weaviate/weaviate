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
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Deep copies: a load's class misses the flip once an UpdateClass or a restore replaced the property objects.
type frozenSchemaGetter struct {
	*fakeSchemaGetter
	mu        sync.Mutex
	afterRead func()
	dropped   bool
}

func (g *frozenSchemaGetter) ReadOnlyClass(name string) *models.Class {
	g.mu.Lock()
	hook, dropped := g.afterRead, g.dropped
	g.afterRead = nil
	g.mu.Unlock()
	live := g.fakeSchemaGetter.ReadOnlyClass(name)
	if live == nil || dropped {
		return nil
	}
	cp := *live
	cp.Properties = make([]*models.Property, len(live.Properties))
	for i, p := range live.Properties {
		pc := *p
		cp.Properties[i] = &pc
	}
	if hook != nil {
		hook()
	}
	return &cp
}

type loadRaceEnv struct {
	idx    *Index
	getter *frozenSchemaGetter
	class  *models.Class
	name   string
	status atomic.Value
	armed  atomic.Bool
	reads  atomic.Int32
	atRead func(n int32)
}

// newLoadRaceEnv leaves an enable-searchable migration swapped (or only merged) on a shard outside the shard map.
func newLoadRaceEnv(t *testing.T, merged bool) *loadRaceEnv {
	ctx := testCtx()
	className := "LoadRace_" + uuid.NewString()[:8]
	class := newEnableSearchableTestClass(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	for _, obj := range makeConvergenceTestObjects(t, 25, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	task, _ := newEnableSearchableTask(t, idx, className, "title", models.PropertyTokenizationWord, shard.migrationUnit())
	desc := distributedtask.TaskDescriptor{ID: "T582", Version: 1}
	payload := &ReindexTaskPayload{
		Collection: className, MigrationType: ReindexTypeEnableSearchable, Properties: []string{"title"},
		TargetTokenization: models.PropertyTokenizationWord,
	}
	task.setMigrationIdentity(desc, shard.migrationUnit(), payload)
	maybeWirePerPropOverlaySet(shard, payload, []*ShardReindexTaskGeneric{task})
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	if !merged {
		require.NoError(t, task.RunSwapOnShard(ctx, shard))
	}

	e := &loadRaceEnv{idx: idx, class: class, name: shard.Name()}
	e.status.Store(distributedtask.TaskStatusSwapping)
	require.NoError(t, shard.Shutdown(ctx))
	idx.shards.LoadAndDelete(e.name)
	idx.shardReindexer = &noRecoveryTaskReindexer{}
	e.getter = &frozenSchemaGetter{fakeSchemaGetter: idx.getSchema.(*fakeSchemaGetter)}
	idx.getSchema = e.getter
	tasks := func() []*distributedtask.Task {
		return []*distributedtask.Task{{
			Namespace: ReindexNamespace, TaskDescriptor: desc,
			Status: e.status.Load().(distributedtask.TaskStatus),
		}}
	}
	idx.db.SetMigrationTaskSources(ctx, func(group string) ([]distributedtask.TaskStatusEntry, bool) {
		if group == e.name && e.armed.Load() {
			if n := e.reads.Add(1); e.atRead != nil {
				e.atRead(n)
			}
		}
		return migrationTaskStatuses(tasks(), group), true
	}, func(context.Context) ([]*distributedtask.Task, error) { return tasks(), nil })
	return e
}

func (e *loadRaceEnv) mergeSearchableOn() *models.Property {
	on := true
	e.class.Properties = clusterschema.MergePropsMasked(e.class.Properties,
		[]*models.Property{{Name: "title", IndexSearchable: &on}}, []string{api.PropertyFieldIndexSearchable})
	merged := *e.class.Properties[0]
	return &merged
}

// The RAFT apply's order: schema half, store half, then FINISHED.
func (e *loadRaceEnv) flip(t *testing.T) {
	require.NoError(t, e.idx.updateProperty(testCtx(), e.mergeSearchableOn()))
	e.status.Store(distributedtask.TaskStatusFinished)
}

// A snapshot restore installs a new schema graph, then the tasks, and runs no store half.
func (e *loadRaceEnv) restore(*testing.T) {
	e.class.Properties = []*models.Property{e.mergeSearchableOn()}
	e.status.Store(distributedtask.TaskStatusFinished)
}

// load runs start right before the load itself, so a lazy registration's class read does not consume a hook.
func (e *loadRaceEnv) load(t *testing.T, lazy bool, start func()) *Shard {
	ctx := testCtx()
	var lz *LazyLoadShard
	if lazy {
		registered, err := e.idx.initShard(ctx, e.name, nil, false, true)
		require.NoError(t, err)
		lz = registered.(*LazyLoadShard)
		e.idx.shards.Store(e.name, lz)
	}
	start()
	e.armed.Store(true)
	defer e.armed.Store(false)
	if lazy {
		require.NoError(t, lz.Load(ctx))
		s, err := lz.Unwrap(ctx)
		require.NoError(t, err)
		return s
	}
	s, release, err := e.idx.getOrInitShard(ctx, e.name)
	require.NoError(t, err)
	release()
	return s.(*Shard)
}

func (e *loadRaceEnv) requireIndexServes(t *testing.T, s *Shard) {
	ctx := testCtx()
	bucket := helpers.BucketSearchableFromPropNameLSM("title")
	require.NotNil(t, s.store.Bucket(bucket), "migrated index open")
	require.NoError(t, s.PutObject(ctx, createTestObjectWithText(e.class.Class, "zulu one")))
	require.Contains(t, fingerprintInvertedBucket(t, s.store.Bucket(bucket)), "zulu", "a write after the flip is indexed")

	require.NoError(t, s.Shutdown(ctx))
	e.idx.shards.LoadAndDelete(e.name)
	re := e.load(t, false, func() {})
	defer re.Shutdown(ctx)
	require.NotNil(t, re.store.Bucket(bucket), "migrated index open after the next load")
	require.Contains(t, fingerprintInvertedBucket(t, re.store.Bucket(bucket)), "alpha", "migrated data indexed after the next load")
}

func TestALoadReadsTheTaskMapOnceBeforeTheClass(t *testing.T) {
	dry := newLoadRaceEnv(t, false)
	dry.load(t, false, func() {}).Shutdown(testCtx())
	reads := dry.reads.Load()
	t.Run("one task read per load", func(t *testing.T) { require.EqualValues(t, 1, reads) })

	type row struct {
		name         string
		merged, lazy bool
		at           int32 // the task read the change lands before; 0 is right after the class read
		change       func(*loadRaceEnv, *testing.T)
	}
	var rows []row
	for k := int32(1); k <= reads; k++ {
		rows = append(rows,
			row{name: "flip", at: k, change: (*loadRaceEnv).flip},
			row{name: "restore, lazy", lazy: true, at: k, change: (*loadRaceEnv).restore},
			row{name: "flip, merged record", merged: true, at: k, change: (*loadRaceEnv).flip},
		)
	}
	rows = append(rows, row{name: "restore, lazy", lazy: true, change: (*loadRaceEnv).restore})
	for _, r := range rows {
		t.Run(fmt.Sprintf("%s at task read %d", r.name, r.at), func(t *testing.T) {
			e := newLoadRaceEnv(t, r.merged)
			fired := false
			fire := func() { r.change(e, t); fired = true }
			s := e.load(t, r.lazy, func() {
				if r.at == 0 {
					e.getter.afterRead = fire
					return
				}
				e.atRead = func(n int32) {
					if n == r.at {
						fire()
					}
				}
			})
			require.True(t, fired, "the change landed inside the load")
			e.requireIndexServes(t, s)
		})
	}
}

func TestALoadWhoseClassLeftTheSchema(t *testing.T) {
	t.Run("lazy load keeps the migrated index on its captured class", func(t *testing.T) {
		e := newLoadRaceEnv(t, false)
		e.status.Store(distributedtask.TaskStatusFinished)
		s := e.load(t, true, func() { e.getter.dropped = true })
		defer s.Shutdown(testCtx())

		require.NotNil(t, s.store.Bucket(helpers.BucketSearchableFromPropNameLSM("title")))
		require.True(t, s.migrationRecords.overridesUnsure.Load(), "the periodic reconciler redecides")
	})
	for _, lazy := range []bool{false, true} {
		t.Run(fmt.Sprintf("registration refuses before touching disk, lazy=%v", lazy), func(t *testing.T) {
			ctx := testCtx()
			shd, idx := testShardWithSettings(t, ctx, newEnableSearchableTestClass("Dropped", []string{"title"}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			defer shd.Shutdown(ctx)
			idx.Config.EnableLazyLoadShards = lazy
			idx.getSchema = &frozenSchemaGetter{fakeSchemaGetter: idx.getSchema.(*fakeSchemaGetter), dropped: true}

			require.ErrorContains(t, idx.initLocalShard(ctx, "never-loaded"), "not found in schema")
			require.Nil(t, idx.shards.Load("never-loaded"))
			require.NoDirExists(t, filepath.Join(idx.path(), "never-loaded"))
		})
	}
}
