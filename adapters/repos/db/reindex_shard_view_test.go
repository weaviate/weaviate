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
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type fakeTaskStates struct {
	mu        sync.Mutex
	states    []distributedtask.ShardTaskState
	afterRead func()
}

func (f *fakeTaskStates) ShardTaskStates(_, _, _, _ string) []distributedtask.ShardTaskState {
	f.mu.Lock()
	out, after := slices.Clone(f.states), f.afterRead
	f.afterRead = nil
	f.mu.Unlock()
	if after != nil {
		after()
	}
	return out
}

func (f *fakeTaskStates) set(states ...distributedtask.ShardTaskState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states = states
}

type viewFixture struct {
	t      *testing.T
	idx    *Index
	shard  *Shard
	tasks  *fakeTaskStates
	schema *fakeSchemaGetter
	class  *models.Class
}

const plainTitle = "property_title"

func newViewFixture(t *testing.T, className string, props ...*models.Property) *viewFixture {
	if len(props) == 0 {
		props = []*models.Property{{
			Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord,
		}}
	}
	f := &viewFixture{t: t, tasks: &fakeTaskStates{}, class: &models.Class{
		Class: className, InvertedIndexConfig: invertedConfig(), Properties: props,
	}}
	sl, idx := testShardWithSettings(t, context.Background(), f.class, enthnsw.UserConfig{Skip: true}, false, false, false,
		func(i *Index) {
			i.db.SetReindexTaskStateSource(f.tasks)
			f.schema = i.getSchema.(*fakeSchemaGetter)
		})
	f.idx, f.shard = idx, sl.(*Shard)
	return f
}

func (f *viewFixture) setLive(gens map[string]uint64) {
	f.schema.live = map[string]map[string]map[string]uint64{f.class.Class: {"title": gens}}
}

func (f *viewFixture) putObjects(n int) {
	for range n {
		obj := testObject(f.class.Class)
		obj.Object.Properties = map[string]any{"title": "hello"}
		require.NoError(f.t, f.shard.PutObject(context.Background(), obj))
	}
}

// startLoop runs the refresh loop of the fixture's DB over the given indexes.
func (f *viewFixture) startLoop(idxs ...*Index) *reindexRefresh {
	db := f.idx.db
	db.indexLock.Lock()
	for _, idx := range append(idxs, f.idx) {
		db.indices[indexID(idx.Config.ClassName)] = idx
	}
	db.indexLock.Unlock()
	monitoring.GetMetrics().SetReindexViewRefreshPending(0)
	db.reindexRefresh.start()
	f.t.Cleanup(db.reindexRefresh.shutdown)
	return db.reindexRefresh
}

func servedFolder(v *shardView, plain string) string {
	if b := v.Bucket(plain); b != nil {
		return filepath.Base(b.GetDir())
	}
	return ""
}

func stagedTasks(v *shardView) []string {
	var out []string
	for _, b := range v.staged {
		out = append(out, b.taskID)
	}
	return out
}

func countHello(t *testing.T, v *shardView) uint64 {
	bm, release, err := v.Bucket(plainTitle).RoaringSetGet(context.Background(), []byte("hello"))
	require.NoError(t, err)
	defer release()
	return uint64(bm.GetCardinality())
}

func refreshPending() float64 {
	return testutil.ToFloat64(monitoring.GetMetrics().ReindexViewRefreshPending)
}

// refreshWithin fails when a refresh blocks longer than limit.
func refreshWithin(t *testing.T, s *Shard, limit time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- s.refreshView(context.Background()) }()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		t.Fatalf("refresh still blocked after %s", limit)
		return nil
	}
}

const gen1Title = "property_title.gen1"

func stagedTask(id string) distributedtask.ShardTaskState {
	return viewTaskState(id, 1, ReindexTypeRepairFilterable, distributedtask.TaskStatusStarted, false, true)
}

func switchedTask(id string) distributedtask.ShardTaskState {
	return viewTaskState(id, 1, ReindexTypeRepairFilterable, distributedtask.TaskStatusStarted, true, true)
}

func TestReindexShardViewLoadRefusesMissingGenerationFolder(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(f *viewFixture)
		wantErr string
	}{
		{
			name:    "a missing generation-1 folder over objects fails the load",
			prepare: func(f *viewFixture) { f.setLive(map[string]uint64{indexTypeFilterable: 1}) },
			wantErr: "served folder",
		},
		{
			name: "a missing plain folder over objects loads empty",
			prepare: func(f *viewFixture) {
				require.NoError(f.t, os.RemoveAll(filepath.Join(f.shard.pathLSM(), plainTitle)))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewFixture(t, "ViewTest")
			f.putObjects(5)
			require.NoError(t, f.shard.Shutdown(context.Background()))
			tc.prepare(f)
			sl, err := f.idx.initShard(context.Background(), f.shard.name, f.class, nil, true, true)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Zero(t, countHello(t, sl.(*Shard).loadView()))
		})
	}
}

func TestReindexShardViewSwitchWaitsForReadersOnlyToDelete(t *testing.T) {
	f := newViewFixture(t, "ViewTest")
	f.putObjects(5)
	f.tasks.set(stagedTask("a"))
	require.NoError(t, f.shard.refreshView(context.Background()))

	held := f.shard.acquireView()
	f.tasks.set(switchedTask("a"))
	require.NoError(t, refreshWithin(t, f.shard, time.Second))

	require.Equal(t, gen1Title, servedFolder(f.shard.loadView(), plainTitle))
	require.Equal(t, uint64(5), countHello(t, held), "the held view still reads the old folder")
	time.Sleep(100 * time.Millisecond)
	require.DirExists(t, filepath.Join(f.shard.pathLSM(), plainTitle))

	held.release()
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(f.shard.pathLSM(), plainTitle))
		return os.IsNotExist(err)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestReindexShardViewFolderPendingRetireIsNotReady(t *testing.T) {
	f := newViewFixture(t, "ViewTest")
	other := newViewFixture(t, "ViewOther")
	f.tasks.set(stagedTask("dead"))
	require.NoError(t, f.shard.refreshView(context.Background()))

	held := f.shard.acquireView()
	f.tasks.set(stagedTask("takeover"))
	require.ErrorIs(t, refreshWithin(t, f.shard, time.Second), errViewNotReady)
	require.Empty(t, stagedTasks(f.shard.loadView()))

	loop := f.startLoop(other.idx)
	other.tasks.set(stagedTask("other"))
	loop.notify(other.class.Class, nil)
	loop.notify(f.class.Class, nil)
	require.Eventually(t, func() bool {
		return slices.Equal(stagedTasks(other.shard.loadView()), []string{"other"})
	}, 5*time.Second, 10*time.Millisecond, "another shard refreshes while this one waits")
	require.Empty(t, stagedTasks(f.shard.loadView()))

	held.release()
	require.Eventually(t, func() bool {
		return slices.Equal(stagedTasks(f.shard.loadView()), []string{"takeover"})
	}, 5*time.Second, 10*time.Millisecond)
}

func lockFolder(t *testing.T, dir string) {
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
}

func TestReindexShardViewFailedRefreshRetries(t *testing.T) {
	f := newViewFixture(t, "ViewTest")
	require.NoError(t, os.MkdirAll(filepath.Join(f.shard.pathLSM(), gen1Title), 0o755))
	lockFolder(t, f.shard.pathLSM())
	loop := f.startLoop()

	f.tasks.set(stagedTask("a"))
	loop.notify(f.class.Class, nil)
	require.Eventually(t, func() bool { return refreshPending() == 1 }, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, os.Chmod(f.shard.pathLSM(), 0o755))
	require.Eventually(t, func() bool {
		return refreshPending() == 0 && slices.Equal(stagedTasks(f.shard.loadView()), []string{"a"})
	}, 10*time.Second, 10*time.Millisecond)
}

func TestReindexShardViewRefusesMissingFolderOnRefresh(t *testing.T) {
	f := newViewFixture(t, "ViewTest")
	f.putObjects(5)
	loop := f.startLoop()

	f.setLive(map[string]uint64{indexTypeFilterable: 1})
	loop.notify(f.class.Class, nil)
	require.Eventually(t, func() bool { return refreshPending() == 1 }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 1.0, refreshPending(), "the shard stays queued")
	require.NoDirExists(t, filepath.Join(f.shard.pathLSM(), gen1Title))
}

func TestReindexShardViewNoticeLoadsNoShard(t *testing.T) {
	tests := []struct {
		name       string
		registered bool
	}{
		{name: "a collection the DB has not loaded"},
		{name: "a cold lazy shard", registered: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewFixture(t, "ViewTest")
			require.NoError(t, f.shard.Shutdown(context.Background()))
			lazy := NewLazyLoadShard(context.Background(), nil, f.shard.name, f.idx, f.class, f.idx.centralJobQueue,
				f.idx.indexCheckpoints, f.idx.allocChecker, f.idx.shardLoadLimiter, f.idx.shardReindexer, false,
				f.idx.bitmapBufPool)
			f.idx.shards.Store(f.shard.name, lazy)
			if tc.registered {
				f.idx.db.indices[indexID(f.idx.Config.ClassName)] = f.idx
			}
			r := f.idx.db.reindexRefresh
			r.notify(f.class.Class, nil)
			for coll, shards := range r.takeWork(time.Now()) {
				r.refreshCollection(context.Background(), coll, shards)
			}
			require.False(t, lazy.isLoaded())
			require.Empty(t, r.retry)
		})
	}
}

func BenchmarkReindexRefreshNoticeBeforeLoad(b *testing.B) {
	r := newReindexRefresh(&DB{indices: map[string]*Index{}})
	for b.Loop() {
		r.notify("Unloaded", nil)
		for coll, shards := range r.takeWork(time.Now()) {
			r.refreshCollection(context.Background(), coll, shards)
		}
	}
}

func TestReindexShardViewSelfNoticeAfterLoad(t *testing.T) {
	tests := []struct {
		name string
		// load makes the shard visible after a task change its load read too early to see.
		load func(f *viewFixture) *Shard
	}{
		{
			name: "a lazy shard loads",
			load: func(f *viewFixture) *Shard {
				require.NoError(f.t, f.shard.Shutdown(context.Background()))
				lazy := NewLazyLoadShard(context.Background(), nil, f.shard.name, f.idx, f.class, f.idx.centralJobQueue,
					f.idx.indexCheckpoints, f.idx.allocChecker, f.idx.shardLoadLimiter, f.idx.shardReindexer, false,
					f.idx.bitmapBufPool)
				f.idx.shards.Store(f.shard.name, lazy)
				f.tasks.mu.Lock()
				f.tasks.afterRead = func() { f.tasks.set(switchedTask("a")) }
				f.tasks.mu.Unlock()
				require.NoError(f.t, lazy.Load(context.Background()))
				return lazy.shard
			},
		},
		{
			name: "an index registers after the loop dropped its notice",
			load: func(f *viewFixture) *Shard {
				r := f.idx.db.reindexRefresh
				f.tasks.set(switchedTask("a"))
				r.notify(f.class.Class, nil)
				for coll, shards := range r.takeWork(time.Now()) {
					r.refreshCollection(context.Background(), coll, shards)
				}
				require.NotEqual(f.t, gen1Title, servedFolder(f.shard.loadView(), plainTitle))
				f.idx.db.registerIndex(f.idx)
				return f.shard
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewFixture(t, "ViewTest")
			sh := tc.load(f)
			f.startLoop()
			require.Eventually(t, func() bool {
				return servedFolder(sh.loadView(), plainTitle) == gen1Title
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}

// Turning an index off that the view serves leaves its folder to the refresh,
// which deletes it once no reader holds it.
func TestReindexShardViewIndexOff(t *testing.T) {
	cancelled := distributedtask.TaskStatusCancelled
	tests := []struct {
		name   string
		live   map[string]uint64
		tasks  []distributedtask.ShardTaskState
		folder string // the served filterable folder
		// offLive is set with the index off, and a refresh runs before the shard's property update.
		offLive map[string]uint64
	}{
		{
			name:   "a plain folder of a property a run governs",
			tasks:  []distributedtask.ShardTaskState{viewTaskState("a", 1, ReindexTypeChangeAlgorithm, cancelled, true, false)},
			folder: plainTitle,
		},
		{
			name:   "a generation-2 folder",
			live:   map[string]uint64{indexTypeFilterable: 2},
			folder: "property_title.gen2",
		},
		{
			name:    "a plain folder of a property the index-off makes governed",
			folder:  plainTitle,
			offLive: map[string]uint64{indexTypeFilterable: 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewFixture(t, "ViewTest")
			f.setLive(tc.live)
			f.refreshTo(tc.tasks...)
			f.startLoop()
			dir := filepath.Join(f.shard.pathLSM(), tc.folder)
			require.Equal(t, tc.folder, servedFolder(f.shard.loadView(), plainTitle))

			held := f.shard.acquireView()
			off := false
			f.class.Properties[0].IndexFilterable = &off
			if tc.offLive != nil {
				f.setLive(tc.offLive)
				require.NoError(t, f.shard.refreshView(context.Background()))
			} else {
				require.NoError(t, f.idx.updateProperty(context.Background(), f.class.Properties[0]))
			}
			require.Eventually(t, func() bool { return f.shard.loadView() != held }, 5*time.Second, 10*time.Millisecond)
			require.Nil(t, f.shard.loadView().Bucket(plainTitle), "queries stop using the index")
			require.DirExists(t, dir, "deleted while a reader holds it")

			held.release()
			require.Eventually(t, func() bool {
				_, err := os.Stat(dir)
				return errors.Is(err, os.ErrNotExist)
			}, 5*time.Second, 10*time.Millisecond)
			require.Nil(t, f.shard.store.Bucket(tc.folder))
		})
	}
}

// A load deletes every index folder of a class property its view does not
// name, and nothing else.
func TestReindexShardViewLoadSweep(t *testing.T) {
	const otherPlain = "property_title__gen1" // plain folder of the property "title__gen1"
	tests := []struct {
		name          string
		prepare       func(f *viewFixture, lsm string)
		gone, kept    []string
		hello, others uint64 // filterable counts of "title" and "title__gen1"
	}{
		{
			name: "a plain folder left beside the served generation 1",
			prepare: func(f *viewFixture, lsm string) {
				require.NoError(f.t, copyTree(filepath.Join(lsm, plainTitle), filepath.Join(lsm, gen1Title)))
				f.setLive(map[string]uint64{indexTypeFilterable: 1})
			},
			gone: []string{plainTitle}, kept: []string{gen1Title, otherPlain}, hello: 5, others: 7,
		},
		{
			name: "an index turned off while the shard was unloaded",
			prepare: func(f *viewFixture, lsm string) {
				off := false
				f.class.Properties[0].IndexFilterable = &off
			},
			gone: []string{plainTitle}, kept: []string{otherPlain}, others: 7,
		},
		{
			name: "a staged build keeps its folder, not its scratch",
			prepare: func(f *viewFixture, lsm string) {
				f.tasks.set(stagedTask("a"))
				for _, dir := range []string{gen1Title, gen1Title + ".build"} {
					require.NoError(f.t, os.Mkdir(filepath.Join(lsm, dir), 0o755))
				}
			},
			gone: []string{gen1Title + ".build"}, kept: []string{plainTitle, gen1Title, otherPlain}, hello: 5, others: 7,
		},
		{
			name: "unnamed generations and sidecars of a class property, not other folders",
			prepare: func(f *viewFixture, lsm string) {
				for _, dir := range []string{
					"property_title.gen3_searchable", "property_title__filt_retokenize_ingest_1",
					"property_gone.gen1", "property_title_propertyLength",
				} {
					require.NoError(f.t, os.MkdirAll(filepath.Join(lsm, dir), 0o755))
				}
			},
			gone: []string{"property_title.gen3_searchable", "property_title__filt_retokenize_ingest_1"},
			kept: []string{plainTitle, otherPlain, "property_gone.gen1", "property_title_propertyLength"}, hello: 5, others: 7,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			text := func(name string) *models.Property {
				return &models.Property{Name: name, DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord}
			}
			f := newViewFixture(t, "ViewTest", text("title"), text("title__gen1"))
			for i := range 7 {
				obj := testObject(f.class.Class)
				obj.Object.Properties = map[string]any{"title__gen1": "hello"}
				if i < 5 {
					obj.Object.Properties.(map[string]any)["title"] = "hello"
				}
				require.NoError(t, f.shard.PutObject(context.Background(), obj))
			}
			require.NoError(t, f.shard.Shutdown(context.Background()))
			lsm := f.shard.pathLSM()
			tc.prepare(f, lsm)

			sl, err := f.idx.initShard(context.Background(), f.shard.name, f.class, nil, true, true)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sl.Shutdown(context.Background())) })
			for _, dir := range tc.gone {
				require.NoDirExists(t, filepath.Join(lsm, dir))
			}
			for _, dir := range tc.kept {
				require.DirExists(t, filepath.Join(lsm, dir))
			}
			v := sl.(*Shard).loadView()
			for plain, want := range map[string]uint64{plainTitle: tc.hello, otherPlain: tc.others} {
				var got uint64
				if b := v.Bucket(plain); b != nil {
					bm, release, err := b.RoaringSetGet(context.Background(), []byte("hello"))
					require.NoError(t, err)
					got = uint64(bm.GetCardinality())
					release()
				}
				require.Equal(t, want, got, plain)
			}
		})
	}
}

func TestReindexShardViewStagingLimit(t *testing.T) {
	const limit = 2 * time.Second
	tests := []struct {
		name string
		// run drives the shard's state while the unit of task "a" waits.
		run          func(f *viewFixture)
		failsAfter   time.Duration
		returnsBuild bool
	}{
		{
			name: "a folder that keeps failing to open fails the waiting unit",
			run: func(f *viewFixture) {
				lockFolder(f.t, f.shard.pathLSM())
				f.tasks.set(stagedTask("a"))
			},
			failsAfter: limit,
		},
		{
			name: "a folder pending retire never fails the waiting unit",
			run: func(f *viewFixture) {
				f.tasks.set(stagedTask("dead"))
				require.Eventually(f.t, func() bool {
					return slices.Equal(stagedTasks(f.shard.loadView()), []string{"dead"})
				}, 5*time.Second, 10*time.Millisecond)
				held := f.shard.acquireView()
				f.tasks.set(stagedTask("a"))
				time.Sleep(limit + time.Second)
				held.release()
			},
			returnsBuild: true,
		},
		{
			name: "a successful refresh restarts the clock",
			run: func(f *viewFixture) {
				lockFolder(f.t, f.shard.pathLSM())
				f.tasks.set(stagedTask("a"))
				time.Sleep(1200 * time.Millisecond)
				f.tasks.set()
				time.Sleep(200 * time.Millisecond)
				f.tasks.set(stagedTask("a"))
			},
			failsAfter: 1400*time.Millisecond + limit,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewFixture(t, "ViewTest")
			f.idx.db.config.ReindexStagingFailureTimeout = limit

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				for ctx.Err() == nil {
					_ = f.shard.refreshView(ctx)
					time.Sleep(50 * time.Millisecond)
				}
			}()
			start := time.Now()
			type result struct {
				builds []*stagedBuild
				err    error
			}
			done := make(chan result, 1)
			go func() {
				builds, err := f.shard.awaitStagedBuilds(ctx, "a", 1, "title")
				done <- result{builds, err}
			}()
			tc.run(f)

			var res result
			select {
			case res = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the waiting unit neither got its build nor failed")
			}
			if tc.returnsBuild {
				require.NoError(t, res.err)
				require.Len(t, res.builds, 1)
				return
			}
			require.ErrorContains(t, res.err, "permission denied")
			require.GreaterOrEqual(t, time.Since(start), tc.failsAfter)
		})
	}
}
