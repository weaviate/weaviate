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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

// loadRecorder lists the wrappers that attempted a load, so a second open is seen even when it fails.
type loadRecorder struct {
	mu     sync.Mutex
	loaded []*LazyLoadShard
}

func (r *loadRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loaded = nil
}

func (r *loadRecorder) attempted(l *LazyLoadShard) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, seen := range r.loaded {
		if seen == l {
			return true
		}
	}
	return false
}

func recordLoadAttempts(f *unloadFixture) *loadRecorder {
	r := &loadRecorder{}
	record := func(l *LazyLoadShard) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.loaded = append(r.loaded, l)
	}
	f.unloader.onBeforeLoad.Store(&record)
	return r
}

func (f *unloadFixture) coldThenHot(t *testing.T) {
	_, err := f.index.UnloadLocalShard(testCtx(), f.name)
	assert.NoError(t, err)
	assert.NoError(t, f.index.LoadLocalShardForTenantActivation(testCtx(), f.name, false))
	assert.NotSame(t, f.lazy, f.index.shards.Load(f.name), "the transition must replace the wrapper")
}

func (f *unloadFixture) goCold(t *testing.T) {
	_, err := f.index.UnloadLocalShard(testCtx(), f.name)
	assert.NoError(t, err)
	assert.Nil(t, f.index.shards.Load(f.name))
}

// transitionAtPin runs fn at the n-th pin of the fixture's shard and reports whether it got there.
func (f *unloadFixture) transitionAtPin(n int32, fn func()) func() bool {
	var pins atomic.Int32
	f.index.beforeUnloadingPin = func(string) {
		if pins.Add(1) == n {
			fn()
		}
	}
	return func() bool { return pins.Load() >= n }
}

func (f *unloadFixture) requireDirectoryReopens(t *testing.T) {
	t.Helper()
	_, err := f.index.UnloadLocalShard(testCtx(), f.name)
	require.NoError(t, err)
	db, err := shardmeta.Open(shardPath(f.index.path(), f.name), time.Second)
	if err == nil {
		err = db.Close()
	}
	require.NoError(t, err, "an orphan instance still holds the shard's metadata db")
}

// legacyFilterableBucket lays out the pre-1.19 buckets the filter migration fixes; the shard must be cold.
func (f *unloadFixture) legacyFilterableBucket(t *testing.T) {
	t.Helper()
	require.False(t, f.lazy.isLoaded())
	f.index.invertedIndexConfig.UsingBlockMaxWAND = false
	dir := filepath.Join(shardPath(f.index.path(), f.name), "lsm")
	store, err := lsmkv.New(dir, dir, f.index.logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	name := helpers.BucketFromPropNameLSM("warm")
	require.NoError(t, store.CreateOrLoadBucket(testCtx(), name, lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
	bucket := store.Bucket(name)
	require.NoError(t, bucket.MapSet([]byte("term"), lsmkv.MapPair{Key: []byte("doc"), Value: []byte("freq")}))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, store.Shutdown(testCtx()))
}

func filterMigration(f *unloadFixture) error {
	class := func(string) *models.Class { return f.schemaClass }
	_, err := newFilterableToSearchableMigrator(f.migrator).migrateClass(testCtx(), f.index, class)
	return err
}

func missingFilterableMigration(t *testing.T, f *unloadFixture) {
	t.Helper()
	require.NoError(t, newFilterableToSearchableMigrationFiles(f.index.Config.RootPath).
		saveMigrationState(&filterableToSearchableMigrationState{
			MissingFilterableClass2Props: map[string]map[string]struct{}{
				f.index.Config.ClassName.String(): {"warm": {}},
			},
			CreatedFilterableClass2Props: map[string]map[string]struct{}{},
		}))
}

type pinWalkSite struct {
	name string
	// pin is the pin the transition lands ahead of.
	pin   int32
	setup func(t *testing.T, f *unloadFixture)
	run   func(f *unloadFixture) error
}

func pinWalkSites() []pinWalkSite {
	return []pinWalkSite{
		{
			name: "filter migration discovery",
			pin:  1,
			setup: func(t *testing.T, f *unloadFixture) {
				f.put(t)
			},
			run: filterMigration,
		},
		{
			name:  "filter migration of a shard",
			pin:   2,
			setup: func(t *testing.T, f *unloadFixture) { f.legacyFilterableBucket(t) },
			run:   filterMigration,
		},
		{
			name: "specified inverted reindex",
			pin:  1,
			setup: func(t *testing.T, f *unloadFixture) {
				f.put(t)
			},
			run: func(f *unloadFixture) error {
				return f.migrator.InvertedReindex(testCtx(), map[string]any{
					"ShardInvertedReindexTask_SpecifiedIndex": map[string][]string{
						f.index.Config.ClassName.String(): {"warm"},
					},
				})
			},
		},
		{
			name: "missing text filterable",
			pin:  1,
			setup: func(t *testing.T, f *unloadFixture) {
				f.put(t)
				missingFilterableMigration(t, f)
			},
			run: func(f *unloadFixture) error {
				return f.migrator.InvertedReindex(testCtx(), map[string]any{
					"ShardInvertedReindexTaskMissingTextFilterable": nil,
				})
			},
		},
	}
}

func TestPinWhileUnloadingWalksWorkOnTheResidentShard(t *testing.T) {
	for _, site := range pinWalkSites() {
		t.Run(site.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			site.setup(t, f)
			loads := recordLoadAttempts(f)
			reached := f.transitionAtPin(site.pin, func() {
				f.coldThenHot(t)
				loads.reset()
			})

			require.NotPanics(t, func() { require.NoError(t, site.run(f)) })

			require.True(t, reached(), "the walk never got to the pin under test")
			require.False(t, loads.attempted(f.lazy), "the walk reloaded the wrapper a COLD/HOT replaced")
			require.False(t, f.lazy.isLoaded())
			f.index.beforeUnloadingPin = nil
			f.requireDirectoryReopens(t)
		})
	}
}

func TestPinWhileUnloadingWalksSkipAShardThatLeftTheMap(t *testing.T) {
	for _, site := range pinWalkSites() {
		t.Run(site.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			site.setup(t, f)
			loads := recordLoadAttempts(f)
			reached := f.transitionAtPin(site.pin, func() {
				f.goCold(t)
				loads.reset()
			})

			require.NotPanics(t, func() { require.NoError(t, site.run(f)) })

			require.True(t, reached(), "the walk never got to the pin under test")
			require.False(t, loads.attempted(f.lazy))
			require.Nil(t, f.index.shards.Load(f.name))
			f.index.beforeUnloadingPin = nil
			f.requireDirectoryReopens(t)
		})
	}
}

func TestPinWhileUnloadingWalksWithoutUnloaderStayOnTheWalkedShard(t *testing.T) {
	for _, site := range pinWalkSites() {
		t.Run(site.name, func(t *testing.T) {
			f := newUnloadFixture(t, withShardUnloader(nil))
			site.setup(t, f)
			f.transitionAtPin(1, func() { t.Error("no unloader must leave the walk unpinned") })

			require.NoError(t, site.run(f))

			require.Same(t, f.lazy, f.index.shards.Load(f.name))
		})
	}
}

func TestPinWhileUnloadingHelper(t *testing.T) {
	t.Run("a COLD/HOT swap pins and returns the resident instance", func(t *testing.T) {
		f := newUnloadFixture(t)
		f.put(t)
		loads := recordLoadAttempts(f)
		f.transitionAtPin(1, func() { f.coldThenHot(t) })

		shard, release, err := f.index.pinWhileUnloading(f.name, f.lazy)
		defer release()
		require.NoError(t, err)

		require.Same(t, f.index.shards.Load(f.name), shard)
		require.NotSame(t, f.lazy, shard)
		require.False(t, f.lazy.isLoaded())
		require.False(t, loads.attempted(f.lazy))
		resident, ok := shard.(*LazyLoadShard)
		require.True(t, ok)
		require.Equal(t, UnloadOutcomeInUse, resident.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))

		release()
		f.index.beforeUnloadingPin = nil
		f.requireDirectoryReopens(t)
	})

	t.Run("no unloader returns walked and pins nothing", func(t *testing.T) {
		f := newUnloadFixture(t, withShardUnloader(nil))
		f.transitionAtPin(1, func() { t.Error("no unloader must not look at the map") })
		walked := NewMockShardLike(t)

		shard, release, err := f.index.pinWhileUnloading(f.name, walked)
		defer release()
		require.NoError(t, err)

		require.Same(t, walked, shard)
		require.False(t, f.lazy.isLoaded(), "nothing was loaded to pin it")
	})

	t.Run("no unloader keeps a nil walked as it was", func(t *testing.T) {
		f := newUnloadFixture(t, withShardUnloader(nil))

		shard, release, err := f.index.pinWhileUnloading(f.name, nil)
		defer release()
		require.NoError(t, err)

		require.Nil(t, shard)
	})
}

func TestWalkSkipsPinRefusal(t *testing.T) {
	inject := func(flag func(*Shard)) func(*testing.T, *unloadFixture) {
		return func(t *testing.T, f *unloadFixture) {
			f.put(t)
			flag(f.lazy.currentShard())
		}
	}
	tests := []struct {
		name  string
		setup func(t *testing.T, f *unloadFixture)
		undo  func(f *unloadFixture)
		want  error
		skip  bool
	}{
		{
			name:  "shard gone from the map",
			setup: func(t *testing.T, f *unloadFixture) { f.goCold(t) },
			want:  enterrors.ErrShardNotRegistered,
			skip:  true,
		},
		{
			name:  "shutting down",
			setup: inject(func(s *Shard) { s.shutdownRequested.Store(true) }),
			undo:  func(f *unloadFixture) { f.lazy.currentShard().shutdownRequested.Store(false) },
			want:  errShutdownInProgress,
			skip:  true,
		},
		{
			name:  "being dropped",
			setup: inject(func(s *Shard) { s.dropRequested.Store(true) }),
			undo:  func(f *unloadFixture) { f.lazy.currentShard().dropRequested.Store(false) },
			want:  errDropInProgress,
			skip:  true,
		},
		{
			name: "replaced by a recovering shard",
			setup: func(t *testing.T, f *unloadFixture) {
				f.index.shards.Store(f.name, NewRecoveringShard(testCtx(), f.lazy.shardOpts.promMetrics, f.name, f.index,
					f.lazy.shardOpts.class, f.lazy.shardOpts.jobQueueCh, f.lazy.memMonitor, f.lazy.shardLoadLimiter,
					f.lazy.shardOpts.shardReindexer, f.lazy.shardOpts.bitmapBufPool))
			},
			undo: func(f *unloadFixture) { f.index.shards.Store(f.name, f.lazy) },
			want: enterrors.ErrShardRecovering,
			skip: true,
		},
		{
			name: "torn stays an error",
			setup: func(t *testing.T, f *unloadFixture) {
				f.put(t)
				s := f.lazy.currentShard()
				replaceLegacyVectorIndex(t, s, func(v VectorIndex) VectorIndex { return failingShutdownVectorIndex{v} })
				require.Equal(t, UnloadOutcomeTorn, f.lazy.TryUnload(testCtx(), UnloadRequest{Reindex: noReindex}))
			},
			want: errAlreadyShutdown,
		},
		{
			name: "closing index stays an error",
			setup: func(t *testing.T, f *unloadFixture) {
				f.put(t)
				f.goCold(t)
				f.index.signalCloseRequested(errIndexClosed)
			},
			want: enterrors.ErrShardNotRegistered,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newUnloadFixture(t)
			tt.setup(t, f)
			if tt.undo != nil {
				defer tt.undo(f)
			}

			shard, release, err := f.index.pinWhileUnloading(f.name, f.lazy)
			defer release()

			require.Nil(t, shard)
			require.ErrorIs(t, err, tt.want)
			require.Equal(t, tt.skip, f.index.walkSkipsPinRefusal(f.name, err))
		})
	}

	t.Run("any other failure stays an error", func(t *testing.T) {
		f := newUnloadFixture(t)
		require.False(t, f.index.walkSkipsPinRefusal(f.name, errors.New("disk on fire")))
	})
}

// debugWorkShard tells which instance a debug handler's background work ran on.
type debugWorkShard struct {
	*LazyLoadShard
	tag string
	ran chan<- string
}

func (s *debugWorkShard) RepairIndex(context.Context, string) error {
	s.ran <- s.tag
	return nil
}

func (s *debugWorkShard) RequantizeIndex(context.Context, string) error {
	s.ran <- s.tag
	return nil
}

// errorMessages forwards each logged error line, so a handler that only logs can be awaited.
type errorMessages chan string

func (errorMessages) Levels() []logrus.Level { return []logrus.Level{logrus.ErrorLevel} }

func (c errorMessages) Fire(e *logrus.Entry) error {
	select {
	case c <- e.Message:
	default:
	}
	return nil
}

func TestDebugVectorIndexTasksWorkOnThePinnedShard(t *testing.T) {
	const wait = 30 * time.Second
	handlers := []struct {
		name string
		run  func(i *Index, ctx context.Context, shard, targetVector string) error
	}{
		{name: "repair", run: (*Index).DebugRepairIndex},
		{name: "requantize", run: (*Index).DebugRequantizeIndex},
	}
	setUp := func(t *testing.T, opts ...func(*Config)) (*unloadFixture, chan string, *LazyLoadShard) {
		f := newUnloadFixture(t, opts...)
		f.put(t)
		ran := make(chan string, 1)
		f.index.shards.Store(f.name, &debugWorkShard{LazyLoadShard: f.lazy, tag: "walked", ran: ran})
		return f, ran, f.lazy
	}
	await := func(t *testing.T, ran <-chan string) string {
		t.Helper()
		select {
		case tag := <-ran:
			return tag
		case <-time.After(wait):
			t.Fatal("the background work never ran")
			return ""
		}
	}

	for _, h := range handlers {
		t.Run(h.name+": a COLD/HOT swap ahead of the pin", func(t *testing.T) {
			f, ran, walked := setUp(t)
			loads := recordLoadAttempts(f)
			f.transitionAtPin(1, func() {
				f.index.shards.Store(f.name, walked)
				f.coldThenHot(t)
				resident, ok := f.index.shards.Load(f.name).(*LazyLoadShard)
				assert.True(t, ok)
				f.index.shards.Store(f.name, &debugWorkShard{LazyLoadShard: resident, tag: "resident", ran: ran})
			})

			require.NoError(t, h.run(f.index, testCtx(), f.name, ""))

			require.Equal(t, "resident", await(t, ran))
			require.False(t, loads.attempted(walked))
			wrapped, ok := f.index.shards.Load(f.name).(*debugWorkShard)
			require.True(t, ok)
			f.index.shards.Store(f.name, wrapped.LazyLoadShard)
			f.index.beforeUnloadingPin = nil
			f.requireDirectoryReopens(t)
		})

		t.Run(h.name+": a shard gone ahead of the pin logs and does no work", func(t *testing.T) {
			f, ran, walked := setUp(t)
			logged := make(errorMessages, 8)
			logger, ok := f.index.logger.(*logrus.Logger)
			require.True(t, ok)
			logger.AddHook(logged)
			f.transitionAtPin(1, func() {
				f.index.shards.Store(f.name, walked)
				f.goCold(t)
			})

			require.NoError(t, h.run(f.index, testCtx(), f.name, ""))

			select {
			case msg := <-logged:
				require.True(t, strings.Contains(msg, "failed to pin the shard"), msg)
			case tag := <-ran:
				t.Fatalf("worked on %s though the shard left the map", tag)
			case <-time.After(wait):
				t.Fatal("the background work never finished")
			}
			require.False(t, walked.isLoaded())
		})

		t.Run(h.name+": no unloader runs on the walked shard unpinned", func(t *testing.T) {
			f, ran, walked := setUp(t, withShardUnloader(nil))
			f.transitionAtPin(1, func() { t.Error("no unloader must not pin") })

			require.NoError(t, h.run(f.index, testCtx(), f.name, ""))

			require.Equal(t, "walked", await(t, ran))
			f.index.shards.Store(f.name, walked)
		})
	}
}
