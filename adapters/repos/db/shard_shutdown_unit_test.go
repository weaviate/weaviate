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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Unit pins for the shard shutdown/restore lifecycle (the integrationTest-
// tagged journeys live in shard_shutdown_test.go; these stay untagged so the
// plain unit gate runs them).

// TestShardStillAlive pins the lifecycle-fence predicate: a shard whose
// Shutdown failed before the shut mark is fully alive and must be restored to
// the shard map (an orphaned live instance lets a reactivation double-open
// the same directory); a shard that got past the mark must not be re-served.
func TestShardStillAlive(t *testing.T) {
	live := &Shard{shutdownLock: new(sync.RWMutex)}
	require.True(t, shardStillAlive(live), "not marked shut => alive")

	shut := &Shard{shutdownLock: new(sync.RWMutex)}
	shut.shut.Store(true)
	require.False(t, shardStillAlive(shut), "marked shut => not restored")

	require.False(t, shardStillAlive(&LazyLoadShard{}),
		"an unloaded lazy shard holds nothing to orphan")

	loaded := newLoadedLazyShard(live)
	require.True(t, shardStillAlive(loaded))

	loadedShut := newLoadedLazyShard(shut)
	require.False(t, shardStillAlive(loadedShut))
}

// TestShutdown_RequestFlagAfterFailure pins the two failure contracts of the
// request flag:
//   - still-in-use: the flag STAYS armed — pending refs exist by definition,
//     and the last release completing the shutdown is the designed
//     eventual-shutdown contract (integration-pinned by
//     TestShardShutdownWhenIdleEventually); guarded ops refuse meanwhile.
//   - any other abort (e.g. a teardown that already failed): the flag is
//     cleared — there may be no pending release left to complete it, and a
//     restored shard would otherwise be a zombie behind a trusted map hit.
func TestShutdown_RequestFlagAfterFailure(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("still in use keeps the deferred shutdown armed", func(t *testing.T) {
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.inUseCounter.Add(1) // an in-flight guarded op

		err := s.Shutdown(context.Background())
		require.ErrorIs(t, err, errShardStillInUse)
		require.True(t, s.shutdownRequested.Load(),
			"pending refs complete the shutdown on release; the flag must stay armed")
		_, err = s.preventShutdown()
		require.ErrorIs(t, err, errShutdownInProgress,
			"new guarded ops are refused while the drain is pending")
	})

	t.Run("ctx cancellation while in use keeps the flag armed", func(t *testing.T) {
		// backoff returns ctx.Err() and swallows the still-in-use attempt
		// error; the pending refs still exist, so the deferred completion
		// must stay armed.
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.inUseCounter.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := s.Shutdown(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.True(t, s.shutdownRequested.Load(),
			"a cancelled wait on an in-use shard is still the in-use case; disarming it strands the deferred shutdown")
	})

	t.Run("non-in-use abort clears the flag", func(t *testing.T) {
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.shut.Store(true)
		s.teardownErr = errors.New("bucket close failed")
		s.shutdownRequested.Store(true)

		start := time.Now()
		err := s.Shutdown(context.Background())
		require.Error(t, err)
		require.ErrorIs(t, err, errTeardownFailed)
		require.NotErrorIs(t, err, errShardStillInUse)
		require.False(t, s.shutdownRequested.Load(),
			"an abort with no pending release must disarm the deferred shutdown")
		require.Less(t, time.Since(start), time.Second,
			"a sticky teardown error must fail fast, not burn the retry backoff under shardCreateLocks")
	})
}

// TestRestoreShardIfStillAlive pins the shared restore helper used by every
// failed-shutdown site (UpdateTenants cold path, ShutdownShard,
// UnloadLocalShard, IncomingReinitShard).
func TestRestoreShardIfStillAlive(t *testing.T) {
	var m shardMap

	live := &Shard{shutdownLock: new(sync.RWMutex)}
	require.True(t, restoreShardIfStillAlive(&m, "s1", live))
	got := m.Load("s1")
	require.NotNil(t, got)

	shut := &Shard{shutdownLock: new(sync.RWMutex)}
	shut.shut.Store(true)
	require.False(t, restoreShardIfStillAlive(&m, "s2", shut),
		"a cleanly-shut shard must not be re-served")
	require.Nil(t, m.Load("s2"))
}

// TestPerformShutdown_TeardownErrorSticks pins that a teardown failure after
// the shut mark stays visible: without the sticky error, the idempotent
// short-circuit converts the retry into a silent nil and callers treat the
// partially-torn shard (open buckets, uncleared registry entries) as cleanly
// closed.
func TestPerformShutdown_TeardownErrorSticks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
	s.shut.Store(true)
	s.teardownErr = errors.New("boom: bucket close failed")

	err := s.Shutdown(context.Background())
	require.Error(t, err, "a swallowed teardown failure must resurface, not read as success")
	require.Contains(t, err.Error(), "boom")

	// A cleanly shut shard keeps the idempotent nil.
	clean := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
	clean.shut.Store(true)
	require.NoError(t, clean.Shutdown(context.Background()))
}

// TestShardReleasesCounterAndVersionFiles pins that a shard holds its indexcount
// file open only while it is loaded, and its version file not at all once it has
// loaded. Each tenant a node loads and unloads would otherwise keep descriptors.
func TestShardReleasesCounterAndVersionFiles(t *testing.T) {
	ctx := context.Background()
	className := "FileDescriptors"
	shard, index := testShard(t, ctx, className)
	s, ok := shard.(*Shard)
	require.True(t, ok, "the counter and versioner live on the concrete shard")
	require.NoError(t, s.PutObject(ctx, testObject(className)))

	shardPath := s.path()
	counterPath := path.Join(shardPath, "indexcount")
	versionPath := path.Join(shardPath, "version")
	loadShard := func() (*Shard, error) {
		return NewShard(ctx, nil, s.Name(), index, &models.Class{Class: className},
			index.centralJobQueue, index.scheduler,
			index.shardReindexer, false, index.bitmapBufPool, monitoring.ShardRegistrationEager)
	}

	// Round 0 shuts down the shard testShard created, and every later round shuts down one loaded from disk.
	for round := range 3 {
		require.Equal(t, 1, openDescriptorCount(t, counterPath),
			"round %d: a loaded shard keeps its counter file open to persist doc IDs", round)
		require.Zero(t, openDescriptorCount(t, versionPath),
			"round %d: a loaded shard has no use for its version file", round)

		require.NoError(t, s.Shutdown(ctx))
		require.Zero(t, openDescriptorCount(t, counterPath), "round %d: shutdown closes the counter file", round)
		require.Zero(t, openDescriptorCount(t, versionPath), "round %d: shutdown leaves the version file closed", round)

		var err error
		s, err = loadShard()
		require.NoError(t, err)
	}
	require.NoError(t, s.Shutdown(ctx))

	// Each case leaves the shard broken on disk, so the case that fails later in the load runs first.
	failedLoads := []struct {
		name      string
		breakDisk func(t *testing.T)
	}{
		{
			name: "version file too short to read",
			breakDisk: func(t *testing.T) {
				require.NoError(t, os.WriteFile(versionPath, []byte{1}, 0o644))
			},
		},
		{
			name: "store fails before the counter opens",
			breakDisk: func(t *testing.T) {
				lsmPath := path.Join(shardPath, "lsm")
				require.NoError(t, os.RemoveAll(lsmPath))
				require.NoError(t, os.WriteFile(lsmPath, nil, 0o644))
			},
		},
	}
	for _, tc := range failedLoads {
		t.Run(tc.name, func(t *testing.T) {
			tc.breakDisk(t)

			_, err := loadShard()
			require.Error(t, err)
			require.Zero(t, openDescriptorCount(t, counterPath), "a failed load closes the counter file")
			require.Zero(t, openDescriptorCount(t, versionPath), "a failed load closes the version file")
		})
	}
}

// TestShardKnownShut pins the reactivation-eviction predicate: ONLY a shard
// that completed a shutdown may be evicted from the map. An unloaded
// LazyLoadShard is the normal steady state of a not-yet-loaded shard —
// evicting it would race a concurrent Load() on the old wrapper into a second
// instance over the same directory.
func TestShardKnownShut(t *testing.T) {
	require.False(t, shardKnownShut(&Shard{shutdownLock: new(sync.RWMutex)}))
	require.False(t, shardKnownShut(&LazyLoadShard{}),
		"never-loaded lazy shard must NOT be treated as shut")

	shut := &Shard{shutdownLock: new(sync.RWMutex)}
	shut.shut.Store(true)
	require.True(t, shardKnownShut(shut))
	require.True(t, shardKnownShut(newLoadedLazyShard(shut)))
	require.False(t, shardKnownShut(newLoadedLazyShard(&Shard{shutdownLock: new(sync.RWMutex)})))

	// A deep-teardown failure (shut=true, sticky teardownErr) is NOT
	// evictable: the map entry is the last reference to its possibly-leaked
	// handles, so the restore predicate KEEPS it and the belts serve the
	// sticky error instead of re-initializing into a bucket-registry
	// collision (that refusal is pinned in lsmkv's
	// TestBucketReinit_RefusedWhileLeakedOpenThenHealsAfterClose). It is
	// never silently double-opened and never treated as healthy.
	torn := &Shard{shutdownLock: new(sync.RWMutex), teardownErr: errors.New("bucket close failed")}
	torn.shut.Store(true)
	require.False(t, shardKnownShut(torn), "a torn shard must not be evicted for re-init")
	require.False(t, shardStillAlive(torn))
	require.ErrorContains(t, shardTeardownError(torn), "bucket close failed")

	var m shardMap
	require.True(t, restoreShardIfStillAlive(&m, "torn", torn),
		"a torn shard is retained as the last reference to its leaked handles")
	require.NotNil(t, m.Load("torn"))
}

// TestShutdownOrRestoreShardOutcome maps each Shutdown error to its outcome and
// the level of the restore line.
func TestShutdownOrRestoreShardOutcome(t *testing.T) {
	tests := []struct {
		name        string
		shutdownErr error
		want        ShardUnloadOutcome
		wantLevels  []logrus.Level
	}{
		{
			name: "a clean shutdown",
			want: ShardUnloadOutcomeUnloaded,
		},
		{
			name:        "a shard that was already shut",
			shutdownErr: errAlreadyShutdown,
			want:        ShardUnloadOutcomeUnloaded,
		},
		{
			// performShutdown wraps errShardStillInUse, so this row does too.
			name:        "a shard still serving requests",
			shutdownErr: fmt.Errorf("shard %q: %w", "s1", errShardStillInUse),
			want:        ShardUnloadOutcomeRefusedInUse,
			wantLevels:  []logrus.Level{logrus.DebugLevel},
		},
		{
			name:        "a backoff that ran out of time",
			shutdownErr: context.DeadlineExceeded,
			want:        ShardUnloadOutcomeRefusedInUse,
			wantLevels:  []logrus.Level{logrus.DebugLevel},
		},
		{
			name:        "a backoff ended by a close request",
			shutdownErr: context.Canceled,
			want:        ShardUnloadOutcomeRefusedInUse,
			wantLevels:  []logrus.Level{logrus.DebugLevel},
		},
		{
			// No other test in the package reaches failed.
			name:        "a teardown that failed for its own reason",
			shutdownErr: errors.New("bucket close failed"),
			want:        ShardUnloadOutcomeFailed,
			wantLevels:  []logrus.Level{logrus.ErrorLevel},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shard := NewMockShardLike(t)
			shard.On("Shutdown", mock.Anything).Return(tc.shutdownErr)
			idx, hook := indexForShutdownOutcomeTest(t)

			got, err := shutdownOrRestoreShard(context.Background(), idx, "s1", shard)

			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantLevels, logLevels(hook))
			if tc.shutdownErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.shutdownErr)
		})
	}
}

// TestShutdownOrRestoreShardOutcomeTorn covers torn with a real *Shard.
func TestShutdownOrRestoreShardOutcomeTorn(t *testing.T) {
	torn := &Shard{shutdownLock: new(sync.RWMutex), teardownErr: errors.New("bucket close failed")}
	torn.shut.Store(true)
	idx, hook := indexForShutdownOutcomeTest(t)

	got, err := shutdownOrRestoreShard(context.Background(), idx, "s1", torn)

	require.Equal(t, ShardUnloadOutcomeTorn, got)
	require.ErrorIs(t, err, errTeardownFailed)
	require.NotNil(t, idx.shards.Load("s1"), "a torn shard is retained in the map")
	require.Equal(t, []logrus.Level{logrus.ErrorLevel}, logLevels(hook))
}

// indexForShutdownOutcomeTest builds only what shutdownOrRestoreShard reads,
// with a hook that records every line its logger writes.
func indexForShutdownOutcomeTest(t *testing.T) (*Index, *test.Hook) {
	t.Helper()

	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	metrics, err := NewMetrics(logger, nil, "Abc", "n/a")
	require.NoError(t, err)

	return &Index{logger: logger, metrics: metrics, shards: shardMap{}}, hook
}

func logLevels(hook *test.Hook) []logrus.Level {
	var levels []logrus.Level
	for _, entry := range hook.AllEntries() {
		levels = append(levels, entry.Level)
	}
	return levels
}

// TestShardLoadDropsNodeIDBeyondDocIDCounter pins that the vector index and each
// geo property index of a shard are bounded by its document-ID counter, so a
// commit log naming a node far beyond it is truncated on load instead of sizing
// the node index to it (weaviate/0-weaviate-issues#649). HFresh's centroid index
// numbers its nodes by posting ID and must keep no limit; it gets none only
// because it lives in HFresh's directory, where there is no counter.
func TestShardLoadDropsNodeIDBeyondDocIDCounter(t *testing.T) {
	type commitLog struct {
		indexPath     func(s *Shard) string // relative to the shard directory
		wantTruncated bool
	}
	geoLog := commitLog{indexPath: func(*Shard) string { return geoPropID("location") }, wantTruncated: true}

	tests := []struct {
		name              string
		vectorIndexConfig schemaConfig.VectorIndexConfig
		logs              []commitLog
	}{
		{
			name:              "hnsw",
			vectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			logs: []commitLog{
				{indexPath: func(s *Shard) string { return s.vectorIndexID("") }, wantTruncated: true},
				geoLog,
			},
		},
		{
			name:              "hfresh",
			vectorIndexConfig: enthfresh.NewDefaultUserConfig(),
			logs: []commitLog{
				{indexPath: func(s *Shard) string {
					id := s.vectorIndexID("")
					return path.Join(helpers.HFreshDirName(id), helpers.CentroidsID(id))
				}},
				geoLog,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			className := "NodeIDLimit"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "location", DataType: schema.DataTypeGeoCoordinates.PropString()},
				},
			}
			shard, index := testShardWithSettings(t, ctx, class, tc.vectorIndexConfig, false, false)
			s, ok := shard.(*Shard)
			require.True(t, ok)

			const objects = 3
			for i := range objects {
				obj := testObject(className)
				obj.Vector = []float32{float32(i), 1, 2, 3}
				obj.Object.Properties = map[string]interface{}{"location": geoCoordinates(float32(i), 1)}
				require.NoError(t, s.PutObject(ctx, obj))
			}
			shardPath := s.path()
			logPaths := make([]string, len(tc.logs))
			for i, l := range tc.logs {
				logPaths[i] = path.Join(shardPath, l.indexPath(s)+".hnsw.commitlog.d", "9999999999")
			}
			require.NoError(t, s.Shutdown(ctx))

			record := walBytesForShardTest(t, objects+1<<24+1000)
			for _, p := range logPaths {
				require.NoError(t, os.WriteFile(p, record, 0o644))
			}

			s, err := NewShard(ctx, nil, s.Name(), index, class,
				index.centralJobQueue, index.scheduler,
				index.shardReindexer, false, index.bitmapBufPool, monitoring.ShardRegistrationEager)
			require.NoError(t, err)
			defer s.Shutdown(ctx)

			for i, l := range tc.logs {
				p := logPaths[i]
				st, err := os.Stat(p)
				if l.wantTruncated {
					// The truncated log is empty, and the commit logger prunes empty raw logs.
					if os.IsNotExist(err) {
						continue
					}
					require.NoError(t, err)
					require.Zero(t, st.Size(), "%s must be truncated before the out-of-limit record", p)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, int64(len(record)), st.Size(), "%s must keep its record", p)
			}
		})
	}
}

// walBytesForShardTest encodes an AddNode record for id.
func walBytesForShardTest(t *testing.T, id uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, compact.NewWALWriter(&buf).WriteAddNode(id, 0))
	return buf.Bytes()
}

// A failed drain leaves a worker's bucket and metadata handles alive. Other
// teardown failures still perform the usual best-effort resource cleanup.
func TestShardTeardownKeepsResourcesForUndrainedWorkers(t *testing.T) {
	for _, drop := range []bool{false, true} {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("flush failed")} {
			t.Run(fmt.Sprintf("drop=%t/%v", drop, cause), func(t *testing.T) {
				ctx := context.Background()
				sl, _ := testShard(t, ctx, "DrainFailure")
				s := sl.(*Shard)
				metrics := *monitoring.GetMetrics()
				metrics.ShardsLoaded = prometheus.NewGauge(prometheus.GaugeOpts{Name: "shards_loaded"})
				metrics.ShardsLoading = prometheus.NewGauge(prometheus.GaugeOpts{Name: "shards_loading"})
				metrics.ShardsUnloaded = prometheus.NewGauge(prometheus.GaugeOpts{Name: "shards_unloaded"})
				metrics.ShardsUnloading = prometheus.NewGauge(prometheus.GaugeOpts{Name: "shards_unloading"})
				metrics.Shards = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "weaviate_shards"}, []string{"state", "registration"})
				s.metrics.baseMetrics = &metrics
				metrics.NewLoadedShard()
				gauges := shardMetricsHarness{metrics: &metrics}
				require.Equal(t, shardGauges{loaded: 1}, gauges.gauges())
				require.Equal(t, shardGauges{loaded: 1}, gauges.gaugesFor(s.registration))
				vi, releaseIndex, ok := s.AcquireVectorIndex("")
				require.True(t, ok)
				releaseIndex()
				held, release := s.store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
				require.NotNil(t, held)
				release()
				ns := s.metadataDB.Namespace("shutdown-test")
				require.NoError(t, ns.Put([]byte("key"), []byte("value")))
				wrapped := &undrainedVectorIndex{VectorIndex: vi, err: cause}
				require.True(t, s.vectors.Replace("", wrapped))
				t.Cleanup(func() { s.vectors.Replace("", vi); s.drop() })
				var err error
				if drop {
					err = s.drop()
				} else {
					err = s.performShutdown(ctx)
				}
				require.ErrorIs(t, err, cause)
				if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
					bucket, release := s.store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
					require.NotNil(t, bucket, "a failed drain must keep the worker's store open")
					require.Same(t, held, bucket, "a worker must retain the same bucket, not a reopened replacement")
					release()
					value, err := ns.Get([]byte("key"))
					require.NoError(t, err, "a failed drain must keep the worker's metadata open")
					require.Equal(t, []byte("value"), value)
				} else {
					bucket, release := s.store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
					release()
					require.Nil(t, bucket, "ordinary teardown errors still close the store")
					_, err = ns.Get([]byte("key"))
					require.Error(t, err, "ordinary teardown errors still close metadata")
				}
				if !drop {
					assert.Equal(t, shardGauges{unloaded: 1}, gauges.gauges(), "a failed shutdown must finish the unloading transition")
					assert.Equal(t, shardGauges{unloaded: 1}, gauges.gaugesFor(s.registration), "a failed shutdown must finish the labeled unloading transition")
				}
				require.True(t, s.vectors.Replace("", vi))
				require.NoError(t, s.drop())
				assert.Equal(t, shardGauges{}, gauges.gauges(), "dropping the shard must remove its count exactly once")
				assert.Equal(t, shardGauges{}, gauges.gaugesFor(s.registration), "dropping the shard must remove its labeled count exactly once")
			})
		}
	}
}

type undrainedVectorIndex struct {
	VectorIndex
	err error
}

func (v *undrainedVectorIndex) Shutdown(ctx context.Context) error {
	if errors.Is(v.err, context.Canceled) || errors.Is(v.err, context.DeadlineExceeded) {
		return v.err
	}
	return errors.Join(v.err, v.VectorIndex.Shutdown(ctx))
}

func (v *undrainedVectorIndex) Drop(ctx context.Context) error {
	if errors.Is(v.err, context.Canceled) || errors.Is(v.err, context.DeadlineExceeded) {
		return v.err
	}
	return errors.Join(v.err, v.VectorIndex.Drop(ctx))
}

func TestShardTeardownKeepsResourcesForUndrainedQueues(t *testing.T) {
	for _, drop := range []bool{false, true} {
		for _, geo := range []bool{false, true} {
			t.Run(fmt.Sprintf("drop=%t/geo=%t", drop, geo), func(t *testing.T) {
				ctx := context.Background()
				sl, _ := testShard(t, ctx, "QueueDrainFailure")
				s := sl.(*Shard)
				started, releaseWorker := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(releaseWorker) }) }
				q, err := queue.NewDiskQueue(queue.DiskQueueOptions{
					ID: "held-shutdown-queue", Dir: t.TempDir(), Logger: s.index.logger, Scheduler: s.scheduler,
					TaskDecoder: heldShutdownTask{started: started, release: releaseWorker},
				})
				require.NoError(t, err)
				require.NoError(t, q.Init())
				s.scheduler.RegisterQueue(q)
				iq, release, ok := s.AcquireVectorIndexQueue("")
				require.True(t, ok)
				release()
				oldQueue, oldAsync := iq.DiskQueue, iq.asyncEnabled
				if geo {
					s.geoQueues["location"] = &VectorIndexQueue{DiskQueue: q, asyncEnabled: true}
				} else {
					iq.DiskQueue, iq.asyncEnabled = q, true
				}
				t.Cleanup(func() {
					unblock()
					q.Close(context.Background())
					iq.DiskQueue, iq.asyncEnabled = oldQueue, oldAsync
					delete(s.geoQueues, "location")
					s.drop()
				})
				require.NoError(t, q.Push([]byte{1}))
				require.NoError(t, q.Flush())
				<-started
				held, release := s.store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
				release()
				ns := s.metadataDB.Namespace("held-queue")
				require.NoError(t, ns.Put([]byte("key"), []byte("value")))
				if drop {
					err = s.drop()
				} else {
					ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
					defer cancel()
					err = s.performShutdown(ctx)
				}
				require.ErrorIs(t, err, context.DeadlineExceeded, "shard teardown must report an undrained queue")
				bucket, release := s.store.AcquireBucketForRead(helpers.ObjectsBucketLSM)
				release()
				require.Same(t, held, bucket, "an undrained queue must keep the worker's store open")
				value, err := ns.Get([]byte("key"))
				require.NoError(t, err, "an undrained queue must keep the worker's metadata open")
				require.Equal(t, []byte("value"), value)
			})
		}
	}
}

type heldShutdownTask struct {
	started chan struct{}
	release chan struct{}
}

func (d heldShutdownTask) DecodeTask([]byte) (queue.Task, error) { return d, nil }
func (d heldShutdownTask) Op() uint8                             { return 1 }
func (d heldShutdownTask) Key() uint64                           { return 0 }
func (d heldShutdownTask) Execute(ctx context.Context) error {
	close(d.started)
	<-d.release
	return ctx.Err()
}

func TestIndexDropRetriesUndrainedShard(t *testing.T) {
	for _, firstWholeIndex := range []bool{false, true} {
		for _, retryWholeIndex := range []bool{false, true} {
			if firstWholeIndex && !retryWholeIndex {
				continue // whole-index close permanently refuses ordinary shard operations
			}
			for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
				t.Run(fmt.Sprintf("whole=%t/retryWhole=%t/%v", firstWholeIndex, retryWholeIndex, cause), func(t *testing.T) {
					sl, idx := testShard(t, context.Background(), "IndexDrainFailure")
					s := sl.(*Shard)
					vi, release, ok := s.AcquireVectorIndex("")
					require.True(t, ok)
					release()
					require.True(t, s.vectors.Replace("", &undrainedVectorIndex{VectorIndex: vi, err: cause}))
					t.Cleanup(func() { s.vectors.Replace("", vi); s.drop() })
					ns := s.metadataDB.Namespace("index-drain")
					require.NoError(t, ns.Put([]byte("key"), []byte("value")))
					drop := func(whole bool) error {
						if whole {
							return idx.drop()
						}
						return idx.dropShards([]string{s.Name()})
					}
					err := drop(firstWholeIndex)
					require.ErrorIs(t, err, cause)
					for range 2 {
						err = drop(retryWholeIndex)
						require.ErrorIs(t, err, cause, "index drop must retry the retained worker's drain")
						require.DirExists(t, s.path(), "index drop must retain an undrained shard's directory")
						value, err := ns.Get([]byte("key"))
						require.NoError(t, err)
						require.Equal(t, []byte("value"), value)
					}
					if !retryWholeIndex {
						idx.purgeUnloadedShardRegistry([]string{s.Name()})
						require.ErrorIs(t, lsmkv.GlobalBucketRegistry.TryAdd(path.Join(s.pathLSM(), helpers.ObjectsBucketLSM)), lsmkv.ErrBucketAlreadyRegistered,
							"a pending drop must keep its live bucket registration")
						// A failed deletion is reachable only for teardown, never for reloading.
						alloc := idx.allocChecker
						idx.allocChecker = failingAllocChecker{}
						_, release, err := idx.getOrInitShard(context.Background(), s.Name())
						release()
						idx.allocChecker = alloc
						require.ErrorIs(t, err, errDropInProgress, "an undrained shard must not be reopened")
						require.ErrorIs(t, idx.LoadLocalShardForMovement(context.Background(), s.Name()), errDropInProgress)
						require.ErrorIs(t, idx.LoadLocalShardForTenantAdd(context.Background(), s.Name()), errDropInProgress)
					}
					require.True(t, s.vectors.Replace("", vi))
					require.NoError(t, drop(retryWholeIndex), "index drop must finish after the retained worker drains")
					require.NoDirExists(t, s.path())
				})
			}
		}
	}
}

func TestRetainedDrainsReachTeardownConsumers(t *testing.T) {
	for _, route := range []string{"delete-index", "orphaned-class", "frozen-tenant", "index-shutdown", "db-shutdown"} {
		for _, lazy := range []bool{false, true} {
			for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
				t.Run(fmt.Sprintf("%s/lazy=%t/%v", route, lazy, cause), func(t *testing.T) {
					sl, idx := testShard(t, context.Background(), "RetainedDrain")
					idx.closeRequestedCtx, idx.signalCloseRequested = context.WithCancelCause(context.Background())
					s := sl.(*Shard)
					vi, release, ok := s.AcquireVectorIndex("")
					require.True(t, ok)
					release()
					worker := &undrainedVectorIndex{VectorIndex: vi, err: cause}
					require.True(t, s.vectors.Replace("", worker))
					t.Cleanup(func() { s.vectors.Replace("", vi); s.drop() })
					if lazy {
						wrapper := &LazyLoadShard{shardOpts: &deferredShardOpts{name: s.Name(), index: idx}}
						wrapper.shard.Store(s)
						idx.shards.Store(s.Name(), wrapper)
					}
					ns := s.metadataDB.Namespace("retained-drain")
					require.NoError(t, ns.Put([]byte("key"), []byte("value")))
					db := newShutdownTestDB(t, logrus.New(), 0)
					db.indices = map[string]*Index{idx.ID(): idx}
					db.config.RootPath, db.promMetrics = idx.Config.RootPath, monitoring.GetMetrics()
					closed := false
					t.Cleanup(func() {
						if !closed {
							db.Shutdown(context.Background())
						}
					})
					m := NewMigrator(db, idx.logger, "node1")
					m.SetCluster(&recordingProcessor{})
					run := func() error {
						switch route {
						case "delete-index":
							return db.DeleteIndex(idx.Config.ClassName)
						case "orphaned-class":
							_, err := db.DropOrphanedClass(idx.Config.ClassName)
							return err
						case "frozen-tenant":
							ec := errorcompounder.NewSafe()
							m.frozen(context.Background(), idx, []string{s.Name()}, ec)
							return ec.ToError()
						case "index-shutdown":
							return idx.Shutdown(context.Background())
						default:
							closed = true
							return db.Shutdown(context.Background())
						}
					}
					if route == "index-shutdown" {
						require.ErrorIs(t, idx.dropShards([]string{s.Name()}), cause)
					} else if route == "db-shutdown" {
						require.ErrorIs(t, db.DeleteIndex(idx.Config.ClassName), cause, "class deletion must report an unfinished drain")
					}
					err := run()
					require.ErrorIs(t, err, cause, "teardown must report the retained worker's unfinished drain")
					require.DirExists(t, s.path(), "teardown must retain an undrained worker's directory")
					value, err := ns.Get([]byte("key"))
					require.NoError(t, err)
					require.Equal(t, []byte("value"), value)
					if route == "delete-index" || route == "orphaned-class" {
						require.Nil(t, db.registeredIndex(idx.Config.ClassName), "a dropping index must not serve requests")
						require.ErrorIs(t, m.AddClass(context.Background(), s.class), errDropInProgress, "a dropping index must not be reopened")
					}
					if route == "delete-index" || route == "orphaned-class" || route == "frozen-tenant" {
						require.ErrorIs(t, run(), cause, "retry must report the retained worker's unfinished drain")
						require.DirExists(t, s.path(), "retry must retain an undrained worker's directory")
					}
					require.True(t, s.vectors.Replace("", vi))
					switch route {
					case "index-shutdown":
						require.NoError(t, idx.drop())
					case "db-shutdown":
						require.NoError(t, db.DeleteIndex(idx.Config.ClassName))
					default:
						require.NoError(t, run())
					}
					require.NoDirExists(t, s.path())
					_, err = ns.Get([]byte("key"))
					require.Error(t, err, "successful teardown must close the retained metadata handle")
				})
			}
		}
	}
}

func TestConcurrentIndexTeardownRetainsDrain(t *testing.T) {
	for _, dropFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("dropFirst=%t", dropFirst), func(t *testing.T) {
			sl, idx := testShard(t, context.Background(), "ConcurrentDrain")
			idx.closeRequestedCtx, idx.signalCloseRequested = context.WithCancelCause(context.Background())
			s := sl.(*Shard)
			vi, release, ok := s.AcquireVectorIndex("")
			require.True(t, ok)
			release()
			started, unblock := make(chan struct{}), make(chan struct{})
			var once sync.Once
			require.True(t, s.vectors.Replace("", &heldTeardownIndex{VectorIndex: vi, drain: func() error {
				once.Do(func() { close(started) })
				<-unblock
				return context.DeadlineExceeded
			}}))
			db := newShutdownTestDB(t, logrus.New(), 0)
			db.indices, db.promMetrics = map[string]*Index{idx.ID(): idx}, monitoring.GetMetrics()
			done := make(chan error, 1)
			enterrors.GoWrapper(func() {
				if dropFirst {
					done <- db.DeleteIndex(idx.Config.ClassName)
				} else {
					done <- idx.Shutdown(context.Background())
				}
			}, idx.logger)
			<-started
			var err error
			if dropFirst {
				err = db.Shutdown(context.Background())
			} else {
				err = db.DeleteIndex(idx.Config.ClassName)
			}
			close(unblock)
			firstErr := <-done
			s.vectors.Replace("", vi)
			defer func() { s.drop(); db.Shutdown(context.Background()) }()
			require.ErrorIs(t, err, context.Canceled, "concurrent teardown must report the active drain")
			require.ErrorIs(t, firstErr, context.DeadlineExceeded)
			require.DirExists(t, s.path())
			require.NoError(t, db.DeleteIndex(idx.Config.ClassName), "deletion must retry after the active drain finishes")
			require.NoDirExists(t, s.path())
		})
	}
}

type heldTeardownIndex struct {
	VectorIndex
	drain func() error
}

func (v *heldTeardownIndex) Shutdown(context.Context) error { return v.drain() }
func (v *heldTeardownIndex) Drop(context.Context) error     { return v.drain() }

func TestClassDropRetainsDrainBeyondErrorLimit(t *testing.T) {
	sl, idx := testShard(t, context.Background(), "LimitedDrain")
	idx.closeRequestedCtx, idx.signalCloseRequested = context.WithCancelCause(context.Background())
	s := sl.(*Shard)
	vi, release, ok := s.AcquireVectorIndex("")
	require.True(t, ok)
	release()
	defer func() { s.vectors.Replace("", vi); s.drop() }()
	ns := s.metadataDB.Namespace("limited-drain")
	require.NoError(t, ns.Put([]byte("key"), []byte("value")))
	reported := make(chan struct{}, maxReportedErrors)
	for n := range maxReportedErrors {
		name := fmt.Sprintf("failed-%d", n)
		shard := NewMockShardLike(t)
		shard.EXPECT().drop().Return(errors.New("drop failed")).Once()
		// The drop worker asks for ID when logging, after recording its error.
		shard.EXPECT().ID().RunAndReturn(func() string { reported <- struct{}{}; return name }).Once()
		idx.shards.Store(name, shard)
	}
	var once sync.Once
	require.True(t, s.vectors.Replace("", &heldTeardownIndex{VectorIndex: vi, drain: func() error {
		once.Do(func() {
			for range maxReportedErrors {
				<-reported
			}
		})
		return context.DeadlineExceeded
	}}))
	db := &DB{indices: map[string]*Index{idx.ID(): idx}, logger: idx.logger, promMetrics: monitoring.GetMetrics(), config: Config{RootPath: idx.Config.RootPath}}
	_, err := db.DropOrphanedClass(idx.Config.ClassName)
	require.Error(t, err, "a hidden drain error must still stop class deletion")
	require.NotErrorIs(t, err, context.DeadlineExceeded, "the bounded summary must omit the last error")
	require.DirExists(t, s.path(), "error truncation must not permit deleting an undrained shard")
	value, err := ns.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
	_, err = db.DropOrphanedClass(idx.Config.ClassName)
	require.ErrorIs(t, err, context.DeadlineExceeded, "a retry must reach the retained drain")
	require.True(t, s.vectors.Replace("", vi))
	_, err = db.DropOrphanedClass(idx.Config.ClassName)
	require.NoError(t, err)
	require.NoDirExists(t, s.path())
}
