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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/replication"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/replica"
)

type checkpointMetricSnapshot struct {
	create, failure, delete, expired, active float64
	lifetime                                 uint64
}

func snapshotCheckpointMetrics(t *testing.T, m *Metrics) checkpointMetricSnapshot {
	t.Helper()
	var h dto.Metric
	require.NoError(t, m.asyncCheckpointLifetimeSeconds.Write(&h))
	return checkpointMetricSnapshot{
		create:   testutil.ToFloat64(m.asyncCheckpointCreateCount),
		failure:  testutil.ToFloat64(m.asyncCheckpointCreateFailureCount),
		delete:   testutil.ToFloat64(m.asyncCheckpointDeleteCount),
		expired:  testutil.ToFloat64(m.asyncCheckpointExpiredCount),
		active:   testutil.ToFloat64(m.asyncCheckpointActive),
		lifetime: h.GetHistogram().GetSampleCount(),
	}
}

func (a checkpointMetricSnapshot) minus(b checkpointMetricSnapshot) checkpointMetricSnapshot {
	return checkpointMetricSnapshot{
		create: a.create - b.create, failure: a.failure - b.failure, delete: a.delete - b.delete,
		expired: a.expired - b.expired, active: a.active - b.active, lifetime: a.lifetime - b.lifetime,
	}
}

func TestUnloadedCheckpointRegistry_Metrics(t *testing.T) {
	expired := replica.AsyncCheckpointMaxLifetime + time.Minute
	cutoffMs := time.Now().Add(time.Hour).UnixMilli()
	seed := func(r *unloadedCheckpointRegistry, name string, age time.Duration) {
		require.NoError(t, r.put(name, unloadedCheckpoint{cutoffMs: cutoffMs, createdAt: time.Now()}))
		backdateUnloadedCheckpoint(r, name, age)
	}
	newer := unloadedCheckpoint{cutoffMs: cutoffMs, createdAt: time.Now().Add(time.Hour)}
	tests := []struct {
		name  string
		setup func(r *unloadedCheckpointRegistry)
		act   func(t *testing.T, r *unloadedCheckpointRegistry)
		want  checkpointMetricSnapshot
		left  int
	}{
		{
			name: "put new entry",
			act:  func(t *testing.T, r *unloadedCheckpointRegistry) { require.NoError(t, r.put("a", newer)) },
			want: checkpointMetricSnapshot{create: 1, active: 1}, left: 1,
		},
		{
			name:  "put replaces a live entry",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", 0) },
			act:   func(t *testing.T, r *unloadedCheckpointRegistry) { require.NoError(t, r.put("a", newer)) },
			want:  checkpointMetricSnapshot{create: 1, lifetime: 1}, left: 1,
		},
		{
			name:  "put stale",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", 0) },
			act: func(t *testing.T, r *unloadedCheckpointRegistry) {
				require.ErrorIs(t, r.put("a", unloadedCheckpoint{cutoffMs: cutoffMs, createdAt: time.Now().Add(-time.Hour)}), errAsyncCheckpointStale)
			},
			want: checkpointMetricSnapshot{failure: 1}, left: 1,
		},
		{
			name: "put cutoff in past",
			act: func(t *testing.T, r *unloadedCheckpointRegistry) {
				require.ErrorIs(t, r.put("a", unloadedCheckpoint{cutoffMs: 1, createdAt: time.Now()}), errAsyncCheckpointCutoffInPast)
			},
			want: checkpointMetricSnapshot{failure: 1},
		},
		{
			name:  "put overwrites an expired entry",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", expired) },
			act:   func(t *testing.T, r *unloadedCheckpointRegistry) { require.NoError(t, r.put("a", newer)) },
			want:  checkpointMetricSnapshot{create: 1, expired: 1, lifetime: 1}, left: 1,
		},
		{
			name:  "failed put still expires the entry it found",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", expired) },
			act: func(t *testing.T, r *unloadedCheckpointRegistry) {
				require.ErrorIs(t, r.put("a", unloadedCheckpoint{cutoffMs: 1, createdAt: time.Now()}), errAsyncCheckpointCutoffInPast)
			},
			want: checkpointMetricSnapshot{failure: 1, expired: 1, active: -1, lifetime: 1},
		},
		{
			name: "put sweep expires another entry",
			setup: func(r *unloadedCheckpointRegistry) {
				seed(r, "a", expired)
				r.lastSweep = time.Now().Add(-unloadedCheckpointSweepInterval - time.Second)
			},
			act:  func(t *testing.T, r *unloadedCheckpointRegistry) { require.NoError(t, r.put("b", newer)) },
			want: checkpointMetricSnapshot{create: 1, expired: 1, lifetime: 1}, left: 1,
		},
		{
			name:  "get expired entry",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", expired) },
			act: func(t *testing.T, r *unloadedCheckpointRegistry) {
				_, ok := r.get("a")
				require.False(t, ok)
			},
			want: checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1},
		},
		{
			name:  "get live entry",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", 0) },
			act: func(t *testing.T, r *unloadedCheckpointRegistry) {
				_, ok := r.get("a")
				require.True(t, ok)
			},
			left: 1,
		},
		{
			name:  "delete live entry",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", 0) },
			act:   func(_ *testing.T, r *unloadedCheckpointRegistry) { r.delete("a") },
			want:  checkpointMetricSnapshot{delete: 1, active: -1, lifetime: 1},
		},
		{
			name:  "delete expired entry",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", expired) },
			act:   func(_ *testing.T, r *unloadedCheckpointRegistry) { r.delete("a") },
			want:  checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1},
		},
		{
			name: "delete absent entry",
			act:  func(_ *testing.T, r *unloadedCheckpointRegistry) { r.delete("a") },
		},
		{
			name:  "clear live entry",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", 0) },
			act:   func(_ *testing.T, r *unloadedCheckpointRegistry) { r.clear("a") },
			want:  checkpointMetricSnapshot{active: -1, lifetime: 1},
		},
		{
			name:  "clear expired entry",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", expired) },
			act:   func(_ *testing.T, r *unloadedCheckpointRegistry) { r.clear("a") },
			want:  checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1},
		},
		{
			name:  "expired entry hit by get then delete counts once",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", expired) },
			act: func(_ *testing.T, r *unloadedCheckpointRegistry) {
				r.get("a")
				r.delete("a")
				r.clear("a")
			},
			want: checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1},
		},
		{
			name:  "sweep expires only outlived entries",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", expired); seed(r, "b", 0) },
			act:   func(t *testing.T, r *unloadedCheckpointRegistry) { require.Equal(t, 1, r.sweep(time.Now())) },
			want:  checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1}, left: 1,
		},
		{
			name:  "clearAll",
			setup: func(r *unloadedCheckpointRegistry) { seed(r, "a", expired); seed(r, "b", 0) },
			act:   func(_ *testing.T, r *unloadedCheckpointRegistry) { r.clearAll() },
			want:  checkpointMetricSnapshot{expired: 1, active: -2, lifetime: 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &unloadedCheckpointRegistry{metrics: checkpointTestMetrics(t), lastSweep: time.Now()}
			if tc.setup != nil {
				tc.setup(r)
			}
			before := snapshotCheckpointMetrics(t, r.metrics)
			tc.act(t, r)
			assert.Equal(t, tc.want, snapshotCheckpointMetrics(t, r.metrics).minus(before))
			assert.Len(t, r.entries, tc.left)
		})
	}
}

func TestUnloadedCheckpointRegistry_EveryEntryLeavesOnce(t *testing.T) {
	r := &unloadedCheckpointRegistry{metrics: checkpointTestMetrics(t)}
	before := snapshotCheckpointMetrics(t, r.metrics)
	cutoffMs := time.Now().Add(time.Hour).UnixMilli()
	exits := []func(name string){
		func(name string) { r.get(name) },
		r.delete,
		r.clear,
		func(string) { r.sweep(time.Now()) },
		func(name string) {
			if err := r.put(name, unloadedCheckpoint{cutoffMs: cutoffMs, createdAt: time.Now().Add(time.Hour)}); err != nil {
				t.Errorf("put: %v", err)
			}
		},
	}
	for i := range 200 {
		name := fmt.Sprintf("s%d", i%7)
		createdAt := time.Now().Add(time.Duration(i) * time.Millisecond)
		if err := r.put(name, unloadedCheckpoint{cutoffMs: cutoffMs, createdAt: createdAt}); err != nil {
			require.ErrorIs(t, err, errAsyncCheckpointStale)
		}
		if i%3 == 0 {
			backdateUnloadedCheckpoint(r, name, replica.AsyncCheckpointMaxLifetime+time.Minute)
		}
		exits[i%len(exits)](name)
	}
	r.clearAll()

	d := snapshotCheckpointMetrics(t, r.metrics).minus(before)
	assert.Zero(t, d.active)
	assert.Equal(t, d.create, float64(d.lifetime))
	assert.Positive(t, d.expired)
	assert.Positive(t, d.delete)
	assert.Empty(t, r.entries)
}

func TestUnloadedAsyncCheckpoint_IndexPathsMeter(t *testing.T) {
	ctx := testCtx()
	expired := replica.AsyncCheckpointMaxLifetime + time.Minute
	createdAt := time.Now().UTC()
	cutoffMs := createdAt.Add(time.Hour).UnixMilli()
	tests := []struct {
		name    string
		inMap   bool
		persist bool
		age     time.Duration
		act     func(t *testing.T, f *unloadedCheckpointFixture)
		want    checkpointMetricSnapshot
	}{
		{
			name: "create in map without snapshot is a failure", inMap: true,
			act: func(t *testing.T, f *unloadedCheckpointFixture) {
				require.ErrorIs(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt.Add(time.Second)), errAsyncReplicationNotActive)
			},
			want: checkpointMetricSnapshot{failure: 1},
		},
		{
			name: "create off map without snapshot is not a failure",
			act: func(t *testing.T, f *unloadedCheckpointFixture) {
				require.NoError(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt.Add(time.Second)))
			},
		},
		{
			name: "explicit delete", inMap: true, persist: true,
			act: func(t *testing.T, f *unloadedCheckpointFixture) {
				require.NoError(t, f.index.deleteAsyncCheckpoint(ctx, f.name))
			},
			want: checkpointMetricSnapshot{delete: 1, active: -1, lifetime: 1},
		},
		{
			name: "explicit delete of an expired entry", inMap: true, persist: true, age: expired,
			act: func(t *testing.T, f *unloadedCheckpointFixture) {
				require.NoError(t, f.index.deleteAsyncCheckpoint(ctx, f.name))
			},
			want: checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1},
		},
		{
			name: "status of an expired entry", inMap: true, persist: true, age: expired,
			act: func(t *testing.T, f *unloadedCheckpointFixture) {
				_, _, _, ok := f.status(t, ctx)
				require.False(t, ok)
			},
			want: checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1},
		},
		{
			name: "status after the shard loaded clears", inMap: true, persist: true,
			act: func(t *testing.T, f *unloadedCheckpointFixture) {
				require.NoError(t, f.lazy.Load(ctx))
				t.Cleanup(func() { require.NoError(t, f.lazy.Shutdown(context.Background())) })
				f.status(t, ctx)
			},
			want: checkpointMetricSnapshot{active: -1, lifetime: 1},
		},
		{
			name: "status after the shard loaded clears an expired entry as expired", inMap: true, persist: true, age: expired,
			act: func(t *testing.T, f *unloadedCheckpointFixture) {
				require.NoError(t, f.lazy.Load(ctx))
				t.Cleanup(func() { require.NoError(t, f.lazy.Shutdown(context.Background())) })
				f.status(t, ctx)
			},
			want: checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1},
		},
		{
			name: "index shutdown clears", inMap: true, persist: true,
			act: func(t *testing.T, f *unloadedCheckpointFixture) {
				require.NoError(t, f.index.Shutdown(ctx))
			},
			want: checkpointMetricSnapshot{active: -1, lifetime: 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newUnloadedCheckpointFixture(t, "UnloadedCkptMetrics", true)
			m := checkpointTestMetrics(t)
			f.index.metrics = m
			f.index.unloadedCheckpoints.metrics = m
			if !tc.inMap {
				f.evict(t)
			}
			if tc.persist {
				writePersistedHashtree(t, f.dir, "hashtree-0000000000000001.ht", 7)
				require.NoError(t, f.index.createAsyncCheckpoint(ctx, f.name, cutoffMs, createdAt))
				backdateUnloadedCheckpoint(&f.index.unloadedCheckpoints, f.name, tc.age)
			}
			before := snapshotCheckpointMetrics(t, m)
			tc.act(t, f)
			assert.Equal(t, tc.want, snapshotCheckpointMetrics(t, m).minus(before))
		})
	}
}

func TestUnloadedCheckpointRegistry_SchedulerLifecycle(t *testing.T) {
	ctx := testCtx()
	f := newUnloadedCheckpointFixture(t, "UnloadedCkptSched", true)
	sched := f.index.asyncReplicationScheduler
	require.NotNil(t, sched)
	registered := func() bool {
		sched.unloadedCheckpointsMu.Lock()
		defer sched.unloadedCheckpointsMu.Unlock()
		_, ok := sched.unloadedCheckpoints[&f.index.unloadedCheckpoints]
		return ok
	}
	require.True(t, registered())
	require.NoError(t, f.index.Shutdown(ctx))
	require.False(t, registered())
}

func TestSweepExpiredCheckpoints_UnloadedRegistries(t *testing.T) {
	sched := newBareScheduler(512, 1)
	sched.ctx = context.Background()
	cutoffMs := time.Now().Add(time.Hour).UnixMilli()
	m := checkpointTestMetrics(t)
	registries := make([]*unloadedCheckpointRegistry, 3)
	for i := range registries {
		r := &unloadedCheckpointRegistry{metrics: m, lastSweep: time.Now()}
		for _, name := range []string{"abandoned", "fresh"} {
			require.NoError(t, r.put(name, unloadedCheckpoint{cutoffMs: cutoffMs, createdAt: time.Now()}))
		}
		backdateUnloadedCheckpoint(r, "abandoned", replica.AsyncCheckpointMaxLifetime+time.Minute)
		sched.registerUnloadedCheckpoints(r)
		registries[i] = r
	}
	sched.deregisterUnloadedCheckpoints(registries[2])
	before := snapshotCheckpointMetrics(t, m)

	sched.sweepExpiredCheckpoints()

	assert.Equal(t, checkpointMetricSnapshot{expired: 2, active: -2, lifetime: 2}, snapshotCheckpointMetrics(t, m).minus(before))
	assert.Equal(t, int64(2), sched.expiredSinceReport.Load())
	for i, r := range registries {
		_, abandoned := r.entries["abandoned"]
		assert.Equal(t, i == 2, abandoned)
		assert.Contains(t, r.entries, "fresh")
	}
}

func TestWorkerWatcherExpiresUnloadedCheckpoints(t *testing.T) {
	prev := asyncWorkerWatcherInterval.Load()
	asyncWorkerWatcherInterval.Store(int64(10 * time.Millisecond))
	t.Cleanup(func() { asyncWorkerWatcherInterval.Store(prev) })
	sched, err := NewAsyncReplicationScheduler(context.Background(), replication.GlobalConfig{
		AsyncReplicationSchedulerWorkers: configRuntime.NewDynamicValue(1),
		AsyncReplicationDisabled:         configRuntime.NewDynamicValue(false),
	}, nil, newNullLogger())
	require.NoError(t, err)
	t.Cleanup(sched.Close)
	r := &unloadedCheckpointRegistry{lastSweep: time.Now()}
	require.NoError(t, r.put("abandoned", unloadedCheckpoint{cutoffMs: time.Now().Add(time.Hour).UnixMilli(), createdAt: time.Now()}))
	backdateUnloadedCheckpoint(r, "abandoned", replica.AsyncCheckpointMaxLifetime+time.Minute)
	sched.registerUnloadedCheckpoints(r)

	require.Eventually(t, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.entries) == 0
	}, 5*time.Second, 5*time.Millisecond)
}

func TestShard_AsyncCheckpointExitIsCountedOnce(t *testing.T) {
	expired := replica.AsyncCheckpointMaxLifetime + time.Minute
	clearCp := func(_ *testing.T, s *Shard) {
		s.asyncReplicationRWMux.Lock()
		defer s.asyncReplicationRWMux.Unlock()
		s.clearAsyncCheckpointLocked()
	}
	del := func(t *testing.T, s *Shard) { require.NoError(t, s.DeleteAsyncCheckpoint(context.Background())) }
	tests := []struct {
		name string
		age  time.Duration
		act  func(t *testing.T, s *Shard)
		want checkpointMetricSnapshot
	}{
		{name: "delete live", act: del, want: checkpointMetricSnapshot{delete: 1, active: -1, lifetime: 1}},
		{name: "delete expired", age: expired, act: del, want: checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1}},
		{name: "stop clears live", act: clearCp, want: checkpointMetricSnapshot{active: -1, lifetime: 1}},
		{name: "stop clears expired", age: expired, act: clearCp, want: checkpointMetricSnapshot{expired: 1, active: -1, lifetime: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := shardWithHashtree(t)
			s.metrics = checkpointTestMetrics(t)
			require.NoError(t, s.CreateAsyncCheckpoint(context.Background(), ckAbs(1_000), time.Now().UTC()))
			backdateAsyncCheckpoint(s, tc.age)
			before := snapshotCheckpointMetrics(t, s.metrics)
			tc.act(t, s)
			tc.act(t, s)
			assert.Equal(t, tc.want, snapshotCheckpointMetrics(t, s.metrics).minus(before))
		})
	}
}
