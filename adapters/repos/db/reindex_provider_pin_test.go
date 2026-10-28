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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

const pinUnit = "u1"

// pinRecorder records what a unit reported.
type pinRecorder struct {
	mu        sync.Mutex
	completed bool
	failures  []string
	// onProgress, when set, answers every progress report.
	onProgress func(ctx context.Context, progress float32) error
}

func (r *pinRecorder) RecordDistributedTaskUnitCompletion(context.Context, string, string, uint64, string, string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed = true
	return nil
}

func (r *pinRecorder) RecordDistributedTaskUnitFailure(_ context.Context, _, _ string, _ uint64, _, _, msg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, msg)
	return nil
}

func (r *pinRecorder) RecordDistributedTaskRetryableUnitFailure(ctx context.Context, ns, id string, v uint64, node, unit, msg string) error {
	return r.RecordDistributedTaskUnitFailure(ctx, ns, id, v, node, unit, msg)
}

func (r *pinRecorder) UpdateDistributedTaskUnitProgress(ctx context.Context, _, _ string, _ uint64, _, _ string, progress float32) error {
	if r.onProgress != nil {
		return r.onProgress(ctx, progress)
	}
	return nil
}

func (r *pinRecorder) result() (bool, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completed, append([]string(nil), r.failures...)
}

type pinProviderFixture struct {
	idx      *Index
	hot      *Shard
	provider *ReindexProvider
	task     *distributedtask.Task
	payload  *ReindexTaskPayload
}

// newPinProviderFixture builds a provider whose one unit targets the hot shard,
// or with cold set, a cold lazy shard that replaces it in the map.
func newPinProviderFixture(t *testing.T, ctx context.Context, cold bool, allocChecker memwatch.AllocChecker,
	hooks ...logrus.Hook,
) (*pinProviderFixture, *LazyLoadShard) {
	t.Helper()
	hot, idx := newPinSweepIndex(t, ctx, nil)
	shardName := hot.Name()
	var lazy *LazyLoadShard
	if cold {
		idx.shards.LoadAndDelete(shardName)
		if allocChecker == nil {
			allocChecker = idx.allocChecker
		}
		lazy = addColdPinSweepShard(t, ctx, idx, allocChecker)
		shardName = pinSweepCold
	}
	className := idx.Config.ClassName.String()
	db := &DB{logger: idx.logger, indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}}
	provider := NewReindexProvider(db, nil, nil, loggerWithHooks(hooks...), "node1", func() int { return 1 }, ctx)
	return &pinProviderFixture{
		idx:      idx,
		hot:      hot,
		provider: provider,
		task: &distributedtask.Task{
			Namespace:      ReindexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_pin", Version: 1},
		},
		payload: &ReindexTaskPayload{
			Collection:    className,
			MigrationType: ReindexTypeRepairFilterable,
			Properties:    []string{pinSweepProp},
			UnitToShard:   map[string]string{pinUnit: shardName},
			UnitToNode:    map[string]string{pinUnit: "node1"},
		},
	}, lazy
}

func (f *pinProviderFixture) runUnit(ctx context.Context, recorder *pinRecorder) {
	f.provider.processOneUnit(ctx, f.task, f.payload, f.idx, pinUnit, recorder)
}

func TestReindexUnitPinsItsShardForTheWholeRun(t *testing.T) {
	tests := []struct {
		name string
		cold bool
	}{
		{name: "loaded shard"},
		{name: "cold shard loaded by the unit", cold: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			var target func() *Shard
			var pinsDuringRun atomic.Int64
			pinsDuringRun.Store(-1)
			hook := onLogMessage{msg: "reindex provider: unit completed", fn: func() {
				if s := target(); s != nil {
					pinsDuringRun.Store(s.inUseCounter.Load())
				}
			}}
			f, lazy := newPinProviderFixture(t, ctx, tc.cold, nil, hook)
			target = func() *Shard { return f.hot }
			if tc.cold {
				target = lazy.currentShard
			}
			recorder := &pinRecorder{}

			f.runUnit(ctx, recorder)

			completed, failures := recorder.result()
			require.Empty(t, failures)
			require.True(t, completed)
			require.EqualValues(t, 1, pinsDuringRun.Load(), "the unit's shard was not pinned against shutdown")
			require.EqualValues(t, 0, target().inUseCounter.Load(), "the unit did not release its pin")
		})
	}
}

// yieldedPins reports whether s holds a yielding pin and every one of them was cancelled.
func yieldedPins(s *Shard) bool {
	s.yieldMu.Lock()
	defer s.yieldMu.Unlock()
	for pin := range s.yielders {
		if pin.ctx.Err() == nil {
			return false
		}
	}
	return len(s.yielders) > 0
}

func TestReindexUnitYieldsToTeardown(t *testing.T) {
	teardowns := []struct {
		name     string
		teardown func(ctx context.Context, f *pinProviderFixture) error
	}{
		{
			name: "unload",
			teardown: func(ctx context.Context, f *pinProviderFixture) error {
				outcome, err := f.idx.UnloadLocalShard(ctx, f.hot.Name())
				if err == nil && outcome != ShardUnloadOutcomeUnloaded {
					return fmt.Errorf("outcome %q", outcome)
				}
				return err
			},
		},
		{
			name: "drop",
			teardown: func(_ context.Context, f *pinProviderFixture) error {
				return f.idx.dropShards([]string{f.hot.Name()})
			},
		},
		{
			name: "tenant deactivation through the schema apply path",
			teardown: func(ctx context.Context, f *pinProviderFixture) error {
				m := &Migrator{db: f.provider.db, logger: f.idx.logger}
				return m.UpdateTenants(ctx, f.idx.getClass(), []*schemaUC.UpdateTenantPayload{
					{Name: f.hot.Name(), Status: models.TenantActivityStatusCOLD},
				}, false)
			},
		},
	}
	moments := []struct {
		name string
		// objects beyond one progress report put the teardown inside a chunk,
		// where the run holds its bucket pins; chunks last up to 10 minutes.
		objects int
		bound   time.Duration
	}{
		{name: "before the first object", bound: time.Second},
		{name: "mid-chunk", objects: 1100, bound: 5 * time.Second},
	}
	for _, tc := range teardowns {
		for _, at := range moments {
			t.Run(tc.name+" "+at.name, func(t *testing.T) {
				ctx := testCtx()
				var (
					f         *pinProviderFixture
					once      sync.Once
					tookCh    = make(chan time.Duration, 1)
					errCh     = make(chan error, 1)
					startTear = func() {
						enterrors.GoWrapper(func() {
							started := time.Now()
							err := tc.teardown(ctx, f)
							tookCh <- time.Since(started)
							errCh <- err
						}, f.idx.logger)
						assert.Eventually(t, func() bool { return yieldedPins(f.hot) },
							at.bound, time.Millisecond, "the teardown did not cancel the unit's pin")
					}
				)
				recorder := &pinRecorder{}
				hook := onLogMessage{msg: "reindexing", fn: func() {
					if at.objects == 0 {
						once.Do(startTear)
					}
				}}
				f, _ = newPinProviderFixture(t, ctx, false, nil, hook)
				if at.objects > 0 {
					for range at.objects {
						require.NoError(t, f.hot.PutObject(ctx, testObject(f.idx.Config.ClassName.String())))
					}
					require.NoError(t, f.hot.Store().FlushMemtables(ctx))
					recorder.onProgress = func(_ context.Context, progress float32) error {
						if progress > 0 {
							once.Do(startTear)
						}
						return nil
					}
				}

				f.runUnit(ctx, recorder)

				require.NoError(t, <-errCh)
				took := <-tookCh
				t.Logf("teardown took %s", took)
				require.Less(t, took, at.bound, "the running unit delayed the teardown")
				completed, failures := recorder.result()
				require.False(t, completed, "a unit whose shard left mid-run must not report completion")
				require.Len(t, failures, 1)
				require.Contains(t, failures[0], "unloaded or dropped mid-run")
				require.Nil(t, f.idx.shards.Load(f.hot.Name()))
				require.True(t, f.hot.shutOrDropped())
			})
		}
	}
}

func TestReindexUnitYieldsDuringItsFirstProgressReport(t *testing.T) {
	ctx := testCtx()
	f, _ := newPinProviderFixture(t, ctx, false, nil)
	unloaded := make(chan error, 1)
	recorder := &pinRecorder{onProgress: func(runCtx context.Context, _ float32) error {
		enterrors.GoWrapper(func() {
			_, err := f.idx.UnloadLocalShard(ctx, f.hot.Name())
			unloaded <- err
		}, f.idx.logger)
		<-runCtx.Done()
		return runCtx.Err()
	}}

	f.runUnit(ctx, recorder)

	completed, failures := recorder.result()
	require.False(t, completed)
	require.Empty(t, failures, "an unclaimed unit retries on the next tick rather than failing")
	require.NoError(t, <-unloaded)
	require.True(t, f.hot.shutOrDropped())
}

func TestReindexUnitHoldsColdShardAgainstUnload(t *testing.T) {
	ctx := testCtx()
	var f *pinProviderFixture
	unloaded := make(chan error, 1)
	probe := func() {
		enterrors.GoWrapper(func() {
			_, err := f.idx.UnloadLocalShard(ctx, pinSweepCold)
			unloaded <- err
		}, f.idx.logger)
		assert.Never(t, func() bool { return f.idx.shards.Load(pinSweepCold) == nil },
			200*time.Millisecond, time.Millisecond, "an unload took the shard out of the map mid-load")
	}
	f, lazy := newPinProviderFixture(t, ctx, true, loadProbeAllocChecker{memwatch.NewDummyMonitor(), probe})
	recorder := &pinRecorder{}

	f.runUnit(ctx, recorder)

	completed, failures := recorder.result()
	require.False(t, completed)
	require.Len(t, failures, 1)
	require.NoError(t, <-unloaded)
	require.Nil(t, f.idx.shards.Load(pinSweepCold))
	require.False(t, lazy.isLoaded())
}

func TestReindexPhasePinsItsShard(t *testing.T) {
	tests := []struct {
		name string
		cold bool
	}{
		{name: "loaded shard"},
		{name: "cold shard", cold: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			f, lazy := newPinProviderFixture(t, ctx, tc.cold, nil)
			target := func() *Shard { return f.hot }
			if tc.cold {
				target = lazy.currentShard
			}
			tasks, err := f.provider.createReindexTasks(f.task.TaskDescriptor, pinUnit, f.payload)
			require.NoError(t, err)
			f.provider.cacheReindexTasks(f.task.TaskDescriptor, pinUnit, tasks)
			pins := int64(-1)

			err = f.provider.runPerUnitPhase(f.task, f.payload, []string{pinUnit}, f.idx, f.provider.logger,
				"test", false, func(context.Context, string, ShardLike, []*ShardReindexTaskGeneric, bool) phaseResult {
					if s := target(); s != nil {
						pins = s.inUseCounter.Load()
					}
					return phaseResult{}
				})

			require.NoError(t, err)
			require.EqualValues(t, 1, pins, "the phase's shard was not pinned against shutdown")
			require.EqualValues(t, 0, target().inUseCounter.Load(), "the phase did not release its pin")
		})
	}
}

func TestReindexPhaseReleasesItsPinWhenItDoesNotRun(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(t *testing.T, s *Shard)
		wantErr string
	}{
		{name: "no in-flight record on the shard"},
		{
			name: "unreadable migration records",
			arrange: func(t *testing.T, s *Shard) {
				migrations := filepath.Join(s.pathLSM(), ".migrations")
				require.NoError(t, os.RemoveAll(migrations))
				require.NoError(t, os.WriteFile(migrations, []byte("not a directory"), 0o600))
				require.Error(t, s.migrationRecords.Load())
			},
			wantErr: "could not be read",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			f, _ := newPinProviderFixture(t, ctx, false, nil)
			if tc.arrange != nil {
				tc.arrange(t, f.hot)
			}

			err := f.provider.runPerUnitPhase(f.task, f.payload, []string{pinUnit}, f.idx, f.provider.logger,
				"test", false, func(context.Context, string, ShardLike, []*ShardReindexTaskGeneric, bool) phaseResult {
					require.Fail(t, "the phase must not run")
					return phaseResult{}
				})

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, 0, f.hot.inUseCounter.Load(), "the phase left its pin behind")
		})
	}
}

// A swap or prep phase renames dirs and moves sentinels; interrupting it on a
// shutdown request would leave its half-shut buckets claimed until restart,
// so it finishes first and the unload completes on its release.
func TestReindexPhaseFinishesThroughAShutdownRequest(t *testing.T) {
	ctx := testCtx()
	f, _ := newPinProviderFixture(t, ctx, false, nil)
	tasks, err := f.provider.createReindexTasks(f.task.TaskDescriptor, pinUnit, f.payload)
	require.NoError(t, err)
	f.provider.cacheReindexTasks(f.task.TaskDescriptor, pinUnit, tasks)
	type unload struct {
		outcome ShardUnloadOutcome
		err     error
	}
	unloaded := make(chan unload, 1)

	err = f.provider.runPerUnitPhase(f.task, f.payload, []string{pinUnit}, f.idx, f.provider.logger,
		"test", false, func(ctx context.Context, _ string, _ ShardLike, _ []*ShardReindexTaskGeneric, _ bool) phaseResult {
			enterrors.GoWrapper(func() {
				outcome, err := f.idx.UnloadLocalShard(context.Background(), f.hot.Name())
				unloaded <- unload{outcome, err}
			}, f.idx.logger)
			require.Eventually(t, f.hot.shutdownRequested.Load, 5*time.Second, time.Millisecond)
			select {
			case <-ctx.Done():
				return phaseResult{Errs: []string{"phase interrupted by a shutdown request: " + context.Cause(ctx).Error()}}
			case <-time.After(300 * time.Millisecond):
			}
			return phaseResult{}
		})

	require.NoError(t, err)
	u := <-unloaded
	if u.err != nil {
		require.Equal(t, ShardUnloadOutcomeRefusedInUse, u.outcome)
	}
	require.Eventually(t, func() bool { return shardKnownShut(f.hot) }, 5*time.Second, 10*time.Millisecond,
		"the unload completes once the phase releases its pin")
}

func TestReindexPhaseYieldsToDrop(t *testing.T) {
	ctx := testCtx()
	f, _ := newPinProviderFixture(t, ctx, false, nil)
	tasks, err := f.provider.createReindexTasks(f.task.TaskDescriptor, pinUnit, f.payload)
	require.NoError(t, err)
	f.provider.cacheReindexTasks(f.task.TaskDescriptor, pinUnit, tasks)
	tookCh := make(chan time.Duration, 1)

	err = f.provider.runPerUnitPhase(f.task, f.payload, []string{pinUnit}, f.idx, f.provider.logger,
		"test", false, func(ctx context.Context, _ string, _ ShardLike, _ []*ShardReindexTaskGeneric, _ bool) phaseResult {
			enterrors.GoWrapper(func() {
				started := time.Now()
				assert.NoError(t, f.hot.drainRefsForDrop())
				tookCh <- time.Since(started)
			}, f.idx.logger)
			<-ctx.Done()
			return phaseResult{Errs: []string{ctx.Err().Error()}, Transient: true}
		})

	require.ErrorContains(t, err, "dropped mid-phase")
	require.NotErrorIs(t, err, context.Canceled, "a yielded phase must fail, not read as a shutdown to resume from")
	require.Less(t, <-tookCh, time.Second, "the running phase delayed the drop")
}
