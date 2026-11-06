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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const logBucketLoaded = "bucket loaded"

func TestWithBucketTeardownCtx(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "a live caller keeps its own wait, with no deadline added",
			run: func(t *testing.T) {
				require.NoError(t, withBucketTeardownCtx(context.Background(), func(ctx context.Context) error {
					_, hasDeadline := ctx.Deadline()
					require.False(t, hasDeadline)
					time.Sleep(10 * bucketTeardownGrace)
					return ctx.Err()
				}))
			},
		},
		{
			name: "a cancellation ends the teardown only after the grace",
			run: func(t *testing.T) {
				caller, cancel := context.WithCancel(context.Background())
				cancel()
				require.NoError(t, withBucketTeardownCtx(caller, func(ctx context.Context) error {
					time.Sleep(bucketTeardownGrace - time.Second)
					require.NoError(t, ctx.Err())
					time.Sleep(2 * time.Second)
					require.ErrorIs(t, ctx.Err(), context.Canceled)
					return nil
				}))
			},
		},
		{
			name: "a cancellation landing mid-teardown starts the grace then",
			run: func(t *testing.T) {
				caller, cancel := context.WithCancel(context.Background())
				defer cancel()
				require.NoError(t, withBucketTeardownCtx(caller, func(ctx context.Context) error {
					time.Sleep(10 * bucketTeardownGrace)
					cancel()
					time.Sleep(bucketTeardownGrace - time.Second)
					require.NoError(t, ctx.Err())
					time.Sleep(2 * time.Second)
					require.Error(t, ctx.Err())
					return nil
				}))
			},
		},
		{
			name: "the caller's deadline still bounds it",
			run: func(t *testing.T) {
				caller, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.NoError(t, withBucketTeardownCtx(caller, func(ctx context.Context) error {
					time.Sleep(2 * time.Second)
					require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
					return nil
				}))
			},
		},
		{
			name: "the teardown's error is returned and nothing outlives it",
			run: func(t *testing.T) {
				caller, cancel := context.WithCancel(context.Background())
				cancel()
				boom := errors.New("boom")
				require.ErrorIs(t, withBucketTeardownCtx(caller, func(context.Context) error { return boom }), boom)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, tt.run)
		})
	}
}

func (f *pinProviderFixture) newTask(t *testing.T) *ShardReindexTaskGeneric {
	t.Helper()
	tasks, err := f.provider.createReindexTasks(f.task.TaskDescriptor, pinUnit, f.payload)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	return tasks[0]
}

// stageUnflippedSwap leaves the flip decided and the pointers unflipped, as a failed swap does.
func (f *pinProviderFixture) stageUnflippedSwap(t *testing.T, ctx context.Context) *ShardReindexTaskGeneric {
	t.Helper()
	task := f.newTask(t)
	task.processOneSwapPropFn = func(context.Context, *lsmkv.Store, int, string) (*lsmkv.Bucket, error) {
		return nil, errors.New("injected swap failure")
	}
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, f.hot))
	require.NoError(t, task.RunPrepareOnShard(ctx, f.hot))
	require.Error(t, task.RunSwapOnShard(ctx, f.hot))
	return task
}

func (f *pinProviderFixture) requireShardReopens(t *testing.T, ctx context.Context) {
	t.Helper()
	reloaded, release, err := f.idx.getOrInitShard(ctx, f.hot.Name())
	require.NoError(t, err, "a bucket's teardown was abandoned, so its directory stays claimed until a restart")
	require.NotNil(t, reloaded.Store().Bucket(helpers.BucketFromPropNameLSM(pinSweepProp)))
	release()
	require.NoError(t, reloaded.Shutdown(ctx))
}

func TestShutdownRequestUnderTheYieldingPinFinishesABucketTeardown(t *testing.T) {
	migrations := []struct {
		name      string
		configure func(p *ReindexTaskPayload)
	}{
		{name: "format-only", configure: func(*ReindexTaskPayload) {}},
		{name: "semantic", configure: func(p *ReindexTaskPayload) {
			p.MigrationType = ReindexTypeChangeTokenizationFilterable
			p.TargetTokenization = models.PropertyTokenizationField
		}},
	}
	sites := []struct {
		name     string
		log      string
		deferred bool
	}{
		{name: "runtime swap re-entered by the iteration", log: logSwapWindow},
		{name: "deferred promotion", log: logBucketLoaded, deferred: true},
	}
	for _, m := range migrations {
		for _, site := range sites {
			t.Run(m.name+" "+site.name, func(t *testing.T) {
				ctx := testCtx()
				var (
					f      *pinProviderFixture
					armed  atomic.Bool
					landed atomic.Bool
					once   sync.Once
				)
				f, _ = newPinProviderFixture(t, ctx, false, nil, onLogMessage{msg: site.log, fn: func() {
					if armed.Load() {
						once.Do(func() {
							requestShutdown(t, f.idx, f.hot.Name())
							landed.Store(true)
						})
					}
				}})
				m.configure(f.payload)
				task := f.stageUnflippedSwap(t, ctx)
				if site.deferred {
					f.hot.migrationRecords.DeferPromotions(task.migrationRecordKey(), []string{pinSweepProp})
				}
				armed.Store(true)
				recorder := &pinRecorder{}

				f.runUnit(ctx, recorder)

				require.True(t, landed.Load(), "the request never landed in the window")
				f.requireShardReopens(t, ctx)
				_, failures := recorder.result()
				for _, failure := range failures {
					require.NotContains(t, failure, "shutting down")
				}
			})
		}
	}
}

func TestDropDuringASwapLeavesTheShardNameReusable(t *testing.T) {
	runs := []struct {
		name string
		run  func(t *testing.T, ctx context.Context, f *pinProviderFixture)
	}{
		{
			name: "format-only unit",
			run: func(_ *testing.T, ctx context.Context, f *pinProviderFixture) {
				f.runUnit(ctx, &pinRecorder{})
			},
		},
		{
			name: "semantic swap phase",
			run: func(t *testing.T, ctx context.Context, f *pinProviderFixture) {
				task := f.newTask(t)
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, f.hot))
				require.NoError(t, task.RunPrepareOnShard(ctx, f.hot))
				f.provider.cacheReindexTasks(f.task.TaskDescriptor, pinUnit, []*ShardReindexTaskGeneric{task})
				err := f.provider.runPerUnitPhase(f.task, f.payload, []string{pinUnit}, f.idx, f.provider.logger,
					"test", false, func(ctx context.Context, _ string, shard ShardLike, tasks []*ShardReindexTaskGeneric, _ bool) phaseResult {
						if err := tasks[0].RunSwapOnShard(ctx, shard); err != nil {
							return phaseResult{Errs: []string{err.Error()}}
						}
						return phaseResult{}
					})
				require.ErrorContains(t, err, "dropped mid-phase")
			},
		},
	}
	for _, r := range runs {
		t.Run(r.name, func(t *testing.T) {
			ctx := testCtx()
			var (
				f    *pinProviderFixture
				once sync.Once
			)
			dropped := make(chan error, 1)
			f, _ = newPinProviderFixture(t, ctx, false, nil, onLogMessage{msg: logSwapWindow, fn: func() {
				once.Do(func() {
					enterrors.GoWrapper(func() { dropped <- f.idx.dropShards([]string{f.hot.Name()}) }, f.idx.logger)
					assert.Eventually(t, func() bool { return yieldedPins(f.hot) }, 5*time.Second, time.Millisecond,
						"the drop did not cancel the swap")
				})
			}})

			r.run(t, ctx, f)

			require.NoError(t, <-dropped)
			f.requireShardReopens(t, ctx)
		})
	}
}

func TestCancelledPrepDoesNotAbandonTheReindexBucket(t *testing.T) {
	ctx := testCtx()
	f, _ := newPinProviderFixture(t, ctx, false, nil)
	task := f.newTask(t)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, f.hot))
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	if err := task.RunPrepareOnShard(cancelled, f.hot); err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}

	require.NoError(t, task.RunPrepareOnShard(ctx, f.hot), "the retried prep must reopen the reindex bucket")
	require.NoError(t, task.RunSwapOnShard(ctx, f.hot))
}

func TestCancelledSwapWithNothingDisplacedStillCompletes(t *testing.T) {
	ctx := testCtx()
	f, _ := newPinProviderFixture(t, ctx, false, nil)
	task := f.newTask(t)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, f.hot))
	require.NoError(t, task.RunPrepareOnShard(ctx, f.hot))
	require.NoError(t, task.RunSwapOnShard(ctx, f.hot))
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	require.NoError(t, task.RunSwapOnShard(cancelled, f.hot), "no teardown ran, so nothing here changes what a cancelled retry does")
}

func TestCancelledStagedBucketCloseDoesNotAbandonIt(t *testing.T) {
	ctx := testCtx()
	f, _ := newPinProviderFixture(t, ctx, false, nil)
	task := f.newTask(t)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, f.hot))
	staged := task.ingestBucketName(pinSweepProp)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	closeErr := f.hot.ShutdownStagedBucketsAt(cancelled, []string{staged})

	require.Nil(t, f.hot.store.Bucket(staged))
	require.NoError(t, task.loadIngestBuckets(ctx, f.idx.logger, f.hot, []string{pinSweepProp}, true, true),
		"the staged directory must stay reopenable")
	require.NoError(t, closeErr)
}

// The after-load hook only logs a failure, and an unloader's reload then opens the same directories.
func TestCancelledLoadDoesNotAbandonADisplacedBucketTheReloadTripsOn(t *testing.T) {
	const propName = "title"
	ctx := testCtx()
	className := "CancelledLoad_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, true, false)
	shard := shd.(*Shard)
	for _, obj := range makeConvergenceTestObjects(t, 10, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	task, _ := newEnableFilterableTask(t, idx, className, shard.migrationUnit(), propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	name := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	recovered, err := DiscoverInFlightReindexTasks(idx.Config.RootPath, true, idx.logger, nil)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	loadCtx, cancelLoad := context.WithCancel(ctx)
	defer cancelLoad()
	recoveredTask := recovered[0].Tasks[0]
	register := recoveredTask.registerDoubleWriteCallbacksFn
	var served atomic.Bool
	recoveredTask.registerDoubleWriteCallbacksFn = func(s *Shard, props []string, namer func(string) string) (func(), error) {
		served.Store(true)
		cancelLoad()
		return register(s, props, namer)
	}
	idx.shardReindexer = NewShardReindexerV3FromRecovered(recovered, idx.logger)

	loaded := openShardFromDisk(t, loadCtx, idx, class, name)
	require.True(t, served.Load(), "the load never reached the deferred promotion")
	require.NoError(t, loaded.Shutdown(ctx))

	class.Properties[0].IndexFilterable = boolPtr(true)
	reloaded, err := idx.initShard(ctx, name, class, nil, true, true)
	require.NoError(t, err, "the reload opens the canonical bucket whose teardown the first load abandoned")
	require.NoError(t, reloaded.Shutdown(ctx))
}
