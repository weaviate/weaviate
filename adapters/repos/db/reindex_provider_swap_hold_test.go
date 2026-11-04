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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	logIterationDone = "reindex iteration complete"
	logPrepDone      = "runtime prepare: all props merged"
	logSwapWindow    = "runtime swap: all props in-memory swapped"
	logTaskDone      = "runtime swap: migration complete (ingest→main rename deferred to next restart)"
)

// requestShutdown returns once the shard was asked to unload and refused while the run holds its pin.
func requestShutdown(t *testing.T, idx *Index, shardName string) {
	reqCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	outcome, err := idx.UnloadLocalShard(reqCtx, shardName)
	assert.Error(t, err)
	assert.Equal(t, ShardUnloadOutcomeRefusedInUse, outcome)
}

func unitShard(f *pinProviderFixture, lazy *LazyLoadShard) *Shard {
	if lazy != nil {
		return lazy.currentShard()
	}
	return f.hot
}

func TestFormatOnlyMigrationFinishesItsPrepAndSwapThroughAShutdownRequest(t *testing.T) {
	windows := []struct {
		name string
		log  string
	}{
		{name: "prep", log: logPrepDone},
		{name: "swap with the old bucket out of the store", log: logSwapWindow},
	}
	shards := []struct {
		name string
		cold bool
	}{
		{name: "loaded shard"},
		{name: "cold shard loaded by the unit", cold: true},
	}
	for _, w := range windows {
		for _, s := range shards {
			t.Run(w.name+" "+s.name, func(t *testing.T) {
				ctx := testCtx()
				var (
					f       *pinProviderFixture
					lazy    *LazyLoadShard
					once    sync.Once
					shard   *Shard
					running bool
				)
				name := func() string { return f.payload.UnitToShard[pinUnit] }
				hook := onLogMessage{msg: w.log, fn: func() {
					once.Do(func() {
						shard = unitShard(f, lazy)
						running = true
						requestShutdown(t, f.idx, name())
					})
				}}
				f, lazy = newPinProviderFixture(t, ctx, s.cold, nil, hook)
				recorder := &pinRecorder{}

				f.runUnit(ctx, recorder)

				require.True(t, running, "the request never landed in the window")
				completed, failures := recorder.result()
				require.Empty(t, failures)
				require.True(t, completed)
				require.True(t, shardKnownShut(shard), "the release must complete the shutdown")

				reloaded, release, err := f.idx.getOrInitShard(ctx, name())
				require.NoError(t, err, "the migration left a bucket claim behind")
				require.NotNil(t, reloaded.Store().Bucket(helpers.BucketFromPropNameLSM(pinSweepProp)))
				release()
				require.NoError(t, reloaded.Shutdown(ctx))
			})
		}
	}
}

func TestFormatOnlyMigrationYieldsToARequestAheadOfItsPrep(t *testing.T) {
	ctx := testCtx()
	var (
		f    *pinProviderFixture
		once sync.Once
	)
	var swapStarted bool
	f, _ = newPinProviderFixture(t, ctx, false, nil,
		onLogMessage{msg: logIterationDone, fn: func() {
			once.Do(func() { requestShutdown(t, f.idx, f.hot.Name()) })
		}},
		onLogMessage{msg: logSwapWindow, fn: func() { swapStarted = true }},
	)
	recorder := &pinRecorder{}

	f.runUnit(ctx, recorder)

	completed, failures := recorder.result()
	require.False(t, completed)
	require.Len(t, failures, 1)
	require.Contains(t, failures[0], "unloaded or dropped mid-run")
	require.False(t, swapStarted, "a request ahead of the prep must stop the run before it")
	require.True(t, shardKnownShut(f.hot))

	reloaded, release, err := f.idx.getOrInitShard(ctx, f.hot.Name())
	require.NoError(t, err)
	require.NotNil(t, reloaded.Store().Bucket(helpers.BucketFromPropNameLSM(pinSweepProp)))
	release()
	require.NoError(t, reloaded.Shutdown(ctx))
}

func TestFormatOnlyMigrationDropCancelsItsSwap(t *testing.T) {
	ctx := testCtx()
	var (
		f    *pinProviderFixture
		once sync.Once
	)
	dropped := make(chan error, 1)
	tookCh := make(chan time.Duration, 1)
	f, _ = newPinProviderFixture(t, ctx, false, nil, onLogMessage{msg: logSwapWindow, fn: func() {
		once.Do(func() {
			enterrors.GoWrapper(func() {
				started := time.Now()
				err := f.hot.drainRefsForDrop()
				tookCh <- time.Since(started)
				dropped <- err
			}, f.idx.logger)
			assert.Eventually(t, func() bool { return yieldedPins(f.hot) }, 5*time.Second, time.Millisecond,
				"the drop did not cancel the swap")
		})
	}})
	recorder := &pinRecorder{}

	f.runUnit(ctx, recorder)

	completed, failures := recorder.result()
	require.False(t, completed)
	require.Len(t, failures, 1)
	require.Contains(t, failures[0], "unloaded or dropped mid-run")
	require.NoError(t, <-dropped)
	require.Less(t, <-tookCh, time.Second, "the swap delayed the drop")
	f.hot.dropRequested.Store(false)
}

// Each task stands in for one of a multi-task unit; no format-only migration type builds more than one today.
func TestFormatOnlyMultiTaskUnitRestoresYielding(t *testing.T) {
	moments := []struct {
		name string
		// inSecondIteration asks for the shutdown once the second task iterates, instead of during the first swap.
		inSecondIteration bool
		wantSecondRan     bool
	}{
		{name: "a request during the first swap stops the second task"},
		{name: "the second task iterates under a yielding pin again", inSecondIteration: true, wantSecondRan: true},
	}
	for _, m := range moments {
		t.Run(m.name, func(t *testing.T) {
			ctx := testCtx()
			var (
				f              *pinProviderFixture
				firstSwapDone  bool
				secondIterated bool
				once           sync.Once
			)
			f, _ = newPinProviderFixture(t, ctx, false, nil,
				onLogMessage{msg: logSwapWindow, fn: func() {
					if !m.inSecondIteration {
						once.Do(func() { requestShutdown(t, f.idx, f.hot.Name()) })
					}
				}},
				onLogMessage{msg: logTaskDone, fn: func() { firstSwapDone = true }},
				onLogMessage{msg: "reindexing", fn: func() {
					if firstSwapDone {
						secondIterated = true
						if m.inSecondIteration {
							once.Do(func() { requestShutdown(t, f.idx, f.hot.Name()) })
						}
					}
				}},
			)
			shard, runCtx, hold, release, err := pinShardByName(ctx, f.idx, f.hot.Name(), false)
			require.NoError(t, err)
			defer release()

			var firstTasks, secondTasks []*ShardReindexTaskGeneric
			firstTasks, err = f.provider.createReindexTasks(f.task.TaskDescriptor, pinUnit, f.payload)
			require.NoError(t, err)
			secondTasks, err = f.provider.createReindexTasks(distributedtask.TaskDescriptor{ID: "T_pin_second", Version: 2},
				pinUnit, f.payload)
			require.NoError(t, err)

			require.NoError(t, runFormatOnlyTask(runCtx, hold, firstTasks[0], shard), "the swap must finish")
			if !m.inSecondIteration {
				require.ErrorIs(t, context.Cause(runCtx), errShardYielded, "the restore must act on the request")
			} else {
				require.NoError(t, runCtx.Err())
			}

			err = runFormatOnlyTask(runCtx, hold, secondTasks[0], shard)

			require.Equal(t, m.wantSecondRan, secondIterated)
			require.Error(t, err)
			require.True(t, shardYielded(ctx, runCtx, f.idx), "got %v", err)
		})
	}
}

func TestPinHold(t *testing.T) {
	tests := []struct {
		name     string
		dropOnly bool
		// beforeHold and duringHold run with the pin yielding and held; each may request a teardown.
		beforeHold   func(s *Shard)
		duringHold   func(s *Shard)
		cancelsHeld  bool
		cancelsAtEnd bool
	}{
		{name: "no request leaves the pin live"},
		{
			name:         "a shutdown requested during the hold cancels only at restore",
			duringHold:   func(s *Shard) { s.requestShutdownFlag() },
			cancelsAtEnd: true,
		},
		{
			name:         "a drop during the hold cancels at once",
			duringHold:   func(s *Shard) { s.requestDropFlag() },
			cancelsHeld:  true,
			cancelsAtEnd: true,
		},
		{
			name:         "a shutdown that beat the hold stays cancelled",
			beforeHold:   func(s *Shard) { s.requestShutdownFlag() },
			cancelsHeld:  true,
			cancelsAtEnd: true,
		},
		{
			name: "a request that set its flag but has not yielded yet is seen by the hold",
			beforeHold: func(s *Shard) {
				s.shutdownRequested.Store(true)
			},
			cancelsHeld:  true,
			cancelsAtEnd: true,
		},
		{
			name:       "a drop-only pin stays drop-only after restore",
			dropOnly:   true,
			duringHold: func(s *Shard) { s.requestShutdownFlag() },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Shard{}
			yield := s.yieldOnTeardown
			if tt.dropOnly {
				yield = s.yieldOnDrop
			}
			pin, release := yield(context.Background(), func() {})
			defer release()

			if tt.beforeHold != nil {
				tt.beforeHold(s)
			}
			restore := s.holdThroughShutdown(pin)
			if tt.duringHold != nil {
				tt.duringHold(s)
			}
			require.Equal(t, tt.cancelsHeld, pin.ctx.Err() != nil)

			restore()
			require.Equal(t, tt.cancelsAtEnd, pin.ctx.Err() != nil)
			if tt.cancelsAtEnd {
				require.ErrorIs(t, context.Cause(pin.ctx), errShardYielded)
			}
		})
	}

	t.Run("restored to yielding, a later shutdown cancels it", func(t *testing.T) {
		s := &Shard{}
		pin, release := s.yieldOnTeardown(context.Background(), func() {})
		defer release()

		s.holdThroughShutdown(pin)()
		require.NoError(t, pin.ctx.Err())

		s.requestShutdownFlag()
		require.ErrorIs(t, context.Cause(pin.ctx), errShardYielded)
	})
}

func (s *Shard) requestShutdownFlag() {
	s.shutdownRequested.Store(true)
	s.yieldPins(false)
}

func (s *Shard) requestDropFlag() {
	s.dropRequested.Store(true)
	s.yieldPins(true)
}
