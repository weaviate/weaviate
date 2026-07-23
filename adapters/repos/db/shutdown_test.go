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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	esync "github.com/weaviate/weaviate/entities/sync"
)

// TestIndexStopCycleManagers pins that every cycle manager stops even when another
// one does not, and that Index.drop can still recognize a context error.
func TestIndexStopCycleManagers(t *testing.T) {
	tests := []struct {
		name string
		// stallCompactions keeps both compaction cycles from reporting a stop
		stallCompactions bool
		// refuseFlush makes the flush cycle report that it kept running
		refuseFlush         bool
		cancelCtx           bool
		separateCompactions bool
		wantErrContains     []string
	}{
		{name: "every cycle stops"},
		{
			name:            "a cycle that refuses to stop is reported",
			refuseFlush:     true,
			wantErrContains: []string{"drop: stop flush cycle: cycle kept running"},
		},
		{
			name:             "a cycle that does not finish stopping is reported once ctx expires",
			stallCompactions: true,
			cancelCtx:        true,
			wantErrContains: []string{
				"drop: stop compaction cycle",
				"drop: stop auxiliary compaction cycle",
			},
		},
		{
			name:                "separate compactions are named by the data they cover",
			stallCompactions:    true,
			cancelCtx:           true,
			separateCompactions: true,
			wantErrContains: []string{
				"drop: stop non objects compaction cycle",
				"drop: stop objects compaction cycle",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newShutdownTestIndex(t, nil)
			idx.Config.SeparateObjectsCompactions = tt.separateCompactions
			if tt.stallCompactions {
				idx.cycleCallbacks.compactionCycle = stallingCycle(idx.cycleCallbacks.compactionCycle)
				idx.cycleCallbacks.compactionAuxCycle = stallingCycle(idx.cycleCallbacks.compactionAuxCycle)
			}
			if tt.refuseFlush {
				idx.cycleCallbacks.flushCycle = refusingCycle(idx.cycleCallbacks.flushCycle)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancelCtx {
				cancel()
			}

			err := idx.stopCycleManagers(ctx, "drop")

			if len(tt.wantErrContains) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, want := range tt.wantErrContains {
					require.Contains(t, err.Error(), want)
				}
			}
			if tt.cancelCtx {
				// Index.drop tolerates context errors, so joining must keep them matchable
				require.ErrorIs(t, err, context.Canceled)
			}

			requireCyclesStopped(t, idx.cycleCallbacks)
		})
	}
}

// stubCycle takes over the stop result of the cycle it wraps. The wrapped cycle
// is never asked to stop, so it keeps running until the test cleanup stops it.
type stubCycle struct {
	cyclemanager.CycleManager
	stopResult chan bool
}

func (c stubCycle) Stop(context.Context) chan bool {
	return c.stopResult
}

// stallingCycle never reports a stop result.
func stallingCycle(cycle cyclemanager.CycleManager) stubCycle {
	return stubCycle{CycleManager: cycle, stopResult: make(chan bool)}
}

// refusingCycle reports that it kept running.
func refusingCycle(cycle cyclemanager.CycleManager) stubCycle {
	stopResult := make(chan bool, 1)
	stopResult <- false
	close(stopResult)
	return stubCycle{CycleManager: cycle, stopResult: stopResult}
}

// requireCyclesStopped asserts that every cycle stopped. Stopping is asynchronous,
// so an expired context can return before the cycles are done. Stubbed cycles are
// skipped, as their stop never reaches the real cycle behind them.
func requireCyclesStopped(t *testing.T, cc *indexCycleCallbacks) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, cycle := range testCycles(cc) {
			if _, stubbed := cycle.(stubCycle); stubbed {
				continue
			}
			if cycle.Running() {
				return false
			}
		}
		return true
	}, time.Second, 10*time.Millisecond, "cycle managers left running")
}

// newShutdownTestIndex builds an index with one mock shard per entry of shardErrs,
// each returning its entry's error from Shutdown, and with all cycle managers running.
func newShutdownTestIndex(t *testing.T, shardErrs map[string]error) *Index {
	t.Helper()
	logger, _ := test.NewNullLogger()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	idx := &Index{
		logger:         logger,
		shards:         shardMap{},
		backupLock:     esync.NewKeyRWLocker(),
		closingCtx:     ctx,
		closingCancel:  cancel,
		cycleCallbacks: newTestCycleCallbacks(logger),
	}
	for _, cycle := range testCycles(idx.cycleCallbacks) {
		cycle.Start()
	}
	t.Cleanup(func() {
		// a cycle the index left running would otherwise outlive the test
		for _, cycle := range testCycles(idx.cycleCallbacks) {
			require.NoError(t, cycle.StopAndWait(context.Background()))
		}
	})

	for name, err := range shardErrs {
		shard := NewMockShardLike(t)
		shard.EXPECT().Name().Return(name).Maybe()
		shard.EXPECT().Shutdown(mock.Anything).Return(err).Once()
		idx.shards.Store(name, shard)
	}
	return idx
}

func newTestCycleCallbacks(logger logrus.FieldLogger) *indexCycleCallbacks {
	newCycle := func(name string) cyclemanager.CycleManager {
		return cyclemanager.NewManager(name, cyclemanager.NewNoopTicker(),
			func(cyclemanager.ShouldAbortCallback) bool { return false }, logger)
	}
	return &indexCycleCallbacks{
		compactionCycle:               newCycle("compaction"),
		compactionAuxCycle:            newCycle("compaction-aux"),
		flushCycle:                    newCycle("flush"),
		vectorCommitLoggerCycle:       newCycle("vector-commit-logger"),
		vectorTombstoneCleanupCycle:   newCycle("vector-tombstone-cleanup"),
		geoPropsCommitLoggerCycle:     newCycle("geo-commit-logger"),
		geoPropsTombstoneCleanupCycle: newCycle("geo-tombstone-cleanup"),
	}
}

func testCycles(cc *indexCycleCallbacks) []cyclemanager.CycleManager {
	return []cyclemanager.CycleManager{
		cc.compactionCycle,
		cc.compactionAuxCycle,
		cc.flushCycle,
		cc.vectorCommitLoggerCycle,
		cc.vectorTombstoneCleanupCycle,
		cc.geoPropsCommitLoggerCycle,
		cc.geoPropsTombstoneCleanupCycle,
	}
}

func TestIndexShutdownStopsCycleManagersDespiteShardFailure(t *testing.T) {
	errInUse := errors.New("still in use")
	errDiskGone := errors.New("disk gone")

	tests := []struct {
		name      string
		shardErrs map[string]error
		// a compaction cycle that never reports a stop fails once ctx expires
		stallCompaction bool
		cancelCtx       bool
		wantErrContains []string
		wantErrIs       []error
	}{
		{
			name:      "no shard fails",
			shardErrs: map[string]error{"shard1": nil, "shard2": nil},
		},
		{
			name:      "already shut down shard is not a failure",
			shardErrs: map[string]error{"shard1": errAlreadyShutdown, "shard2": nil},
		},
		{
			name:            "one of two shards fails",
			shardErrs:       map[string]error{"shard1": errInUse, "shard2": nil},
			wantErrContains: []string{`shutdown shard "shard1"`, "still in use"},
			wantErrIs:       []error{errInUse},
		},
		{
			name:            "both shards fail",
			shardErrs:       map[string]error{"shard1": errInUse, "shard2": errDiskGone},
			wantErrContains: []string{`shutdown shard "shard1"`, `shutdown shard "shard2"`, "disk gone"},
			wantErrIs:       []error{errInUse, errDiskGone},
		},
		{
			name:            "shard failure and cycle manager failure are both reported",
			shardErrs:       map[string]error{"shard1": errInUse},
			stallCompaction: true,
			cancelCtx:       true,
			wantErrContains: []string{
				`shutdown shard "shard1"`,
				"shutdown: stop compaction cycle",
				context.Canceled.Error(),
			},
			wantErrIs: []error{errInUse, context.Canceled},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newShutdownTestIndex(t, tt.shardErrs)
			if tt.stallCompaction {
				idx.cycleCallbacks.compactionCycle = stallingCycle(idx.cycleCallbacks.compactionCycle)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancelCtx {
				cancel()
			}

			err := idx.Shutdown(ctx)

			if len(tt.wantErrContains) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, want := range tt.wantErrContains {
					require.Contains(t, err.Error(), want)
				}
			}
			for _, want := range tt.wantErrIs {
				require.ErrorIs(t, err, want)
			}

			requireCyclesStopped(t, idx.cycleCallbacks)
		})
	}
}

func TestDBShutdownRunsEveryIndexAndCleanup(t *testing.T) {
	type indexSpec struct {
		name          string
		shardErr      error
		alreadyClosed bool
	}

	errInUse := errors.New("still in use")
	errDiskGone := errors.New("disk gone")

	tests := []struct {
		name            string
		indices         []indexSpec
		wantErrContains []string
		wantErrIs       []error
	}{
		{
			name:    "no index fails",
			indices: []indexSpec{{name: "Alpha"}, {name: "Beta"}},
		},
		{
			name:    "already shut down index is not a failure",
			indices: []indexSpec{{name: "Alpha", alreadyClosed: true}, {name: "Beta"}},
		},
		{
			name:            "one of two indices fails",
			indices:         []indexSpec{{name: "Alpha", shardErr: errInUse}, {name: "Beta"}},
			wantErrContains: []string{`shutdown index "Alpha"`, "still in use"},
			wantErrIs:       []error{errInUse},
		},
		{
			name: "every index fails",
			indices: []indexSpec{
				{name: "Alpha", shardErr: errInUse},
				{name: "Beta", shardErr: errDiskGone},
			},
			wantErrContains: []string{`shutdown index "Alpha"`, `shutdown index "Beta"`, "disk gone"},
			wantErrIs:       []error{errInUse, errDiskGone},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()

			checkpoints, err := indexcheckpoint.New(t.TempDir(), logger)
			require.NoError(t, err)

			db := &DB{
				logger:                    logger,
				shutdown:                  make(chan struct{}, 1),
				bitmapBufPoolClose:        func() {},
				AsyncIndexingEnabled:      true,
				asyncReplicationScheduler: newSchedulerForUnitTest(t),
				indexCheckpoints:          checkpoints,
				indices:                   map[string]*Index{},
			}

			for _, spec := range tt.indices {
				// a closed index has no shard to shut down, so it gets no mock shard
				var idx *Index
				if spec.alreadyClosed {
					idx = newShutdownTestIndex(t, nil)
					idx.closed = true
				} else {
					idx = newShutdownTestIndex(t, map[string]error{"shard1": spec.shardErr})
				}
				db.indices[spec.name] = idx
			}

			err = db.Shutdown(context.Background())

			if len(tt.wantErrContains) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, want := range tt.wantErrContains {
					require.Contains(t, err.Error(), want)
				}
			}
			for _, want := range tt.wantErrIs {
				require.ErrorIs(t, err, want)
			}

			// closing the checkpoint store is the last statement after the index
			// loop, so a closed store proves the whole post-loop cleanup ran
			_, _, err = checkpoints.Get("shard1", "")
			require.Error(t, err)
		})
	}
}
