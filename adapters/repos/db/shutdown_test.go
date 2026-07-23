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

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	esync "github.com/weaviate/weaviate/entities/sync"
)

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
		// a cancelled context makes every cycle manager fail to stop
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
			name:      "shard failure and cycle manager failure are both reported",
			shardErrs: map[string]error{"shard1": errInUse},
			cancelCtx: true,
			wantErrContains: []string{
				`shutdown shard "shard1"`,
				"shutdown: stop objects compaction cycle",
				context.Canceled.Error(),
			},
			wantErrIs: []error{errInUse, context.Canceled},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newShutdownTestIndex(t, tt.shardErrs)

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

			if !tt.cancelCtx {
				for _, cycle := range testCycles(idx.cycleCallbacks) {
					require.False(t, cycle.Running())
				}
			}
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
