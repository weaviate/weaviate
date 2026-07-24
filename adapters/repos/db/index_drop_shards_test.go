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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
)

func TestIndexDropShards(t *testing.T) {
	const (
		shardName = "shard1"
		// what ShardLike.ID() reports for a loaded shard of class "Abc"
		loadedShardID = "abc_" + shardName
	)

	tests := []struct {
		name string
		// loaded stores a mock shard under shardName, so drop() runs instead of
		// removing the shard directory
		loaded       bool
		shardDropErr error
		// blockRemoval makes the index directory read-only so os.RemoveAll fails
		blockRemoval    bool
		wantErrContains string
		wantShardLog    string
	}{
		{
			name: "unloaded shard is removed from disk",
		},
		{
			name:            "unloaded shard removal failure is reported",
			blockRemoval:    true,
			wantErrContains: "permission denied",
			wantShardLog:    shardName,
		},
		{
			name:   "loaded shard is dropped",
			loaded: true,
		},
		{
			name:            "loaded shard drop failure is reported",
			loaded:          true,
			shardDropErr:    errors.New("drop failed"),
			wantErrContains: "drop failed",
			wantShardLog:    loadedShardID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, hook := newDropTestIndex(t)

			shardDir := shardPath(idx.path(), shardName)
			require.NoError(t, os.MkdirAll(shardDir, 0o755))

			if tt.loaded {
				shard := NewMockShardLike(t)
				shard.EXPECT().ID().Return(loadedShardID).Maybe()
				shard.EXPECT().drop(false).Return(tt.shardDropErr).Once()
				idx.shards.Store(shardName, shard)
			}

			if tt.blockRemoval {
				blockRemovalIn(t, idx.path())
			}

			err := idx.dropShards([]string{shardName})

			for _, entry := range hook.AllEntries() {
				require.NotContains(t, entry.Message, "Recovered from panic")
			}

			if tt.wantErrContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErrContains)
				require.Equal(t, tt.wantShardLog, dropShardLogShard(t, hook))
			}

			require.Nil(t, idx.shards.Load(shardName))
			if !tt.loaded {
				_, err := os.Stat(shardDir)
				require.Equal(t, tt.blockRemoval, err == nil)
			}
		})
	}
}

// TestIndexDropShardsCollectsConcurrentFailures pins that no shard error is lost
// when many shards are dropped in parallel, and that the returned message stays
// bounded. Run with -race.
func TestIndexDropShardsCollectsConcurrentFailures(t *testing.T) {
	const shardCount = 32

	tests := []struct {
		name string
		drop func(t *testing.T, idx *Index, names []string) error
		// appears once per failed shard in the returned error
		wantErrPerShard string
	}{
		{
			name: "dropShards",
			drop: func(t *testing.T, idx *Index, names []string) error {
				blockRemovalIn(t, idx.path())
				return idx.dropShards(names)
			},
			wantErrPerShard: "permission denied",
		},
		{
			name: "dropCloudShards",
			drop: func(t *testing.T, idx *Index, names []string) error {
				cloud := fakeOffloadCloud{deleteErrs: map[string]error{}}
				for _, name := range names {
					cloud.deleteErrs[name] = errors.New("cloud delete failed")
				}
				return idx.dropCloudShards(context.Background(), cloud, names, "node1")
			},
			wantErrPerShard: "cloud delete failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, _ := newDropTestIndex(t)

			names := make([]string, shardCount)
			for i := range names {
				names[i] = fmt.Sprintf("shard%d", i)
				require.NoError(t, os.MkdirAll(shardPath(idx.path(), names[i]), 0o755))
			}

			err := tt.drop(t, idx, names)

			require.Error(t, err)
			reported := strings.Count(err.Error(), tt.wantErrPerShard)
			require.Equal(t, maxReportedErrors, reported)
			require.Contains(t, err.Error(), fmt.Sprintf("(and %d more)", shardCount-reported))
		})
	}
}

func TestIndexDropCloudShards(t *testing.T) {
	tests := []struct {
		name            string
		names           []string
		deleteErrs      map[string]error
		wantErrContains string
	}{
		{
			name:  "no shard to drop",
			names: []string{},
		},
		{
			name:  "every shard is deleted",
			names: []string{"shard1", "shard2", "shard3"},
		},
		{
			name:            "only the failing shard is reported",
			names:           []string{"shard1", "shard2", "shard3"},
			deleteErrs:      map[string]error{"shard2": errors.New("cloud delete failed")},
			wantErrContains: "cloud delete failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, _ := newDropTestIndex(t)

			cloud := fakeOffloadCloud{deleteErrs: tt.deleteErrs}
			err := idx.dropCloudShards(context.Background(), cloud, tt.names, "node1")

			if tt.wantErrContains == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErrContains)
			require.Equal(t, len(tt.deleteErrs), strings.Count(err.Error(), tt.wantErrContains))
		})
	}
}

// TestIndexDropShardsReportsWorkerPanic pins that a panicking drop worker makes
// the call fail. The error group only logs the recovered panic, so a caller that
// trusts the return value would otherwise treat the shard as dropped.
func TestIndexDropShardsReportsWorkerPanic(t *testing.T) {
	const (
		shardName    = "shard1"
		failingShard = "shard2"
	)

	panickingShard := func(t *testing.T) *MockShardLike {
		shard := NewMockShardLike(t)
		shard.EXPECT().drop(false).Run(func(bool) { panic("drop panicked") }).Return(nil).Once()
		return shard
	}
	failingShardMock := func(t *testing.T) *MockShardLike {
		shard := NewMockShardLike(t)
		shard.EXPECT().ID().Return(failingShard).Maybe()
		shard.EXPECT().drop(false).Return(errors.New("shard drop failed")).Once()
		return shard
	}

	tests := []struct {
		name            string
		drop            func(t *testing.T, idx *Index) error
		wantErrContains []string
	}{
		{
			name: "dropShards",
			drop: func(t *testing.T, idx *Index) error {
				idx.shards.Store(shardName, panickingShard(t))
				return idx.dropShards([]string{shardName})
			},
			wantErrContains: []string{"drop panicked"},
		},
		{
			name: "dropCloudShards",
			drop: func(t *testing.T, idx *Index) error {
				cloud := fakeOffloadCloud{panicShard: shardName}
				return idx.dropCloudShards(context.Background(), cloud, []string{shardName}, "node1")
			},
			wantErrContains: []string{"drop panicked"},
		},
		{
			name: "dropShards reports the panic alongside a shard failure",
			drop: func(t *testing.T, idx *Index) error {
				idx.shards.Store(shardName, panickingShard(t))
				idx.shards.Store(failingShard, failingShardMock(t))
				return idx.dropShards([]string{shardName, failingShard})
			},
			wantErrContains: []string{"drop panicked", "shard drop failed"},
		},
		{
			name: "dropCloudShards reports the panic alongside a delete failure",
			drop: func(t *testing.T, idx *Index) error {
				cloud := fakeOffloadCloud{
					panicShard: shardName,
					deleteErrs: map[string]error{failingShard: errors.New("cloud delete failed")},
				}
				return idx.dropCloudShards(context.Background(), cloud, []string{shardName, failingShard}, "node1")
			},
			wantErrContains: []string{"drop panicked", "cloud delete failed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// the integration suite exports this, which would let the panic kill the test
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			idx, _ := newDropTestIndex(t)
			err := tt.drop(t, idx)

			require.Error(t, err)
			for _, want := range tt.wantErrContains {
				require.Contains(t, err.Error(), want)
			}
		})
	}
}

// TestIndexDropReportsFailures pins that dropping a whole index reports every
// failure instead of only logging it, and still tears the index down: the cycle
// managers stop and the index directory is renamed away even when a shard fails.
func TestIndexDropReportsFailures(t *testing.T) {
	const (
		shardName   = "shard1"
		secondShard = "shard2"
		// what ShardLike.ID() reports for a loaded shard of class "Abc"
		loadedShardID = "abc_" + shardName
	)
	errDrop := errors.New("drop failed")

	tests := []struct {
		name string
		// noShards drops an index that has no shard left
		noShards     bool
		shardDropErr error
		// secondShardFails adds a second shard whose drop fails
		secondShardFails bool
		// panicDrop makes the drop worker panic, which the error group only recovers
		panicDrop bool
		// refuseFlush makes the flush cycle report that it kept running
		refuseFlush bool
		// stallCompactions keeps both compaction cycles from reporting a stop, so
		// they only fail once the drop timeout expires
		stallCompactions bool
		// keepFiles stands in for a backup in progress
		keepFiles bool
		// blockRename makes the root read-only so renaming the index fails
		blockRename     bool
		wantErrContains []string
		wantShardLog    string
	}{
		{
			name: "index is dropped",
		},
		{
			name:     "index without shards is dropped",
			noShards: true,
		},
		{
			name:            "shard drop failure is reported",
			shardDropErr:    errDrop,
			wantErrContains: []string{"drop failed"},
			wantShardLog:    loadedShardID,
		},
		{
			name:            "worker panic is reported",
			panicDrop:       true,
			wantErrContains: []string{"drop panicked"},
		},
		{
			name:             "panic and shard failure are both reported",
			panicDrop:        true,
			secondShardFails: true,
			wantErrContains:  []string{"drop panicked", "second drop failed"},
			wantShardLog:     "abc_" + secondShard,
		},
		{
			name:            "cycle that refuses to stop is reported",
			refuseFlush:     true,
			wantErrContains: []string{"drop: stop flush cycle: cycle kept running"},
		},
		{
			name:             "cycle that runs out of time is not a failure",
			stallCompactions: true,
		},
		{
			name:             "cycle failure is reported next to a cycle that ran out of time",
			stallCompactions: true,
			refuseFlush:      true,
			wantErrContains:  []string{"drop: stop flush cycle: cycle kept running"},
		},
		{
			name:            "shard and cycle failures are both reported",
			shardDropErr:    errDrop,
			refuseFlush:     true,
			wantErrContains: []string{"drop failed", "drop: stop flush cycle"},
			wantShardLog:    loadedShardID,
		},
		{
			name:            "rename failure is reported alongside the shard failure",
			shardDropErr:    errDrop,
			blockRename:     true,
			wantErrContains: []string{"drop failed", "rename index for async delete"},
			wantShardLog:    loadedShardID,
		},
		{
			name:      "backup in progress keeps the files",
			keepFiles: true,
		},
		{
			name:            "backup in progress still reports the shard failure",
			keepFiles:       true,
			shardDropErr:    errDrop,
			wantErrContains: []string{"drop failed"},
			wantShardLog:    loadedShardID,
		},
		{
			name:            "delete marker rename failure is reported",
			keepFiles:       true,
			blockRename:     true,
			wantErrContains: []string{"rename index for delete marker"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// the integration suite exports this, which would let the panic kill the test
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			idx, hook := newDropTestIndex(t)
			require.NoError(t, os.MkdirAll(shardPath(idx.path(), shardName), 0o755))

			if tt.keepFiles {
				idx.lastBackup.Store(&BackupState{BackupID: "backup1", InProgress: true})
			}

			if !tt.noShards {
				shard := NewMockShardLike(t)
				shard.EXPECT().ID().Return(loadedShardID).Maybe()
				if tt.panicDrop {
					shard.EXPECT().drop(tt.keepFiles).Run(func(bool) { panic("drop panicked") }).Return(nil).Once()
				} else {
					shard.EXPECT().drop(tt.keepFiles).Return(tt.shardDropErr).Once()
				}
				idx.shards.Store(shardName, shard)
			}
			if tt.secondShardFails {
				shard := NewMockShardLike(t)
				shard.EXPECT().ID().Return("abc_" + secondShard).Maybe()
				shard.EXPECT().drop(tt.keepFiles).Return(errors.New("second drop failed")).Once()
				idx.shards.Store(secondShard, shard)
			}

			if tt.refuseFlush {
				idx.cycleCallbacks.flushCycle = refusingCycle(idx.cycleCallbacks.flushCycle)
			}
			if tt.stallCompactions {
				idx.cycleCallbacks.compactionCycle = stallingCycle(idx.cycleCallbacks.compactionCycle)
				idx.cycleCallbacks.compactionAuxCycle = stallingCycle(idx.cycleCallbacks.compactionAuxCycle)
			}
			if tt.blockRename {
				blockRemovalIn(t, idx.Config.RootPath)
			}

			err := idx.drop()

			if len(tt.wantErrContains) == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				for _, want := range tt.wantErrContains {
					require.Contains(t, err.Error(), want)
				}
			}
			if tt.wantShardLog != "" {
				require.Equal(t, tt.wantShardLog, dropShardLogShard(t, hook))
			}

			require.Nil(t, idx.shards.Load(shardName))
			require.Nil(t, idx.shards.Load(secondShard))
			requireCyclesStopped(t, idx.cycleCallbacks)

			_, pathErr := os.Stat(idx.path())
			require.Equal(t, tt.blockRename, pathErr == nil)
			_, markerErr := os.Stat(filepath.Join(idx.Config.RootPath, backup.DeleteMarkerAdd(idx.ID())))
			require.Equal(t, tt.keepFiles && !tt.blockRename, markerErr == nil)
		})
	}
}

// newDropTestIndex returns an index with an empty shard map under a temp root
// and all cycle managers running, plus the hook capturing its log output.
func newDropTestIndex(t *testing.T) (*Index, *test.Hook) {
	t.Helper()
	logger, hook := test.NewNullLogger()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	idx := &Index{
		logger:           logger,
		shards:           shardMap{},
		closingCtx:       ctx,
		closingCancel:    cancel,
		cycleCallbacks:   newTestCycleCallbacks(logger),
		backupLock:       esync.NewKeyRWLocker(),
		shardCreateLocks: esync.NewKeyRWLocker(),
		Config:           IndexConfig{RootPath: t.TempDir(), ClassName: schema.ClassName("Abc")},
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
	return idx, hook
}

// blockRemovalIn makes dir read-only so os.RemoveAll fails for its contents.
func blockRemovalIn(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so os.RemoveAll cannot be made to fail")
	}
	require.NoError(t, os.Chmod(dir, 0o555))
	t.Cleanup(func() { os.Chmod(dir, 0o755) })
}

// fakeOffloadCloud fails Delete for the shards named in deleteErrs, panics for
// panicShard, and succeeds for the rest.
type fakeOffloadCloud struct {
	deleteErrs map[string]error
	panicShard string
}

func (c fakeOffloadCloud) VerifyBucket(ctx context.Context) error { return nil }

func (c fakeOffloadCloud) Upload(ctx context.Context, className, shardName, nodeName string) error {
	return nil
}

func (c fakeOffloadCloud) Download(ctx context.Context, className, shardName, nodeName string) error {
	return nil
}

func (c fakeOffloadCloud) Delete(ctx context.Context, className, shardName, nodeName string) error {
	if shardName == c.panicShard {
		panic("drop panicked")
	}
	return c.deleteErrs[shardName]
}

func dropShardLogShard(t *testing.T, hook *test.Hook) any {
	t.Helper()
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.ErrorLevel && entry.Data["action"] == "drop_shard" {
			return entry.Data["shard"]
		}
	}
	t.Fatalf("no drop_shard error log entry, got %d entries", len(hook.AllEntries()))
	return nil
}
