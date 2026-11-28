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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// haltShard halts as a backup would that passed its reindex check before the
// fixture's tasks were applied.
func haltShard(t *testing.T, f *viewFixture) error {
	f.tasks.mu.Lock()
	states := f.tasks.states
	f.tasks.states = nil
	f.tasks.mu.Unlock()
	defer f.tasks.set(states...)
	return f.shard.HaltForTransfer(context.Background(), false, 0)
}

func folderExists(dir string) bool {
	_, err := os.Stat(dir)
	return err == nil
}

// A backup lists and copies a halted shard's files, so no reindex delete may run on it until the resume.
func TestReindexShardViewDeletesWaitForBackupHalt(t *testing.T) {
	const quiet = 300 * time.Millisecond
	tests := []struct {
		name string
		run  func(t *testing.T, f *viewFixture)
	}{
		{name: "a retire queued during a halt deletes nothing until the resume", run: func(t *testing.T, f *viewFixture) {
			plain := filepath.Join(f.shard.pathLSM(), plainTitle)
			require.NoError(t, haltShard(t, f))
			f.tasks.set(switchedTask("a"))
			require.NoError(t, f.shard.refreshView(context.Background()))
			require.Equal(t, gen1Title, servedFolder(f.shard.loadView(), plainTitle))
			require.Never(t, func() bool { return !folderExists(plain) }, quiet, 10*time.Millisecond,
				"the retire deleted a folder of a halted shard")
			require.NoError(t, f.shard.resumeMaintenanceCycles(context.Background()))
			require.Eventually(t, func() bool { return !folderExists(plain) }, 5*time.Second, 10*time.Millisecond)
		}},
		{name: "a halt is refused while a retire is pending", run: func(t *testing.T, f *viewFixture) {
			held := f.shard.acquireView()
			f.tasks.set(switchedTask("a"))
			require.NoError(t, f.shard.refreshView(context.Background()))
			require.ErrorIs(t, haltShard(t, f), enterrors.ErrShardBusyStructuralOp)
			require.False(t, f.shard.haltedForTransfer())
			held.release()
			require.Eventually(t, func() bool { return haltShard(t, f) == nil }, 5*time.Second, 10*time.Millisecond)
			require.NoError(t, f.shard.resumeMaintenanceCycles(context.Background()))
		}},
		{name: "a takeover of the same generation waits for the retire a halt holds", run: func(t *testing.T, f *viewFixture) {
			build := filepath.Join(f.shard.pathLSM(), f.shard.loadView().staged[0].name)
			marker := filepath.Join(build, "dead-run")
			require.NoError(t, os.WriteFile(marker, nil, 0o644))
			require.NoError(t, haltShard(t, f))
			f.tasks.set(stagedTask("takeover"))
			require.ErrorIs(t, f.shard.refreshView(context.Background()), errViewNotReady)
			require.Empty(t, stagedTasks(f.shard.loadView()))
			require.Never(t, func() bool { return !folderExists(marker) }, quiet, 10*time.Millisecond,
				"the dead run's folder was deleted on a halted shard")
			require.NoError(t, f.shard.resumeMaintenanceCycles(context.Background()))
			require.Eventually(t, func() bool {
				return f.shard.refreshView(context.Background()) == nil
			}, 5*time.Second, 10*time.Millisecond)
			require.Equal(t, []string{"takeover"}, stagedTasks(f.shard.loadView()))
			require.False(t, folderExists(marker), "the takeover builds into an emptied folder")
		}},
		{name: "a scan waits for the resume", run: func(t *testing.T, f *viewFixture) {
			b := f.shard.loadView().staged[0]
			scratch := filepath.Join(f.shard.pathLSM(), b.name+reindexBuildSuffix)
			require.NoError(t, os.MkdirAll(scratch, 0o755))
			require.NoError(t, haltShard(t, f))
			done := make(chan error, 1)
			enterrors.GoWrapper(func() { done <- f.shard.scanIntoBuild(context.Background(), b) }, f.idx.logger)
			require.Never(t, func() bool { return !folderExists(scratch) || len(done) > 0 }, quiet, 10*time.Millisecond,
				"the scan deleted its scratch on a halted shard")
			require.NoError(t, f.shard.resumeMaintenanceCycles(context.Background()))
			require.NoError(t, <-done)
			require.False(t, folderExists(scratch), "the scan deletes its scratch")
			require.Equal(t, uint64(5), countHelloIn(t, b), "the scan filled the build")
		}},
		{name: "a halt is refused while a build scans", run: func(t *testing.T, f *viewFixture) {
			b := f.shard.loadView().staged[0]
			end, err := f.shard.beginScan(context.Background(), b.name+reindexBuildSuffix)
			require.NoError(t, err)
			require.ErrorIs(t, haltShard(t, f), enterrors.ErrShardBusyStructuralOp)
			end()
			require.NoError(t, haltShard(t, f))
			require.NoError(t, f.shard.resumeMaintenanceCycles(context.Background()))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newViewFixture(t, "ViewTest")
			f.putObjects(5)
			f.tasks.set(stagedTask("a"))
			require.NoError(t, f.shard.refreshView(context.Background()))
			tc.run(t, f)
		})
	}
}

func countHelloIn(t *testing.T, b *stagedBuild) uint64 {
	bm, release, err := b.bucket.RoaringSetGet(context.Background(), []byte("hello"))
	require.NoError(t, err)
	defer release()
	return uint64(bm.GetCardinality())
}
