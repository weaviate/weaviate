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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
)

func TestIndexReleaseBackupOwnership(t *testing.T) {
	t.Run("release does not resume a halt it did not acquire", func(t *testing.T) {
		index, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		// Another operation's hold (replica transfer, second backup, or offload).
		require.NoError(t, shard.HaltForTransfer(ctx, false, 0))

		// Backup A never halts the shard: on the production hardlink path,
		// CreateBackupSnapshot releases its own halt.
		require.NoError(t, index.initBackup("bak-a"))
		require.NoError(t, index.ReleaseBackup(ctx, "bak-a"))

		shard.haltForTransferMux.Lock()
		count := shard.haltForTransferCount.Load()
		shard.haltForTransferMux.Unlock()

		require.EqualValues(t, 1, count,
			"ReleaseBackup must not decrement a hold it did not acquire")

		_, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err, "shard must still be paused for transfer")

		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	})

	t.Run("straggler release cannot touch a successor backup's state", func(t *testing.T) {
		index, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		require.NoError(t, index.initBackup("a"))
		require.NoError(t, index.ReleaseBackup(ctx, "a"))

		require.NoError(t, index.initBackup("b"))
		require.NoError(t, shard.HaltForTransfer(ctx, false, 0))

		// A late duplicate release: backup A was already released above.
		require.NoError(t, index.ReleaseBackup(ctx, "a"))

		shard.haltForTransferMux.Lock()
		count := shard.haltForTransferCount.Load()
		shard.haltForTransferMux.Unlock()

		require.EqualValues(t, 1, count,
			"straggler release must not consume the successor backup's hold")

		cur := index.lastBackup.Load()
		require.NotNil(t, cur)
		require.Equal(t, "b", cur.BackupID)

		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
		require.NoError(t, index.ReleaseBackup(ctx, "b"))
	})

	t.Run("concurrent stale releases never consume a successor's holds", func(t *testing.T) {
		for iteration := range 50 {
			func() {
				index, shard := newSharedHaltTestShard(t)
				ctx := context.Background()

				require.NoError(t, index.initBackup("a"),
					"iteration %d: initBackup(a)", iteration)

				var wg sync.WaitGroup
				for range 3 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = index.ReleaseBackup(ctx, "a")
					}()
				}

				// Retry until one of the racing releases clears A's slot.
				for {
					if err := index.initBackup("b"); err == nil {
						break
					}
				}

				// Record one halt under B via the no-hardlink path.
				sd, err := index.backupShardWithoutHardlinks(ctx, "shard1", nil, "b")
				require.NoError(t, err, "iteration %d: backupShardWithoutHardlinks", iteration)
				require.NotNil(t, sd)

				wg.Wait()

				shard.haltForTransferMux.Lock()
				count := shard.haltForTransferCount.Load()
				shard.haltForTransferMux.Unlock()

				require.EqualValues(t, 1, count,
					"iteration %d: stale goroutines must not consume B's hold", iteration)

				cur := index.lastBackup.Load()
				require.NotNil(t, cur, "iteration %d: lastBackup must still be B", iteration)
				require.Equal(t, "b", cur.BackupID)

				require.NoError(t, index.ReleaseBackup(ctx, "b"))

				shard.haltForTransferMux.Lock()
				finalCount := shard.haltForTransferCount.Load()
				shard.haltForTransferMux.Unlock()
				require.Zero(t, finalCount, "iteration %d: final count must be 0", iteration)
			}()
		}
	})

	t.Run("release still resumes the no-hardlink halts it recorded", func(t *testing.T) {
		index, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		require.NoError(t, index.initBackup("test-id"))

		sd, err := index.backupShardWithoutHardlinks(ctx, "shard1", nil, "test-id")
		require.NoError(t, err)
		require.NotNil(t, sd)

		shard.haltForTransferMux.Lock()
		countBefore := shard.haltForTransferCount.Load()
		shard.haltForTransferMux.Unlock()
		require.EqualValues(t, 1, countBefore)

		require.NoError(t, index.ReleaseBackup(ctx, "test-id"))

		shard.haltForTransferMux.Lock()
		countAfter := shard.haltForTransferCount.Load()
		shard.haltForTransferMux.Unlock()
		require.Zero(t, countAfter, "ReleaseBackup must resume halts it recorded")
	})

	t.Run("record after release fails and self-resumes", func(t *testing.T) {
		index, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		require.NoError(t, index.initBackup("test-id"))
		require.NoError(t, index.ReleaseBackup(ctx, "test-id"))

		// The halt inside backupShardWithoutHardlinks succeeds, but the
		// ownership record fails because the backup is gone; the halt is rolled back.
		sd, err := index.backupShardWithoutHardlinks(ctx, "shard1", nil, "test-id")
		require.Error(t, err, "record must fail when backup is no longer active")
		require.Nil(t, sd)

		shard.haltForTransferMux.Lock()
		count := shard.haltForTransferCount.Load()
		shard.haltForTransferMux.Unlock()
		require.Zero(t, count, "failed record must self-resume the halt")
	})
}

func TestShardHaltForTransferForcedResume(t *testing.T) {
	t.Run("watchdog fire clears only timeout-armed holds", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		// A hold with no inactivity timeout, as backups take.
		require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
		// A hold with an inactivity timeout, as replica transfers take; it arms the watchdog.
		require.NoError(t, shard.HaltForTransfer(ctx, false, time.Hour))

		timer := time.NewTimer(time.Hour)
		defer timer.Stop()

		shard.haltForTransferMux.Lock()
		shard.haltForTransferInactivityDeadline = time.Now().Add(-time.Hour)
		shard.haltForTransferMux.Unlock()

		keepWatching := shard.handleInactivityFire(context.Background(), timer)

		shard.haltForTransferMux.Lock()
		countAfterFire := shard.haltForTransferCount.Load()
		cancelAfterFire := shard.haltForTransferCtxCancel
		shard.haltForTransferMux.Unlock()

		require.False(t, keepWatching)
		require.EqualValues(t, 1, countAfterFire,
			"fire must clear only the armed hold, leaving the unarmed hold intact")
		require.Nil(t, cancelAfterFire,
			"monitor sentinel must be cleared after fire")

		_, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err, "shard must still be paused for transfer")

		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	})

	t.Run("solo armed hold fully resumes on fire", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		require.NoError(t, shard.HaltForTransfer(ctx, false, time.Hour))

		timer := time.NewTimer(time.Hour)
		defer timer.Stop()

		shard.haltForTransferMux.Lock()
		shard.haltForTransferInactivityDeadline = time.Now().Add(-time.Hour)
		shard.haltForTransferMux.Unlock()

		keepWatching := shard.handleInactivityFire(context.Background(), timer)

		shard.haltForTransferMux.Lock()
		countAfterFire := shard.haltForTransferCount.Load()
		cancelAfterFire := shard.haltForTransferCtxCancel
		shard.haltForTransferMux.Unlock()

		require.False(t, keepWatching)
		require.Zero(t, countAfterFire, "solo armed hold must be fully resumed")
		require.Nil(t, cancelAfterFire, "monitor sentinel must be cleared")
	})

	t.Run("fire with clamped-zero armed count still clears monitor", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		release, err := shard.haltForTransferOwned(ctx)
		require.NoError(t, err)

		require.NoError(t, shard.HaltForTransfer(ctx, false, time.Hour))

		require.NoError(t, shard.resumeMaintenanceCycles(ctx))

		shard.haltForTransferMux.Lock()
		require.EqualValues(t, 1, shard.haltForTransferCount.Load(), "owned hold keeps count at 1")
		require.Equal(t, 0, shard.haltForTransferArmedCount, "anonymous release clamped armed to 0")
		shard.haltForTransferMux.Unlock()

		timer := time.NewTimer(time.Hour)
		defer timer.Stop()

		shard.haltForTransferMux.Lock()
		shard.haltForTransferInactivityDeadline = time.Now().Add(-time.Hour)
		shard.haltForTransferMux.Unlock()

		keepWatching := shard.handleInactivityFire(context.Background(), timer)

		shard.haltForTransferMux.Lock()
		countAfterFire := shard.haltForTransferCount.Load()
		cancelAfterFire := shard.haltForTransferCtxCancel
		timeoutAfterFire := shard.haltForTransferInactivityTimeout
		deadlineAfterFire := shard.haltForTransferInactivityDeadline
		shard.haltForTransferMux.Unlock()

		require.False(t, keepWatching)
		require.EqualValues(t, 1, countAfterFire,
			"owned hold survives: count must stay 1")
		require.Nil(t, cancelAfterFire,
			"monitor sentinel must be nil after fire even with zero armed holds")
		require.Zero(t, timeoutAfterFire, "timeout must be reset")
		require.True(t, deadlineAfterFire.IsZero(), "deadline must be reset")

		require.NoError(t, shard.HaltForTransfer(ctx, false, time.Hour))

		shard.haltForTransferMux.Lock()
		freshCancel := shard.haltForTransferCtxCancel
		shard.haltForTransferMux.Unlock()

		require.NotNil(t, freshCancel,
			"new armed halt must start a fresh monitor (sentinel non-nil)")

		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
		require.NoError(t, release(ctx))
	})

	t.Run("forced call on an idle shard is a no-op", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)

		shard.haltForTransferMux.Lock()
		err := shard.mayForceResumeMaintenanceCycles(context.Background(), true)
		count := shard.haltForTransferCount.Load()
		shard.haltForTransferMux.Unlock()

		require.NoError(t, err)
		require.Zero(t, count)
	})
}

func TestShardHaltForTransferOwned(t *testing.T) {
	t.Run("anonymous resume cannot consume an owned hold", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		release, err := shard.haltForTransferOwned(ctx)
		require.NoError(t, err)

		require.NoError(t, shard.resumeMaintenanceCycles(ctx))

		shard.haltForTransferMux.Lock()
		count := shard.haltForTransferCount.Load()
		owned := shard.haltForTransferOwnedCount
		shard.haltForTransferMux.Unlock()

		require.EqualValues(t, 1, count, "owned hold must survive anonymous resume")
		require.Equal(t, 1, owned)

		require.NoError(t, release(ctx))

		shard.haltForTransferMux.Lock()
		count = shard.haltForTransferCount.Load()
		owned = shard.haltForTransferOwnedCount
		shard.haltForTransferMux.Unlock()

		require.Zero(t, count)
		require.Zero(t, owned)
	})

	t.Run("watchdog fire cannot consume an owned hold", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		release, err := shard.haltForTransferOwned(ctx)
		require.NoError(t, err)

		require.NoError(t, shard.HaltForTransfer(ctx, false, time.Hour))

		timer := time.NewTimer(time.Hour)
		defer timer.Stop()

		shard.haltForTransferMux.Lock()
		shard.haltForTransferInactivityDeadline = time.Now().Add(-time.Hour)
		shard.haltForTransferMux.Unlock()

		keepWatching := shard.handleInactivityFire(context.Background(), timer)
		require.False(t, keepWatching)

		shard.haltForTransferMux.Lock()
		count := shard.haltForTransferCount.Load()
		shard.haltForTransferMux.Unlock()

		require.EqualValues(t, 1, count, "owned hold must survive watchdog fire")

		require.NoError(t, release(ctx))

		shard.haltForTransferMux.Lock()
		count = shard.haltForTransferCount.Load()
		shard.haltForTransferMux.Unlock()
		require.Zero(t, count)
	})

	t.Run("release is idempotent", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		release, err := shard.haltForTransferOwned(ctx)
		require.NoError(t, err)

		require.NoError(t, release(ctx))
		require.NoError(t, release(ctx)) // second call is a no-op

		shard.haltForTransferMux.Lock()
		count := shard.haltForTransferCount.Load()
		owned := shard.haltForTransferOwnedCount
		shard.haltForTransferMux.Unlock()

		require.Zero(t, count)
		require.Zero(t, owned)
	})

	t.Run("failed owned acquire rolls back both counts", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		// First take an anonymous hold so the count is above 1; the pause
		// steps run only for the first hold.
		require.NoError(t, shard.HaltForTransfer(ctx, false, 0))

		// Acquire an owned hold with a cancelled context: at count above 1 the
		// pause steps are skipped, and the seal steps (FlushMemtables) fail
		// deterministically.
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := shard.haltForTransferOwned(cancelledCtx)
		require.Error(t, err)

		shard.haltForTransferMux.Lock()
		count := shard.haltForTransferCount.Load()
		owned := shard.haltForTransferOwnedCount
		shard.haltForTransferMux.Unlock()

		require.EqualValues(t, 1, count, "failed owned acquire must roll back count")
		require.Zero(t, owned, "failed owned acquire must roll back owned count")

		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	})

	t.Run("CreateBackupSnapshot self-releases on success", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		ctx := context.Background()

		sd := &backup.ShardDescriptor{}
		stagingRoot := t.TempDir()

		_, err := shard.CreateBackupSnapshot(ctx, sd, stagingRoot)
		require.NoError(t, err)

		shard.haltForTransferMux.Lock()
		count := shard.haltForTransferCount.Load()
		owned := shard.haltForTransferOwnedCount
		shard.haltForTransferMux.Unlock()

		require.Zero(t, count, "owned hold must be released after successful snapshot")
		require.Zero(t, owned, "owned count must be zero after successful snapshot")
	})

	t.Run("CreateBackupSnapshot self-releases on error", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		sd := &backup.ShardDescriptor{}
		stagingRoot := t.TempDir()

		_, err := shard.CreateBackupSnapshot(cancelledCtx, sd, stagingRoot)
		require.Error(t, err)

		shard.haltForTransferMux.Lock()
		count := shard.haltForTransferCount.Load()
		owned := shard.haltForTransferOwnedCount
		shard.haltForTransferMux.Unlock()

		require.Zero(t, count, "owned hold must be released after failed snapshot")
		require.Zero(t, owned, "owned count must be zero after failed snapshot")
	})
}
