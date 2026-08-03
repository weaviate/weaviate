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
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	// backupProtectionWait caps how long an activation waits for a cold backup
	// to release a shard: the upload time is unbounded, so past the cap a
	// retryable error beats a stalled connection.
	backupProtectionWait = 30 * time.Second

	// backupProtectionPoll is how often a waiter rechecks the marker. Only
	// requests that were already refused ever poll, and the interval is the
	// worst-case latency they pay after the backup releases the shard.
	backupProtectionPoll = 50 * time.Millisecond

	// backupProtectionWaiters caps how many requests park on one index at once.
	// Nothing else limits how many requests a client can have in flight, so
	// without this a slow upload would trade an error per request for a parked
	// goroutine per request. Over the cap a request gets the refusal it would
	// have got before waiting existed.
	backupProtectionWaiters = 512
)

// refuseIfBackupProtected reports whether a non-hardlink backup currently has
// the shard's files listed but not yet uploaded.
func (i *Index) refuseIfBackupProtected(shardName string) error {
	if _, protected := i.backupProtectedShards.Load(shardName); protected {
		return fmt.Errorf("shard %q is %w", shardName, enterrors.ErrShardBackupProtected)
	}
	return nil
}

// waitForBackupProtection blocks until the backup releases shardName, ctx
// ends, or backupProtectionWait elapses. It holds no index lock, so a waiter
// never blocks the backup, readers, or shutdown, and it polls rather than
// waits on a signal so it still gives up if release never runs.
func (i *Index) waitForBackupProtection(ctx context.Context, shardName string) error {
	if _, protected := i.backupProtectedShards.Load(shardName); !protected {
		return nil
	}

	if i.backupProtectionWaiting.Add(1) > backupProtectionWaiters {
		i.backupProtectionWaiting.Add(-1)
		return fmt.Errorf("shard %q is %w: %d requests are already waiting for the backup",
			shardName, enterrors.ErrShardBackupProtected, backupProtectionWaiters)
	}
	defer i.backupProtectionWaiting.Add(-1)

	giveUp := time.NewTimer(backupProtectionWait)
	defer giveUp.Stop()
	ticker := time.NewTicker(backupProtectionPoll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("shard %q is %w: %w", shardName, enterrors.ErrShardBackupProtected, ctx.Err())
		case <-giveUp.C:
			return fmt.Errorf("shard %q is %w: gave up after %s",
				shardName, enterrors.ErrShardBackupProtected, backupProtectionWait)
		case <-ticker.C:
			if _, protected := i.backupProtectedShards.Load(shardName); !protected {
				return nil
			}
		}
	}
}
