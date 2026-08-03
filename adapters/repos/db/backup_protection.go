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
	// to release a shard. The window equals the class upload, which grows with
	// the data (~51s for 1 GB) and with the backend's speed, so waiting it out
	// unconditionally would hold a request for an unbounded time. Past the cap
	// a retryable error is more useful to a client than a stalled connection.
	backupProtectionWait = 30 * time.Second

	// backupProtectionPoll is how often a waiter rechecks the marker. Only
	// requests that were already refused ever poll, and the interval is the
	// worst-case latency they pay after the backup releases the shard.
	backupProtectionPoll = 50 * time.Millisecond
)

// refuseIfBackupProtected reports whether a non-hardlink backup currently has
// the shard's files listed but not yet uploaded.
func (i *Index) refuseIfBackupProtected(shardName string) error {
	if _, protected := i.backupProtectedShards.Load(shardName); protected {
		return fmt.Errorf("shard %q is %w", shardName, enterrors.ErrShardBackupProtected)
	}
	return nil
}

// waitForBackupProtection blocks until the backup releases shardName, the
// caller's context ends, or backupProtectionWait elapses. It holds no index
// lock, so a waiter never blocks the backup it is waiting for, the shard's
// other readers, or shutdown.
//
// The marker is polled rather than signalled on release: a waiter then also
// gives up on a backup whose release never runs, and the read path stays
// independent of the release path.
func (i *Index) waitForBackupProtection(ctx context.Context, shardName string) error {
	if _, protected := i.backupProtectedShards.Load(shardName); !protected {
		return nil
	}

	giveUp := time.NewTimer(backupProtectionWait)
	defer giveUp.Stop()
	ticker := time.NewTicker(backupProtectionPoll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("shard %q: %w while it was %w", shardName, ctx.Err(), enterrors.ErrShardBackupProtected)
		case <-giveUp.C:
			return fmt.Errorf("shard %q: still %w after %s", shardName, enterrors.ErrShardBackupProtected, backupProtectionWait)
		case <-ticker.C:
			if _, protected := i.backupProtectedShards.Load(shardName); !protected {
				return nil
			}
		}
	}
}
