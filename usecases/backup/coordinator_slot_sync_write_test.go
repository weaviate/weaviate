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

package backup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// coordinator.Restore's synchronous error returns run on the caller's
// goroutine, after the slot may already have changed hands: a cancel frees the
// slot (resetIfCancelled), a newer restore claims it (renew), and only then does
// the older restore's canCommit or initial PutMeta fail. Writing the slot
// unconditionally there clears or restamps the newer restore's claim, and
// [NodeActivityProbe] then reports the node idle over a live restore — which is
// what the reindex submission gate reads, so it admits a migration on top of it.
//
// The takeover is staged from inside the CanCommit mock, which runs on the
// Restore goroutine itself, immediately before the writes under test.
func TestCoordinatorRestoreSyncErrorsOnlyWriteTheSlotTheyOwn(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		newerID     = "live-restore"
	)
	var (
		ctx    = context.Background()
		anyArg = mock.Anything
		nodes  = []string{"N1"}
	)

	tests := []struct {
		name string
		// canCommitErr fails the canCommit phase, which is the reset at the
		// top of Restore's error handling.
		canCommitErr bool
		// putMetaErr fails the initial PutMeta, which is reached only after
		// the status write that precedes it.
		putMetaErr bool
		reason     string
	}{
		{
			name:         "canCommit fails after the slot changed hands",
			canCommitErr: true,
			reason:       "the failed restore cleared a slot the newer restore owns",
		},
		{
			name:       "initial PutMeta fails after the slot changed hands",
			putMetaErr: true,
			reason:     "the failed restore cleared a slot the newer restore owns",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(newFakeNodeResolver(nodes))
			desc := &backup.DistributedBackupDescriptor{
				ID:      backupID,
				Status:  backup.Started,
				Version: Version,
				Nodes: map[string]*backup.NodeDescriptor{
					nodes[0]: {Classes: []string{"C1"}, Status: backup.Started},
				},
			}
			fc.backend.On("HomeDir", anyArg, anyArg, anyArg).Return("bucket/" + backupID)
			fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
			fc.client.On("Abort", anyArg, anyArg, anyArg).Return(nil).Maybe()

			c := fc.coordinator()

			cresp := &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
			if tc.canCommitErr {
				cresp = &CanCommitResponse{Method: OpRestore, ID: backupID}
			}
			var stolen atomic.Bool
			fc.client.On("CanCommit", anyArg, nodes[0], anyArg).Return(cresp, nil).
				Run(func(mock.Arguments) {
					// assert, not require: Goexit here would abandon Restore
					// mid-flight and surface as a hang instead of this failure.
					if !stolen.CompareAndSwap(false, true) {
						return
					}
					c.lastOp.set(backup.Cancelling)
					assert.True(t, c.lastOp.resetIfCancelled(backupID))
					assert.Empty(t, c.lastOp.renew(newerID, "path", "", ""))
				})
			if tc.putMetaErr {
				fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).
					Return(errors.New("object storage unavailable"))
			}

			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			require.Error(t, c.Restore(ctx, store, &req, desc, nil))
			require.True(t, stolen.Load(), "the newer restore never got to claim the slot")

			held := c.lastOp.get()
			require.Equal(t, newerID, held.ID, tc.reason)
			require.Equal(t, backup.Started, held.Status,
				"the failed restore restamped the status of a slot the newer restore owns")

			probe := NewNodeActivityProbe(nil)
			probe.AttachScheduler(&Scheduler{restorer: c})
			require.Equal(t,
				NodeActivity{Busy: true, Kind: NodeActivityKindRestore, ID: newerID},
				probe.Activity(),
				"the probe reports the node idle while a restore is live, so a reindex is admitted on top of it")
		})
	}
}

// The same rule for the writes the restore goroutine makes after commit: the
// slot can change hands mid-flight (the release is already ownership-checked),
// but the status writes on the way there were not, so a finished restore
// restamped whatever the newer one holds. commit() reads Cancelled on the slot
// as "cancelled externally", so a stale Cancelled write aborts a restore nobody
// cancelled.
//
// The takeover is staged from inside the participant Status call, which runs on
// the restore goroutine before any of those writes.
func TestCoordinatorRestoreGoroutineOnlyWritesTheSlotItOwns(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		newerID     = "live-restore"
	)
	var (
		ctx    = context.Background()
		anyArg = mock.Anything
		nodes  = []string{"N1"}
		cresp  = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sresp  = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)

	tests := []struct {
		name string
		// cancelledInStorage makes the goroutine's post-commit storage read
		// report the restore cancelled, which is the arm that stamps the slot
		// Cancelled — the one status commit() reads as "cancelled externally".
		cancelledInStorage bool
	}{
		{name: "the restore runs to completion"},
		{name: "the restore finds itself cancelled in storage", cancelledInStorage: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(newFakeNodeResolver(nodes))
			desc := &backup.DistributedBackupDescriptor{
				ID:      backupID,
				Status:  backup.Started,
				Version: Version,
				Nodes: map[string]*backup.NodeDescriptor{
					nodes[0]: {Classes: []string{"C1"}, Status: backup.Started},
				},
			}
			fc.backend.On("HomeDir", anyArg, anyArg, anyArg).Return("bucket/" + backupID)
			fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
			fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).Return(nil)
			fc.client.On("CanCommit", anyArg, nodes[0], anyArg).Return(cresp, nil)
			fc.client.On("Commit", anyArg, nodes[0], anyArg).Return(nil)

			c := fc.coordinator()

			var stolen atomic.Bool
			fc.client.On("Status", anyArg, nodes[0], anyArg).Return(sresp, nil).
				Run(func(mock.Arguments) {
					if !stolen.CompareAndSwap(false, true) {
						return
					}
					c.lastOp.set(backup.Cancelling)
					assert.True(t, c.lastOp.resetIfCancelled(backupID))
					assert.Empty(t, c.lastOp.renew(newerID, "path", "", ""))
					if tc.cancelledInStorage {
						fc.backend.Lock()
						fc.backend.glMeta.Status = backup.Cancelling
						fc.backend.Unlock()
					}
				})

			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			require.NoError(t, c.Restore(ctx, store, &req, desc, nil))
			require.Eventually(t, stolen.Load, 10*time.Second, 10*time.Millisecond,
				"the newer restore never got to claim the slot")

			require.Never(t, func() bool {
				held := c.lastOp.get()
				return held.ID != newerID || held.Status != backup.Started
			}, 2*time.Second, 20*time.Millisecond,
				"the finished restore wrote a slot the newer restore owns")
		})
	}
}
