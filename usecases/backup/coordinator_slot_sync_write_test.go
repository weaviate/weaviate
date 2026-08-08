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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// Pins: coordinator.Restore's synchronous error returns run on the caller's
// goroutine after the slot may have changed hands (cancel frees it, a newer
// restore claims it), so writing it unconditionally would clobber the newer
// restore's claim and let the reindex gate admit a migration over it.
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
			fc, desc := newRestoreCoordFixture(ctx, backupID, nodes)
			fc.client.On("Abort", anyArg, anyArg, anyArg).Return(nil).Maybe()

			c := fc.coordinator()

			cresp := &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
			if tc.canCommitErr {
				cresp = &CanCommitResponse{Method: OpRestore, ID: backupID}
			}
			var stolen atomic.Bool
			fc.client.On("CanCommit", anyArg, nodes[0], anyArg).Return(cresp, nil).
				Run(func(mock.Arguments) {
					if !stolen.CompareAndSwap(false, true) {
						return
					}
					stealSlot(t, &c.lastOp, backup.Cancelling, backupID, newerID)
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

			requireProbeSees(t, &Scheduler{restorer: c},
				NodeActivity{Busy: true, Kind: NodeActivityKindRestore, ID: newerID},
				"the probe reports the node idle while a restore is live, so a reindex is admitted on top of it")
		})
	}
}

// Same rule for the status writes the restore goroutine makes after commit
// (the release is already ownership-checked, but these weren't): a finished
// restore could restamp whatever the newer one holds, and a stale Cancelled
// write makes commit() abort a restore nobody cancelled.
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
			fc, desc := newRestoreCoordFixture(ctx, backupID, nodes)
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
					stealSlot(t, &c.lastOp, backup.Cancelling, backupID, newerID)
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

// newRestoreCoordFixture builds a coordinator whose backend holds no restore
// meta yet, plus the descriptor a fresh restore of backupID starts from.
func newRestoreCoordFixture(ctx context.Context, backupID string, nodes []string,
) (*fakeCoordinator, *backup.DistributedBackupDescriptor) {
	fc := newFakeCoordinator(newFakeNodeResolver(nodes))
	desc := &backup.DistributedBackupDescriptor{
		ID:      backupID,
		Status:  backup.Started,
		Version: Version,
		Nodes:   make(map[string]*backup.NodeDescriptor, len(nodes)),
	}
	for _, node := range nodes {
		desc.Nodes[node] = &backup.NodeDescriptor{Classes: []string{"C1"}, Status: backup.Started}
	}
	fc.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
	fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
	return fc, desc
}
