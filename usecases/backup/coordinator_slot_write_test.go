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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestCoordinatorRestoreGoroutineOnlyWritesTheSlotItOwns pins the write half
// of the slot-ownership invariant: a cancel frees the slot, so a newer
// restore can be holding it by the time the finished one writes its status.
func TestCoordinatorRestoreGoroutineOnlyWritesTheSlotItOwns(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		backupID    = "1"
		ctx         = context.Background()
		anyArg      = mock.Anything
		nodes       = []string{"N1", "N2"}
		classes     = []string{"C1"}
		cresp       = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sresp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)

	tests := []struct {
		name string
		// stealID is the id the newer restore claims the freed slot with.
		stealID string
		// cancelledInStorage makes the post-commit storage read report the
		// restore cancelled, which is the arm that stamps CANCELLED.
		cancelledInStorage bool
		// forbidden are the statuses the finished restore must never write onto
		// a claim it no longer owns.
		forbidden []backup.Status
	}{
		{
			name:      "a newer restore claimed the slot",
			stealID:   "live-restore",
			forbidden: []backup.Status{backup.Transferring, backup.Transferred, backup.Finalizing, backup.Success},
		},
		{
			// Retrying a cancelled restore under its original id is a normal
			// flow, so the id alone does not tell the two claims apart. Only
			// the terminal write is keyed on the generation, so only it is
			// pinned here.
			name:      "a retry of the same id claimed the slot",
			stealID:   backupID,
			forbidden: []backup.Status{backup.Success},
		},
		{
			name:               "storage reports the restore cancelled",
			stealID:            "live-restore",
			cancelledInStorage: true,
			forbidden:          []backup.Status{backup.Transferring, backup.Cancelled},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(newFakeNodeResolver(nodes))
			c := fc.coordinator()

			// The takeover is staged from inside CanCommit, which runs on the
			// Restore call itself, before the first status write.
			var once sync.Once
			for _, node := range nodes {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil).
					Run(func(mock.Arguments) {
						// assert, not require: Goexit inside a mock callback
						// surfaces as a hang instead of this failure.
						once.Do(func() {
							c.lastOp.set(backup.Cancelled)
							assert.True(t, c.lastOp.resetIfCancelled(backupID))
							prevID, _ := c.lastOp.renew(tc.stealID, "path", "", "")
							assert.Empty(t, prevID)
						})
					})
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
			}

			var committed atomic.Bool
			for _, node := range nodes {
				fc.client.On("Status", anyArg, node, anyArg).Return(sresp, nil).
					Run(func(mock.Arguments) {
						if tc.cancelledInStorage {
							// The cancel lands while the goroutine is in its
							// commit phase, so its post-commit read of storage
							// finds it.
							fc.backend.Lock()
							fc.backend.glMeta.Status = backup.Cancelling
							fc.backend.Unlock()
						}
						committed.Store(true)
					})
			}

			fc.backend.On("HomeDir", anyArg, anyArg, backupID).Return("bucket/" + backupID)
			fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).Return(nil)
			// Only Restore's own read, before it claims the slot, reaches the
			// mock: the fake serves what it was last given from there on.
			fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).
				Return(nil, backup.ErrNotFound{})

			desc := &backup.DistributedBackupDescriptor{
				ID:            backupID,
				Status:        backup.Success,
				Version:       Version,
				ServerVersion: config.ServerVersion,
				Nodes: map[string]*backup.NodeDescriptor{
					nodes[0]: {Classes: classes, Status: backup.Success},
					nodes[1]: {Classes: classes, Status: backup.Success},
				},
			}
			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			require.NoError(t, c.Restore(ctx, store, &req, desc, nil))

			// Without this the assertion below could pass on a goroutine that
			// never got as far as its status writes.
			require.Eventually(t, committed.Load, 10*time.Second, 10*time.Millisecond,
				"the restore goroutine never reached its commit phase")

			require.Never(t, func() bool {
				return slices.Contains(tc.forbidden, c.lastOp.get().Status)
			}, time.Second, 10*time.Millisecond,
				"the finished restore published its own outcome onto a claim it no longer owns")
			require.Equal(t, tc.stealID, c.lastOp.get().ID,
				"the finished restore took the newer claim's slot over")
		})
	}
}

// TestBackupStatPublishIfOwned pins the terminal slot write on its own, since
// the coordinator tests can only reach the states a full restore walks through.
func TestBackupStatPublishIfOwned(t *testing.T) {
	t.Parallel()
	const (
		holderID = "backup-a"
		newerID  = "backup-b"
		reason   = "no space left on device"
	)

	tests := []struct {
		name string
		// stale releases the claim and hands the slot to newerID before the
		// publish, so the publishing operation is no longer the holder.
		stale bool
		// released frees the slot without anyone claiming it again.
		released   bool
		status     backup.Status
		reason     string
		wantWrote  bool
		wantStatus backup.Status
		wantErr    string
		// wantRemembered is the reason a later poll for holderID reads.
		wantRemembered string
	}{
		{
			name:       "the holder publishes success",
			status:     backup.Success,
			wantWrote:  true,
			wantStatus: backup.Success,
		},
		{
			name:           "the holder publishes a failure with its reason",
			status:         backup.Failed,
			reason:         reason,
			wantWrote:      true,
			wantStatus:     backup.Failed,
			wantErr:        reason,
			wantRemembered: reason,
		},
		{
			// One row for all three terminal statuses: the ownership check runs
			// before publishIfOwned looks at the status, so none of them can
			// reach the slot without the others. Cancelled is the worst of the
			// three: commit() reads it as "cancelled externally" and aborts,
			// ending an operation nobody cancelled.
			name:       "a stale claim does not cancel the newer operation",
			stale:      true,
			status:     backup.Cancelled,
			wantStatus: backup.Started,
		},
		{
			name:     "a released slot stays free",
			released: true,
			status:   backup.Failed,
			reason:   reason,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var slot backupStat
			prevID, generation := slot.renew(holderID, "bucket/backups/a", "", "")
			require.Empty(t, prevID)

			switch {
			case tc.stale:
				require.True(t, slot.resetIfOwned(generation))
				newPrevID, _ := slot.renew(newerID, "bucket/backups/b", "", "")
				require.Empty(t, newPrevID)
			case tc.released:
				require.True(t, slot.resetIfOwned(generation))
			}
			require.Equal(t, tc.wantWrote, slot.publishIfOwned(generation, tc.status, tc.reason))

			st := slot.get()
			require.Equal(t, tc.wantStatus, st.Status)
			require.Equal(t, tc.wantErr, st.Err)
			if tc.stale {
				require.Equal(t, newerID, st.ID, "the publish must never take the slot over")
			}

			gotRemembered, found := slot.rememberedFailure(holderID)
			require.Equal(t, tc.wantRemembered != "", found)
			require.Equal(t, tc.wantRemembered, gotRemembered)
		})
	}
}
