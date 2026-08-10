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
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestCoordinatorRestoreReleaseOnlyClearsItsOwnSlot pins the release half of
// the slot-ownership invariant: the restore goroutine gives the slot back only
// while it still holds it. A newer restore can take the slot over while the
// old goroutine is still finishing (cancellation frees it, then another
// restore claims it), and clearing that claim reports the node idle while a
// restore is live.
func TestCoordinatorRestoreReleaseOnlyClearsItsOwnSlot(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		backupID    = "1"
		ctx         = context.Background()
		anyArg      = mock.Anything
		nodes       = []string{"N1", "N2"}
		classes     = []string{"C1"}
		cresp       = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq        = &StatusRequest{OpRestore, backupID, backendName, "", "", ""}
		sresp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)

	tests := []struct {
		name string
		// steal reproduces a newer restore taking the slot over while the
		// goroutine of the previous one is still in its final PutMeta.
		steal      bool
		wantSlotID string
	}{
		{name: "slot still held by this restore", steal: false, wantSlotID: ""},
		{name: "slot taken over by a newer restore", steal: true, wantSlotID: "live-restore"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(newFakeNodeResolver(nodes))
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
			for _, node := range nodes {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil)
				fc.client.On("Commit", anyArg, node, sReq).Return(nil)
				fc.client.On("Status", anyArg, node, sReq).Return(sresp, nil)
			}
			fc.backend.On("HomeDir", anyArg, anyArg, backupID).Return("bucket/" + backupID)
			fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})

			var (
				puts    atomic.Int32
				blocked = make(chan struct{})
				release = make(chan struct{})
			)
			// The third and last PutMeta is the goroutine's final round trip, so
			// holding it there parks the goroutine right before it releases.
			fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).
				Return(nil).
				Run(func(mock.Arguments) {
					if puts.Add(1) != 3 {
						return
					}
					close(blocked)
					<-release
				})

			c := fc.coordinator()
			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			require.NoError(t, c.Restore(ctx, store, &req, desc, nil))

			select {
			case <-blocked:
			case <-time.After(20 * time.Second):
				t.Fatal("restore goroutine never reached its final PutMeta")
			}

			if tc.steal {
				// assert, not require: a failure here must not skip the close
				// below, which is what unparks the restore goroutine.
				c.lastOp.set(backup.Cancelled)
				assert.True(t, c.lastOp.resetIfCancelled(backupID))
				assert.Empty(t, c.lastOp.renew("live-restore", "path", "", ""))
			}
			close(release)

			if tc.steal {
				require.Never(t, func() bool {
					return c.lastOp.get().ID != tc.wantSlotID
				}, time.Second, 10*time.Millisecond,
					"the finished restore released a slot a newer restore owns")
				return
			}
			require.Eventually(t, func() bool {
				return c.lastOp.get().ID == tc.wantSlotID
			}, 10*time.Second, 10*time.Millisecond,
				"the finished restore never released its own slot")
		})
	}
}

// TestCoordinatorBackupReleaseOnlyClearsItsOwnSlot is the backup-side mirror of
// the test above. Both goroutines release the same way, so pinning one and not
// the other leaves half the invariant free to regress: reverting the backup
// release to an unconditional reset left this whole package green.
//
// Same mechanism, and it is why the invariant is load-bearing: the slot is the
// node's busy signal, so one cleared by the wrong goroutine reports the node
// idle while a backup is live.
//
// The takeover is staged from inside the participant Status call, which runs on
// the backup goroutine itself, before its deferred release. That is the same
// interleaving as the restore case (cancel frees the slot, a newer operation
// claims it, the old goroutine then returns) without depending on how many
// times the coordinator happens to write its meta file.
func TestCoordinatorBackupReleaseOnlyClearsItsOwnSlot(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		backupID    = "1"
		ctx         = context.Background()
		anyArg      = mock.Anything
		nodes       = []string{"N1", "N2"}
		classes     = []string{"C1"}
		cresp       = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sresp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
	)

	tests := []struct {
		name       string
		steal      bool
		wantSlotID string
	}{
		{name: "slot still held by this backup", steal: false, wantSlotID: ""},
		{name: "slot taken over by a newer backup", steal: true, wantSlotID: "live-backup"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(newFakeNodeResolver(nodes))
			fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
			fc.backend.On("HomeDir", anyArg, anyArg, backupID).Return("bucket/" + backupID)
			fc.backend.On("PutObject", anyArg, backupID, GlobalBackupFile, anyArg).Return(nil)
			for _, node := range nodes {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil)
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
			}

			c := fc.coordinator()

			var stolen atomic.Bool
			for _, node := range nodes {
				fc.client.On("Status", anyArg, node, anyArg).Return(sresp, nil).
					Run(func(mock.Arguments) {
						// Runs on the backup goroutine, before its deferred release.
						// assert, not require: require's Goexit would kill that
						// goroutine mid-flight, surfacing as a hang or an unrelated
						// downstream failure instead of this one.
						if !tc.steal || !stolen.CompareAndSwap(false, true) {
							return
						}
						c.lastOp.set(backup.Cancelled)
						assert.True(t, c.lastOp.resetIfCancelled(backupID))
						assert.Empty(t, c.lastOp.renew("live-backup", "path", "", ""))
					})
			}

			req := newReq(classes, backendName, backupID)
			store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
			require.NoError(t, c.Backup(ctx, store, &req))
			if tc.steal {
				// The takeover runs on the backup goroutine, so wait for it
				// rather than racing the assertion against it.
				require.Eventually(t, stolen.Load, 10*time.Second, 10*time.Millisecond,
					"the newer backup never got to claim the slot")
			}

			if tc.steal {
				require.Never(t, func() bool {
					return c.lastOp.get().ID != tc.wantSlotID
				}, 2*time.Second, 20*time.Millisecond,
					"the finished backup released a slot a newer backup owns")
				return
			}
			require.Eventually(t, func() bool {
				return c.lastOp.get().ID == tc.wantSlotID
			}, 10*time.Second, 20*time.Millisecond,
				"the finished backup never released its own slot")
		})
	}
}

// The write half of the same ownership invariant. CancelRestore stamps the
// coordinator's restore slot from what it read in object storage, not from what
// the slot holds — and that slot is one per node, shared by every restore this
// node coordinates. So a cancel aimed at one restore lands on whichever restore
// currently holds it, and commit() reads Cancelled there as "cancelled
// externally" and aborts a restore nobody asked to cancel.
func TestCancelRestoreOnlyStampsTheSlotItOwns(t *testing.T) {
	t.Parallel()
	const (
		backendName  = "s3"
		beingCancely = "restore-being-cancelled"
		stillRunning = "restore-still-running"
	)

	tests := []struct {
		name       string
		slotHolder string
		wantStatus backup.Status
		reason     string
	}{
		{
			name:       "the slot is held by the restore being cancelled",
			slotHolder: beingCancely,
			wantStatus: backup.Cancelled,
			reason: "this node coordinates the restore being cancelled; leaving its slot Started " +
				"makes OnStatus report a cancelled restore as running",
		},
		{
			name:       "the slot is held by a different, live restore",
			slotHolder: stillRunning,
			wantStatus: backup.Started,
			reason: "a cancel aimed at a different restore stamped this one Cancelled; commit() reads " +
				"that as 'cancelled externally' and aborts a restore nobody cancelled",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			fakeScheduler := newFakeScheduler(newFakeNodeResolver([]string{"node1"}))
			meta, err := json.Marshal(backup.DistributedBackupDescriptor{
				Status: backup.Transferring,
				ID:     beingCancely,
				Nodes:  map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Class1"}}},
			})
			require.NoError(t, err)

			fakeScheduler.backend.On("GetObject", mock.Anything, beingCancely, GlobalRestoreFile).Return(meta, nil)
			fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
			fakeScheduler.backend.On("PutObject", mock.Anything, beingCancely, GlobalRestoreFile, mock.Anything).Return(nil)
			fakeScheduler.selector.On("ListClasses", ctx).Return([]string{"Class1"})
			fakeScheduler.selector.On("Shards", ctx, "Class1").Return([]string{"node1"}, nil)
			fakeScheduler.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			s := fakeScheduler.scheduler()
			require.Empty(t, s.restorer.lastOp.renew(test.slotHolder, "", "", ""))

			require.NoError(t, s.CancelRestore(ctx, nil, backendName, beingCancely, "", ""))

			held := s.restorer.lastOp.get()
			require.Equal(t, test.slotHolder, held.ID, "the cancel must never take a slot over")
			require.Equal(t, test.wantStatus, held.Status, test.reason)
		})
	}
}
