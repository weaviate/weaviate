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

// TestCoordinatorRestoreReleaseOnlyClearsItsOwnSlot pins the release half of
// the slot-ownership invariant: the restore goroutine gives the slot back only
// while it still holds it. A newer restore can take the slot over while the
// old goroutine is still finishing (cancellation frees it, then another
// restore claims it), and clearing that claim reports the node idle while a
// restore is live.
//
// The takeover is staged from inside the participant Status call, which runs on
// the restore goroutine itself, before its deferred release — rather than from
// a particular meta write, which pins the test to how many times the
// coordinator happens to write its meta file.
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
		sresp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)

	tests := []struct {
		name string
		// steal reproduces a newer restore taking the slot over while the
		// goroutine of the previous one is still running.
		steal bool
		// newID is the id the newer restore claims the slot with. The retry
		// case reuses this restore's own id, which is what a cancel-then-retry
		// looks like and what an id-keyed ownership check cannot tell from the
		// first attempt still holding the slot.
		newID      string
		wantSlotID string
	}{
		{name: "slot still held by this restore", steal: false, wantSlotID: ""},
		{name: "slot taken over by a newer restore", steal: true, newID: "live-restore", wantSlotID: "live-restore"},
		{name: "slot taken over by a retry of the same id", steal: true, newID: backupID, wantSlotID: backupID},
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
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
			}
			fc.backend.On("HomeDir", anyArg, anyArg, backupID).Return("bucket/" + backupID)
			fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
			fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).Return(nil)

			c := fc.coordinator()
			c.timeoutNextRound = time.Millisecond

			var (
				once   sync.Once
				stolen = make(chan struct{})
			)
			for _, node := range nodes {
				fc.client.On("Status", anyArg, node, anyArg).Return(sresp, nil).
					Run(func(mock.Arguments) {
						// Runs on the restore goroutine, before its deferred
						// release. assert, not require: require's Goexit would
						// kill that goroutine mid-flight, surfacing as a hang or
						// an unrelated downstream failure instead of this one.
						if !tc.steal {
							return
						}
						once.Do(func() {
							assert.True(t, c.lastOp.setIfOwned(backupID, backup.Cancelled))
							assert.True(t, c.lastOp.resetIfCancelled(backupID))
							prevID, _ := c.lastOp.renew(tc.newID, "path", "", "")
							assert.Empty(t, prevID)
							close(stolen)
						})
					})
			}

			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			require.NoError(t, c.Restore(ctx, store, &req, desc, nil))

			if tc.steal {
				// The takeover runs on the restore goroutine, so wait for it
				// rather than racing the assertion against it.
				select {
				case <-stolen:
				case <-time.After(20 * time.Second):
					t.Fatal("the newer restore never got to claim the slot")
				}
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
// the other leaves half the invariant free to regress.
//
// The backup and restore coordinators are separate values with separate slots,
// and nothing outside Backup writes the backup one today: there is no
// CancelBackup, so the takeover below has no production path and is staged by
// hand. What the guard buys is that the backup side does not acquire this bug
// the day it grows a cancel — which is the natural place for one, and the
// restore side is what that would look like.
//
// The takeover is staged from inside the participant Status call, which runs on
// the backup goroutine itself, before its deferred release.
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
						assert.True(t, c.lastOp.setIfOwned(backupID, backup.Cancelled))
						assert.True(t, c.lastOp.resetIfCancelled(backupID))
						prevID, _ := c.lastOp.renew("live-backup", "path", "", "")
						assert.Empty(t, prevID)
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
		backendName    = "s3"
		beingCancelled = "restore-being-cancelled"
		stillRunning   = "restore-still-running"
	)

	tests := []struct {
		name       string
		slotHolder string
		wantStatus backup.Status
		reason     string
	}{
		{
			name:       "the slot is held by the restore being cancelled",
			slotHolder: beingCancelled,
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
				ID:     beingCancelled,
				Nodes:  map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Class1"}}},
			})
			require.NoError(t, err)

			fakeScheduler.backend.On("GetObject", mock.Anything, beingCancelled, GlobalRestoreFile).Return(meta, nil)
			fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
			fakeScheduler.backend.On("PutObject", mock.Anything, beingCancelled, GlobalRestoreFile, mock.Anything).Return(nil)
			fakeScheduler.selector.On("ListClasses", ctx).Return([]string{"Class1"})
			fakeScheduler.selector.On("Shards", ctx, "Class1").Return([]string{"node1"}, nil)
			fakeScheduler.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			s := fakeScheduler.scheduler()
			prevID, _ := s.restorer.lastOp.renew(test.slotHolder, "", "", "")
			require.Empty(t, prevID)

			require.NoError(t, s.CancelRestore(ctx, nil, backendName, beingCancelled, "", ""))

			held := s.restorer.lastOp.get()
			require.Equal(t, test.slotHolder, held.ID, "the cancel must never take a slot over")
			require.Equal(t, test.wantStatus, held.Status, test.reason)
		})
	}
}

// Restore's two synchronous error paths give the slot back before returning,
// and must give back only their own. The restorer slot has writers outside
// Restore — a cancel, and a retried Restore's early return — so between the
// claim and the error the slot can already belong to a newer restore, and
// clearing that claim reports the node idle while a restore is live.
func TestCoordinatorRestoreErrorPathReleasesOnlyItsOwnSlot(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		backupID    = "1"
		ctx         = context.Background()
		anyArg      = mock.Anything
		nodes       = []string{"N1", "N2"}
		classes     = []string{"C1"}
		cresp       = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
	)

	tests := []struct {
		name string
		// fail wires the mock that breaks one of the two error paths. Its hook
		// runs on the Restore call itself, after the claim and before the
		// error return, which is the window a takeover lands in.
		fail  func(fc *fakeCoordinator, hook func())
		steal bool
	}{
		{
			name: "canCommit refused",
			fail: func(fc *fakeCoordinator, hook func()) {
				fc.client.On("CanCommit", anyArg, anyArg, anyArg).Return(nil, ErrAny).
					Run(func(mock.Arguments) { hook() })
			},
		},
		{
			name: "initial meta write failed",
			fail: func(fc *fakeCoordinator, hook func()) {
				fc.client.On("CanCommit", anyArg, anyArg, anyArg).Return(cresp, nil)
				fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).
					Return(ErrAny).Run(func(mock.Arguments) { hook() })
			},
		},
	}

	for _, tc := range tests {
		for _, steal := range []bool{false, true} {
			name := tc.name
			wantSlotID := ""
			if steal {
				name += " (slot taken over)"
				wantSlotID = "live-restore"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				fc := newFakeCoordinator(newFakeNodeResolver(nodes))
				fc.backend.On("HomeDir", anyArg, anyArg, backupID).Return("bucket/" + backupID)
				fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
				fc.client.On("Abort", anyArg, anyArg, anyArg).Return(nil)

				c := fc.coordinator()
				var once sync.Once
				tc.fail(fc, func() {
					if !steal {
						return
					}
					// canCommit fans out to both nodes, so only the first
					// caller stages the takeover. assert, not require: this
					// runs inside a mock callback, and Goexit there would
					// surface as a hang instead of this failure.
					once.Do(func() {
						assert.True(t, c.lastOp.setIfOwned(backupID, backup.Cancelled))
						assert.True(t, c.lastOp.resetIfCancelled(backupID))
						prevID, _ := c.lastOp.renew("live-restore", "path", "", "")
						assert.Empty(t, prevID)
					})
				})

				desc := &backup.DistributedBackupDescriptor{
					ID:            backupID,
					Version:       Version,
					ServerVersion: config.ServerVersion,
					Nodes: map[string]*backup.NodeDescriptor{
						nodes[0]: {Classes: classes},
						nodes[1]: {Classes: classes},
					},
				}
				req := newReq(nil, backendName, backupID)
				store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}

				require.Error(t, c.Restore(ctx, store, &req, desc, nil))
				require.Equal(t, wantSlotID, c.lastOp.get().ID,
					"the failed restore released a slot it no longer owns")
			})
		}
	}
}

// The write half of the release invariant, and the reason the release check
// alone is not enough: the goroutine of a cancelled restore keeps writing to
// the slot for as long as it takes to unwind, and by then the slot can already
// belong to the restore that was started right after the cancel. Every one of
// those writes has to no-op.
//
// Left unchecked, a restore that has just started is reported to the API as
// SUCCESS, or as CANCELLED — which coordinator.commit reads as "cancelled
// externally" and acts on, aborting a restore nobody cancelled. The failure
// case reaches further still: the reason is remembered under whichever id holds
// the slot, so a poll for the new restore is answered with what happened to the
// old one.
func TestCoordinatorRestoreStaleGoroutineDoesNotStampANewerClaim(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		backupID    = "1"
		newID       = "live-restore"
		ctx         = context.Background()
		anyArg      = mock.Anything
		node        = "N1"
		classes     = []string{"C1"}
		cresp       = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
	)
	cancelled := marshalCoordinatorMeta(backup.DistributedBackupDescriptor{
		ID: backupID, Status: backup.Cancelled,
	})

	tests := []struct {
		name string
		// wire sets up the participant mocks and fires hook from the call the
		// takeover is staged in. Every hook runs on the goroutine (or the
		// Restore call) whose writes must stop at the claim boundary.
		wire func(fc *fakeCoordinator, hook func())
	}{
		{
			// The window the coordinator documents at its own error return,
			// which the set(TRANSFERRING) right after it sits in too.
			name: "staging begins after the takeover",
			wire: func(fc *fakeCoordinator, hook func()) {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil).
					Run(func(mock.Arguments) { hook() })
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
				fc.client.On("Status", anyArg, node, anyArg).Return(
					&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil)
				fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
			},
		},
		{
			name: "the restore ends successfully",
			wire: func(fc *fakeCoordinator, hook func()) {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil)
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
				fc.client.On("Status", anyArg, node, anyArg).Return(
					&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).
					Run(func(mock.Arguments) { hook() })
				fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
			},
		},
		{
			name: "the restore ends failed",
			wire: func(fc *fakeCoordinator, hook func()) {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil)
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
				fc.client.On("Status", anyArg, node, anyArg).Return(
					&StatusResponse{
						Status: backup.Failed, Err: "no space left on device",
						ID: backupID, Method: OpRestore,
					}, nil).
					Run(func(mock.Arguments) { hook() })
				fc.client.On("Abort", anyArg, anyArg, anyArg).Return(nil)
				fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
			},
		},
		{
			name: "a cancel turns up in object storage",
			wire: func(fc *fakeCoordinator, hook func()) {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil)
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
				fc.client.On("Status", anyArg, node, anyArg).Return(
					&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).
					Run(func(mock.Arguments) { hook() })
				// Nothing to cancel when Restore starts; the cancel lands while
				// the goroutine is in its commit phase.
				fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).
					Return(nil, backup.ErrNotFound{}).Once()
				fc.backend.On("GetObject", anyArg, backupID, GlobalRestoreFile).Return(cancelled, nil)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
			fc.backend.On("HomeDir", anyArg, anyArg, backupID).Return("bucket/" + backupID)
			fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).Return(nil)

			c := fc.coordinator()
			c.timeoutNextRound = time.Millisecond

			var (
				once   sync.Once
				stolen = make(chan struct{})
			)
			tc.wire(fc, func() {
				// assert, not require: this runs inside a mock callback, where
				// Goexit surfaces as a hang instead of this failure.
				once.Do(func() {
					assert.True(t, c.lastOp.setIfOwned(backupID, backup.Cancelled))
					assert.True(t, c.lastOp.resetIfCancelled(backupID))
					prevID, _ := c.lastOp.renew(newID, "path", "", "")
					assert.Empty(t, prevID)
					close(stolen)
				})
			})

			desc := &backup.DistributedBackupDescriptor{
				ID:            backupID,
				Version:       Version,
				ServerVersion: config.ServerVersion,
				Nodes:         map[string]*backup.NodeDescriptor{node: {Classes: classes}},
			}
			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			require.NoError(t, c.Restore(ctx, store, &req, desc, nil))

			select {
			case <-stolen:
			case <-time.After(20 * time.Second):
				t.Fatal("the newer restore never got to claim the slot")
			}
			require.Never(t, func() bool {
				st := c.lastOp.get()
				return st.ID != newID || st.Status != backup.Started || st.Err != ""
			}, 2*time.Second, 10*time.Millisecond,
				"the cancelled restore stamped the slot of the one that replaced it")

			_, remembered := c.lastOp.rememberedFailure(newID)
			require.False(t, remembered,
				"a poll for the new restore must not be answered with the old one's failure")
		})
	}
}
