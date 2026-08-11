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
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
)

// Pins that the restore goroutine releases only the slot it still holds; a
// newer restore can take the slot over while the old goroutine unwinds.
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
		// newID is the id the newer restore claims the slot with; the retry
		// case reuses this restore's own id.
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
			for _, node := range nodes {
				fc.client.On("Status", anyArg, node, anyArg).Return(sresp, nil)
			}

			c := fc.coordinator()
			c.timeoutNextRound = time.Millisecond

			var (
				once   sync.Once
				stolen = make(chan struct{})
			)
			// Staging the takeover from the outcome write puts it in the exact
			// window release has to refuse.
			fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).Return(nil).
				Run(func(args mock.Arguments) {
					if !tc.steal || restoreMetaStatus(t, args) != backup.Success {
						return
					}
					// assert, not require: this runs on the restore goroutine,
					// where Goexit surfaces as a hang instead of the failure.
					once.Do(func() {
						takeOverSlot(t, &c.lastOp, backupID, tc.newID)
						close(stolen)
					})
				})

			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			require.NoError(t, c.Restore(ctx, store, &req, desc, nil))

			if tc.steal {
				// The takeover runs on the restore goroutine, so wait for it
				// rather than racing the assertion against it.
				awaitInterference(t, stolen, "the newer restore never got to claim the slot")
				awaitOutcome(t, fc.backend.doneChan, "the restore goroutine never stored its outcome")
				require.Never(t, func() bool {
					return c.lastOp.get().ID != tc.wantSlotID
				}, 200*time.Millisecond, 10*time.Millisecond,
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

// Backup-side mirror of TestCoordinatorRestoreReleaseOnlyClearsItsOwnSlot; the
// takeover is staged by hand since nothing outside the uploader writes a backup
// slot.
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
			// The node descriptors the coordinator reads to total up the
			// backup's size, on the way to writing its own.
			fc.backend.On("GetObject", anyArg, anyArg, anyArg).
				Return(marshalMeta(backup.BackupDescriptor{Status: backup.Success}), nil)
			for _, node := range nodes {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil)
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
			}

			c := fc.coordinator()
			c.backends = &fakeBackupBackendProvider{backend: fc.backend}

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
						takeOverSlot(t, &c.lastOp, backupID, "live-backup")
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
				awaitOutcome(t, fc.backend.doneChan, "the backup goroutine never stored its outcome")
				require.Never(t, func() bool {
					return c.lastOp.get().ID != tc.wantSlotID
				}, 200*time.Millisecond, 20*time.Millisecond,
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

// Pins that CancelRestore only stamps the slot it owns, not whichever restore
// the node's single slot happens to hold.
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

// Pins that Restore's synchronous error paths release only the slot they
// still own, not one already taken over by a newer restore.
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
						takeOverSlot(t, &c.lastOp, backupID, "live-restore")
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

// Pins that a cancelled restore's goroutine, still writing while it unwinds,
// never stamps the slot a newer restore has since claimed.
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
	tests := []struct {
		name string
		// wire sets up the participant mocks and fires hook from the call the
		// takeover is staged in. polled fires on every participant poll, so
		// the test can wait for the stale goroutine to get far enough for its
		// writes to be worth asserting absent.
		wire func(fc *fakeCoordinator, hook, polled func())
	}{
		{
			// The window the coordinator documents at its own error return,
			// which the set(TRANSFERRING) right after it sits in too.
			name: "staging begins after the takeover",
			wire: func(fc *fakeCoordinator, hook, polled func()) {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil).
					Run(func(mock.Arguments) { hook() })
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
				fc.client.On("Status", anyArg, node, anyArg).Return(
					&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).
					Run(func(mock.Arguments) { polled() })
				fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
			},
		},
		{
			name: "the restore ends successfully",
			wire: func(fc *fakeCoordinator, hook, polled func()) {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil)
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
				fc.client.On("Status", anyArg, node, anyArg).Return(
					&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).
					Run(func(mock.Arguments) { polled(); hook() })
				fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
			},
		},
		{
			name: "the restore ends failed",
			wire: func(fc *fakeCoordinator, hook, polled func()) {
				fc.client.On("CanCommit", anyArg, node, anyArg).Return(cresp, nil)
				fc.client.On("Commit", anyArg, node, anyArg).Return(nil)
				fc.client.On("Status", anyArg, node, anyArg).Return(
					&StatusResponse{
						Status: backup.Failed, Err: "no space left on device",
						ID: backupID, Method: OpRestore,
					}, nil).
					Run(func(mock.Arguments) { polled(); hook() })
				fc.client.On("Abort", anyArg, anyArg, anyArg).Return(nil)
				fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
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
				polls  atomic.Int64
			)
			tc.wire(fc, func() {
				// assert, not require: this runs inside a mock callback, where
				// Goexit surfaces as a hang instead of this failure.
				once.Do(func() {
					takeOverSlot(t, &c.lastOp, backupID, newID)
					close(stolen)
				})
			}, func() { polls.Add(1) })

			desc := &backup.DistributedBackupDescriptor{
				ID:            backupID,
				Version:       Version,
				ServerVersion: config.ServerVersion,
				Nodes:         map[string]*backup.NodeDescriptor{node: {Classes: classes}},
			}
			req := newReq(nil, backendName, backupID)
			store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
			require.NoError(t, c.Restore(ctx, store, &req, desc, nil))

			awaitInterference(t, stolen, "the newer restore never got to claim the slot")
			// Without this the window below can close before the stale
			// goroutine ever reaches a write, which would make its absence
			// prove nothing.
			require.Eventually(t, func() bool { return polls.Load() > 0 },
				20*time.Second, time.Millisecond,
				"the stale restore never got past staging")
			require.Never(t, func() bool {
				st := c.lastOp.get()
				return st.ID != newID || st.Status != backup.Started || st.Err != ""
			}, 200*time.Millisecond, 10*time.Millisecond,
				"the cancelled restore stamped the slot of the one that replaced it")

			_, remembered := c.lastOp.rememberedFailure(newID)
			require.False(t, remembered,
				"a poll for the new restore must not be answered with the old one's failure")
		})
	}
}

// Pins that a restore losing its slot to the read that also hands it a
// cancellation does not stamp that cancellation on the claim that replaced it.
func TestCoordinatorRestoreStaleGoroutineDoesNotStampACancellationItReadFromStorage(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		newID       = "live-restore"
		node        = "N1"
	)

	schemaManager := &countingSchemaManager{}
	c, fc := newStagingRestore(node, backupID, schemaManager)
	fc.client.On("Status", mock.Anything, node, mock.Anything).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil)

	// Read one answers Restore itself; read two is the post-commit check for a
	// cancellation, which is where both the cancel and the takeover land.
	stolen := make(chan struct{})
	backend := &restoreMetaBackend{}
	backend.onRead = func(n int) {
		if n != 2 {
			return
		}
		// assert, not require: this runs on the restore goroutine, where
		// Goexit surfaces as a hang instead of this failure.
		backend.setStored(t, backup.DistributedBackupDescriptor{ID: backupID, Status: backup.Cancelled})
		takeOverSlot(t, &c.lastOp, backupID, newID)
		close(stolen)
	}

	startRestore(t, c, backend, backendName, backupID, node)

	awaitInterference(t, stolen, "the newer restore never got to claim the slot")
	require.Never(t, func() bool {
		st := c.lastOp.get()
		return st.ID != newID || st.Status != backup.Started || schemaManager.applies.Load() != 0
	}, 500*time.Millisecond, 10*time.Millisecond,
		"the cancelled restore stamped its cancellation on the slot of the one that replaced it")
}

// Pins that a restore which lost its slot stops without reading the stored
// descriptor again. That descriptor belongs to the restore holding the slot
// now, and every step after this one is driven by what the read says.
func TestCoordinatorRestoreStaleGoroutineStopsBeforeRereadingTheStoredMeta(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		newID       = "live-restore"
		node        = "N1"
	)

	schemaManager := &countingSchemaManager{}
	c, fc := newStagingRestore(node, backupID, schemaManager)

	backend := &restoreMetaBackend{}
	var (
		once   sync.Once
		stolen = make(chan struct{})
	)
	fc.client.On("Status", mock.Anything, node, mock.Anything).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).
		Run(func(mock.Arguments) {
			// The commit phase is the step right before the slot check, so a
			// takeover staged here lands in exactly that gap. assert, not
			// require: Goexit on the restore goroutine surfaces as a hang.
			once.Do(func() {
				takeOverSlot(t, &c.lastOp, backupID, newID)
				close(stolen)
			})
		})

	startRestore(t, c, backend, backendName, backupID, node)

	awaitInterference(t, stolen, "the newer restore never got to claim the slot")
	// Read one is the one Restore itself does before claiming the slot.
	require.Never(t, func() bool {
		return backend.readCount() > 1 || schemaManager.applies.Load() != 0
	}, 500*time.Millisecond, 10*time.Millisecond,
		"the restore that lost the slot went on working with the stored descriptor")
}

// countingSchemaManager counts the schema applies a restore performs, which is
// the step a cancel has to land before.
type countingSchemaManager struct {
	fakeSchemaManger
	applies atomic.Int32
}

// newStagingRestore wires a coordinator with one participant that has
// accepted the commit and is staging.
func newStagingRestore(node, backupID string, schema schemaManger) (*coordinator, *fakeCoordinator) {
	fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
	c := newCoordinator(&fc.selector, &fc.client, schema, fc.log, fc.nodeResolver, nil)
	c.timeoutNextRound = time.Millisecond
	fc.client.On("CanCommit", mock.Anything, node, mock.Anything).
		Return(&CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}, nil)
	fc.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil)
	return c, fc
}

// startRestore runs the synchronous half of a restore of one class on one node.
func startRestore(t *testing.T, c *coordinator, backend modulecapabilities.BackupBackend, backendName, backupID, node string) {
	t.Helper()
	req := newReq(nil, backendName, backupID)
	store := coordStore{objectStore{backend, backupID, "", "", ""}}
	require.NoError(t, c.Restore(context.Background(), store, &req,
		restoreDescriptor(backupID, node), []backup.ClassDescriptor{{Name: "C1"}}))
}

// awaitInterference waits for the staged interference before assertions run.
func awaitInterference(t *testing.T, done <-chan struct{}, missed string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal(missed)
	}
}

// awaitOutcome waits for the operation to store its outcome, the step right
// before its deferred release, so a "release did not fire" window can't close
// before the goroutine ever reached it.
func awaitOutcome(t *testing.T, stored <-chan bool, missed string) {
	t.Helper()
	select {
	case <-stored:
	case <-time.After(20 * time.Second):
		t.Fatal(missed)
	}
}

// restoreMetaStatus is the status of the descriptor a PutObject mock was
// handed.
func restoreMetaStatus(t *testing.T, args mock.Arguments) backup.Status {
	t.Helper()
	var desc backup.DistributedBackupDescriptor
	// assert, not require: this runs on the restore goroutine, where Goexit
	// surfaces as a hang instead of the failure.
	assert.NoError(t, json.Unmarshal(args.Get(3).([]byte), &desc))
	return desc.Status
}

func (c *countingSchemaManager) RestoreClass(context.Context, *backup.ClassDescriptor, map[string]string, bool, bool) error {
	c.applies.Add(1)
	return nil
}

// Pins that a restore whose slot was handed to a retry stops instead of
// applying the schema over that retry.
func TestCoordinatorRestoreStopsWhenTheFinalizingWriteIsRefused(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		retryID     = "live-restore"
		node        = "N1"
	)

	schemaManager := &countingSchemaManager{}
	c, fc := newStagingRestore(node, backupID, schemaManager)
	fc.client.On("Status", mock.Anything, node, mock.Anything).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil)

	// Read one answers Restore itself, read two is the one the goroutine does
	// right before deciding to finalize, so the takeover lands in exactly that
	// gap.
	interfered := make(chan struct{})
	backend := &restoreMetaBackend{onRead: func(n int) {
		if n != 2 {
			return
		}
		takeOverSlot(t, &c.lastOp, backupID, retryID)
		close(interfered)
	}}

	startRestore(t, c, backend, backendName, backupID, node)

	awaitInterference(t, interfered, "the restore never reached the finalizing decision")
	require.Eventually(t, func() bool { return c.lastOp.get().ID == retryID },
		10*time.Second, 10*time.Millisecond, "the restore goroutine never stopped")

	// The goroutine may still be unwinding, so give it the chance to apply the
	// schema or to report an outcome of its own. The retry owns the stored
	// descriptor now, so the restore that lost the slot leaves it as it found it.
	require.Never(t, func() bool {
		return backend.storedStatus(t) != backup.Transferring || schemaManager.applies.Load() != 0
	}, 500*time.Millisecond, 10*time.Millisecond,
		"the restore that lost its slot applied the schema or reported an outcome over the retry")
}

// Pins that a restore recognises CANCELLING (not just CANCELLED) and stops
// before applying the schema.
func TestCoordinatorRestoreCancelInFlightStopsBeforeSchemaApply(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		backupID    = "1"
		ctx         = context.Background()
		anyArg      = mock.Anything
		node        = "N1"
		classes     = []string{"C1"}
	)

	fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
	fc.backend.On("HomeDir", anyArg, anyArg, backupID).Return("bucket/" + backupID)
	fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
	fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).Return(nil)
	fc.client.On("CanCommit", anyArg, node, anyArg).
		Return(&CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}, nil)
	fc.client.On("Commit", anyArg, node, anyArg).Return(nil)

	c := fc.coordinator()
	c.timeoutNextRound = time.Millisecond
	schemaManager := &countingSchemaManager{}
	c.schema = schemaManager

	var once sync.Once
	fc.client.On("Status", anyArg, node, anyArg).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).
		Run(func(mock.Arguments) {
			// What CancelRestore does to the slot before it aborts the
			// participants. assert, not require: Goexit inside a mock callback
			// surfaces as a hang instead of this failure.
			once.Do(func() {
				stamped, _ := c.lastOp.setIfOwned(backupID, backup.Cancelling)
				assert.True(t, stamped)
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
	require.NoError(t, c.Restore(ctx, store, &req, desc,
		[]backup.ClassDescriptor{{Name: classes[0]}}))

	require.Eventually(t, func() bool { return c.lastOp.get().ID == "" },
		10*time.Second, 10*time.Millisecond, "the restore goroutine never released its slot")

	require.Zero(t, schemaManager.applies.Load(),
		"a restore cancelled while it was staging must not go on to apply the schema")
	require.Equal(t, backup.Cancelled, fc.backend.getGlobalMetaStatus())
}

// Pins that the restore stops on both CANCELLING and CANCELLED read from
// storage.
func TestCoordinatorRestoreStopsOnACancellationInStorage(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		node        = "N1"
	)

	for _, stored := range []backup.Status{backup.Cancelling, backup.Cancelled} {
		t.Run(string(stored), func(t *testing.T) {
			t.Parallel()
			schemaManager := &countingSchemaManager{}
			c, fc := newStagingRestore(node, backupID, schemaManager)
			// The cancel lands in storage while the participants are still
			// staging, which is where another coordinator's does, and the
			// restore reads it once staging is done.
			backend := &restoreMetaBackend{}
			var once sync.Once
			fc.client.On("Status", mock.Anything, node, mock.Anything).
				Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil).
				Run(func(mock.Arguments) {
					once.Do(func() {
						backend.setStored(t, backup.DistributedBackupDescriptor{ID: backupID, Status: stored})
					})
				})

			startRestore(t, c, backend, backendName, backupID, node)

			require.Eventually(t, func() bool { return c.lastOp.get().ID == "" },
				10*time.Second, 10*time.Millisecond, "the restore goroutine never released its slot")
			require.Zero(t, schemaManager.applies.Load(),
				"a restore cancelled while it was staging must not go on to apply the schema")
			require.Equal(t, stored, backend.storedStatus(t),
				"the cancelled restore wrote over the cancellation in storage")
		})
	}
}
