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

// Pins that CancelRestore only stamps the slot it owns, not whichever restore
// the node's single slot happens to hold.
func TestCancelRestoreOnlyStampsTheSlotItOwns(t *testing.T) {
	t.Parallel()
	const (
		backendName    = "s3"
		beingCancelled = "restore-being-cancelled"
		stillRunning   = "restore-still-running"
	)

	// claimed then completed is what a cancel writes when it carries through.
	claimedThenCompleted := []backup.Status{backup.Cancelling, backup.Cancelled}

	tests := []struct {
		name       string
		slotHolder string
		// duringAbort runs while the nodes are being aborted, the last step
		// before the cancel stamps the slot.
		duringAbort func(t *testing.T, stat *backupStat)
		wantHolder  string
		wantStatus  backup.Status
		wantWrites  []backup.Status
		reason      string
	}{
		{
			name:       "the slot is held by the restore being cancelled",
			slotHolder: beingCancelled,
			wantHolder: beingCancelled,
			wantStatus: backup.Cancelled,
			wantWrites: claimedThenCompleted,
			reason: "this node coordinates the restore being cancelled; leaving its slot Started " +
				"makes OnStatus report a cancelled restore as running",
		},
		{
			name:       "the slot is held by a different, live restore",
			slotHolder: stillRunning,
			wantHolder: stillRunning,
			wantStatus: backup.Started,
			wantWrites: claimedThenCompleted,
			reason: "a cancel aimed at a different restore stamped this one Cancelled; commit() reads " +
				"that as 'cancelled externally' and aborts a restore nobody cancelled",
		},
		{
			// The id is the same, so only the claim tells the two apart.
			name:       "the slot is handed to a retry of the same id",
			slotHolder: beingCancelled,
			duringAbort: func(t *testing.T, stat *backupStat) {
				takeOverSlot(t, stat, beingCancelled, beingCancelled)
			},
			wantHolder: beingCancelled,
			wantStatus: backup.Started,
			wantWrites: claimedThenCompleted,
			reason: "the retry was aborted on every node, so leaving its descriptor on CANCELLING " +
				"answers 204 while refusing every later restore of this id",
		},
		{
			// Nothing left to stamp, everything left to write.
			name:       "the slot is released while the nodes are aborted",
			slotHolder: beingCancelled,
			duringAbort: func(t *testing.T, stat *backupStat) {
				cancelAndFreeSlot(t, stat, beingCancelled)
			},
			wantWrites: claimedThenCompleted,
			reason: "a released slot carries no cancellation to stamp, which is no reason to leave " +
				"the descriptor on CANCELLING",
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
			writes := recordRestoreMetaWrites(t, fakeScheduler, beingCancelled, nil)
			fakeScheduler.selector.On("ListClasses", ctx).Return([]string{"Class1"})
			fakeScheduler.selector.On("Shards", ctx, "Class1").Return([]string{"node1"}, nil)

			s := fakeScheduler.scheduler()
			prevID, _ := s.restorer.lastOp.renew(test.slotHolder, "", "", "")
			require.Empty(t, prevID)

			var once sync.Once
			fakeScheduler.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil).
				Run(func(mock.Arguments) {
					if test.duringAbort == nil {
						return
					}
					// assert, not require: abortAll fans out, so this runs on a
					// goroutine where Goexit surfaces as a hang.
					once.Do(func() { test.duringAbort(t, &s.restorer.lastOp) })
				})

			require.NoError(t, s.CancelRestore(ctx, nil, backendName, beingCancelled, "", ""))

			held := s.restorer.lastOp.get()
			require.Equal(t, test.wantHolder, held.ID, "the cancel must never take a slot over")
			require.Equal(t, test.wantStatus, held.Status, test.reason)
			require.Equal(t, test.wantWrites, writes.recorded(), test.reason)
		})
	}
}

// Pins what a cancel repeated on a CANCELLING descriptor does when a retry of
// the same id holds the slot: the stored descriptor decides, not the slot. One
// that has moved on proves the cancellation finished, so the retry is a restore
// nobody asked to stop. One that still reads CANCELLING is the stuck state the
// repeat exists to clear, and the 422 that refuses a restore during a
// cancellation tells operators to repeat the cancel for exactly that.
func TestCancelRestoreRepeatedWhileARetryOfTheSameIdHoldsTheSlot(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "abc"
	)

	tests := []struct {
		name string
		// reread is what the descriptor reads once the retry has started.
		reread     backup.Status
		wantAbort  bool
		wantWrites []backup.Status
		reason     string
	}{
		{
			name:   "the descriptor has moved on",
			reread: backup.Cancelled,
			reason: "the cancellation this call read has finished, so the id now runs a restore " +
				"nobody asked to stop",
		},
		{
			name:       "the descriptor still reads CANCELLING",
			reread:     backup.Cancelling,
			wantAbort:  true,
			wantWrites: []backup.Status{backup.Cancelled},
			reason: "a descriptor stuck on CANCELLING refuses every later restore of this id, and " +
				"the retry running under it cancels itself off that same descriptor",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			fs := newFakeScheduler(newFakeNodeResolver([]string{"node1"}))
			fs.selector.On("ListClasses", ctx).Return([]string{"Class1"})
			fs.selector.On("Shards", ctx, "Class1").Return([]string{"node1"}, nil)
			fs.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			storedRestoreMeta(fs, backupID, backup.Cancelling, test.reread)
			writes := recordRestoreMetaWrites(t, fs, backupID, nil)

			s := fs.scheduler()
			// The restore this cancel reads: already cancelled by the first
			// cancel, still holding the slot it is about to give back.
			prevID, slot := s.restorer.lastOp.renew(backupID, "path", "", "")
			require.Empty(t, prevID)
			require.True(t, slot.set(backup.Cancelled))

			// Initialize runs between the descriptor read this cancel acts on
			// and the abort it drives, which is the window the retry starts in.
			// It is called on the test's own goroutine, so require is safe here.
			var once sync.Once
			fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil).
				Run(func(mock.Arguments) {
					once.Do(func() {
						freeSlot(t, &s.restorer.lastOp, backupID)
						retryID, _ := s.restorer.lastOp.renew(backupID, "path", "", "")
						require.Empty(t, retryID, "the retry could not claim the freed slot")
					})
				})

			require.NoError(t, s.CancelRestore(ctx, nil, backendName, backupID, "", ""))

			held := s.restorer.lastOp.get()
			require.Equal(t, backupID, held.ID)
			require.Equal(t, backup.Started, held.Status, "the cancel stamped a retry it holds no claim on")
			require.Equal(t, test.wantWrites, writes.recorded(), test.reason)
			if test.wantAbort {
				fs.client.AssertCalled(t, "Abort", mock.Anything, mock.Anything, mock.Anything)
				return
			}
			fs.client.AssertNotCalled(t, "Abort", mock.Anything, mock.Anything, mock.Anything)
		})
	}
}

// The re-read above decides whether a repeated cancel aborts, and it lands in
// the window a coordinator writes the descriptor in, so it can come back
// half-written. Reading it once would treat a torn file as "the cancellation
// finished", skip the abort, and answer 204 with the descriptor still stuck on
// CANCELLING — the state that refuses every later restore of this id, which is
// the state the repeat exists to clear.
func TestCancelRestoreRepeatedSurvivesATornDescriptorReRead(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "abc"
	)
	ctx := context.Background()

	fs := newFakeScheduler(newFakeNodeResolver([]string{"node1"}))
	fs.selector.On("ListClasses", ctx).Return([]string{"Class1"})
	fs.selector.On("Shards", ctx, "Class1").Return([]string{"node1"}, nil)
	fs.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	stuck := marshalCoordinatorMeta(backup.DistributedBackupDescriptor{
		Status: backup.Cancelling,
		ID:     backupID,
		Nodes:  map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Class1"}}},
	})
	// The read this cancel acts on, then a descriptor caught mid-write, then
	// the same stuck descriptor once the write has landed.
	fs.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(stuck, nil).Once()
	fs.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).
		Return(stuck[:len(stuck)/2], nil).Once()
	fs.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(stuck, nil)
	writes := recordRestoreMetaWrites(t, fs, backupID, nil)

	s := fs.scheduler()
	prevID, slot := s.restorer.lastOp.renew(backupID, "path", "", "")
	require.Empty(t, prevID)
	require.True(t, slot.set(backup.Cancelled))

	// A retry takes the id over between the descriptor read and the abort, so
	// the slot alone cannot say whether the cancellation finished.
	var once sync.Once
	fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil).
		Run(func(mock.Arguments) {
			once.Do(func() {
				freeSlot(t, &s.restorer.lastOp, backupID)
				retryID, _ := s.restorer.lastOp.renew(backupID, "path", "", "")
				require.Empty(t, retryID, "the retry could not claim the freed slot")
			})
		})

	require.NoError(t, s.CancelRestore(ctx, nil, backendName, backupID, "", ""))

	fs.client.AssertCalled(t, "Abort", mock.Anything, mock.Anything, mock.Anything)
	require.Equal(t, []backup.Status{backup.Cancelled}, writes.recorded(),
		"a torn re-read is not evidence that the cancellation finished")
}

// Pins that the cancel's CANCELLING stamp goes through the claim it took next
// to its descriptor read, not through the id: a retry that claims the slot
// before the stamp is a restore this cancel never read.
func TestClaimCancellationDoesNotStampARetryOfTheSameId(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "abc"
	)
	ctx := context.Background()

	fs := newFakeScheduler(newFakeNodeResolver([]string{"node1"}))
	fs.selector.On("ListClasses", ctx).Return([]string{"Class1"})
	fs.selector.On("Shards", ctx, "Class1").Return([]string{"node1"}, nil)
	fs.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	storedRestoreMeta(fs, backupID, backup.Transferring)
	writes := recordRestoreMetaWrites(t, fs, backupID, nil)

	s := fs.scheduler()
	// The restore this cancel reads, still staging.
	prevID, _ := s.restorer.lastOp.renew(backupID, "path", "", "")
	require.Empty(t, prevID)

	// Initialize runs between the claim this cancel takes and the CANCELLING
	// it stamps, which is the window the retry takes the slot over in. It is
	// called on the test's own goroutine, so require is safe here.
	var once sync.Once
	fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil).
		Run(func(mock.Arguments) {
			once.Do(func() { takeOverSlot(t, &s.restorer.lastOp, backupID, backupID) })
		})

	require.NoError(t, s.CancelRestore(ctx, nil, backendName, backupID, "", ""))

	held := s.restorer.lastOp.get()
	require.Equal(t, backupID, held.ID)
	require.Equal(t, backup.Started, held.Status,
		"the cancel stamped CANCELLING on a retry it holds no claim on, so a poll reports a "+
			"restore nobody cancelled as cancelling")
	require.Equal(t, []backup.Status{backup.Cancelling, backup.Cancelled}, writes.recorded(),
		"the abort went out keyed on the id, so the descriptor has to record the outcome")
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
		fail func(fc *fakeCoordinator, hook func())
		// Both paths end at the one release site Restore has, so only one of
		// them needs the takeover staged.
		steals []bool
	}{
		{
			name:   "canCommit refused",
			steals: []bool{false, true},
			fail: func(fc *fakeCoordinator, hook func()) {
				fc.client.On("CanCommit", anyArg, anyArg, anyArg).Return(nil, ErrAny).
					Run(func(mock.Arguments) { hook() })
			},
		},
		{
			name:   "initial meta write failed",
			steals: []bool{false},
			fail: func(fc *fakeCoordinator, hook func()) {
				fc.client.On("CanCommit", anyArg, anyArg, anyArg).Return(cresp, nil)
				fc.backend.On("PutObject", anyArg, backupID, GlobalRestoreFile, anyArg).
					Return(ErrAny).Run(func(mock.Arguments) { hook() })
			},
		},
	}

	for _, tc := range tests {
		for _, steal := range tc.steals {
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
					// caller stages the takeover. assert, not require: on that
					// row the hook runs on an errgroup child, where Goexit
					// surfaces as a hang instead of this failure.
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
		// takeover is staged in.
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
			// A failed outcome is the one that would also leave a remembered
			// failure behind, which the retry must not be answered with.
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
					takeOverSlot(t, &c.lastOp, backupID, newID)
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

			awaitInterference(t, stolen, "the newer restore never got to claim the slot")
			// The takeover only opens the window; both rows end at the same
			// decision to stop, and waiting for it is what makes the absence
			// of a write below mean something.
			awaitLog(t, fc.logs, staleRestoreStopped)
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
			// require: polls run on errgroup children, where Goexit surfaces
			// as a hang.
			once.Do(func() {
				takeOverSlot(t, &c.lastOp, backupID, newID)
				close(stolen)
			})
		})

	startRestore(t, c, backend, backendName, backupID, node)

	awaitInterference(t, stolen, "the newer restore never got to claim the slot")
	// The takeover only opens the window; wait for the goroutine to reach the
	// decision itself, otherwise the absence of a read below proves nothing.
	awaitLog(t, fc.logs, staleRestoreStopped)
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
	// The takeover only opens the window; wait for the goroutine to reach the
	// decision itself, otherwise the absence of a write below proves nothing.
	awaitLog(t, fc.logs, staleOutcomeRefused)

	// The goroutine may still be unwinding, so give it the chance to apply the
	// schema or to report an outcome of its own. The retry owns the stored
	// descriptor now, so the restore that lost the slot leaves it as it found it.
	require.Never(t, func() bool {
		return backend.storedStatus(t) != backup.Transferring || schemaManager.applies.Load() != 0
	}, 500*time.Millisecond, 10*time.Millisecond,
		"the restore that lost its slot applied the schema or reported an outcome over the retry")
}

// Pins that the restore stops on both CANCELLING and CANCELLED read from
// storage, and that it leaves the slot as it found it on the way out. The
// deferred release clears the slot immediately after, so a poll landing in
// that window is answered TRANSFERRED and then, once the slot is gone, from
// the descriptor — which already reads the cancellation.
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
			// The decision to stop is logged just before the release, which is
			// the only moment the slot state below is observable.
			atStop := watchSlotAt(fc.log, &c.lastOp, restoreCancelledInStorage)
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

			held := atStop.await(t)
			require.Equal(t, backupID, held.ID)
			require.Equal(t, backup.Transferred, held.Status,
				"a restore reading its own cancellation back stamped the slot on its way out, "+
					"which the release right after makes pointless")

			require.Eventually(t, func() bool { return c.lastOp.get().ID == "" },
				10*time.Second, 10*time.Millisecond, "the restore goroutine never released its slot")
			require.Zero(t, schemaManager.applies.Load(),
				"a restore cancelled while it was staging must not go on to apply the schema")
			require.Equal(t, stored, backend.storedStatus(t),
				"the cancelled restore wrote over the cancellation in storage")
		})
	}
}
