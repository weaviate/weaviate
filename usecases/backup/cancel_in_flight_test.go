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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// Pins that commit treats an in-flight cancel (CANCELLING) as a cancel, both
// before it starts and in its polling loop.
func TestCoordinatorCommitAbortsOnACancelInFlight(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		node        = "N1"
	)
	ctx := context.Background()

	tests := []struct {
		name string
		// If false, the cancel lands during the first participant poll instead.
		stampBeforeCommit bool
		wantCommits       int
	}{
		{name: "the cancel is already on the slot", stampBeforeCommit: true, wantCommits: 0},
		{name: "the cancel lands while polling", stampBeforeCommit: false, wantCommits: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
			c := fc.coordinator()
			c.timeoutNextRound = time.Millisecond

			prevID, slot := c.lastOp.renew(backupID, "path", "", "")
			require.Empty(t, prevID)

			var (
				once    sync.Once
				commits atomic.Int64
			)
			fc.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil).
				Run(func(mock.Arguments) { commits.Add(1) })
			// Staging ends after a handful of polls so a commit that fails to
			// abort still finishes instead of timing out.
			fc.client.On("Status", mock.Anything, node, mock.Anything).
				Return(&StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpRestore}, nil).
				Times(4).
				Run(func(mock.Arguments) {
					// assert, not require: Goexit inside a mock callback surfaces
					// as a hang instead of this failure.
					once.Do(func() {
						stamped, _ := c.lastOp.setIfOwned(backupID, backup.Cancelling)
						assert.True(t, stamped)
					})
				})
			fc.client.On("Status", mock.Anything, node, mock.Anything).
				Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil)

			if tc.stampBeforeCommit {
				stamped, _ := c.lastOp.setIfOwned(backupID, backup.Cancelling)
				require.True(t, stamped)
			}

			op := newOperation(&backup.DistributedBackupDescriptor{
				ID:          backupID,
				NodeMapping: map[string]string{},
				Nodes:       map[string]*backup.NodeDescriptor{node: {Classes: []string{"C1"}}},
			})
			op.participants[node] = participantStatus{Status: backup.Transferring, LastTime: time.Now()}

			req := &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName}
			c.commit(ctx, op, req, map[string]string{node: node}, true, slot)

			require.Equal(t, backup.Cancelled, op.descriptor.Status,
				"a cancel in flight is a cancel: commit must not carry on to Transferred")
			require.Equal(t, errCancelled.Error(), op.descriptor.Error)
			require.Equal(t, int64(tc.wantCommits), commits.Load())
		})
	}
}

// Pins the restore's cancellation guard between staging and schema apply.
func TestCoordinatorRestoreStopsBeforeSchemaApplyWhenTheCancelLandsAfterStaging(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		node        = "N1"
	)

	backend := &restoreMetaBackend{}
	fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
	schemaManager := &countingSchemaManager{}
	c := newCoordinator(&fc.selector, &fc.client, schemaManager, fc.log, fc.nodeResolver, nil)
	c.timeoutNextRound = time.Millisecond

	fc.client.On("CanCommit", mock.Anything, node, mock.Anything).
		Return(&CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}, nil)
	fc.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil)
	fc.client.On("Status", mock.Anything, node, mock.Anything).
		Return(&StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}, nil)

	// Read one is the check Restore opens with; read two is the one right after
	// staging ends, which is where a cancel claimed on another node lands.
	backend.onRead = func(n int) {
		if n != 2 {
			return
		}
		stamped, _ := c.lastOp.setIfOwned(backupID, backup.Cancelling)
		assert.True(t, stamped)
	}

	store := coordStore{objectStore{backend, backupID, "", "", ""}}
	req := newReq(nil, backendName, backupID)
	require.NoError(t, c.Restore(context.Background(), store, &req,
		restoreDescriptor(backupID, node), []backup.ClassDescriptor{{Name: "C1"}}))

	require.Eventually(t, func() bool { return c.lastOp.get().ID == "" },
		20*time.Second, 10*time.Millisecond, "the restore goroutine never released its slot")

	require.Zero(t, schemaManager.applies.Load(),
		"a restore cancelled before the schema apply must not go on to apply it")
	require.Equal(t, backup.Cancelled, backend.storedStatus(t))
	require.NotContains(t, backend.storedStatuses(t), backup.Finalizing,
		"FINALIZING is the point past which a cancel is refused; a cancelled restore must never reach it")
}

// Pins that a cancel arriving while a restore is staging stops it before schema apply.
func TestCancelRestoreStopsARestoreThatIsStillStaging(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		node        = "node1"
		class       = "Class1"
	)
	ctx := context.Background()

	fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, backupID).Return("bucket/" + backupID)
	fs.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
	fs.backend.On("PutObject", mock.Anything, backupID, GlobalRestoreFile, mock.Anything).Return(nil)
	fs.backend.On("Initialize", mock.Anything, backupID).Return(nil)
	fs.selector.On("Shards", ctx, class).Return([]string{node}, nil)
	fs.client.On("CanCommit", mock.Anything, node, mock.Anything).
		Return(&CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}, nil)
	fs.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil)
	fs.client.On("Abort", mock.Anything, node, mock.Anything).Return(nil)

	// The restore releases the slot as soon as its poll loop sees the cancel,
	// which would empty the slot before the read below. Parking the loop inside
	// its participant poll holds it still for the length of the cancel without
	// touching the path under test.
	pollGate := make(chan struct{})
	unblockPoll := sync.OnceFunc(func() { close(pollGate) })
	t.Cleanup(unblockPoll)
	fs.client.On("Status", mock.Anything, node, mock.Anything).
		Return(&StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpRestore}, nil).
		Run(func(mock.Arguments) { <-pollGate })

	s := fs.scheduler()
	s.restorer.timeoutNextRound = time.Millisecond

	// ListClasses runs between the cancel's two slot stamps, so only the first
	// is reliably observed here.
	var (
		once    sync.Once
		stamped backup.Status
	)
	fs.selector.On("ListClasses", ctx).Return([]string{class}).Run(func(mock.Arguments) {
		once.Do(func() { stamped = s.restorer.lastOp.get().Status })
	})

	store := coordStore{objectStore{fs.backend, backupID, "", "", ""}}
	req := newReq(nil, backendName, backupID)
	require.NoError(t, s.restorer.Restore(ctx, store, &req,
		restoreDescriptor(backupID, node), []backup.ClassDescriptor{{Name: class}}))
	require.Eventually(t, func() bool { return s.restorer.lastOp.get().Status == backup.Transferring },
		20*time.Second, time.Millisecond, "the restore never reached its staging phase")

	require.NoError(t, s.CancelRestore(ctx, nil, backendName, backupID, "", ""))

	require.True(t, stamped.IsCancellation(),
		"the cancel must stamp the slot before aborting the participants, or the restore never learns of it")

	unblockPoll()
	require.Eventually(t, func() bool { return s.restorer.lastOp.get().ID == "" },
		20*time.Second, time.Millisecond, "the cancelled restore never stopped")
}

// Pins that a restore applying its schema refuses a cancel even when the
// stored descriptor still reads TRANSFERRING.
func TestCancelRestoreRefusesARestoreThatIsApplyingItsSchema(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		node        = "node1"
		class       = "Class1"
	)
	ctx := context.Background()

	stale := marshalCoordinatorMeta(backup.DistributedBackupDescriptor{
		ID:     backupID,
		Status: backup.Transferring,
		Nodes:  map[string]*backup.NodeDescriptor{node: {Classes: []string{class}}},
	})

	fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
	fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
	fs.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(stale, nil)
	fs.backend.On("PutObject", mock.Anything, backupID, GlobalRestoreFile, mock.Anything).Return(nil)
	// No participant stubs: the refusal comes before a single node is reached,
	// which is what the AssertNotCalled below pins.

	s := fs.scheduler()
	prevID, slot := s.restorer.lastOp.renew(backupID, "path", "", "")
	require.Empty(t, prevID)
	require.True(t, slot.set(backup.Finalizing))

	err := s.CancelRestore(ctx, nil, backendName, backupID, "", "")
	require.Error(t, err)
	require.IsType(t, backup.ErrUnprocessable{}, err)
	require.Contains(t, err.Error(), "cannot be cancelled")

	require.Equal(t, backup.Finalizing, s.restorer.lastOp.get().Status)
	fs.client.AssertNotCalled(t, "Abort", mock.Anything, mock.Anything, mock.Anything)
	fs.backend.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// Pins that a coordinator whose CANCELLING claim is overtaken by one that
// already finished cancelling does not carry the cancellation out itself.
func TestClaimCancellationLosesToACancellationAlreadyFinished(t *testing.T) {
	t.Parallel()
	const backupID = "1"

	tests := []struct {
		name string
		// afterClaim is what storage reads once this coordinator has written
		// its CANCELLING claim.
		afterClaim backup.Status
		wantWon    bool
		wantSlot   backup.Status
	}{
		{
			name:       "the claim stands",
			afterClaim: backup.Cancelling,
			wantWon:    true,
			wantSlot:   backup.Cancelling,
		},
		{
			name:       "another coordinator already finished the cancellation",
			afterClaim: backup.Cancelled,
			wantWon:    false,
			wantSlot:   backup.Transferring,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := &restoreMetaBackend{}
			backend.onRead = func(int) {
				backend.setStored(t, backup.DistributedBackupDescriptor{ID: backupID, Status: tc.afterClaim})
			}
			store := coordStore{objectStore{backend, backupID, "", "", ""}}

			s := newFakeScheduler(newFakeNodeResolver([]string{"node1"})).scheduler()
			prevID, slot := s.restorer.lastOp.renew(backupID, "path", "", "")
			require.Empty(t, prevID)
			require.True(t, slot.set(backup.Transferring))

			meta := &backup.DistributedBackupDescriptor{ID: backupID, Status: backup.Transferring}
			won, _, err := s.claimCancellation(context.Background(), store, meta, backupID, "", "")
			require.NoError(t, err)
			require.Equal(t, tc.wantWon, won)
			require.Equal(t, tc.wantSlot, s.restorer.lastOp.get().Status,
				"a coordinator that did not win the claim must not stamp the slot")
		})
	}
}

// Pins that a cancel does not inherit the reason of the failure it lands on: a
// restore can fail on disk while the cancel that overtakes it is in flight.
func TestSetIfOwnedDropsTheReasonOfTheStatusItReplaces(t *testing.T) {
	t.Parallel()
	const (
		id     = "restore-1"
		reason = "no space left on device"
	)

	var s backupStat
	prevID, slot := s.renew(id, "path", "", "")
	require.Empty(t, prevID)
	require.True(t, slot.setFailed(reason))

	stamped, held := s.setIfOwned(id, backup.Cancelling)
	require.True(t, stamped)
	require.Equal(t, backup.Failed, held.Status, "the state reported is the one the stamp decided on")

	got := s.get()
	require.Equal(t, backup.Cancelling, got.Status)
	require.Empty(t, got.Err,
		"a poll would read CANCELLING next to a disk-space error that is not why it is cancelling")

	remembered, ok := s.rememberedFailure(id)
	require.True(t, ok, "the failure itself is still the answer once the slot is gone")
	require.Equal(t, reason, remembered)
}

// Pins that the cancel log calls out only the one outcome that is an anomaly.
// Warning on the ordinary ones trains operators to ignore the row that matters.
func TestLogCancelStampSeparatesTheAnomalyFromTheOrdinaryOutcomes(t *testing.T) {
	t.Parallel()
	const backupID = "restore-1"

	tests := []struct {
		name string
		// st is the status the cancel tried to stamp: CANCELLING when the
		// claim is taken, CANCELLED once the nodes have been aborted.
		st        backup.Status
		stamped   bool
		held      reqState
		wantLevel logrus.Level
		wantMsg   string
	}{
		{
			name:      "the stamp landed",
			st:        backup.Cancelling,
			stamped:   true,
			held:      reqState{ID: backupID, Status: backup.Transferring},
			wantLevel: logrus.InfoLevel,
			wantMsg:   "restore slot stamped with the cancellation",
		},
		{
			// A cancel in flight must not read as the anomaly the next row is.
			name:      "the restore is already cancelling",
			st:        backup.Cancelling,
			held:      reqState{ID: backupID, Status: backup.Cancelling},
			wantLevel: logrus.InfoLevel,
			wantMsg:   "restore slot already carries the cancellation",
		},
		{
			name:      "the restore already finished cancelling",
			st:        backup.Cancelled,
			held:      reqState{ID: backupID, Status: backup.Cancelled},
			wantLevel: logrus.InfoLevel,
			wantMsg:   "restore slot already carries the cancellation",
		},
		{
			name:      "the restore is applying its schema",
			st:        backup.Cancelling,
			held:      reqState{ID: backupID, Status: backup.Finalizing},
			wantLevel: logrus.WarnLevel,
			wantMsg:   "can no longer be cancelled",
		},
		{
			name:      "no restore holds the slot",
			st:        backup.Cancelled,
			held:      reqState{},
			wantLevel: logrus.InfoLevel,
			wantMsg:   "no restore holds the slot on this node",
		},
		{
			name:      "another restore holds the slot",
			st:        backup.Cancelled,
			held:      reqState{ID: "other-restore", Status: backup.Transferring},
			wantLevel: logrus.WarnLevel,
			wantMsg:   `it is held by restore "other-restore"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, hook := test.NewNullLogger()
			logCancelStamp(logger, backupID, tc.st, tc.stamped, tc.held)

			entry := hook.LastEntry()
			require.NotNil(t, entry)
			require.Equal(t, tc.wantLevel, entry.Level)
			require.Contains(t, entry.Message, tc.wantMsg)
			require.Equal(t, tc.st, entry.Data["cancel_status"])
			require.Equal(t, tc.held.ID, entry.Data["slot_holder"])
			require.Equal(t, tc.held.Status, entry.Data["slot_status"])
		})
	}
}
