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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// Pins that commit treats a cancel still in flight (CANCELLING) as a cancel,
// at both the guard before it starts and the one in its polling loop. The
// restore's other cancellation guard covers the same journey, so each of these
// has to be driven on its own or the two hide each other's regressions.
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
		// stampBeforeCommit puts the cancel on the slot before commit runs,
		// which is the guard commit opens with. Otherwise it lands on the
		// first participant poll, which is the guard in the polling loop.
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
				commits int
				polls   int
				mu      sync.Mutex
			)
			fc.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil).
				Run(func(mock.Arguments) {
					mu.Lock()
					commits++
					mu.Unlock()
				})
			// Staging that ends after a handful of polls, so a commit that does
			// not abort still finishes and fails on the assertion rather than
			// polling until the test times out.
			staging := func(*StatusRequest) bool {
				mu.Lock()
				defer mu.Unlock()
				polls++
				return polls < 5
			}
			fc.client.On("Status", mock.Anything, node, mock.MatchedBy(staging)).
				Return(&StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpRestore}, nil).
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

			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, tc.wantCommits, commits)
		})
	}
}

// Pins the restore's own cancellation guard, the one between staging and the
// schema apply. Driven on its own: the cancel arrives after commit has already
// returned, so commit's guards never see it.
func TestCoordinatorRestoreStopsBeforeSchemaApplyWhenTheCancelLandsAfterStaging(t *testing.T) {
	t.Parallel()
	const (
		backendName = "s3"
		backupID    = "1"
		node        = "N1"
	)

	backend := &restoreMetaBackend{}
	fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
	c := newCoordinator(&fc.selector, &fc.client, &fc.schema, fc.log, fc.nodeResolver, nil)
	c.timeoutNextRound = time.Millisecond
	schemaManager := &countingSchemaManager{}
	c.schema = schemaManager

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

// Pins the whole journey the cancel endpoint's first stamp exists for: a cancel
// arriving while a restore is staging makes that restore stop before the schema
// apply. Without the stamp the restore never learns of the cancel from this
// node, and keeps going.
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
	fs.client.On("Status", mock.Anything, node, mock.Anything).
		Return(&StatusResponse{Status: backup.Transferring, ID: backupID, Method: OpRestore}, nil)
	fs.client.On("Abort", mock.Anything, node, mock.Anything).Return(nil)

	s := fs.scheduler()
	s.restorer.timeoutNextRound = time.Millisecond
	schemaManager := &countingSchemaManager{}
	s.restorer.schema = schemaManager

	// ListClasses runs between the cancel's two slot stamps, so it is where the
	// first one can be observed before the second overwrites it.
	var (
		once       sync.Once
		stamped    backup.Status
		gaveItBack bool
	)
	fs.selector.On("ListClasses", ctx).Return([]string{class}).Run(func(mock.Arguments) {
		once.Do(func() {
			stamped = s.restorer.lastOp.get().Status
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if s.restorer.lastOp.get().ID == "" {
					gaveItBack = true
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	})

	store := coordStore{objectStore{fs.backend, backupID, "", "", ""}}
	req := newReq(nil, backendName, backupID)
	require.NoError(t, s.restorer.Restore(ctx, store, &req,
		restoreDescriptor(backupID, node), []backup.ClassDescriptor{{Name: class}}))
	require.Eventually(t, func() bool { return s.restorer.lastOp.get().Status == backup.Transferring },
		20*time.Second, time.Millisecond, "the restore never reached its staging phase")

	require.NoError(t, s.CancelRestore(ctx, nil, backendName, backupID, "", ""))

	require.Equal(t, backup.Cancelling, stamped,
		"the cancel must stamp the slot before aborting the participants, or the restore never learns of it")
	require.True(t, gaveItBack, "the cancelled restore never stopped")
	require.Zero(t, schemaManager.applies.Load(),
		"a restore cancelled while staging must not go on to apply the schema")
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
		name      string
		stamped   bool
		held      reqState
		wantLevel logrus.Level
		wantMsg   string
	}{
		{
			name:      "the stamp landed",
			stamped:   true,
			held:      reqState{ID: backupID, Status: backup.Transferring},
			wantLevel: logrus.InfoLevel,
			wantMsg:   "restore slot stamped with the cancellation",
		},
		{
			name:      "the restore already finished cancelling",
			held:      reqState{ID: backupID, Status: backup.Cancelled},
			wantLevel: logrus.InfoLevel,
			wantMsg:   "restore slot already carries the cancellation",
		},
		{
			name:      "the restore already finished and gave the slot back",
			held:      reqState{},
			wantLevel: logrus.InfoLevel,
			wantMsg:   "the restore has already finished and given it back",
		},
		{
			name:      "another restore holds the slot",
			held:      reqState{ID: "other-restore", Status: backup.Transferring},
			wantLevel: logrus.WarnLevel,
			wantMsg:   `it is held by restore "other-restore"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, hook := test.NewNullLogger()
			logCancelStamp(logger, backupID, backup.Cancelled, tc.stamped, tc.held)

			entry := hook.LastEntry()
			require.NotNil(t, entry)
			require.Equal(t, tc.wantLevel, entry.Level)
			require.Contains(t, entry.Message, tc.wantMsg)
			require.Equal(t, tc.held.ID, entry.Data["slot_holder"])
			require.Equal(t, tc.held.Status, entry.Data["slot_status"])
		})
	}
}
