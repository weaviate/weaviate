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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// resetIfCancelled must clear the slot only for an operation that has fully
// cancelled, not one still Cancelling or running under the same id.
func TestBackupStatResetIfCancelled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		claimedID  string
		status     backup.Status
		resetID    string
		wantOK     bool
		wantSlotID string
	}{
		{
			name:       "cancelled op with the id being released",
			claimedID:  "op-1",
			status:     backup.Cancelled,
			resetID:    "op-1",
			wantOK:     true,
			wantSlotID: "",
		},
		{
			name:       "op mid-cancel with the id being released",
			claimedID:  "op-1",
			status:     backup.Cancelling,
			resetID:    "op-1",
			wantOK:     false,
			wantSlotID: "op-1",
		},
		{
			name:       "live op sharing the id being released",
			claimedID:  "op-1",
			status:     backup.Transferring,
			resetID:    "op-1",
			wantOK:     false,
			wantSlotID: "op-1",
		},
		{
			name:       "cancelled op under a different id",
			claimedID:  "op-2",
			status:     backup.Cancelled,
			resetID:    "op-1",
			wantOK:     false,
			wantSlotID: "op-2",
		},
		{
			name:       "slot is free",
			claimedID:  "",
			resetID:    "op-1",
			wantOK:     false,
			wantSlotID: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s backupStat
			if tc.claimedID != "" {
				prevID, slot := s.renew(tc.claimedID, "path", "bucket", "override")
				require.Empty(t, prevID)
				require.True(t, slot.set(tc.status))
			}

			freed, _ := s.resetIfCancelled(tc.resetID)
			require.Equal(t, tc.wantOK, freed)
			require.Equal(t, tc.wantSlotID, s.get().ID)
		})
	}
}

// release must free the slot only while its claim still holds it, even when a
// retry reuses the same backup id under a new claim.
func TestSlotOwnerRelease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// arrange sets the slot up and returns the claim held by the
		// operation that is about to release.
		arrange    func(t *testing.T, s *backupStat) slotOwner
		wantOK     bool
		wantSlotID string
	}{
		{
			name: "holder releases its own slot",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				_, slot := s.renew("op-1", "path", "", "")
				slot.set(backup.Success)
				return slot
			},
			wantOK:     true,
			wantSlotID: "",
		},
		{
			name: "slot taken over by a different operation",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				_, slot := s.renew("op-1", "path", "", "")
				slot.release()
				prevID, _ := s.renew("op-2", "path", "", "")
				require.Empty(t, prevID)
				return slot
			},
			wantOK:     false,
			wantSlotID: "op-2",
		},
		{
			// Cancel a restore, then retry it under the same id: a normal
			// flow, and the one an id-keyed check cannot tell apart from the
			// first attempt still holding the slot.
			name: "slot taken over by a retry of the same id",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				_, slot := s.renew("op-1", "path", "", "")
				slot.set(backup.Cancelled)
				freeSlot(t, s, "op-1")
				prevID, _ := s.renew("op-1", "path", "", "")
				require.Empty(t, prevID, "the retry could not claim the freed slot")
				return slot
			},
			wantOK:     false,
			wantSlotID: "op-1",
		},
		{
			name: "slot already released",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				_, slot := s.renew("op-1", "path", "", "")
				slot.release()
				return slot
			},
			wantOK:     false,
			wantSlotID: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s backupStat
			slot := tc.arrange(t, &s)

			require.Equal(t, tc.wantOK, slot.release())
			require.Equal(t, tc.wantSlotID, s.get().ID)
		})
	}
}

// setIfOwned must stamp only the matching id, and must never walk a finished
// cancel back to Cancelling.
func TestBackupStatSetIfOwned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		claimedID  string
		status     backup.Status
		setID      string
		set        backup.Status
		wantOK     bool
		wantStatus backup.Status
	}{
		{
			name:       "holder gets the status",
			claimedID:  "op-1",
			status:     backup.Transferring,
			setID:      "op-1",
			set:        backup.Cancelling,
			wantOK:     true,
			wantStatus: backup.Cancelling,
		},
		{
			name:       "slot held by a different operation",
			claimedID:  "op-2",
			status:     backup.Transferring,
			setID:      "op-1",
			set:        backup.Cancelled,
			wantOK:     false,
			wantStatus: backup.Transferring,
		},
		{
			// A cancel reading the older CANCELLING out of storage after
			// commit already stamped the slot CANCELLED. Letting it through
			// reports a finished cancel as still in progress.
			name:       "cancelled is terminal",
			claimedID:  "op-1",
			status:     backup.Cancelled,
			setID:      "op-1",
			set:        backup.Cancelling,
			wantOK:     false,
			wantStatus: backup.Cancelled,
		},
		{
			// The cancel endpoint's own write, aimed at a restore that has
			// started applying its schema and can no longer be stopped.
			name:       "a schema apply refuses the cancel stamp",
			claimedID:  "op-1",
			status:     backup.Finalizing,
			setID:      "op-1",
			set:        backup.Cancelling,
			wantOK:     false,
			wantStatus: backup.Finalizing,
		},
		{
			name:       "slot is free",
			claimedID:  "",
			setID:      "op-1",
			set:        backup.Cancelled,
			wantOK:     false,
			wantStatus: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s backupStat
			if tc.claimedID != "" {
				prevID, slot := s.renew(tc.claimedID, "path", "bucket", "override")
				require.Empty(t, prevID)
				require.True(t, slot.set(tc.status))
			}

			stamped, _ := s.setIfOwned(tc.setID, tc.set)
			require.Equal(t, tc.wantOK, stamped)
			require.Equal(t, tc.wantStatus, s.get().Status)
		})
	}
}

// The whole point of the single acquisition: whoever holds the slot after a
// losing resetIf must still hold every field of its claim.
func TestBackupStatResetIfCancelledLeavesNewOwnerIntact(t *testing.T) {
	t.Parallel()

	var s backupStat
	prevID, slot := s.renew("live-restore", "home/dir", "bucket", "override")
	require.Empty(t, prevID)
	slot.set(backup.Cancelled)

	requireSlotNotFreed(t, &s, "cancelled-restore", "live-restore")

	got := s.get()
	require.Equal(t, "live-restore", got.ID)
	require.Equal(t, "home/dir", got.Path)
	require.Equal(t, "bucket", got.OverrideBucket)
	require.Equal(t, "override", got.OverridePath)
}

// Pins resetIfCancelled's check-and-clear as a single lock acquisition: two
// separate ones would let a concurrent renew's claim get dropped. Racers spin
// on a shared flag rather than a channel to line up tightly enough for the
// bug to reproduce reliably across all 20000 iterations.
func TestBackupStatResetIfCancelledDoesNotDropAConcurrentRenew(t *testing.T) {
	t.Parallel()

	const (
		cancelled  = "cancelled-restore"
		fresh      = "fresh-restore"
		iterations = 20000
	)

	for i := 0; i < iterations; i++ {
		var s backupStat
		prevID, slot := s.renew(cancelled, "path", "", "")
		require.Empty(t, prevID)
		require.True(t, slot.set(backup.Cancelled))

		var (
			wg       sync.WaitGroup
			gate     atomic.Bool
			renewErr string
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			for !gate.Load() {
			}
			s.resetIfCancelled(cancelled) //nolint:errcheck // the race, not the outcome, is what this drives
		}()
		go func() {
			defer wg.Done()
			for !gate.Load() {
			}
			// Whichever of the two frees the slot, the fresh claim follows it.
			slot.release()
			renewErr, _ = s.renew(fresh, "path", "", "")
		}()
		gate.Store(true)
		wg.Wait()

		require.Empty(t, renewErr)
		require.Equal(t, fresh, s.get().ID,
			"iteration %d: the fresh restore's claim was cleared by a stale release", i)
	}
}

// Pins that every slotOwner write no-ops once a claim has lost the slot to a
// newer operation.
func TestSlotOwnerWritesStopAtTheClaimBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// write is what the outlived goroutine does after losing its claim.
		write func(slot slotOwner) bool
	}{
		{
			name:  "status",
			write: func(slot slotOwner) bool { return slot.set(backup.Success) },
		},
		{
			name:  "failure",
			write: func(slot slotOwner) bool { return slot.setFailed("late failure") },
		},
		// release is the third write a lost claim can make; it has its own
		// table in TestSlotOwnerRelease.
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s backupStat
			prevID, stale := s.renew("op-1", "path", "", "")
			require.Empty(t, prevID)
			require.True(t, stale.set(backup.Cancelled))
			freeSlot(t, &s, "op-1")

			// The retry carries the same id, which is what makes an id-keyed
			// check unable to tell the two claims apart.
			prevID, live := s.renew("op-1", "path", "", "")
			require.Empty(t, prevID)
			require.True(t, live.set(backup.Transferring))

			require.False(t, tc.write(stale), "a claim that lost the slot must not write to it")

			got := s.get()
			require.Equal(t, "op-1", got.ID)
			require.Equal(t, backup.Transferring, got.Status)
			require.Empty(t, got.Err)
			_, remembered := s.rememberedFailure("op-1")
			require.False(t, remembered,
				"a poll must never be answered with the outcome of an operation that is gone")
		})
	}
}

// Pins that status() answers only for a claim that still holds the slot.
func TestSlotOwnerStatus(t *testing.T) {
	t.Parallel()

	t.Run("the holder reads its own status", func(t *testing.T) {
		t.Parallel()
		var s backupStat
		_, slot := s.renew("op-1", "path", "", "")
		require.True(t, slot.set(backup.Cancelling))

		st, ok := slot.status()
		require.True(t, ok)
		require.Equal(t, backup.Cancelling, st)
	})

	t.Run("a claim that lost the slot reads nothing", func(t *testing.T) {
		t.Parallel()
		var s backupStat
		_, stale := s.renew("op-1", "path", "", "")
		require.True(t, stale.set(backup.Cancelled))
		freeSlot(t, &s, "op-1")
		_, live := s.renew("op-1", "path", "", "")
		require.True(t, live.set(backup.Cancelled))

		_, ok := stale.status()
		require.False(t, ok, "the newer claim's cancellation is not this operation's")
	})
}

// Pins the cancellation one-way rule: Cancelled and Cancelling refuse being
// walked back to a running status.
func TestBackupStatCancellationIsNotOverwritten(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     backup.Status
		next       backup.Status
		wantOK     bool
		wantStatus backup.Status
	}{
		{
			name:       "cancel in flight refuses a running status",
			status:     backup.Cancelling,
			next:       backup.Finalizing,
			wantOK:     false,
			wantStatus: backup.Cancelling,
		},
		{
			name:       "cancel in flight may finish",
			status:     backup.Cancelling,
			next:       backup.Cancelled,
			wantOK:     true,
			wantStatus: backup.Cancelled,
		},
		{
			name:       "cancel in flight refuses a success",
			status:     backup.Cancelling,
			next:       backup.Success,
			wantOK:     false,
			wantStatus: backup.Cancelling,
		},
		{
			name:       "a finished cancel refuses everything",
			status:     backup.Cancelled,
			next:       backup.Success,
			wantOK:     false,
			wantStatus: backup.Cancelled,
		},
		{
			name:       "a running operation takes any status",
			status:     backup.Transferring,
			next:       backup.Finalizing,
			wantOK:     true,
			wantStatus: backup.Finalizing,
		},
		{
			// A schema apply over RAFT cannot be undone, so a cancel landing on
			// it would report CANCELLED for classes that do get restored.
			name:       "a schema apply refuses a cancel in flight",
			status:     backup.Finalizing,
			next:       backup.Cancelling,
			wantOK:     false,
			wantStatus: backup.Finalizing,
		},
		{
			name:       "a schema apply refuses a finished cancel",
			status:     backup.Finalizing,
			next:       backup.Cancelled,
			wantOK:     false,
			wantStatus: backup.Finalizing,
		},
		{
			name:       "a schema apply still reports its own outcome",
			status:     backup.Finalizing,
			next:       backup.Success,
			wantOK:     true,
			wantStatus: backup.Success,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s backupStat
			_, slot := s.renew("op-1", "path", "", "")
			require.True(t, slot.set(tc.status))

			require.Equal(t, tc.wantOK, slot.set(tc.next))
			require.Equal(t, tc.wantStatus, s.get().Status)
		})
	}
}

// Pins that a refused write logs why, so silence isn't confused with a refusal.
func TestSlotOwnerSaysWhyItDroppedAWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// arrange returns the claim whose next write is about to be refused.
		arrange func(s *backupStat) slotOwner
		write   backup.Status
		wantMsg string
	}{
		{
			name: "the claim no longer holds the slot",
			arrange: func(s *backupStat) slotOwner {
				_, slot := s.renew("op-1", "path", "", "")
				slot.release()
				s.renew("op-2", "path", "", "")
				return slot
			},
			write:   backup.Success,
			wantMsg: "this operation no longer holds the slot",
		},
		{
			name: "the restore is applying its schema",
			arrange: func(s *backupStat) slotOwner {
				_, slot := s.renew("op-1", "path", "", "")
				slot.set(backup.Finalizing)
				return slot
			},
			write:   backup.Cancelled,
			wantMsg: "can no longer be cancelled",
		},
		{
			name: "the slot already reads a cancellation",
			arrange: func(s *backupStat) slotOwner {
				_, slot := s.renew("op-1", "path", "", "")
				slot.set(backup.Cancelled)
				return slot
			},
			write:   backup.Success,
			wantMsg: "which is its last word",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			s := backupStat{log: logger}
			slot := tc.arrange(&s)

			require.False(t, slot.set(tc.write))
			entry := hook.LastEntry()
			require.NotNil(t, entry, "the refused write left nothing behind")
			require.Contains(t, entry.Message, tc.wantMsg)
			require.Equal(t, tc.write, entry.Data["dropped_status"])
		})
	}
}

// Pins that every operation slot is wired to a logger. The slot itself is
// silent by default, so an unwired constructor drops writes without a trace.
func TestOperationSlotsAreWiredToALogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		slot func(logrus.FieldLogger) *backupStat
	}{
		{
			name: "coordinator",
			slot: func(l logrus.FieldLogger) *backupStat {
				return &newCoordinator(nil, nil, nil, l, nil, nil).lastOp
			},
		},
		{
			name: "backupper",
			slot: func(l logrus.FieldLogger) *backupStat {
				return &newBackupper("node1", l, config.Backup{}, nil, nil, nil, nil).lastOp
			},
		},
		{
			name: "restorer",
			slot: func(l logrus.FieldLogger) *backupStat {
				return &newRestorer("node1", l, nil, nil, nil, nil, false).lastOp
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, hook := test.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			stat := tc.slot(logger)

			_, slot := stat.renew("op-1", "path", "", "")
			require.True(t, slot.set(backup.Cancelled))
			require.False(t, slot.set(backup.Success))
			require.NotNil(t, hook.LastEntry(), "the refused write left nothing behind")
		})
	}
}
