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
	"runtime"
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
// cancelled, not one still Cancelling.
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

// stamp is how a cancel writes to a slot it did not claim itself. It must
// reach only the operation the cancel took its claim on, and must never walk a
// finished cancel back to Cancelling.
func TestSlotOwnerStamp(t *testing.T) {
	t.Parallel()

	// held claims the slot for op-1 and leaves it reading st.
	held := func(t *testing.T, s *backupStat, st backup.Status) slotOwner {
		t.Helper()
		prevID, slot := s.renew("op-1", "path", "bucket", "override")
		require.Empty(t, prevID)
		require.True(t, slot.set(st))
		return s.claimOf("op-1")
	}

	tests := []struct {
		name string
		// arrange sets the slot up and returns the claim the cancel took.
		arrange    func(t *testing.T, s *backupStat) slotOwner
		stamp      backup.Status
		wantOK     bool
		wantStatus backup.Status
	}{
		{
			name: "the operation the claim was taken on gets the status",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				return held(t, s, backup.Transferring)
			},
			stamp:      backup.Cancelling,
			wantOK:     true,
			wantStatus: backup.Cancelling,
		},
		{
			name: "slot held by a different operation",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				prevID, slot := s.renew("op-2", "path", "", "")
				require.Empty(t, prevID)
				require.True(t, slot.set(backup.Transferring))
				return s.claimOf("op-1")
			},
			stamp:      backup.Cancelled,
			wantOK:     false,
			wantStatus: backup.Transferring,
		},
		{
			// Cancel a restore, then retry it under the same id: a normal
			// flow, and the one an id-keyed stamp cannot tell apart from the
			// restore the cancel read.
			name: "slot taken over by a retry of the same id",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				claim := held(t, s, backup.Cancelled)
				freeSlot(t, s, "op-1")
				prevID, _ := s.renew("op-1", "path", "", "")
				require.Empty(t, prevID, "the retry could not claim the freed slot")
				return claim
			},
			stamp:      backup.Cancelled,
			wantOK:     false,
			wantStatus: backup.Started,
		},
		{
			// A cancel reading the older CANCELLING out of storage after
			// commit already stamped the slot CANCELLED. Letting it through
			// reports a finished cancel as still in progress.
			name: "cancelled is terminal",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				return held(t, s, backup.Cancelled)
			},
			stamp:      backup.Cancelling,
			wantOK:     false,
			wantStatus: backup.Cancelled,
		},
		{
			// The cancel endpoint's own write, aimed at a restore that has
			// started applying its schema and can no longer be stopped.
			name: "a schema apply refuses the cancel stamp",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				return held(t, s, backup.Finalizing)
			},
			stamp:      backup.Cancelling,
			wantOK:     false,
			wantStatus: backup.Finalizing,
		},
		{
			name:       "slot is free",
			arrange:    func(_ *testing.T, s *backupStat) slotOwner { return s.claimOf("op-1") },
			stamp:      backup.Cancelled,
			wantOK:     false,
			wantStatus: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s backupStat
			claim := tc.arrange(t, &s)

			stamped, _ := claim.stamp(tc.stamp)
			require.Equal(t, tc.wantOK, stamped)
			require.Equal(t, tc.wantStatus, s.get().Status)
		})
	}
}

// Pins resetIfCancelled's check-and-clear as a single lock acquisition: a
// split one could drop a concurrent renew's claim.
func TestBackupStatResetIfCancelledDoesNotDropAConcurrentRenew(t *testing.T) {
	t.Parallel()

	const (
		cancelled  = "cancelled-restore"
		fresh      = "fresh-restore"
		iterations = 2000
	)

	for i := 0; i < iterations; i++ {
		var s backupStat
		prevID, slot := s.renew(cancelled, "path", "", "")
		require.Empty(t, prevID)
		require.True(t, slot.set(backup.Cancelled))

		var (
			wg          sync.WaitGroup
			gate        atomic.Bool
			freshPrevID string
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			for !gate.Load() {
				runtime.Gosched()
			}
			s.resetIfCancelled(cancelled) //nolint:errcheck // the race, not the outcome, is what this drives
		}()
		go func() {
			defer wg.Done()
			for !gate.Load() {
				runtime.Gosched()
			}
			// Whichever of the two frees the slot, the fresh claim follows it.
			slot.release()
			freshPrevID, _ = s.renew(fresh, "path", "", "")
		}()
		gate.Store(true)
		wg.Wait()

		require.Empty(t, freshPrevID)
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
			s := backupStat{log: logger}
			slot := tc.arrange(&s)

			require.False(t, slot.set(tc.write))
			entry := hook.LastEntry()
			require.NotNil(t, entry, "the refused write left nothing behind")
			require.Contains(t, entry.Message, tc.wantMsg)
			require.Equal(t, tc.write, entry.Data["dropped_status"])
			require.Equal(t, logrus.InfoLevel, entry.Level,
				"a dropped write is what a support case starts from, so it has to survive the default log level")
		})
	}
}

// lockProbeHook reports, from inside the log call, whether the slot's lock
// was free. TryLock from the logging goroutine is what makes that observable
// without deadlocking on a lock still held.
type lockProbeHook struct {
	stat  *backupStat
	fired atomic.Bool
	free  atomic.Bool
}

func (h *lockProbeHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *lockProbeHook) Fire(*logrus.Entry) error {
	h.fired.Store(true)
	if h.stat.TryLock() {
		h.stat.Unlock()
		h.free.Store(true)
	}
	return nil
}

// Pins that a refused write is logged with the slot's lock released, so a
// blocking log hook can't stall pollers reading the slot behind that lock.
func TestSlotOwnerLogsDroppedWritesOutsideTheLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, _ := test.NewNullLogger()
			s := &backupStat{log: logger}
			hook := &lockProbeHook{stat: s}
			logger.AddHook(hook)

			_, slot := s.renew("op-1", "path", "", "")
			require.True(t, slot.set(backup.Cancelled))

			require.False(t, tc.write(slot))
			require.True(t, hook.fired.Load(), "the refused write left nothing behind")
			require.True(t, hook.free.Load(), "the slot's lock was held while its log hook ran")
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
			stat := tc.slot(logger)

			_, slot := stat.renew("op-1", "path", "", "")
			require.True(t, slot.set(backup.Cancelled))
			require.False(t, slot.set(backup.Success))
			require.NotNil(t, hook.LastEntry(), "the refused write left nothing behind")
		})
	}
}
