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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
)

// resetIfCancelled is the slot's check-and-clear. It releases only an
// operation that has been cancelled: one still running under the same id is
// writing files, and clearing its claim lets a second operation start
// alongside it. A cancel that has only been claimed so far (CANCELLING) is
// exactly that case — the operation is cancelled once its own goroutine says
// so, not when the cancel starts.
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
			name:       "freshly claimed op sharing the id being released",
			claimedID:  "op-1",
			status:     backup.Started,
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

			require.Equal(t, tc.wantOK, s.resetIfCancelled(tc.resetID))
			require.Equal(t, tc.wantSlotID, s.get().ID)
		})
	}
}

// release is the give-back half of a claim: an operation frees the slot only
// while it still holds it. Ownership is the generation renew handed out, not
// the backup id, because ids are reusable — a cancelled operation retried
// under the same id is a different claim, and the first one's release must not
// free it.
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
			name: "holder releases after a cancel",
			arrange: func(t *testing.T, s *backupStat) slotOwner {
				_, slot := s.renew("op-1", "path", "", "")
				slot.set(backup.Cancelled)
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
				require.True(t, s.resetIfCancelled("op-1"))
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

// setIfOwned is the write half. Its caller is a cancel, which takes the id from
// object storage rather than from the slot, so it must not stamp whatever
// operation happens to be holding it — and must not walk a finished cancel back
// to CANCELLING, which is what OnStatus would then report.
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

			require.Equal(t, tc.wantOK, s.setIfOwned(tc.setID, tc.set))
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

	require.False(t, s.resetIfCancelled("cancelled-restore"))

	got := s.get()
	require.Equal(t, "live-restore", got.ID)
	require.Equal(t, "home/dir", got.Path)
	require.Equal(t, "bucket", got.OverrideBucket)
	require.Equal(t, "override", got.OverridePath)
}

// The cancelled op releases the slot and a new restore claims it while a second
// caller is releasing the cancelled id. A check and a clear under separate lock
// acquisitions throw the new claim away here; one acquisition cannot.
//
// The interleaving is staged rather than raced for: both callers are parked on
// the slot's own lock before either runs, so the release goes first and the
// takeover lands in the gap a two-acquisition check-and-clear would leave
// between its check and its clear. Racing for that gap on timing alone finds it
// about one run in three.
func TestBackupStatResetIfCancelledDoesNotDropAConcurrentRenew(t *testing.T) {
	t.Parallel()

	const (
		cancelled = "cancelled-restore"
		fresh     = "fresh-restore"
		// One iteration is enough to stage the interleaving; the rest cover
		// the runs where the goroutines reach the lock in the other order.
		iterations = 200
		// Long enough for a goroutine that has been started to reach the lock
		// and block on it. Too short only costs the staging, never a false red.
		parkFor = 200 * time.Microsecond
	)

	for i := 0; i < iterations; i++ {
		var s backupStat
		prevID, slot := s.renew(cancelled, "path", "", "")
		require.Empty(t, prevID)
		require.True(t, slot.set(backup.Cancelled))

		var (
			wg       sync.WaitGroup
			renewErr string
		)
		s.Lock()
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.resetIfCancelled(cancelled)
		}()
		time.Sleep(parkFor)
		go func() {
			defer wg.Done()
			// Whichever of the two frees the slot, the fresh claim follows it.
			slot.release()
			renewErr, _ = s.renew(fresh, "path", "", "")
		}()
		time.Sleep(parkFor)
		s.Unlock()
		wg.Wait()

		require.Empty(t, renewErr)
		require.Equal(t, fresh, s.get().ID,
			"iteration %d: the fresh restore's claim was cleared by a stale release", i)
	}
}

// A goroutine can outlive its claim: a cancel frees the slot while it is still
// unwinding and the next operation claims it right away. Every write it makes
// from then on belongs to the operation that is gone, not to the one holding
// the slot.
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
		{
			name:  "release",
			write: func(slot slotOwner) bool { return slot.release() },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s backupStat
			prevID, stale := s.renew("op-1", "path", "", "")
			require.Empty(t, prevID)
			require.True(t, stale.set(backup.Cancelled))
			require.True(t, s.resetIfCancelled("op-1"))

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

// status is how an operation asks its own slot whether it has been cancelled.
// Answering from a slot it no longer holds hands it the newer operation's
// status, which coordinator.commit reads as a cancel and acts on.
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
		require.True(t, s.resetIfCancelled("op-1"))
		_, live := s.renew("op-1", "path", "", "")
		require.True(t, live.set(backup.Cancelled))

		_, ok := stale.status()
		require.False(t, ok, "the newer claim's cancellation is not this operation's")
	})
}

// A cancellation is the operation's last word. Walking the slot back to a
// running status reports an operation the operator has already cancelled as
// still going, on the slot a poll reads before the descriptor is written.
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
