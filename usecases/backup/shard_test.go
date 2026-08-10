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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
)

// resetIfCancelled is the slot's check-and-clear. It releases only a cancelled
// operation: a live one under the same id is still writing files, and clearing
// its claim lets a second operation start alongside it.
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
			wantOK:     true,
			wantSlotID: "",
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
				prevID, _ := s.renew(tc.claimedID, "path", "bucket", "override")
				require.Empty(t, prevID)
				s.set(tc.status)
			}

			require.Equal(t, tc.wantOK, s.resetIfCancelled(tc.resetID))
			require.Equal(t, tc.wantSlotID, s.get().ID)
		})
	}
}

// resetIfOwned is the release half: an operation gives the slot back only while
// it still holds it. Ownership is the generation renew handed out, not the
// backup id, because ids are reusable — a cancelled operation retried under the
// same id is a different claim, and the first one's release must not free it.
func TestBackupStatResetIfOwned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// arrange sets the slot up and returns the generation held by the
		// operation that is about to release.
		arrange    func(t *testing.T, s *backupStat) uint64
		wantOK     bool
		wantSlotID string
	}{
		{
			name: "holder releases its own slot",
			arrange: func(t *testing.T, s *backupStat) uint64 {
				_, gen := s.renew("op-1", "path", "", "")
				s.set(backup.Success)
				return gen
			},
			wantOK:     true,
			wantSlotID: "",
		},
		{
			name: "holder releases after a cancel",
			arrange: func(t *testing.T, s *backupStat) uint64 {
				_, gen := s.renew("op-1", "path", "", "")
				s.set(backup.Cancelled)
				return gen
			},
			wantOK:     true,
			wantSlotID: "",
		},
		{
			name: "slot taken over by a different operation",
			arrange: func(t *testing.T, s *backupStat) uint64 {
				_, gen := s.renew("op-1", "path", "", "")
				s.reset()
				prevID, _ := s.renew("op-2", "path", "", "")
				require.Empty(t, prevID)
				return gen
			},
			wantOK:     false,
			wantSlotID: "op-2",
		},
		{
			// Cancel a restore, then retry it under the same id: a normal
			// flow, and the one an id-keyed check cannot tell apart from the
			// first attempt still holding the slot.
			name: "slot taken over by a retry of the same id",
			arrange: func(t *testing.T, s *backupStat) uint64 {
				_, gen := s.renew("op-1", "path", "", "")
				s.set(backup.Cancelled)
				require.True(t, s.resetIfCancelled("op-1"))
				prevID, _ := s.renew("op-1", "path", "", "")
				require.Empty(t, prevID, "the retry could not claim the freed slot")
				return gen
			},
			wantOK:     false,
			wantSlotID: "op-1",
		},
		{
			name: "slot already released",
			arrange: func(t *testing.T, s *backupStat) uint64 {
				_, gen := s.renew("op-1", "path", "", "")
				s.reset()
				return gen
			},
			wantOK:     false,
			wantSlotID: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s backupStat
			generation := tc.arrange(t, &s)

			require.Equal(t, tc.wantOK, s.resetIfOwned(generation))
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
				prevID, _ := s.renew(tc.claimedID, "path", "bucket", "override")
				require.Empty(t, prevID)
				s.set(tc.status)
			}

			require.Equal(t, tc.wantOK, s.setIfOwned(tc.setID, tc.set))
			require.Equal(t, tc.wantStatus, s.get().Status)
		})
	}
}

// setFailedIfOwned carries the reason next to the FAILED, under the same
// ownership rule as setIfOwned. A failure published onto someone else's claim
// is worse than a lost one: the coordinator latches what a participant reports,
// so it would end a live operation with an unrelated reason.
func TestBackupStatSetFailedIfOwned(t *testing.T) {
	t.Parallel()

	const reason = "object storage unreachable"

	tests := []struct {
		name       string
		claimedID  string
		status     backup.Status
		setID      string
		reason     string
		wantOK     bool
		wantStatus backup.Status
		wantErr    string
		wantRemem  bool
	}{
		{
			name:       "holder gets the failure and its reason",
			claimedID:  "op-1",
			status:     backup.Transferring,
			setID:      "op-1",
			reason:     reason,
			wantOK:     true,
			wantStatus: backup.Failed,
			wantErr:    reason,
			wantRemem:  true,
		},
		{
			name:       "slot held by a different operation",
			claimedID:  "op-2",
			status:     backup.Transferring,
			setID:      "op-1",
			reason:     reason,
			wantOK:     false,
			wantStatus: backup.Transferring,
		},
		{
			name:       "cancelled is terminal",
			claimedID:  "op-1",
			status:     backup.Cancelled,
			setID:      "op-1",
			reason:     reason,
			wantOK:     false,
			wantStatus: backup.Cancelled,
		},
		{
			name:       "slot is free",
			claimedID:  "",
			setID:      "op-1",
			reason:     reason,
			wantOK:     false,
			wantStatus: "",
		},
		{
			// A failure published with no text reads to a poller as no
			// failure, so the stand-in is what gets carried and remembered.
			name:       "no reason gets the stand-in",
			claimedID:  "op-1",
			status:     backup.Transferring,
			setID:      "op-1",
			reason:     "",
			wantOK:     true,
			wantStatus: backup.Failed,
			wantErr:    failureWithoutReason,
			wantRemem:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s backupStat
			if tc.claimedID != "" {
				prevID, _ := s.renew(tc.claimedID, "path", "bucket", "override")
				require.Empty(t, prevID)
				s.set(tc.status)
			}

			require.Equal(t, tc.wantOK, s.setFailedIfOwned(tc.setID, tc.reason))
			require.Equal(t, tc.wantStatus, s.get().Status)
			if tc.wantErr != "" {
				require.Equal(t, tc.wantErr, s.get().Err)
			}

			remembered, ok := s.rememberedFailure(tc.setID)
			require.Equal(t, tc.wantRemem, ok)
			if tc.wantRemem {
				require.Equal(t, tc.wantErr, remembered)
			}
		})
	}
}

// The whole point of the single acquisition: whoever holds the slot after a
// losing resetIf must still hold every field of its claim.
func TestBackupStatResetIfCancelledLeavesNewOwnerIntact(t *testing.T) {
	t.Parallel()

	var s backupStat
	prevID, _ := s.renew("live-restore", "home/dir", "bucket", "override")
	require.Empty(t, prevID)
	s.set(backup.Cancelled)

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
func TestBackupStatResetIfCancelledDoesNotDropAConcurrentRenew(t *testing.T) {
	t.Parallel()

	const (
		cancelled = "cancelled-restore"
		fresh     = "fresh-restore"
	)

	for i := 0; i < 5000; i++ {
		var s backupStat
		prevID, _ := s.renew(cancelled, "path", "", "")
		require.Empty(t, prevID)
		s.set(backup.Cancelled)

		var (
			wg       sync.WaitGroup
			start    = make(chan struct{})
			renewErr string
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			s.resetIfCancelled(cancelled)
		}()
		go func() {
			defer wg.Done()
			<-start
			s.reset()
			renewErr, _ = s.renew(fresh, "path", "", "")
		}()
		close(start)
		wg.Wait()

		require.Empty(t, renewErr)
		require.Equal(t, fresh, s.get().ID,
			"iteration %d: the fresh restore's claim was cleared by a stale release", i)
	}
}
