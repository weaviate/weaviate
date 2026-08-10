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
				require.Empty(t, s.renew(tc.claimedID, "path", "bucket", "override"))
				s.set(tc.status)
			}

			require.Equal(t, tc.wantOK, s.resetIfCancelled(tc.resetID))
			require.Equal(t, tc.wantSlotID, s.get().ID)
		})
	}
}

// resetIfOwned is the release half: an operation gives the slot back only while
// it still holds it. Unlike resetIfCancelled it does not look at the status,
// because the caller is the holder itself and releases from every outcome.
func TestBackupStatResetIfOwned(t *testing.T) {
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
			name:       "holder releases its own slot",
			claimedID:  "op-1",
			status:     backup.Success,
			resetID:    "op-1",
			wantOK:     true,
			wantSlotID: "",
		},
		{
			name:       "holder releases after a cancel",
			claimedID:  "op-1",
			status:     backup.Cancelled,
			resetID:    "op-1",
			wantOK:     true,
			wantSlotID: "",
		},
		{
			name:       "slot already taken over by another operation",
			claimedID:  "op-2",
			status:     backup.Transferring,
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
				require.Empty(t, s.renew(tc.claimedID, "path", "bucket", "override"))
				s.set(tc.status)
			}

			require.Equal(t, tc.wantOK, s.resetIfOwned(tc.resetID))
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
				require.Empty(t, s.renew(tc.claimedID, "path", "bucket", "override"))
				s.set(tc.status)
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
	require.Empty(t, s.renew("live-restore", "home/dir", "bucket", "override"))
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
		require.Empty(t, s.renew(cancelled, "path", "", ""))
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
			renewErr = s.renew(fresh, "path", "", "")
		}()
		close(start)
		wg.Wait()

		require.Empty(t, renewErr)
		require.Equal(t, fresh, s.get().ID,
			"iteration %d: the fresh restore's claim was cleared by a stale release", i)
	}
}
