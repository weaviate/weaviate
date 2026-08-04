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

// TestBackupStatResetIfCancelled pins that resetIfCancelled only clears a slot
// still owned by the given, cancelled id.
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

// TestBackupStatResetIfCancelledLeavesNewOwnerIntact pins that a losing
// resetIfCancelled leaves every field of the current claim untouched.
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

// TestBackupStatResetIfCancelledDoesNotDropAConcurrentRenew pins that a
// concurrent renew never loses to a stale resetIfCancelled release.
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
