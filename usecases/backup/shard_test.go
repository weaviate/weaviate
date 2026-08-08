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
			got := s.get()
			require.Equal(t, tc.wantSlotID, got.ID)
			if tc.wantSlotID != "" {
				require.Equal(t, []string{"path", "bucket", "override"},
					[]string{got.Path, got.OverrideBucket, got.OverridePath},
					"a losing release must leave every field of the current claim intact")
			}
		})
	}
}

// TestBackupStatSetIfOwned pins that setIfOwned writes only to a slot the given
// id still holds and never over a slot that already reached Cancelled: a second
// cancel arriving after the first stamped the terminal state would otherwise
// re-open it.
func TestBackupStatSetIfOwned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		claimedID  string
		status     backup.Status
		setID      string
		setStatus  backup.Status
		wantOK     bool
		wantStatus backup.Status
	}{
		{
			name:       "live op under the id being written",
			claimedID:  "op-1",
			status:     backup.Transferring,
			setID:      "op-1",
			setStatus:  backup.Cancelling,
			wantOK:     true,
			wantStatus: backup.Cancelling,
		},
		{
			name:       "second cancel re-stamping an already cancelled op",
			claimedID:  "op-1",
			status:     backup.Cancelled,
			setID:      "op-1",
			setStatus:  backup.Cancelling,
			wantOK:     false,
			wantStatus: backup.Cancelled,
		},
		{
			name:       "live op under a different id",
			claimedID:  "op-2",
			status:     backup.Transferring,
			setID:      "op-1",
			setStatus:  backup.Cancelling,
			wantOK:     false,
			wantStatus: backup.Transferring,
		},
		{
			name:      "slot is free",
			claimedID: "",
			setID:     "op-1",
			setStatus: backup.Cancelling,
			wantOK:    false,
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

			require.Equal(t, tc.wantOK, s.setIfOwned(tc.setID, tc.setStatus))
			require.Equal(t, tc.wantStatus, s.get().Status)
		})
	}
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
