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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// The coordinator latches the first terminal answer a participant gives and
// stops polling it, so a reason dropped here is the permanent answer the
// operator gets: FAILED with nothing to act on.
func TestHandlerOnStatusServesTheReasonFromTheOperationSlot(t *testing.T) {
	const backupID = "1"

	cases := []struct {
		name       string
		stamp      func(s *backupStat)
		wantStatus backup.Status
		wantErr    string
	}{
		{
			name:       "failure carries its reason",
			stamp:      func(s *backupStat) { s.setFailed("object storage unreachable") },
			wantStatus: backup.Failed,
			wantErr:    "object storage unreachable",
		},
		{
			name:       "a running operation has no reason to carry",
			stamp:      func(s *backupStat) { s.set(backup.Transferring) },
			wantStatus: backup.Transferring,
			wantErr:    "",
		},
		{
			name: "a cancelled slot keeps its status and reports no failure",
			stamp: func(s *backupStat) {
				s.set(backup.Cancelled)
				s.setFailed("late failure that must not overwrite the cancellation")
			},
			wantStatus: backup.Cancelled,
			wantErr:    "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bp := &backupper{}
			require.Empty(t, bp.lastOp.renew(backupID, "bucket/backups/1", "", ""))
			tc.stamp(&bp.lastOp)

			res := (&Handler{backupper: bp}).OnStatus(context.Background(),
				&StatusRequest{Method: OpCreate, ID: backupID})

			require.Equal(t, tc.wantStatus, res.Status)
			require.Equal(t, tc.wantErr, res.Err,
				"the reason on the slot is the only one a poll can read before the descriptor is written")
		})
	}
}

// renew hands the slot to a new operation, so a reason left by the previous one
// must not be served as this one's.
func TestBackupStatRenewClearsThePreviousFailure(t *testing.T) {
	var s backupStat

	require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))
	s.setFailed("object storage unreachable")
	s.reset()

	require.Empty(t, s.renew("2", "bucket/backups/2", "", ""))
	require.Empty(t, s.get().Err)
}
