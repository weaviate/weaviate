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
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
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

// A reason belongs to the failure that produced it. Every path that moves the
// slot off that failure has to drop it, or the next reader is served a reason
// for something that did not happen.
func TestBackupStatDropsAReasonThatNoLongerApplies(t *testing.T) {
	const reason = "object storage unreachable"

	t.Run("set drops it", func(t *testing.T) {
		var s backupStat
		require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))
		s.setFailed(reason)

		s.set(backup.Success)

		require.Equal(t, backup.Success, s.get().Status)
		require.Empty(t, s.get().Err)
	})

	t.Run("reset drops it", func(t *testing.T) {
		var s backupStat
		require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))
		s.setFailed(reason)

		s.reset()

		require.Empty(t, s.get().Err)
	})

	t.Run("renew drops it", func(t *testing.T) {
		// A free slot (empty ID) is the only one renew takes, so it is stamped
		// directly here rather than through a failure the slot still owns.
		var s backupStat
		s.Err = reason

		require.Empty(t, s.renew("2", "bucket/backups/2", "", ""))

		require.Empty(t, s.get().Err)
	})
}

// The reason has to survive the whole way out of the uploader: a poll landing
// before the operation slot is released reads it from there, not from the
// descriptor.
func TestHandlerOnStatusServesTheReasonAFailedUploadPublished(t *testing.T) {
	const (
		backupID = "1"
		class    = "Article"
	)
	uploadErr := errors.New("collect shard files: no space left on device")

	descriptors := make(chan backup.ClassDescriptor, 1)
	descriptors <- backup.ClassDescriptor{Name: class, Error: uploadErr}
	close(descriptors)

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", mock.Anything, backupID, []string{class}, mock.Anything).
		Return((<-chan backup.ClassDescriptor)(descriptors))
	sourcer.On("ReleaseBackup", mock.Anything, backupID, class).Return(nil)

	backend := newFakeBackend()
	backend.On("PutObject", mock.Anything, backupID, BackupFile, mock.Anything).Return(nil)

	logger, _ := test.NewNullLogger()
	bp := &backupper{logger: logger}
	require.Empty(t, bp.lastOp.renew(backupID, "bucket/backups/1", "", ""))

	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, &bp.lastOp, logger)
	desc := backup.BackupDescriptor{ID: backupID}
	require.ErrorIs(t, uploader.all(context.Background(), []string{class}, &desc, nil, "", ""), uploadErr)

	res := (&Handler{backupper: bp}).OnStatus(context.Background(),
		&StatusRequest{Method: OpCreate, ID: backupID})

	require.Equal(t, backup.Failed, res.Status)
	require.Equal(t, uploadErr.Error(), res.Err)
}
