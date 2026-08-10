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
	"time"

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
			prevID, _ := bp.lastOp.renew(backupID, "bucket/backups/1", "", "")
			require.Empty(t, prevID)
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

	t.Run("reset drops it", func(t *testing.T) {
		var s backupStat
		prevID, _ := s.renew("1", "bucket/backups/1", "", "")
		require.Empty(t, prevID)
		s.setFailed(reason)

		s.reset()

		require.Empty(t, s.get().Err)
	})

	t.Run("renew drops it", func(t *testing.T) {
		// A free slot (empty ID) is the only one renew takes, so it is stamped
		// directly here rather than through a failure the slot still owns.
		var s backupStat
		s.Err = reason

		prevID, _ := s.renew("2", "bucket/backups/2", "", "")
		require.Empty(t, prevID)

		require.Empty(t, s.get().Err)
	})
}

// The slot is released as soon as the operation returns, so what a failure
// leaves behind has to outlive it: the descriptor is the other place a poll
// could read, and there is no descriptor when writing it is what failed.
func TestBackupStatRemembersAFailureBeyondTheSlot(t *testing.T) {
	const reason = "object storage unreachable"

	t.Run("a poll after the slot is released still gets the reason", func(t *testing.T) {
		var s backupStat
		prevID, _ := s.renew("1", "bucket/backups/1", "", "")
		require.Empty(t, prevID)
		s.setFailed(reason)

		s.reset()

		got, ok := s.rememberedFailure("1")
		require.True(t, ok)
		require.Equal(t, reason, got)
	})

	t.Run("a poll for another backup is not answered with this failure", func(t *testing.T) {
		var s backupStat
		prevID, _ := s.renew("1", "bucket/backups/1", "", "")
		require.Empty(t, prevID)
		s.setFailed(reason)
		s.reset()

		_, ok := s.rememberedFailure("2")
		require.False(t, ok)
	})

	t.Run("a retry under the same id drops it", func(t *testing.T) {
		var s backupStat
		prevID, _ := s.renew("1", "bucket/backups/1", "", "")
		require.Empty(t, prevID)
		s.setFailed(reason)
		s.reset()

		prevID, _ = s.renew("1", "bucket/backups/1", "", "")
		require.Empty(t, prevID)

		_, ok := s.rememberedFailure("1")
		require.False(t, ok)
	})

	t.Run("an operation that ended some other way leaves nothing", func(t *testing.T) {
		var s backupStat
		prevID, _ := s.renew("1", "bucket/backups/1", "", "")
		require.Empty(t, prevID)
		s.set(backup.Success)
		s.reset()

		_, ok := s.rememberedFailure("1")
		require.False(t, ok)
	})
}

// The reason has to survive the whole way out of the uploader, including the
// meta write that may itself be what failed.
func TestHandlerOnStatusServesTheReasonAFailedUploadPublished(t *testing.T) {
	const (
		backupID = "1"
		class    = "Article"
	)
	uploadErr := errors.New("collect shard files: no space left on device")
	metaErr := errors.New("meta write rejected by object storage")

	cases := []struct {
		name      string
		uploadErr error
		metaErr   error
		wantIn    []string
	}{
		{
			name:      "the upload failure is the reason",
			uploadErr: uploadErr,
			wantIn:    []string{uploadErr.Error()},
		},
		{
			name:      "a failed meta write reaches the reason too",
			uploadErr: uploadErr,
			metaErr:   metaErr,
			wantIn:    []string{uploadErr.Error(), metaErr.Error()},
		},
		{
			name:      "a failure with no text still says there was one",
			uploadErr: errors.New(""),
			wantIn:    []string{"backup failed without a reported reason"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			descriptors := make(chan backup.ClassDescriptor, 1)
			descriptors <- backup.ClassDescriptor{Name: class, Error: tc.uploadErr}
			close(descriptors)

			sourcer := &fakeSourcer{}
			sourcer.On("BackupDescriptors", mock.Anything, backupID, []string{class}, mock.Anything).
				Return((<-chan backup.ClassDescriptor)(descriptors))
			sourcer.On("ReleaseBackup", mock.Anything, backupID, class).Return(nil)

			backend := newFakeBackend()
			backend.On("PutObject", mock.Anything, backupID, BackupFile, mock.Anything).Return(tc.metaErr)

			logger, _ := test.NewNullLogger()
			bp := &backupper{logger: logger}
			prevID, _ := bp.lastOp.renew(backupID, "bucket/backups/1", "", "")
			require.Empty(t, prevID)

			store := nodeStore{objectStore{backend: backend, backupId: backupID}}
			uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, &bp.lastOp, logger)
			desc := backup.BackupDescriptor{ID: backupID}
			require.ErrorIs(t, uploader.all(context.Background(), []string{class}, &desc, nil, "", ""), tc.uploadErr)

			res := (&Handler{backupper: bp}).OnStatus(context.Background(),
				&StatusRequest{Method: OpCreate, ID: backupID})

			require.Equal(t, backup.Failed, res.Status)
			for _, want := range tc.wantIn {
				require.Contains(t, res.Err, want)
			}
		})
	}
}

// The goroutine that runs a create owns the slot and releases it on the way
// out, so the poll that matters is the one landing after that. Here the meta
// write is what failed, which is the case that leaves nothing else to read: a
// poll falling through to the backend finds no descriptor and can only report
// that it is missing.
func TestHandlerOnStatusServesTheReasonAfterTheCreateGoroutineExits(t *testing.T) {
	const (
		backupID    = "status-after-release"
		backendName = "s3"
		class       = "Article"
	)
	var (
		ctx       = context.Background()
		nodeHome  = backupID + "/" + nodeName
		path      = "bucket/backups/" + nodeHome
		uploadErr = errors.New("collect shard files: no space left on device")
		metaErr   = errors.New("meta write rejected by object storage")
	)

	descriptors := make(chan backup.ClassDescriptor, 1)
	descriptors <- backup.ClassDescriptor{Name: class, Error: uploadErr}
	close(descriptors)

	sourcer := &fakeSourcer{}
	sourcer.On("Backupable", mock.Anything, []string{class}).Return(nil)
	sourcer.On("BackupDescriptors", mock.Anything, backupID, mock.Anything, mock.Anything).
		Return((<-chan backup.ClassDescriptor)(descriptors))
	sourcer.On("ReleaseBackup", mock.Anything, backupID, mock.Anything).Return(nil)

	backend := newFakeBackend()
	backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
	backend.On("SourceDataPath").Return(t.TempDir())
	backend.On("GetObject", mock.Anything, nodeHome, BackupFile).Return(nil, errNotFound)
	// What a poll falling through to the backend would find: the descriptor
	// this operation failed to write.
	backend.On("GetObject", mock.Anything, backupID, BackupFile).Return(nil, errNotFound)
	backend.On("Initialize", mock.Anything, nodeHome).Return(nil)
	backend.On("PutObject", mock.Anything, nodeHome, BackupFile, mock.Anything).Return(metaErr)

	m := createManager(sourcer, nil, backend, nil)
	req := Request{
		Method:   OpCreate,
		ID:       backupID,
		Classes:  []string{class},
		Backend:  backendName,
		Duration: time.Hour,
	}
	require.Empty(t, m.OnCanCommit(ctx, &req).Err)
	require.NoError(t, m.OnCommit(ctx, &StatusRequest{Method: OpCreate, ID: backupID, Backend: backendName}))

	require.Eventually(t, func() bool { return m.backupper.lastOp.get().ID == "" },
		10*time.Second, 5*time.Millisecond, "the create goroutine never released the slot")

	res := m.OnStatus(ctx, &StatusRequest{Method: OpCreate, ID: backupID, Backend: backendName})

	require.Equal(t, backup.Failed, res.Status)
	require.Contains(t, res.Err, uploadErr.Error())
	require.Contains(t, res.Err, metaErr.Error())
}
