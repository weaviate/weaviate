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
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// A reason dropped here becomes the permanent FAILED answer with nothing to act on.
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

// A reason must not survive the failure that produced it.
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

	t.Run("a later status drops it", func(t *testing.T) {
		var s backupStat
		prevID, _ := s.renew("1", "bucket/backups/1", "", "")
		require.Empty(t, prevID)
		s.setFailed(reason)

		s.set(backup.Cancelled)

		got := s.get()
		require.Equal(t, backup.Cancelled, got.Status)
		require.Empty(t, got.Err, "a cancellation must not be served with a failure's reason")
	})
}

// A failure must be remembered after the slot is released, since writing the
// descriptor is what may have failed.
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

// The reason must survive out of the uploader, including a failing meta write.
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
			require.Equal(t, backup.Failed, desc.Status,
				"the persisted descriptor must not claim the backup was still transferring")
			require.NotEmpty(t, desc.Error,
				"the descriptor is what a poll reads once the slot is gone, so it has to state the failure too")
			for _, want := range tc.wantIn {
				require.Contains(t, res.Err, want)
			}
		})
	}
}

// SUCCESS must not be published before the descriptor is written, or the
// coordinator counts an unrestorable backup as done.
func TestUploaderPublishesSuccessOnlyOnceTheDescriptorIsWritten(t *testing.T) {
	const (
		backupID = "1"
		class    = "Article"
	)
	metaErr := errors.New("meta write rejected by object storage")

	cases := []struct {
		name       string
		metaErr    error
		wantStatus backup.Status
		wantErr    string
	}{
		{
			name:       "the descriptor lands",
			wantStatus: backup.Success,
		},
		{
			name:       "the descriptor does not land",
			metaErr:    metaErr,
			wantStatus: backup.Failed,
			wantErr:    metaErr.Error(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			descriptors := make(chan backup.ClassDescriptor, 1)
			descriptors <- backup.ClassDescriptor{Name: class}
			close(descriptors)

			sourcer := &fakeSourcer{}
			sourcer.On("BackupDescriptors", mock.Anything, backupID, []string{class}, mock.Anything).
				Return((<-chan backup.ClassDescriptor)(descriptors))
			sourcer.On("ReleaseBackup", mock.Anything, backupID, class).Return(nil)

			backend := newFakeBackend()
			backend.On("SourceDataPath").Return(t.TempDir())
			backend.On("PutObject", mock.Anything, backupID, BackupFile, mock.Anything).Return(tc.metaErr)

			logger, _ := test.NewNullLogger()
			bp := &backupper{logger: logger}
			prevID, _ := bp.lastOp.renew(backupID, "bucket/backups/1", "", "")
			require.Empty(t, prevID)

			store := nodeStore{objectStore{backend: backend, backupId: backupID}}
			uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, &bp.lastOp, logger)
			desc := backup.BackupDescriptor{ID: backupID}
			err := uploader.all(context.Background(), []string{class}, &desc, nil, "", "")

			res := (&Handler{backupper: bp}).OnStatus(context.Background(),
				&StatusRequest{Method: OpCreate, ID: backupID})

			require.Equal(t, tc.wantStatus, res.Status,
				"a node the coordinator counts as done must really be done")
			require.Contains(t, res.Err, tc.wantErr)
			if tc.metaErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.metaErr)
		})
	}
}

// An abort must be detected via the operation's own cancelled context, since
// the interrupted work's error doesn't always wrap it.
func TestUploaderPublishesAnAbortAsCancelled(t *testing.T) {
	const (
		backupID = "1"
		class    = "Article"
	)

	descriptors := make(chan backup.ClassDescriptor, 1)
	descriptors <- backup.ClassDescriptor{Name: class}
	close(descriptors)

	sourcer := &fakeSourcer{}
	sourcer.On("BackupDescriptors", mock.Anything, backupID, []string{class}, mock.Anything).
		Return((<-chan backup.ClassDescriptor)(descriptors))
	sourcer.On("ReleaseBackup", mock.Anything, backupID, class).Return(nil)

	backend := newFakeBackend()
	backend.On("SourceDataPath").Return(t.TempDir())
	backend.On("PutObject", mock.Anything, backupID, BackupFile, mock.Anything).Return(nil)

	logger, _ := test.NewNullLogger()
	bp := &backupper{logger: logger}
	prevID, _ := bp.lastOp.renew(backupID, "bucket/backups/1", "", "")
	require.Empty(t, prevID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Roles snapshot fails with an error unrelated to the cancellation.
	rbac := &abortingSnapshotter{cancel: cancel, err: errors.New("roles snapshot interrupted")}

	store := nodeStore{objectStore{backend: backend, backupId: backupID}}
	uploader := newUploader(config.Backup{}, sourcer, rbac, nil, nil, nil, store, backupID, &bp.lastOp, logger)
	desc := backup.BackupDescriptor{ID: backupID}
	require.Error(t, uploader.all(ctx, []string{class}, &desc, nil, "", ""))

	res := (&Handler{backupper: bp}).OnStatus(context.Background(),
		&StatusRequest{Method: OpCreate, ID: backupID})

	require.Equal(t, backup.Cancelled, res.Status,
		"an operator who aborted a backup must not be told it failed")
	require.Equal(t, backup.Cancelled, desc.Status)
}

type abortingSnapshotter struct {
	cancel context.CancelFunc
	err    error
}

func (s *abortingSnapshotter) Snapshot(roles ...string) ([]byte, error) {
	s.cancel()
	return nil, s.err
}

func (s *abortingSnapshotter) Restore(snapshot []byte, stripNamespaces bool) error {
	return nil
}

// A poll landing after the create goroutine released the slot must still get
// an answer, even when the meta write itself failed.
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
	// The descriptor this operation failed to write.
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

// Pre-upload failures write no descriptor, so the slot is the only place the
// reason can live.
func TestHandlerOnStatusServesTheReasonACreateFailedWithBeforeUploading(t *testing.T) {
	const (
		backupID    = "before-upload"
		baseID      = "base"
		backendName = "s3"
		class       = "Article"
	)
	var (
		ctx      = context.Background()
		nodeHome = backupID + "/" + nodeName
		baseHome = baseID + "/" + nodeName
		path     = "bucket/backups/" + nodeHome
	)

	base := func(d backup.BackupDescriptor) []byte {
		b, err := json.Marshal(d)
		require.NoError(t, err)
		return b
	}
	zstd := backup.CompressionZSTD

	cases := []struct {
		name     string
		commit   bool
		level    CompressionLevel
		baseID   string
		baseMeta []byte
		wantIn   string
	}{
		{
			name:   "the coordinator never commits",
			level:  GzipDefaultCompression,
			wantIn: "timed out waiting for coordinator to commit",
		},
		{
			name:   "the compression level is not one this build knows",
			commit: true,
			level:  CompressionLevel(99),
			wantIn: "invalid compression level: 99",
		},
		{
			name:   "the base backup was taken with another compression type",
			commit: true,
			level:  GzipDefaultCompression,
			baseID: baseID,
			baseMeta: base(backup.BackupDescriptor{
				ID: baseID, Status: backup.Success, CompressionType: &zstd,
				StartedAt: time.Now().Add(-time.Hour),
			}),
			wantIn: `has compression type "zstd", expected "gzip"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sourcer := &fakeSourcer{}
			sourcer.On("Backupable", mock.Anything, []string{class}).Return(nil)
			sourcer.On("ReleaseBackup", mock.Anything, backupID, mock.Anything).Return(nil)

			backend := newFakeBackend()
			backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
			backend.On("SourceDataPath").Return(t.TempDir())
			backend.On("Initialize", mock.Anything, nodeHome).Return(nil)
			// No descriptor for this backup: the operation failed before writing one.
			backend.On("GetObject", mock.Anything, nodeHome, BackupFile).Return(nil, errNotFound)
			backend.On("GetObject", mock.Anything, backupID, BackupFile).Return(nil, errNotFound)
			backend.On("GetObject", mock.Anything, baseHome, BackupFile).Return(tc.baseMeta, nil)

			m := createManager(sourcer, nil, backend, nil)
			req := Request{
				Method:       OpCreate,
				ID:           backupID,
				Classes:      []string{class},
				Backend:      backendName,
				Duration:     50 * time.Millisecond,
				Compression:  Compression{Level: tc.level, CPUPercentage: DefaultCPUPercentage},
				BaseBackupID: tc.baseID,
			}
			require.Empty(t, m.OnCanCommit(ctx, &req).Err)
			if tc.commit {
				require.NoError(t, m.OnCommit(ctx, &StatusRequest{Method: OpCreate, ID: backupID, Backend: backendName}))
			}

			require.Eventually(t, func() bool { return m.backupper.lastOp.get().ID == "" },
				10*time.Second, 5*time.Millisecond, "the create goroutine never released the slot")

			res := m.OnStatus(ctx, &StatusRequest{Method: OpCreate, ID: backupID, Backend: backendName})

			require.Equal(t, backup.Failed, res.Status)
			require.Contains(t, res.Err, tc.wantIn,
				"the reason exists, and the slot is the only place a poll can find it")
		})
	}
}

// A descriptor on the backend must win over a remembered failure, which is
// only a fallback.
func TestHandlerOnStatusPrefersTheDescriptorOverARememberedFailure(t *testing.T) {
	const (
		backupID    = "descriptor-wins"
		backendName = "s3"
	)
	var (
		ctx      = context.Background()
		nodeHome = backupID + "/" + nodeName
		path     = "bucket/backups/" + nodeHome
	)

	meta, err := json.Marshal(backup.BackupDescriptor{
		ID: backupID, Status: backup.Success, StartedAt: time.Now(),
	})
	require.NoError(t, err)

	backend := newFakeBackend()
	backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
	backend.On("GetObject", mock.Anything, nodeHome, BackupFile).Return(meta, nil)

	m := createManager(nil, nil, backend, nil)
	prevID, _ := m.backupper.lastOp.renew(backupID, path, "", "")
	require.Empty(t, prevID)
	m.backupper.lastOp.setFailed("object storage unreachable")
	m.backupper.lastOp.reset()

	res := m.OnStatus(ctx, &StatusRequest{Method: OpCreate, ID: backupID, Backend: backendName})

	require.Equal(t, backup.Success, res.Status)
	require.Empty(t, res.Err)
}

// Same bug as above, one level up: a poll landing between the slot update and
// the global-descriptor write must still get the reason.
func TestCoordinatorOnStatusServesTheReasonBeforeTheGlobalDescriptorIsWritten(t *testing.T) {
	const (
		backupID    = "coordinated"
		backendName = "s3"
	)
	ctx := context.Background()

	cases := []struct {
		name           string
		participantErr string
		wantErr        string
	}{
		{
			name:           "the participant's reason is what the poll gets",
			participantErr: "no space left on device",
			wantErr:        "no space left on device",
		},
		{
			// Simulates an older participant during a rolling upgrade.
			name:           "a participant that reports no reason still ends as a stated failure",
			participantErr: "",
			wantErr:        failureWithoutReason,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := newFakeCoordinator(newFakeNodeResolver([]string{"N1"}))
			c := fc.coordinator()
			c.timeoutNextRound = time.Millisecond
			c.descriptor = &backup.DistributedBackupDescriptor{
				ID:          backupID,
				NodeMapping: map[string]string{},
				Nodes:       map[string]*backup.NodeDescriptor{"N1": {Classes: []string{"Article"}}},
			}
			c.Participants["N1"] = participantStatus{Status: backup.Transferring, LastTime: time.Now()}
			prevID, _ := c.lastOp.renew(backupID, "bucket/backups/"+backupID, "", "")
			require.Empty(t, prevID)

			fc.client.On("Commit", mock.Anything, "N1", mock.Anything).Return(nil)
			fc.client.On("Status", mock.Anything, "N1", mock.Anything).Return(&StatusResponse{
				Status: backup.Failed, Err: tc.participantErr, ID: backupID, Method: OpCreate,
			}, nil)
			fc.client.On("Abort", mock.Anything, "N1", mock.Anything).Return(nil)

			req := &StatusRequest{Method: OpCreate, ID: backupID, Backend: backendName}
			c.commit(ctx, req, map[string]string{"N1": "N1"}, false)

			st, err := c.OnStatus(ctx, coordStore{}, req)

			require.NoError(t, err)
			require.Equal(t, backup.Failed, st.Status)
			require.Equal(t, tc.wantErr, st.Err,
				"the descriptor is not on the backend yet, so the slot is the only answer")
		})
	}
}

// When the global-descriptor write fails, later polls must not keep reading
// the stale pre-operation descriptor and reporting STARTED forever.
func TestCoordinatorOnStatusServesTheFailureTheGlobalDescriptorNeverGot(t *testing.T) {
	const (
		backupID    = "meta-write-failed"
		backendName = "s3"
		class       = "Article"
		node        = "N1"
		reason      = "no space left on device"
	)
	ctx := context.Background()

	fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
	fc.selector.On("Shards", ctx, class).Return([]string{node}, nil)
	fc.client.On("CanCommit", mock.Anything, node, mock.Anything).
		Return(&CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}, nil)
	fc.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil)
	fc.client.On("Status", mock.Anything, node, mock.Anything).Return(&StatusResponse{
		Status: backup.Failed, Err: reason, ID: backupID, Method: OpCreate,
	}, nil)
	fc.client.On("Abort", mock.Anything, node, mock.Anything).Return(nil)
	fc.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
	// The initial descriptor lands, the one carrying the outcome does not.
	fc.backend.On("PutObject", mock.Anything, backupID, GlobalBackupFile, mock.Anything).Return(nil).Once()
	fc.backend.On("PutObject", mock.Anything, backupID, GlobalBackupFile, mock.Anything).Return(ErrAny)
	fc.backend.On("GetObject", mock.Anything, backupID, GlobalBackupFile).
		Return(marshalCoordinatorMeta(backup.DistributedBackupDescriptor{
			ID: backupID, Status: backup.Started,
		}), nil)

	c := fc.coordinator()
	c.timeoutNextRound = time.Millisecond
	req := newReq([]string{class}, backendName, backupID)
	store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
	require.NoError(t, c.Backup(ctx, store, &req))

	require.Eventually(t, func() bool { return c.lastOp.get().ID == "" },
		10*time.Second, 5*time.Millisecond, "the create goroutine never released the slot")

	st, err := c.OnStatus(ctx, store, &StatusRequest{Method: OpCreate, ID: backupID, Backend: backendName})

	require.NoError(t, err)
	require.Equal(t, backup.Failed, st.Status,
		"a backup that failed must not report STARTED once its goroutine is gone")
	require.Equal(t, reason, st.Err)
}

// Same hole on the restore side, where the descriptor left behind is the one
// written when staging began.
func TestCoordinatorOnStatusServesTheFailureTheGlobalRestoreDescriptorNeverGot(t *testing.T) {
	const (
		backupID    = "restore-meta-write-failed"
		backendName = "s3"
		class       = "Article"
		node        = "N1"
		reason      = "restore class Article: schema apply rejected"
	)
	ctx := context.Background()

	fc := newFakeCoordinator(newFakeNodeResolver([]string{node}))
	fc.client.On("CanCommit", mock.Anything, node, mock.Anything).
		Return(&CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}, nil)
	fc.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil)
	fc.client.On("Status", mock.Anything, node, mock.Anything).Return(&StatusResponse{
		Status: backup.Failed, Err: reason, ID: backupID, Method: OpRestore,
	}, nil)
	fc.client.On("Abort", mock.Anything, node, mock.Anything).Return(nil)
	fc.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
	// Nothing to cancel yet, and after that the fake serves what was last
	// written.
	fc.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
	// The descriptor written when staging began lands, the one carrying the
	// outcome does not.
	fc.backend.On("PutObject", mock.Anything, backupID, GlobalRestoreFile, mock.Anything).Return(nil).Once()
	fc.backend.On("PutObject", mock.Anything, backupID, GlobalRestoreFile, mock.Anything).Return(ErrAny)

	c := fc.coordinator()
	c.timeoutNextRound = time.Millisecond
	req := newReq([]string{class}, backendName, backupID)
	desc := &backup.DistributedBackupDescriptor{
		ID:          backupID,
		Nodes:       map[string]*backup.NodeDescriptor{node: {Classes: []string{class}}},
		NodeMapping: map[string]string{},
	}
	store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
	require.NoError(t, c.Restore(ctx, store, &req, desc, nil))

	require.Eventually(t, func() bool { return c.lastOp.get().ID == "" },
		10*time.Second, 5*time.Millisecond, "the restore goroutine never released the slot")

	st, err := c.OnStatus(ctx, store, &StatusRequest{Method: OpRestore, ID: backupID, Backend: backendName})

	require.NoError(t, err)
	require.Equal(t, backup.Failed, st.Status,
		"a restore that failed must not report TRANSFERRING once its goroutine is gone")
	require.Equal(t, reason, st.Err)
}

// End-to-end check of the whole path GET /backups/{backend}/{id} hits, down to
// BackupStatus copying the reason into the response.
func TestSchedulerBackupStatusServesTheReasonOfAFailedBackup(t *testing.T) {
	const (
		backupID    = "polled"
		backendName = "s3"
		class       = "Article"
		node        = "N1"
		reason      = "no space left on device"
	)
	ctx := context.Background()

	fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
	fs.selector.On("ListClasses", ctx).Return([]string{class})
	fs.selector.On("Backupable", ctx, []string{class}).Return(nil)
	fs.selector.On("Shards", ctx, class).Return([]string{node}, nil)
	fs.client.On("CanCommit", mock.Anything, node, mock.Anything).
		Return(&CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}, nil)
	fs.client.On("Commit", mock.Anything, node, mock.Anything).Return(nil)
	fs.client.On("Status", mock.Anything, node, mock.Anything).Return(&StatusResponse{
		Status: backup.Failed, Err: reason, ID: backupID, Method: OpCreate,
	}, nil)
	fs.client.On("Abort", mock.Anything, node, mock.Anything).Return(nil)
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
	fs.backend.On("Initialize", ctx, backupID).Return(nil)
	fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
	// No backup under this id yet, then the descriptor written when the
	// operation started, which is all the failed write leaves behind.
	fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{}).Once()
	fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).
		Return(marshalCoordinatorMeta(backup.DistributedBackupDescriptor{
			ID: backupID, Status: backup.Started,
		}), nil)
	fs.backend.On("PutObject", mock.Anything, backupID, GlobalBackupFile, mock.Anything).Return(nil).Once()
	fs.backend.On("PutObject", mock.Anything, backupID, GlobalBackupFile, mock.Anything).Return(ErrAny)

	s := fs.scheduler()
	s.backupper.timeoutNextRound = time.Millisecond
	_, err := s.Backup(ctx, nil, &BackupRequest{ID: backupID, Backend: backendName, Include: []string{class}})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return s.backupper.lastOp.get().ID == "" },
		10*time.Second, 5*time.Millisecond, "the create goroutine never released the slot")

	st, err := s.BackupStatus(ctx, nil, backendName, backupID, "", "")

	require.NoError(t, err)
	require.Equal(t, backup.Failed, st.Status)
	require.Equal(t, reason, st.Err)
}

// The shard name must be redacted at both places the uploader can publish a reason.
func TestUploaderPublishesAGateRefusalRedacted(t *testing.T) {
	const (
		backupID = "1"
		class    = "Article"
		shard    = "zmDMRo4olU4c"
	)
	refusal := backup.ReindexBlockedError{
		Msg: `backup blocked: runtime-reindex in flight: collection "Article" has an active runtime-reindex task in DTM`,
	}
	wrapped := fmt.Errorf("snapshot shard %s: halt for snapshot: %w", shard, refusal)
	metaErr := errors.New("meta write rejected by object storage")

	cases := []struct {
		name string
		// uploadErr arrives on the class descriptor; metaErr from the meta write.
		uploadErr error
		metaErr   error
		// wantErr is the whole published reason; wantIn is used instead when
		// the backend wraps the meta fault with its own path detail.
		wantErr string
		wantIn  []string
	}{
		{
			name:      "a refused upload",
			uploadErr: wrapped,
			wantErr:   refusal.Msg,
		},
		{
			name:      "a refused upload whose meta write also failed",
			uploadErr: wrapped,
			metaErr:   metaErr,
			wantIn: []string{
				refusal.Msg + "; uploading the backup metadata also failed: ",
				metaErr.Error(),
			},
		},
		{
			// The files went up; writing the descriptor is what hit the gate.
			name:    "a refused meta write",
			metaErr: wrapped,
			wantErr: refusal.Msg,
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
			backend.On("SourceDataPath").Return(t.TempDir())
			backend.On("PutObject", mock.Anything, backupID, BackupFile, mock.Anything).Return(tc.metaErr)

			logger, _ := test.NewNullLogger()
			bp := &backupper{logger: logger}
			prevID, _ := bp.lastOp.renew(backupID, "bucket/backups/1", "", "")
			require.Empty(t, prevID)

			store := nodeStore{objectStore{backend: backend, backupId: backupID}}
			uploader := newUploader(config.Backup{}, sourcer, nil, nil, nil, nil, store, backupID, &bp.lastOp, logger)
			desc := backup.BackupDescriptor{ID: backupID}
			require.Error(t, uploader.all(context.Background(), []string{class}, &desc, nil, "", ""))

			res := (&Handler{backupper: bp}).OnStatus(context.Background(),
				&StatusRequest{Method: OpCreate, ID: backupID})

			require.Equal(t, backup.Failed, res.Status)
			if tc.wantErr != "" {
				require.Equal(t, tc.wantErr, res.Err)
			}
			for _, want := range tc.wantIn {
				require.Contains(t, res.Err, want)
			}
			require.NotContains(t, res.Err, shard,
				"a backup caller is granted nothing on shard names")
			require.NotContains(t, desc.Error, shard,
				"the descriptor is what a poll reads once the slot is gone")
		})
	}
}

// The paths that fail before any descriptor exists publish their reason
// straight onto the slot, so the redaction has to happen there too.
func TestBackupperPublishesAGateRefusalRedacted(t *testing.T) {
	const (
		backupID = "1"
		shard    = "zmDMRo4olU4c"
	)
	refusal := backup.ReindexBlockedError{
		Msg: `backup blocked: runtime-reindex in flight: collection "Article" has an active runtime-reindex task in DTM`,
	}

	logger, _ := test.NewNullLogger()
	bp := &backupper{logger: logger}
	prevID, _ := bp.lastOp.renew(backupID, "bucket/backups/1", "", "")
	require.Empty(t, prevID)

	bp.publishFailure(fmt.Errorf("snapshot shard %s: %w", shard, refusal))

	res := (&Handler{backupper: bp}).OnStatus(context.Background(),
		&StatusRequest{Method: OpCreate, ID: backupID})

	require.Equal(t, backup.Failed, res.Status)
	require.Equal(t, refusal.Msg, res.Err,
		"a backup caller is granted nothing on shard names")
}
