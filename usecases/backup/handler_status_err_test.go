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

	t.Run("reset drops it", func(t *testing.T) {
		var s backupStat
		require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))
		s.setFailed(reason)

		s.reset()

		require.Empty(t, s.get().Err)
	})

	// A restore whose commit failed is then found cancelled in object storage,
	// and the slot moves to Cancelled a moment after the failure was published
	// on it.
	t.Run("a later status drops it", func(t *testing.T) {
		var s backupStat
		require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))
		s.setFailed(reason)

		s.set(backup.Cancelled)

		got := s.get()
		require.Equal(t, backup.Cancelled, got.Status)
		require.Empty(t, got.Err, "a cancellation must not be served with a failure's reason")
	})
}

// The slot is released as soon as the operation returns, so what a failure
// leaves behind has to outlive it: the descriptor is the other place a poll
// could read, and there is no descriptor when writing it is what failed.
func TestBackupStatRemembersAFailureBeyondTheSlot(t *testing.T) {
	const reason = "object storage unreachable"

	t.Run("a poll after the slot is released still gets the reason", func(t *testing.T) {
		var s backupStat
		require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))
		s.setFailed(reason)

		s.reset()

		got, ok := s.rememberedFailure("1")
		require.True(t, ok)
		require.Equal(t, reason, got)
	})

	t.Run("a poll for another backup is not answered with this failure", func(t *testing.T) {
		var s backupStat
		require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))
		s.setFailed(reason)
		s.reset()

		_, ok := s.rememberedFailure("2")
		require.False(t, ok)
	})

	t.Run("a retry under the same id drops it", func(t *testing.T) {
		var s backupStat
		require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))
		s.setFailed(reason)
		s.reset()

		require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))

		_, ok := s.rememberedFailure("1")
		require.False(t, ok)
	})

	t.Run("an operation that ended some other way leaves nothing", func(t *testing.T) {
		var s backupStat
		require.Empty(t, s.renew("1", "bucket/backups/1", "", ""))
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
			require.Empty(t, bp.lastOp.renew(backupID, "bucket/backups/1", "", ""))

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

// The descriptor is the whole backup: with no descriptor there is nothing to
// restore from. Publishing SUCCESS because the file uploads went fine has the
// coordinator count the node done and report an unrestorable backup as good.
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
			require.Empty(t, bp.lastOp.renew(backupID, "bucket/backups/1", "", ""))

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

// An operator abort reaches the uploader as a cancelled context, but the error
// that comes back from the work it interrupted does not always wrap it. The
// operation's own context is the signal that says which one it was.
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
	require.Empty(t, bp.lastOp.renew(backupID, "bucket/backups/1", "", ""))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The abort lands while the roles are being snapshotted, and what comes back
	// is that step's own error, which says nothing about a cancellation.
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

// A create can fail before it ever reaches the upload, and those paths write no
// descriptor at all. The slot is then the only place the reason can live, and
// the diagnostics from the base-backup chain are the ones an operator most
// needs: they say what is wrong with the request.
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

// The remembered failure is a fallback, not an override. A descriptor on the
// backend is the durable record of what happened to that backup, and answering
// a poll from memory instead would report a backup that finished as failed.
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
	require.Empty(t, m.backupper.lastOp.renew(backupID, path, "", ""))
	m.backupper.lastOp.setFailed("object storage unreachable")
	m.backupper.lastOp.reset()

	res := m.OnStatus(ctx, &StatusRequest{Method: OpCreate, ID: backupID, Backend: backendName})

	require.Equal(t, backup.Success, res.Status)
	require.Empty(t, res.Err)
}

// The coordinator publishes the outcome on its slot and only then writes the
// global descriptor, which is a round trip to object storage. A user polling
// GET /backups/{backend}/{id} in between is answered from the slot, so a FAILED
// with no reason there is the same bug one level up, on the path an operator
// actually hits.
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
			// A node still running a pre-fix build, which every participant is
			// for the length of a rolling upgrade.
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
			require.Empty(t, c.lastOp.renew(backupID, "bucket/backups/"+backupID, "", ""))

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

// The create goroutine publishes the outcome on the slot, writes the global
// descriptor, and releases the slot. When that write is what fails, the
// descriptor left on the backend is the one from before the operation ended,
// and every later poll of GET /backups/{backend}/{id} reads it: a failed backup
// reporting STARTED for as long as the node is up.
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

// BackupStatus is what the REST layer calls for GET /backups/{backend}/{id},
// and it copies the reason straight into the response payload. Everything below
// it is covered a layer down; this is the whole path an operator polls.
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
