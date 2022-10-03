//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package backup

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSchedulerValidateCreateBackup(t *testing.T) {
	t.Parallel()
	var (
		cls         = "MyClass"
		backendName = "s3"
		s           = newFakeScheduler().scheduler()
		ctx         = context.Background()
		id          = "123"
		path        = "root/123"
	)
	t.Run("ValidateEmptyID", func(t *testing.T) {
		_, err := s.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "",
			Include: []string{cls},
		})
		assert.NotNil(t, err)
	})
	t.Run("ValidateID", func(t *testing.T) {
		_, err := s.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "A*:",
			Include: []string{cls},
		})
		assert.NotNil(t, err)
	})
	t.Run("IncludeExclude", func(t *testing.T) {
		_, err := s.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "1234",
			Include: []string{cls},
			Exclude: []string{cls},
		})
		assert.NotNil(t, err)
	})
	t.Run("ResultingClassListIsEmpty", func(t *testing.T) {
		// return one class and exclude it in the request
		fs := newFakeScheduler()
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		_, err := fs.scheduler().Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "1234",
			Include: []string{},
			Exclude: []string{cls},
		})
		assert.NotNil(t, err)
	})
	t.Run("ClassNotBackupable", func(t *testing.T) {
		// return an error in case index doesn't exist or a shard has multiple nodes
		fs := newFakeScheduler()
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		fs.selector.On("Backupable", ctx, []string{cls}).Return(ErrAny)
		_, err := fs.scheduler().Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "1234",
			Include: []string{},
		})
		assert.NotNil(t, err)
	})

	t.Run("GetMetadataFails", func(t *testing.T) {
		fs := newFakeScheduler()
		fs.selector.On("Backupable", ctx, []string{cls}).Return(nil)
		fs.backend.On("HomeDir", mock.Anything).Return(path)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, errors.New("can not be read"))
		meta, err := fs.scheduler().Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("check if backup %q exists", id))
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})
	t.Run("MetadataNotFound", func(t *testing.T) {
		fs := newFakeScheduler()
		fs.selector.On("Backupable", ctx, []string{cls}).Return(nil)
		fs.backend.On("HomeDir", mock.Anything).Return(path)
		bytes := marshalMeta(backup.BackupDescriptor{ID: id})
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		meta, err := fs.scheduler().Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), fmt.Sprintf("backup %q already exists", id))
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})
}

func TestSchedulerCreateBackup(t *testing.T) {
	t.Parallel()
	var (
		cls         = "Class-A"
		node        = "Node-A"
		backendName = "gcs"
		backupID    = "1"
		any         = mock.Anything
		ctx         = context.Background()
		path        = "dst/path"
		req         = BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}
		cresp = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sReq  = &StatusRequest{OpCreate, backupID, backendName}
		sresp = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
	)

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		req1 := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}

		fs := newFakeScheduler()
		// first
		fs.selector.On("Backupable", ctx, req1.Include).Return(nil)
		fs.selector.On("Shards", ctx, cls).Return([]string{node})

		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("HomeDir", mock.Anything).Return(path)
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.client.On("CanCommit", any, node, any).Return(cresp, nil)
		fs.client.On("Commit", any, node, sReq).Return(nil)
		fs.client.On("Status", any, node, sReq).Return(sresp, nil)
		fs.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Once()
		m := fs.scheduler()
		resp1, err := m.Backup(ctx, nil, &req1)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupCreateResponse{
			Backend: backendName,
			Classes: req1.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp1, want1)
		resp2, err := m.Backup(ctx, nil, &req1)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "already in progress")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Nil(t, resp2)
	})

	t.Run("BackendUnregistered", func(t *testing.T) {
		classes := []string{cls}
		backendError := errors.New("I do not exist")
		fs := newFakeScheduler()
		fs.backendErr = backendError
		meta, err := fs.scheduler().Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backendName)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("InitMetadata", func(t *testing.T) {
		classes := []string{cls}
		fs := newFakeScheduler()
		fs.selector.On("Backupable", ctx, classes).Return(nil)
		fs.backend.On("HomeDir", mock.Anything).Return(path)
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		fs.backend.On("Initialize", ctx, backupID).Return(errors.New("init meta failed"))
		meta, err := fs.scheduler().Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "init")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("Success", func(t *testing.T) {
		fs := newFakeScheduler()
		fs.selector.On("Backupable", ctx, req.Include).Return(nil)
		fs.selector.On("Shards", ctx, cls).Return([]string{node})

		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("HomeDir", mock.Anything).Return(path)
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.client.On("CanCommit", any, node, any).Return(cresp, nil)
		fs.client.On("Commit", any, node, sReq).Return(nil)
		fs.client.On("Status", any, node, sReq).Return(sresp, nil)
		fs.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Once()
		s := fs.scheduler()
		resp, err := s.Backup(ctx, nil, &req)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupCreateResponse{
			Backend: backendName,
			Classes: req.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp, want1)

		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 50)
			if i > 0 && s.backupper.lastOp.get().Status == "" {
				break
			}
		}
		assert.Equal(t, fs.backend.glMeta.Status, backup.Success)
		assert.Equal(t, fs.backend.glMeta.Error, "")
	})
}

type fakeScheduler struct {
	selector   fakeSelector
	client     fakeClient
	backend    *fakeBackend
	backendErr error
	auth       *fakeAuthorizer
	log        logrus.FieldLogger
}

func newFakeScheduler() *fakeScheduler {
	fc := fakeScheduler{}
	fc.backend = newFakeBackend()
	fc.backendErr = nil
	logger, _ := test.NewNullLogger()
	fc.auth = &fakeAuthorizer{}
	fc.log = logger
	return &fc
}

func (f *fakeScheduler) scheduler() *Scheduler {
	provider := &fakeBackupBackendProvider{f.backend, f.backendErr}
	c := NewScheduler(f.auth, &f.client, &f.selector, provider, nil, f.log)
	c.backupper.timeoutNextRound = time.Millisecond * 200
	c.restorer.timeoutNextRound = time.Millisecond * 200
	return c
}
