//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestSchedulerValidateCreateBackup(t *testing.T) {
	t.Parallel()
	var (
		cls         = "C1"
		backendName = "s3"
		s           = newFakeScheduler(nil).scheduler()
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

	t.Run("RequestIncludeHasDuplicate", func(t *testing.T) {
		_, err := s.Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      "1234",
			Include: []string{"C2", "C2", "C1"},
			Exclude: []string{},
		})
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "C2")
	})

	t.Run("ResultingClassListIsEmpty", func(t *testing.T) {
		// return one class and exclude it in the request
		fs := newFakeScheduler(nil)
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
		fs := newFakeScheduler(nil)
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
		fs := newFakeScheduler(nil)
		fs.selector.On("Backupable", ctx, []string{cls}).Return(nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, errors.New("can not be read"))
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})

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
		fs := newFakeScheduler(nil)
		fs.selector.On("Backupable", ctx, []string{cls}).Return(nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
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

func TestSchedulerBackupStatus(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		id          = "1234"
		ctx         = context.Background()
		starTime    = time.Date(2022, 1, 1, 1, 0, 0, 0, time.UTC)
		nodeHome    = id + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		want        = &Status{
			Path:      path,
			StartedAt: starTime,
			Status:    backup.Transferring,
		}
	)

	t.Run("ActiveState", func(t *testing.T) {
		s := newFakeScheduler(nil).scheduler()
		s.backupper.lastOp.reqState = reqState{
			Starttime: starTime,
			ID:        id,
			Status:    backup.Transferring,
			Path:      path,
		}
		st, err := s.BackupStatus(ctx, nil, backendName, id, "", "")
		assert.Nil(t, err)
		assert.Equal(t, want, st)
	})

	t.Run("GetBackupProvider", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backendErr = ErrAny
		_, err := fs.scheduler().BackupStatus(ctx, nil, backendName, id, "", "")
		assert.NotNil(t, err)
	})

	t.Run("MetadataNotFound", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, ErrAny)
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})

		_, err := fs.scheduler().BackupStatus(ctx, nil, backendName, id, "", "")
		assert.NotNil(t, err)
		nerr := backup.ErrNotFound{}
		if !errors.As(err, &nerr) {
			t.Errorf("error want=%v got=%v", nerr, err)
		}
	})

	t.Run("ReadFromMetadata", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		completedAt := starTime.Add(time.Hour)
		bytes := marshalCoordinatorMeta(
			backup.DistributedBackupDescriptor{
				StartedAt: starTime, CompletedAt: completedAt,
				Nodes:  map[string]*backup.NodeDescriptor{"N1": {Classes: []string{"C1"}}},
				Status: backup.Success,
			})
		want := want
		want.CompletedAt = completedAt
		want.Status = backup.Success
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		got, err := fs.scheduler().BackupStatus(ctx, nil, backendName, id, "", "")
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("ReadFromOldMetadata", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		completedAt := starTime.Add(time.Hour)
		bytes := marshalMeta(backup.BackupDescriptor{StartedAt: starTime, CompletedAt: completedAt, Status: string(backup.Success)})
		want := want
		want.CompletedAt = completedAt
		want.Status = backup.Success
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, ErrAny)
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		got, err := fs.scheduler().BackupStatus(ctx, nil, backendName, id, "", "")
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})
}

func TestSchedulerRestorationStatus(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		id          = "1234"
		ctx         = context.Background()
		starTime    = time.Date(2022, 1, 1, 1, 0, 0, 0, time.UTC)
		nodeHome    = id + "/" + nodeName
		path        = "bucket/backups/" + nodeHome
		want        = &Status{
			Path:      path,
			StartedAt: starTime,
			Status:    backup.Transferring,
		}
	)

	t.Run("ActiveState", func(t *testing.T) {
		s := newFakeScheduler(nil).scheduler()
		s.restorer.lastOp.reqState = reqState{
			Starttime: starTime,
			ID:        id,
			Status:    backup.Transferring,
			Path:      path,
		}
		st, err := s.RestorationStatus(ctx, nil, backendName, id, "", "")
		assert.Nil(t, err)
		assert.Equal(t, want, st)
	})

	t.Run("GetBackupProvider", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backendErr = ErrAny
		_, err := fs.scheduler().RestorationStatus(ctx, nil, backendName, id, "", "")
		assert.NotNil(t, err)
	})

	t.Run("MetadataNotFound", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backend.On("GetObject", ctx, id, GlobalRestoreFile).Return(nil, ErrAny)
		_, err := fs.scheduler().RestorationStatus(ctx, nil, backendName, id, "", "")
		assert.NotNil(t, err)
		nerr := backup.ErrNotFound{}
		if !errors.As(err, &nerr) {
			t.Errorf("error want=%v got=%v", nerr, err)
		}
	})

	t.Run("ReadFromMetadata", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		completedAt := starTime.Add(time.Hour)
		bytes := marshalMeta(backup.BackupDescriptor{StartedAt: starTime, CompletedAt: completedAt, Status: string(backup.Success)})
		want := want
		want.CompletedAt = completedAt
		want.Status = backup.Success
		fs.backend.On("GetObject", ctx, id, GlobalRestoreFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		got, err := fs.scheduler().RestorationStatus(ctx, nil, backendName, id, "", "")
		assert.Nil(t, err)
		assert.Equal(t, want, got)
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
		sReq  = &StatusRequest{OpCreate, backupID, backendName, "", ""}
		sresp = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
	)

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		req1 := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}

		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		// first
		fs.selector.On("Backupable", ctx, req1.Include).Return(nil)
		fs.selector.On("Shards", ctx, cls).Return([]string{node}, nil)

		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.client.On("CanCommit", any, node, any).Return(cresp, nil)
		fs.client.On("Commit", any, node, sReq).Return(nil)
		fs.client.On("Status", any, node, sReq).Return(sresp, nil)
		fs.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()
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
		fs := newFakeScheduler(nil)
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
		fs := newFakeScheduler(nil)
		fs.selector.On("Backupable", ctx, classes).Return(nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})

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
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		fs.selector.On("Backupable", ctx, req.Include).Return(nil)
		fs.selector.On("Shards", ctx, cls).Return([]string{node}, nil)

		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})

		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.client.On("CanCommit", any, node, any).Return(cresp, nil)
		fs.client.On("Commit", any, node, sReq).Return(nil)
		fs.client.On("Status", any, node, sReq).Return(sresp, nil)
		fs.backend.On("PutObject", any, backupID, GlobalBackupFile, any).Return(nil).Twice()
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

func TestSchedulerRestoration(t *testing.T) {
	var (
		cls         = "MyClass-A"
		nodeA       = "Node-A"
		nodeB       = "Node-B"
		any         = mock.Anything
		backendName = "gcs"
		backupID    = "1"
		timePt      = time.Now().UTC()
		ctx         = context.Background()
		bucket      = "bucket/backups/"
		path        = bucket + backupID
		keyNodeA    = backupID + "/" + nodeA
		keyNodeB    = backupID + "/" + nodeB
		cResp       = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq        = &StatusRequest{OpRestore, backupID, backendName, "", ""}
		sresp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)
	meta := backup.DistributedBackupDescriptor{
		ID:            backupID,
		StartedAt:     timePt,
		Version:       "1",
		ServerVersion: "1",
		Status:        backup.Success,
		Nodes: map[string]*backup.NodeDescriptor{
			nodeA: {Classes: []string{cls}},
			nodeB: {Classes: []string{cls}},
		},
	}

	shardingStateBytes1, _ := json.Marshal(&sharding.State{
		IndexID: cls,
		Physical: map[string]sharding.Physical{"S1": {
			Name: "S1",
		}},
	})
	shardingStateBytes2, _ := json.Marshal(&sharding.State{
		IndexID:  cls,
		Physical: map[string]sharding.Physical{"S1": {Name: "S1"}, "S2": {Name: "S2"}},
	})
	rawClassBytes, _ := json.Marshal(&models.Class{Class: cls})
	meta1 := backup.BackupDescriptor{
		ID:     backupID,
		Status: string(backup.Success),
		Classes: []backup.ClassDescriptor{{
			Name:          cls,
			Schema:        rawClassBytes,
			ShardingState: shardingStateBytes1,
		}},
	}
	meta2 := backup.BackupDescriptor{
		ID:     backupID,
		Status: string(backup.Success),
		Classes: []backup.ClassDescriptor{{
			Name:          cls,
			Schema:        rawClassBytes,
			ShardingState: shardingStateBytes2,
		}},
	}
	metaBytes1, _ := json.Marshal(meta1)
	metaBytes2, _ := json.Marshal(meta2)

	t.Run("AnotherBackupIsInProgress", func(t *testing.T) {
		req1 := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}
		fs := newFakeScheduler(newFakeNodeResolver([]string{nodeA, nodeB}))
		bytes := marshalCoordinatorMeta(meta)
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("GetObject", ctx, keyNodeA, BackupFile).Return(metaBytes1, nil)
		fs.backend.On("GetObject", ctx, keyNodeB, BackupFile).Return(metaBytes2, nil)

		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("PutObject", mock.Anything, mock.Anything, GlobalRestoreFile, mock.AnythingOfType("[]uint8")).Return(nil)
		fs.client.On("CanCommit", any, nodeA, any).Return(cResp, nil)
		fs.client.On("Commit", any, nodeA, sReq).Return(nil)
		fs.client.On("Status", any, nodeA, sReq).Return(sresp, nil).After(time.Minute)
		fs.client.On("CanCommit", any, nodeB, any).Return(cResp, nil)
		fs.client.On("Commit", any, nodeB, sReq).Return(nil)
		fs.client.On("Status", any, nodeB, sReq).Return(sresp, nil).After(time.Minute)

		s := fs.scheduler()
		resp, err := s.Restore(ctx, nil, &req1)
		assert.Nil(t, err)
		status1 := string(backup.Started)
		want1 := &models.BackupRestoreResponse{
			Backend: backendName,
			Classes: req1.Include,
			ID:      backupID,
			Status:  &status1,
			Path:    path,
		}
		assert.Equal(t, resp, want1)

		resp, err = s.Restore(ctx, nil, &req1)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "already in progress")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Nil(t, resp)
	})

	restore := func(fs *fakeScheduler) {
		{
			req := BackupRequest{
				ID:      backupID,
				Include: []string{cls},
				Backend: backendName,
			}
			bytes := marshalCoordinatorMeta(meta)
			fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
			fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(bytes, nil)
			fs.backend.On("GetObject", ctx, keyNodeA, BackupFile).Return(metaBytes1, nil)
			fs.backend.On("GetObject", ctx, keyNodeB, BackupFile).Return(metaBytes2, nil)
			fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
			// first for initial "STARTED", second for updated participant status
			fs.backend.On("PutObject", mock.Anything, mock.Anything, GlobalRestoreFile, mock.AnythingOfType("[]uint8")).Return(nil)
			fs.backend.On("PutObject", mock.Anything, mock.Anything, GlobalRestoreFile, mock.AnythingOfType("[]uint8")).Return(nil)
			fs.client.On("CanCommit", any, nodeA, any).Return(cResp, nil)
			fs.client.On("Commit", any, nodeA, sReq).Return(nil)
			fs.client.On("Status", any, nodeA, sReq).Return(sresp, nil)
			fs.client.On("CanCommit", any, nodeB, any).Return(cResp, nil)
			fs.client.On("Commit", any, nodeB, sReq).Return(nil)
			fs.client.On("Status", any, nodeB, sReq).Return(sresp, nil)
			fs.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil).Twice()
			s := fs.scheduler()
			resp, err := s.Restore(ctx, nil, &req)
			assert.Nil(t, err)
			status1 := string(backup.Started)
			want1 := &models.BackupRestoreResponse{
				Backend: backendName,
				Classes: req.Include,
				ID:      backupID,
				Status:  &status1,
				Path:    path,
			}
			assert.Equal(t, resp, want1)
			for i := 0; i < 10; i++ {
				time.Sleep(time.Millisecond * 60)
				if i > 0 && s.restorer.lastOp.get().Status == "" {
					break
				}
			}
		}
	}

	t.Run("CannotRestoreClass", func(t *testing.T) {
		fs := newFakeScheduler(newFakeNodeResolver([]string{nodeA, nodeB}))
		fs.schema.errRestoreClass = ErrAny
		restore(fs)
		assert.Equal(t, fs.backend.glMeta.Status, backup.Failed)
		assert.Contains(t, fs.backend.glMeta.Error, ErrAny.Error())
	})

	t.Run("Success", func(t *testing.T) {
		fs := newFakeScheduler(newFakeNodeResolver([]string{nodeA, nodeB}))
		restore(fs)
		assert.Equal(t, fs.backend.glMeta.Status, backup.Success)
		assert.Contains(t, fs.backend.glMeta.Error, "")
	})
}

func TestSchedulerRestoreRequestValidation(t *testing.T) {
	var (
		cls         = "MyClass"
		backendName = "s3"
		s           = newFakeScheduler(nil).scheduler()
		id          = "1234"
		timePt      = time.Now().UTC()
		ctx         = context.Background()
		path        = "bucket/backups/" + id
		req         = &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{},
		}
	)
	meta := backup.DistributedBackupDescriptor{
		ID:            id,
		StartedAt:     timePt,
		Version:       "1",
		ServerVersion: "1",
		Status:        backup.Success,
		Nodes: map[string]*backup.NodeDescriptor{
			nodeName: {Classes: []string{cls}},
		},
	}

	t.Run("NonEmptyIncludeAndExclude", func(t *testing.T) {
		_, err := s.Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{cls},
		})
		assert.NotNil(t, err)
	})

	t.Run("RequestIncludeHasDuplicates", func(t *testing.T) {
		_, err := s.Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{"C1", "C2", "C1"},
			Exclude: []string{},
		})
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "C1")
	})

	t.Run("BackendFailure", func(t *testing.T) { //  backend provider fails
		fs := newFakeScheduler(nil)
		fs.backendErr = ErrAny
		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{cls},
			Exclude: []string{},
		})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backendName)
	})

	t.Run("GetMetadataFile", func(t *testing.T) {
		fs := newFakeScheduler(nil)

		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, ErrAny)
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})

		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req)
		if err == nil || !strings.Contains(err.Error(), "find") {
			t.Errorf("must return an error if it fails to get meta data: %v", err)
		}
		// meta data not found
		fs = newFakeScheduler(nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})

		_, err = fs.scheduler().Restore(ctx, nil, req)
		if !errors.As(err, &backup.ErrNotFound{}) {
			t.Errorf("must return an error if meta data doesn't exist: %v", err)
		}
	})

	t.Run("FailedBackup", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		bytes := marshalMeta(backup.BackupDescriptor{ID: id, Status: string(backup.Failed)})
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backup.Failed)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("BackupWithHigherVersion", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		version := "3.0"
		meta := backup.DistributedBackupDescriptor{
			ID:            id,
			StartedAt:     timePt,
			Version:       version,
			ServerVersion: "2",
			Status:        backup.Success,
			Nodes: map[string]*backup.NodeDescriptor{
				nodeName: {Classes: []string{cls}},
			},
		}

		bytes := marshalCoordinatorMeta(meta)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), errMsgHigherVersion)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("CorruptedBackupFile", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		bytes := marshalMeta(backup.BackupDescriptor{ID: id, Status: string(backup.Success)})
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), "corrupted")
	})

	t.Run("WrongBackupFile", func(t *testing.T) {
		fs := newFakeScheduler(nil)

		bytes := marshalMeta(backup.BackupDescriptor{ID: "123", Status: string(backup.Success)})
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), "wrong backup file")
	})

	t.Run("UnknownClass", func(t *testing.T) {
		fs := newFakeScheduler(nil)

		bytes := marshalCoordinatorMeta(meta)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{ID: id, Include: []string{"unknown"}})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unknown")
	})

	t.Run("EmptyResultClassList", func(t *testing.T) { //  backup was successful but class list is empty
		fs := newFakeScheduler(&fakeNodeResolver{})

		bytes := marshalCoordinatorMeta(meta)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{ID: id, Exclude: []string{cls}})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), cls)
	})
}

func TestSchedulerList(t *testing.T) {
	t.Parallel()
	var (
		backendName = "s3"
		ctx         = context.Background()
		backupID1   = "backup-1"
		backupID2   = "backup-2"
		cls1        = "Class1"
		cls2        = "Class2"
	)

	t.Run("BackendNotFound", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backendErr = ErrAny
		_, err := fs.scheduler().List(ctx, nil, backendName)
		assert.NotNil(t, err)
	})

	t.Run("AllBackupsFails", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backend.On("AllBackups", mock.Anything).Return(nil, ErrAny)
		_, err := fs.scheduler().List(ctx, nil, backendName)
		assert.NotNil(t, err)
		assert.Equal(t, ErrAny, err)
	})

	t.Run("Success", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		backups := []*backup.DistributedBackupDescriptor{
			{
				ID:     backupID1,
				Status: backup.Success,
				Nodes: map[string]*backup.NodeDescriptor{
					"node1": {Classes: []string{cls1}},
				},
			},
			{
				ID:     backupID2,
				Status: backup.Failed,
				Nodes: map[string]*backup.NodeDescriptor{
					"node2": {Classes: []string{cls2}},
				},
			},
		}
		fs.backend.On("AllBackups", mock.Anything).Return(backups, nil)

		resp, err := fs.scheduler().List(ctx, nil, backendName)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Len(t, *resp, 2)

		// Check first backup
		assert.Equal(t, backupID1, (*resp)[0].ID)
		assert.Equal(t, string(backup.Success), (*resp)[0].Status)
		assert.Equal(t, []string{cls1}, (*resp)[0].Classes)

		// Check second backup
		assert.Equal(t, backupID2, (*resp)[1].ID)
		assert.Equal(t, string(backup.Failed), (*resp)[1].Status)
		assert.Equal(t, []string{cls2}, (*resp)[1].Classes)
	})

	t.Run("EmptyList", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backend.On("AllBackups", mock.Anything).Return([]*backup.DistributedBackupDescriptor{}, nil)

		resp, err := fs.scheduler().List(ctx, nil, backendName)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Len(t, *resp, 0)
	})
}

type fakeScheduler struct {
	selector     fakeSelector
	client       fakeClient
	schema       fakeSchemaManger
	backend      *fakeBackend
	backendErr   error
	auth         authorization.Authorizer
	nodeResolver NodeResolver
	log          logrus.FieldLogger
}

func newFakeScheduler(resolver NodeResolver) *fakeScheduler {
	fc := fakeScheduler{}
	fc.backend = newFakeBackend()
	fc.backendErr = nil
	logger, _ := test.NewNullLogger()
	fc.auth = mocks.NewMockAuthorizer()
	fc.log = logger
	if resolver == nil {
		fc.nodeResolver = &fakeNodeResolver{}
	} else {
		fc.nodeResolver = resolver
	}
	return &fc
}

func (f *fakeScheduler) scheduler() *Scheduler {
	provider := &fakeBackupBackendProvider{f.backend, f.backendErr}
	c := NewScheduler(f.auth, &f.client, &f.selector, provider,
		f.nodeResolver, &f.schema, f.log)
	c.backupper.timeoutNextRound = time.Millisecond * 200
	c.restorer.timeoutNextRound = time.Millisecond * 200
	return c
}

func marshalCoordinatorMeta(m backup.DistributedBackupDescriptor) []byte {
	bytes, _ := json.MarshalIndent(m, "", "")
	return bytes
}

func TestFirstDuplicate(t *testing.T) {
	tests := []struct {
		in   []string
		want string
	}{
		{},
		{[]string{"1"}, ""},
		{[]string{"1", "1"}, "1"},
		{[]string{"1", "2", "2", "1"}, "2"},
		{[]string{"1", "2", "3", "1"}, "1"},
	}
	for _, test := range tests {
		got := findDuplicate(test.in)
		if got != test.want {
			t.Errorf("firstDuplicate(%v) want=%s got=%s", test.in, test.want, got)
		}
	}
}

func TestCancellingBackup(t *testing.T) {
	var (
		ctx           = context.Background()
		backendName   = "s3"
		backupID      = "abc"
		fakeScheduler = newFakeScheduler(nil)
		scheduler     = fakeScheduler.scheduler()
	)

	t.Run("ValidateEmptyID-Cancellation", func(t *testing.T) {
		assert.NotNil(t, scheduler.Cancel(ctx, nil, backendName, "", "", ""))
	})

	t.Run("ValidateID", func(t *testing.T) {
		assert.NotNil(t, scheduler.Cancel(ctx, nil, backendName, "A*:", "", ""))
	})

	t.Run("CancellingSucceeded", func(t *testing.T) {
		fakeScheduler := newFakeScheduler(nil)
		ds := backup.BackupDescriptor{
			Status: string(backup.Success),
		}
		b, err := json.Marshal(ds)
		assert.Nil(t, err)

		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalBackupFile).Return(b, nil)
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)

		err = fakeScheduler.scheduler().Cancel(ctx, nil, backendName, "abc", "", "")
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Sprintf("backup %q already succeeded", backupID), err.Error())
		fakeScheduler.backend.AssertExpectations(t)
	})
}
