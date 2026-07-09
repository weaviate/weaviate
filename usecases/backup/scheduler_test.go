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
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
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

	t.Run("ValidateBaseBackupID", func(t *testing.T) {
		tests := []struct {
			name            string
			baseID          string
			wantBaseIDError bool
		}{
			{"empty string is skipped", "", false},
			{"valid lowercase accepted", "valid_id-123", false},
			{"uppercase rejected", "BadID", true},
			{"special characters rejected", "bad*id", true},
			{"whitespace rejected", "bad id", true},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				// Include + Exclude is set so that valid/empty base IDs trip
				// the downstream errIncludeExclude check (mirroring the
				// IncludeExclude sub-test above), giving us a predictable
				// non-mock failure path that cannot mention "base backup id".
				_, err := s.Backup(ctx, nil, &BackupRequest{
					Backend:      backendName,
					ID:           "1234",
					Include:      []string{cls},
					Exclude:      []string{cls},
					BaseBackupID: tc.baseID,
				})
				assert.NotNil(t, err)
				if tc.wantBaseIDError {
					assert.ErrorContains(t, err, "base backup id:")
					assert.ErrorContains(t, err, "invalid backup id")
				} else {
					assert.NotContains(t, err.Error(), "base backup id:")
				}
			})
		}
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
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
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
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
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
		fs := newFakeScheduler(nil)
		s := fs.scheduler()
		s.backupper.lastOp.reqState = reqState{
			Starttime: starTime,
			ID:        id,
			Status:    backup.Transferring,
			Path:      path,
		}
		// In-flight backup: meta has not yet been persisted, so the meta read
		// that backs authorizeBackupByID returns ErrNotFound and is a no-op.
		// OnStatus then short-circuits on lastOp.
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
		st, err := s.BackupStatus(ctx, nil, backendName, id, "", "")
		assert.Nil(t, err)
		assert.Equal(t, want, st)
	})

	t.Run("ActiveStatePartialMeta", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		s := fs.scheduler()
		s.backupper.lastOp.reqState = reqState{
			Starttime: starTime,
			ID:        id,
			Status:    backup.Transferring,
			Path:      path,
		}
		// A status poll racing an in-progress meta write reads a partial file
		// that fails to unmarshal; authz must tolerate it so the poll succeeds.
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return([]byte("{"), nil)
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})
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
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})

		_, err := fs.scheduler().BackupStatus(ctx, nil, backendName, id, "", "")
		assert.NotNil(t, err)
		nerr := backup.ErrNotFound{}
		if !errors.As(err, &nerr) {
			t.Errorf("error want=%v got=%v", nerr, err)
		}
	})

	t.Run("MetadataReadFails", func(t *testing.T) {
		// A transient/operational read failure propagates raw so the handler
		// default arm maps it to 500 instead of misclassifying as 404.
		fs := newFakeScheduler(nil)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, ErrAny)
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, ErrAny)

		_, err := fs.scheduler().BackupStatus(ctx, nil, backendName, id, "", "")
		assert.ErrorIs(t, err, ErrAny, "underlying backend error should propagate unwrapped")
	})

	t.Run("ReadFromMetadata", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		completedAt := starTime.Add(time.Hour)
		bytes := marshalCoordinatorMeta(
			backup.DistributedBackupDescriptor{
				StartedAt: starTime, CompletedAt: completedAt,
				Nodes:  map[string]*backup.NodeDescriptor{"N1": {Classes: []string{"C1"}}},
				Status: backup.Success,
				// 2.5Gb
				PreCompressionSizeBytes: 2684354560,
			},
		)
		want := want
		want.CompletedAt = completedAt
		want.Status = backup.Success
		want.Size = 2.5
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		got, err := fs.scheduler().BackupStatus(ctx, nil, backendName, id, "", "")
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("ReadFromOldMetadata", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		completedAt := starTime.Add(time.Hour)
		bytes := marshalMeta(
			backup.BackupDescriptor{
				StartedAt:   starTime,
				CompletedAt: completedAt,
				Status:      backup.Success,
				// 1.5Gb
				PreCompressionSizeBytes: 1610612736,
			},
		)
		want := want
		want.CompletedAt = completedAt
		want.Status = backup.Success
		want.Size = 1.5
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
		fs := newFakeScheduler(nil)
		s := fs.scheduler()
		s.restorer.lastOp.reqState = reqState{
			Starttime: starTime,
			ID:        id,
			Status:    backup.Transferring,
			Path:      path,
		}
		// In-flight restore: meta has not yet been persisted, so the meta read
		// that backs authorizeBackupByID returns ErrNotFound and is a no-op.
		// OnStatus then short-circuits on lastOp.
		fs.backend.On("GetObject", ctx, id, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
		st, err := s.RestorationStatus(ctx, nil, backendName, id, "", "")
		assert.Nil(t, err)
		assert.Equal(t, want, st)
	})

	t.Run("ActiveStatePartialMeta", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		s := fs.scheduler()
		s.restorer.lastOp.reqState = reqState{
			Starttime: starTime,
			ID:        id,
			Status:    backup.Transferring,
			Path:      path,
		}
		// A status poll racing an in-progress meta write reads a partial file
		// that fails to unmarshal; authz must tolerate it so the poll succeeds.
		fs.backend.On("GetObject", ctx, id, GlobalRestoreFile).Return([]byte("{"), nil)
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
		fs.backend.On("GetObject", ctx, id, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
		_, err := fs.scheduler().RestorationStatus(ctx, nil, backendName, id, "", "")
		assert.NotNil(t, err)
		nerr := backup.ErrNotFound{}
		if !errors.As(err, &nerr) {
			t.Errorf("error want=%v got=%v", nerr, err)
		}
	})

	t.Run("MetadataReadFails", func(t *testing.T) {
		// A transient/operational read failure propagates raw so the handler
		// default arm maps it to 500 instead of misclassifying as 404.
		fs := newFakeScheduler(nil)
		fs.backend.On("GetObject", ctx, id, GlobalRestoreFile).Return(nil, ErrAny)
		_, err := fs.scheduler().RestorationStatus(ctx, nil, backendName, id, "", "")
		assert.ErrorIs(t, err, ErrAny, "underlying backend error should propagate unwrapped")
	})

	t.Run("ReadFromMetadata", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		completedAt := starTime.Add(time.Hour)
		bytes := marshalMeta(backup.BackupDescriptor{StartedAt: starTime, CompletedAt: completedAt, Status: backup.Success})
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
		sReq  = &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
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
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
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
		fs.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, backup.ErrNotFound{})
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
		fs.selector.On("ListClasses", ctx).Return(classes)
		fs.selector.On("Backupable", ctx, classes).Return(nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(nil, backup.NewErrNotFound(errors.New("not found")))
		fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})

		initErr := errors.New("init meta failed")
		fs.backend.On("Initialize", ctx, backupID).Return(initErr)
		meta, err := fs.scheduler().Backup(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      backupID,
			Include: classes,
		})

		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, initErr, "underlying init error should propagate unwrapped")
		assert.Contains(t, err.Error(), "init uploader")
	})

	t.Run("Success", func(t *testing.T) {
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
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
		bytes := marshalMeta(backup.BackupDescriptor{Status: backup.Success})
		fs.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(bytes, nil)

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
		assert.Equal(t, backup.Success, fs.backend.glMeta.Status)
		assert.Equal(t, "", fs.backend.glMeta.Error)
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
		sReq        = &StatusRequest{OpRestore, backupID, backendName, "", "", ""}
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
		Status: backup.Success,
		Classes: []backup.ClassDescriptor{{
			Name:          cls,
			Schema:        rawClassBytes,
			ShardingState: shardingStateBytes1,
		}},
	}
	meta2 := backup.BackupDescriptor{
		ID:     backupID,
		Status: backup.Success,
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
		fs.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(bytes, nil)
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
		resp, err := s.Restore(ctx, nil, &req1, false)
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

		resp, err = s.Restore(ctx, nil, &req1, false)
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
			fs.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(bytes, nil)
			fs.backend.On("GetObject", ctx, keyNodeA, BackupFile).Return(metaBytes1, nil)
			fs.backend.On("GetObject", ctx, keyNodeB, BackupFile).Return(metaBytes2, nil)
			fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
			// PutMeta is called 3 times: initial (TRANSFERRING), Finalizing, and final (SUCCESS)
			fs.backend.On("PutObject", mock.Anything, mock.Anything, GlobalRestoreFile, mock.AnythingOfType("[]uint8")).Return(nil).Times(3)
			fs.client.On("CanCommit", any, nodeA, any).Return(cResp, nil)
			fs.client.On("Commit", any, nodeA, sReq).Return(nil)
			fs.client.On("Status", any, nodeA, sReq).Return(sresp, nil)
			fs.client.On("CanCommit", any, nodeB, any).Return(cResp, nil)
			fs.client.On("Commit", any, nodeB, sReq).Return(nil)
			fs.client.On("Status", any, nodeB, sReq).Return(sresp, nil)
			s := fs.scheduler()
			resp, err := s.Restore(ctx, nil, &req, false)
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

	t.Run("SuccessWithBaseBackup", func(t *testing.T) {
		baseID := "base-1"
		metaWithBase := meta
		metaWithBase.CompressionType = backup.CompressionGZIP
		metaWithBase.BaseBackupID = baseID
		baseMeta := backup.DistributedBackupDescriptor{
			ID:              baseID,
			Status:          backup.Success,
			CompressionType: backup.CompressionGZIP,
		}

		fs := newFakeScheduler(newFakeNodeResolver([]string{nodeA, nodeB}))
		bytes := marshalCoordinatorMeta(metaWithBase)
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(bytes, nil)
		fs.backend.On("GetObject", ctx, baseID, GlobalBackupFile).Return(marshalCoordinatorMeta(baseMeta), nil)
		fs.backend.On("GetObject", ctx, keyNodeA, BackupFile).Return(metaBytes1, nil)
		fs.backend.On("GetObject", ctx, keyNodeB, BackupFile).Return(metaBytes2, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("PutObject", mock.Anything, mock.Anything, GlobalRestoreFile, mock.AnythingOfType("[]uint8")).Return(nil)
		fs.client.On("CanCommit", any, nodeA, any).Return(cResp, nil)
		fs.client.On("Commit", any, nodeA, sReq).Return(nil)
		fs.client.On("Status", any, nodeA, sReq).Return(sresp, nil)
		fs.client.On("CanCommit", any, nodeB, any).Return(cResp, nil)
		fs.client.On("Commit", any, nodeB, sReq).Return(nil)
		fs.client.On("Status", any, nodeB, sReq).Return(sresp, nil)

		s := fs.scheduler()
		req := BackupRequest{ID: backupID, Include: []string{cls}, Backend: backendName}
		resp, err := s.Restore(ctx, nil, &req, false)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("NodeMappingPassedCorrectly", func(t *testing.T) {
		oldNodeA := "Old-Node-A"
		oldNodeB := "Old-Node-B"
		newNodeA := "New-Node-A"
		newNodeB := "New-Node-B"
		nodeMapping := map[string]string{
			oldNodeA: newNodeA,
			oldNodeB: newNodeB,
		}

		// Create meta with old node names
		metaWithOldNodes := backup.DistributedBackupDescriptor{
			ID:            backupID,
			StartedAt:     timePt,
			Version:       "1",
			ServerVersion: "1",
			Status:        backup.Success,
			Nodes: map[string]*backup.NodeDescriptor{
				oldNodeA: {Classes: []string{cls}},
				oldNodeB: {Classes: []string{cls}},
			},
		}

		fs := newFakeScheduler(newFakeNodeResolver([]string{newNodeA, newNodeB}))
		bytes := marshalCoordinatorMeta(metaWithOldNodes)
		fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
		fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(bytes, nil)
		fs.backend.On("GetObject", ctx, backupID+"/"+oldNodeA, BackupFile).Return(metaBytes1, nil)
		fs.backend.On("GetObject", ctx, backupID+"/"+oldNodeB, BackupFile).Return(metaBytes2, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("PutObject", mock.Anything, mock.Anything, GlobalRestoreFile, mock.AnythingOfType("[]uint8")).Return(nil).Twice()
		fs.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).Return(nil).Twice()

		// Verify CanCommit is called with new node names and Request contains NodeMapping
		fs.client.On("CanCommit", any, newNodeA, mock.MatchedBy(func(r *Request) bool {
			return r.Method == OpRestore && r.ID == backupID && r.Backend == backendName &&
				len(r.Classes) == 1 && r.Classes[0] == cls &&
				len(r.NodeMapping) == 2 &&
				r.NodeMapping[oldNodeA] == newNodeA &&
				r.NodeMapping[oldNodeB] == newNodeB
		})).Return(cResp, nil)
		fs.client.On("CanCommit", any, newNodeB, mock.MatchedBy(func(r *Request) bool {
			return r.Method == OpRestore && r.ID == backupID && r.Backend == backendName &&
				len(r.Classes) == 1 && r.Classes[0] == cls &&
				len(r.NodeMapping) == 2 &&
				r.NodeMapping[oldNodeA] == newNodeA &&
				r.NodeMapping[oldNodeB] == newNodeB
		})).Return(cResp, nil)
		fs.client.On("Commit", any, newNodeA, sReq).Return(nil)
		fs.client.On("Commit", any, newNodeB, sReq).Return(nil)
		fs.client.On("Status", any, newNodeA, sReq).Return(sresp, nil)
		fs.client.On("Status", any, newNodeB, sReq).Return(sresp, nil)

		// Ensure RestoreClass succeeds so we can verify NodeMapping was passed
		fs.schema.errRestoreClass = nil

		s := fs.scheduler()
		req := BackupRequest{
			ID:          backupID,
			Include:     []string{cls},
			Backend:     backendName,
			NodeMapping: nodeMapping,
		}
		resp, err := s.Restore(ctx, nil, &req, false)
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

		// Wait for restore to complete
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 60)
			if i > 0 && s.restorer.lastOp.get().Status == "" {
				break
			}
		}

		// Verify node mapping was applied to meta descriptor
		// The meta should have new node names after ApplyNodeMapping
		assert.Equal(t, fs.backend.glMeta.Status, backup.Success)
		assert.Equal(t, fs.backend.glMeta.NodeMapping, nodeMapping)
		assert.Equal(t, nodeMapping, fs.schema.lastNodeMapping)
		fs.client.AssertExpectations(t)
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
		}, false)
		assert.NotNil(t, err)
	})

	t.Run("RequestIncludeHasDuplicates", func(t *testing.T) {
		_, err := s.Restore(ctx, nil, &BackupRequest{
			Backend: backendName,
			ID:      id,
			Include: []string{"C1", "C2", "C1"},
			Exclude: []string{},
		}, false)
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
		}, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), backendName)
	})

	t.Run("GetMetadataFile", func(t *testing.T) {
		fs := newFakeScheduler(nil)

		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, ErrAny)
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})

		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req, false)
		if err == nil || !strings.Contains(err.Error(), "find") {
			t.Errorf("must return an error if it fails to get meta data: %v", err)
		}
		// meta data not found
		fs = newFakeScheduler(nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("GetObject", ctx, id, BackupFile).Return(nil, backup.ErrNotFound{})

		_, err = fs.scheduler().Restore(ctx, nil, req, false)
		if !errors.As(err, &backup.ErrNotFound{}) {
			t.Errorf("must return an error if meta data doesn't exist: %v", err)
		}
	})

	t.Run("FailedBackup", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		bytes := marshalMeta(backup.BackupDescriptor{ID: id, Status: backup.Failed})
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req, false)
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
		_, err := fs.scheduler().Restore(ctx, nil, req, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), errMsgHigherVersion)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("CorruptedBackupFile", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		bytes := marshalMeta(backup.BackupDescriptor{ID: id, Status: backup.Success})
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req, false)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), "corrupted")
	})

	t.Run("WrongBackupFile", func(t *testing.T) {
		fs := newFakeScheduler(nil)

		bytes := marshalMeta(backup.BackupDescriptor{ID: "123", Status: backup.Success})
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req, false)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.Contains(t, err.Error(), "wrong backup file")

		assert.Contains(t, err.Error(), req.ID,
			"error must surface the request ID")
		assert.Contains(t, err.Error(), "123",
			"error must surface the metadata's stored ID")
		assert.Contains(t, err.Error(), GlobalBackupFile,
			"error must name the descriptor file path")
		assert.Contains(t, err.Error(), path,
			"error must include the destination path")
	})

	t.Run("UnknownClass", func(t *testing.T) {
		fs := newFakeScheduler(nil)

		bytes := marshalCoordinatorMeta(meta)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{ID: id, Include: []string{"unknown"}}, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unknown")
	})

	t.Run("EmptyResultClassList", func(t *testing.T) { //  backup was successful but class list is empty
		fs := newFakeScheduler(&fakeNodeResolver{})

		bytes := marshalCoordinatorMeta(meta)
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(bytes, nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, &BackupRequest{ID: id, Exclude: []string{cls}}, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), cls)
	})

	t.Run("MissingBaseBackup", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		metaWithBase := meta
		metaWithBase.CompressionType = backup.CompressionGZIP
		metaWithBase.BaseBackupID = "base-1"
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(marshalCoordinatorMeta(metaWithBase), nil)
		fs.backend.On("GetObject", ctx, "base-1", GlobalBackupFile).Return(nil, backup.ErrNotFound{})
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req, false)
		require.Error(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.ErrorContains(t, err, "resolve base backup chain")
	})

	t.Run("BaseBackupNotSuccessful", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		metaWithBase := meta
		metaWithBase.CompressionType = backup.CompressionGZIP
		metaWithBase.BaseBackupID = "base-1"
		baseMeta := backup.DistributedBackupDescriptor{
			ID:              "base-1",
			Status:          backup.Failed,
			CompressionType: backup.CompressionGZIP,
		}
		fs.backend.On("GetObject", ctx, id, GlobalBackupFile).Return(marshalCoordinatorMeta(metaWithBase), nil)
		fs.backend.On("GetObject", ctx, "base-1", GlobalBackupFile).Return(marshalCoordinatorMeta(baseMeta), nil)
		fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
		_, err := fs.scheduler().Restore(ctx, nil, req, false)
		require.Error(t, err)
		assert.IsType(t, backup.ErrUnprocessable{}, err)
		assert.ErrorContains(t, err, "resolve base backup chain")
	})
}

func TestSchedulerList(t *testing.T) {
	t.Parallel()
	var (
		backendName         = "s3"
		ctx                 = context.Background()
		backupID1           = "backup-1"
		backupID2           = "backup-2"
		cls1                = "Class1"
		cls2                = "Class2"
		defaultListOrdering = func(s string) *string { return &s }("desc")
	)

	t.Run("BackendNotFound", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backendErr = ErrAny
		_, err := fs.scheduler().List(ctx, nil, backendName, defaultListOrdering, false)
		assert.NotNil(t, err)
		assert.ErrorAs(t, err, &backup.ErrUnprocessable{}, "missing backend module should map to 422 unprocessable")
		assert.Contains(t, err.Error(), backendName)
		assert.Contains(t, err.Error(), ErrAny.Error())
	})

	t.Run("AllBackupsFails", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backend.On("AllBackups", mock.Anything).Return(nil, ErrAny)
		_, err := fs.scheduler().List(ctx, nil, backendName, defaultListOrdering, false)
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
				PreCompressionSizeBytes: 16106127360, // 15 GB
				BaseBackupID:            "base-1",
			},
			{
				ID:     backupID2,
				Status: backup.Failed,
				Nodes: map[string]*backup.NodeDescriptor{
					"node2": {Classes: []string{cls2}},
				},
				PreCompressionSizeBytes: 2147483648, // 2 GB
				BaseBackupID:            "base-2",
			},
		}
		fs.backend.On("AllBackups", mock.Anything).Return(backups, nil)

		resp, err := fs.scheduler().List(ctx, nil, backendName, defaultListOrdering, true)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Len(t, *resp, 2)

		// Check first backup
		assert.Equal(t, backupID1, (*resp)[0].ID)
		assert.Equal(t, string(backup.Success), (*resp)[0].Status)
		assert.Equal(t, []string{cls1}, (*resp)[0].Classes)
		assert.Equal(t, float64(15), (*resp)[0].Size)
		assert.Equal(t, "base-1", (*resp)[0].IncrementalBaseBackupID)

		// Check second backup
		assert.Equal(t, backupID2, (*resp)[1].ID)
		assert.Equal(t, string(backup.Failed), (*resp)[1].Status)
		assert.Equal(t, []string{cls2}, (*resp)[1].Classes)
		assert.Equal(t, float64(2), (*resp)[1].Size)
		assert.Equal(t, "base-2", (*resp)[1].IncrementalBaseBackupID)
	})

	t.Run("BaseBackupIDHiddenWhenNotIncluded", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		backups := []*backup.DistributedBackupDescriptor{
			{
				ID:           backupID1,
				Status:       backup.Success,
				BaseBackupID: "base-1",
			},
		}
		fs.backend.On("AllBackups", mock.Anything).Return(backups, nil)

		resp, err := fs.scheduler().List(ctx, nil, backendName, defaultListOrdering, false)
		require.NoError(t, err)
		require.Len(t, *resp, 1)
		assert.Empty(t, (*resp)[0].IncrementalBaseBackupID)
	})

	t.Run("EmptyList", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.backend.On("AllBackups", mock.Anything).Return([]*backup.DistributedBackupDescriptor{}, nil)

		resp, err := fs.scheduler().List(ctx, nil, backendName, defaultListOrdering, false)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		assert.Len(t, *resp, 0)
	})

	t.Run("SortedList", func(t *testing.T) {
		timestamp := time.Now()

		fs := newFakeScheduler(nil)
		backups := []*backup.DistributedBackupDescriptor{
			{
				ID:                      "mock-backup-0",
				Status:                  backup.Success,
				PreCompressionSizeBytes: 100,
				StartedAt:               timestamp.Add(-5 * time.Minute),
			},
			{
				ID:                      "mock-backup-4",
				Status:                  backup.Started,
				PreCompressionSizeBytes: 10,
				StartedAt:               timestamp.Add(-1 * time.Minute),
			},
			{
				ID:                      "mock-backup-2",
				Status:                  backup.Failed,
				PreCompressionSizeBytes: 0,
				StartedAt:               timestamp.Add(-3 * time.Minute),
			},
			{
				ID:                      "mock-backup-1",
				Status:                  backup.Failed,
				PreCompressionSizeBytes: 0,
				StartedAt:               timestamp.Add(-4 * time.Minute),
			},
			{
				ID:                      "mock-backup-3",
				Status:                  backup.Success,
				PreCompressionSizeBytes: 120,
				StartedAt:               timestamp.Add(-2 * time.Minute),
			},
		}
		fs.backend.On("AllBackups", mock.Anything).Return(backups, nil)

		t.Run("return results sorted by default (desc)", func(t *testing.T) {
			resp, err := fs.scheduler().List(ctx, nil, backendName, defaultListOrdering, false)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			assert.Len(t, *resp, 5)

			expectedOrder := []string{
				"mock-backup-4",
				"mock-backup-3",
				"mock-backup-2",
				"mock-backup-1",
				"mock-backup-0",
			}

			for i, backup := range *resp {
				assert.Equal(t, expectedOrder[i], backup.ID, "backups are not in expected order")
			}
		})

		t.Run("return results sorted (asc)", func(t *testing.T) {
			resp, err := fs.scheduler().List(ctx, nil, backendName, func(s string) *string { return &s }("asc"), false)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			assert.Len(t, *resp, 5)

			expectedOrder := []string{
				"mock-backup-0",
				"mock-backup-1",
				"mock-backup-2",
				"mock-backup-3",
				"mock-backup-4",
			}

			for i, backup := range *resp {
				assert.Equal(t, expectedOrder[i], backup.ID, "backups are not in expected order")
			}
		})
	})
}

type fakeScheduler struct {
	selector     fakeSelector
	userLister   fakeUserLister
	client       fakeClient
	schema       fakeSchemaManger
	backend      *fakeBackend
	backendErr   error
	auth         authorization.Authorizer
	nodeResolver NodeResolver
	log          logrus.FieldLogger
}

// fakeUserLister is a static UserLister for scheduler tests.
type fakeUserLister struct {
	users []string
}

func (f *fakeUserLister) ListAllUsers() []string { return f.users }

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
	c := NewScheduler(f.auth, &f.client, &f.selector, &f.userLister, provider,
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
			Status: backup.Success,
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

	t.Run("PartialMetaRetriesAndScopesAuthz", func(t *testing.T) {
		fakeScheduler := newFakeScheduler(nil)
		ds := backup.DistributedBackupDescriptor{
			Status: backup.Cancelled,
			Nodes:  map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Class1"}}},
		}
		b, err := json.Marshal(ds)
		assert.NoError(t, err)

		// First read races an in-progress meta write and returns a partial file;
		// the retry resolves the real meta so authz is scoped to the backup's
		// classes instead of a wildcard DELETE that a scoped caller would fail.
		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalBackupFile).Return([]byte("{"), nil).Once()
		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalBackupFile).Return(b, nil)
		// The partial GlobalBackupFile read triggers the old-format fallback; deny it
		// so the json.SyntaxError surfaces and the read is retried.
		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, BackupFile).Return(nil, backup.ErrNotFound{})
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)

		err = fakeScheduler.scheduler().Cancel(ctx, nil, backendName, backupID, "", "")
		assert.NoError(t, err)

		calls := fakeScheduler.auth.(*mocks.FakeAuthorizer).Calls()
		assert.Len(t, calls, 1)
		assert.Equal(t, authorization.DELETE, calls[0].Verb)
		assert.Equal(t, authorization.Backups("Class1"), calls[0].Resources)
		fakeScheduler.backend.AssertExpectations(t)
	})
}

func TestWildcardExpansion(t *testing.T) {
	t.Parallel()

	t.Run("MatchesWildcard", func(t *testing.T) {
		tests := []struct {
			pattern   string
			className string
			expected  bool
		}{
			{"data-202212*", "data-20221223", true},
			{"data-202212*", "data-20221122", false},
			{"data-*", "data-20221223", true},
			{"*-December", "Backup-December", true},
			{"Class?", "ClassA", true},
			{"Class?", "ClassAB", false},
			{"ExactMatch", "ExactMatch", true},
			{"ExactMatch", "NotMatch", false},
		}
		for _, tc := range tests {
			got := matchesWildcard(tc.pattern, tc.className)
			assert.Equal(t, tc.expected, got, "pattern=%s class=%s", tc.pattern, tc.className)
		}
	})

	t.Run("ExpandWildcards", func(t *testing.T) {
		candidates := []string{
			"data-20221122",
			"data-20221223",
			"data-20221224",
			"data-20221225",
			"Article",
			"Blog",
		}

		tests := []struct {
			patterns []string
			expected []string
		}{
			// Empty patterns returns empty
			{[]string{}, []string{}},
			// Exact match, no wildcards
			{[]string{"Article"}, []string{"Article"}},
			// Wildcard matching December dates
			{[]string{"data-202212*"}, []string{"data-20221223", "data-20221224", "data-20221225"}},
			// Wildcard matching all data classes
			{[]string{"data-*"}, []string{"data-20221122", "data-20221223", "data-20221224", "data-20221225"}},
			// Mixed: exact and wildcard
			{[]string{"Article", "data-202212*"}, []string{"Article", "data-20221223", "data-20221224", "data-20221225"}},
			// Pattern that matches nothing (stays as-is for non-wildcard)
			{[]string{"NonExistent"}, []string{"NonExistent"}},
			// Wildcard that matches nothing returns empty for that pattern
			{[]string{"nothing-*"}, []string{}},
		}
		for _, tc := range tests {
			got := expandWildcards(tc.patterns, candidates)
			assert.ElementsMatch(t, tc.expected, got, "patterns=%v", tc.patterns)
		}
	})
}

func TestCancellingRestore(t *testing.T) {
	var (
		ctx           = context.Background()
		backendName   = "s3"
		backupID      = "abc"
		fakeScheduler = newFakeScheduler(nil)
		scheduler     = fakeScheduler.scheduler()
	)

	t.Run("ValidateEmptyID-Cancellation", func(t *testing.T) {
		assert.NotNil(t, scheduler.CancelRestore(ctx, nil, backendName, "", "", ""))
	})

	t.Run("ValidateID", func(t *testing.T) {
		assert.NotNil(t, scheduler.CancelRestore(ctx, nil, backendName, "A*:", "", ""))
	})

	t.Run("CancellingSucceeded", func(t *testing.T) {
		fakeScheduler := newFakeScheduler(nil)
		ds := backup.DistributedBackupDescriptor{
			Status: backup.Success,
		}
		b, err := json.Marshal(ds)
		assert.Nil(t, err)

		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(b, nil)
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)

		err = fakeScheduler.scheduler().CancelRestore(ctx, nil, backendName, backupID, "", "")
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Sprintf("restore %q already succeeded", backupID), err.Error())
		fakeScheduler.backend.AssertExpectations(t)
	})

	t.Run("CancellingAlreadyCancelled", func(t *testing.T) {
		fakeScheduler := newFakeScheduler(nil)
		ds := backup.DistributedBackupDescriptor{
			Status: backup.Cancelled,
		}
		b, err := json.Marshal(ds)
		assert.Nil(t, err)

		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(b, nil)
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)

		err = fakeScheduler.scheduler().CancelRestore(ctx, nil, backendName, backupID, "", "")
		assert.Nil(t, err) // Should return nil for already cancelled
		fakeScheduler.backend.AssertExpectations(t)
	})

	t.Run("CancellingFinalizing", func(t *testing.T) {
		fakeScheduler := newFakeScheduler(nil)
		ds := backup.DistributedBackupDescriptor{
			Status: backup.Finalizing,
		}
		b, err := json.Marshal(ds)
		assert.Nil(t, err)

		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(b, nil)
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)

		err = fakeScheduler.scheduler().CancelRestore(ctx, nil, backendName, backupID, "", "")
		assert.NotNil(t, err)
		assert.Equal(t, fmt.Sprintf("restore %q is applying schema changes and cannot be cancelled", backupID), err.Error())
		fakeScheduler.backend.AssertExpectations(t)
	})

	t.Run("CancellingInProgress", func(t *testing.T) {
		fakeScheduler := newFakeScheduler(newFakeNodeResolver([]string{"node1"}))
		ds := backup.DistributedBackupDescriptor{
			Status: backup.Transferring,
			ID:     backupID,
			Nodes: map[string]*backup.NodeDescriptor{
				"node1": {Classes: []string{"Class1"}},
			},
		}
		b, err := json.Marshal(ds)
		assert.Nil(t, err)

		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(b, nil)
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
		fakeScheduler.selector.On("ListClasses", ctx).Return([]string{"Class1"})
		fakeScheduler.selector.On("Shards", ctx, "Class1").Return([]string{"node1"}, nil)
		fakeScheduler.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		fakeScheduler.backend.On("PutObject", mock.Anything, backupID, GlobalRestoreFile, mock.Anything).Return(nil)

		err = fakeScheduler.scheduler().CancelRestore(ctx, nil, backendName, backupID, "", "")
		assert.Nil(t, err)
		fakeScheduler.backend.AssertExpectations(t)
	})

	t.Run("CancellingAlreadyInCancellingState", func(t *testing.T) {
		// When status is already CANCELLING, another coordinator is handling it - return early
		fakeScheduler := newFakeScheduler(nil)
		ds := backup.DistributedBackupDescriptor{
			Status: backup.Cancelling,
			ID:     backupID,
		}
		b, err := json.Marshal(ds)
		assert.Nil(t, err)

		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(b, nil)
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)

		err = fakeScheduler.scheduler().CancelRestore(ctx, nil, backendName, backupID, "", "")
		assert.Nil(t, err) // Should return nil - another coordinator is handling
		fakeScheduler.backend.AssertExpectations(t)
		// Verify no PutObject was called - we should NOT try to write when already CANCELLING
		fakeScheduler.backend.AssertNotCalled(t, "PutObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("CancellingWritesCANCELLINGFirst", func(t *testing.T) {
		// Verify that CancelRestore writes CANCELLING status before proceeding
		fakeScheduler := newFakeScheduler(newFakeNodeResolver([]string{"node1"}))
		ds := backup.DistributedBackupDescriptor{
			Status: backup.Transferring,
			ID:     backupID,
			Nodes: map[string]*backup.NodeDescriptor{
				"node1": {Classes: []string{"Class1"}},
			},
		}
		b, err := json.Marshal(ds)
		assert.Nil(t, err)

		var putObjectCalls []backup.Status
		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(b, nil)
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
		fakeScheduler.selector.On("ListClasses", ctx).Return([]string{"Class1"})
		fakeScheduler.selector.On("Shards", ctx, "Class1").Return([]string{"node1"}, nil)
		fakeScheduler.client.On("Abort", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		fakeScheduler.backend.On("PutObject", mock.Anything, backupID, GlobalRestoreFile, mock.Anything).Run(func(args mock.Arguments) {
			data := args.Get(3).([]byte)
			var desc backup.DistributedBackupDescriptor
			json.Unmarshal(data, &desc)
			putObjectCalls = append(putObjectCalls, desc.Status)
		}).Return(nil)

		err = fakeScheduler.scheduler().CancelRestore(ctx, nil, backendName, backupID, "", "")
		assert.Nil(t, err)
		// Verify CANCELLING was written first, then CANCELLED
		assert.Len(t, putObjectCalls, 2)
		assert.Equal(t, backup.Cancelling, putObjectCalls[0], "First write should be CANCELLING")
		assert.Equal(t, backup.Cancelled, putObjectCalls[1], "Second write should be CANCELLED")
	})

	t.Run("CancellingPutMetaFailsExitsEarly", func(t *testing.T) {
		// When PutMeta fails to write CANCELLING (e.g., storage contention),
		// we should exit early without calling Abort - another coordinator may be handling it
		fakeScheduler := newFakeScheduler(newFakeNodeResolver([]string{"node1"}))
		ds := backup.DistributedBackupDescriptor{
			Status: backup.Transferring,
			ID:     backupID,
			Nodes: map[string]*backup.NodeDescriptor{
				"node1": {Classes: []string{"Class1"}},
			},
		}
		b, _ := json.Marshal(ds)

		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(b, nil)
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
		// PutObject fails - simulating storage contention or another coordinator winning
		fakeScheduler.backend.On("PutObject", mock.Anything, backupID, GlobalRestoreFile, mock.Anything).Return(fmt.Errorf("storage write failed")).Once()

		err := fakeScheduler.scheduler().CancelRestore(ctx, nil, backendName, backupID, "", "")
		assert.Nil(t, err) // Should return nil - let another coordinator handle it
		// Should NOT call Abort since we couldn't claim cancellation
		fakeScheduler.client.AssertNotCalled(t, "Abort", mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("PartialMetaRetriesAndScopesAuthz", func(t *testing.T) {
		fakeScheduler := newFakeScheduler(nil)
		ds := backup.DistributedBackupDescriptor{
			Status: backup.Cancelled,
			Nodes:  map[string]*backup.NodeDescriptor{"node1": {Classes: []string{"Class1"}}},
		}
		b, err := json.Marshal(ds)
		assert.NoError(t, err)

		// First read races an in-progress meta write and returns a partial file;
		// the retry resolves the real meta so authz is scoped to the backup's
		// classes instead of a wildcard DELETE that a scoped caller would fail.
		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return([]byte("{"), nil).Once()
		fakeScheduler.backend.On("GetObject", mock.Anything, backupID, GlobalRestoreFile).Return(b, nil)
		fakeScheduler.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)

		err = fakeScheduler.scheduler().CancelRestore(ctx, nil, backendName, backupID, "", "")
		assert.NoError(t, err)

		calls := fakeScheduler.auth.(*mocks.FakeAuthorizer).Calls()
		assert.Len(t, calls, 1)
		assert.Equal(t, authorization.DELETE, calls[0].Verb)
		assert.Equal(t, authorization.Backups("Class1"), calls[0].Resources)
		fakeScheduler.backend.AssertExpectations(t)
	})
}

// resolveUserSelectors is the pure core of includeUsers resolution: it must
// behave like the class include-list (dedup + wildcard expansion) while
// rejecting selectors that name nothing.
func TestResolveUserSelectors(t *testing.T) {
	t.Parallel()

	// stored ids are the qualified "namespace:userId" form; "dave" is a
	// pre-namespace unqualified user.
	allUsers := []string{"ns1:alice", "ns1:bob", "ns2:carol", "dave"}

	tests := []struct {
		name        string
		include     []string
		want        []string
		wantErrPart string
	}{
		{
			name:    "exact match",
			include: []string{"ns1:alice"},
			want:    []string{"ns1:alice"},
		},
		{
			name:    "namespace wildcard",
			include: []string{"ns1:*"},
			want:    []string{"ns1:alice", "ns1:bob"},
		},
		{
			name:    "question-mark wildcard",
			include: []string{"dav?"},
			want:    []string{"dave"},
		},
		{
			name:    "bare star matches every user",
			include: []string{"*"},
			want:    []string{"ns1:alice", "ns1:bob", "ns2:carol", "dave"},
		},
		{
			name:    "exact selector plus wildcard, deduplicated",
			include: []string{"ns2:carol", "ns2:*"},
			want:    []string{"ns2:carol"},
		},
		{
			name:        "duplicate selector",
			include:     []string{"ns1:*", "ns1:*"},
			wantErrPart: "duplicate",
		},
		{
			name:        "exact selector for a missing user",
			include:     []string{"ns1:zoe"},
			wantErrPart: `user "ns1:zoe" in 'includeUsers' does not exist`,
		},
		{
			name:        "wildcard matches nothing",
			include:     []string{"ns9:*"},
			wantErrPart: "no dynamic users match",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveUserSelectors(tt.include, allUsers)
			if tt.wantErrPart != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrPart)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

// resolveUsers wraps resolveUserSelectors with the empty-input and
// users-disabled handling that callers depend on.
func TestResolveUsers(t *testing.T) {
	t.Parallel()

	t.Run("absent includeUsers yields no users", func(t *testing.T) {
		s := &Scheduler{} // no userLister: must not be consulted at all
		users, err := s.resolveUsers(nil)
		require.NoError(t, err)
		assert.Empty(t, users)
	})

	t.Run("includeUsers without a user lister is rejected", func(t *testing.T) {
		s := &Scheduler{} // userLister nil => dynamic DB users disabled
		users, err := s.resolveUsers([]string{"ns1:*"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dynamic DB users are not enabled")
		assert.Nil(t, users)
	})

	t.Run("resolves against the user lister", func(t *testing.T) {
		s := &Scheduler{userLister: &fakeUserLister{users: []string{"ns1:alice", "ns2:bob"}}}
		users, err := s.resolveUsers([]string{"ns1:*"})
		require.NoError(t, err)
		assert.Equal(t, []string{"ns1:alice"}, users)
	})
}

// userBackupAuthz returns the Authorize call the scheduler made for the
// user-scoped backup permission (backups/users/<id>), failing if none was
// recorded.
func userBackupAuthz(t *testing.T, fs *fakeScheduler) mocks.AuthZReq {
	t.Helper()
	fa, ok := fs.auth.(*mocks.FakeAuthorizer)
	require.True(t, ok, "fakeScheduler.auth must be a *mocks.FakeAuthorizer")
	for _, c := range fa.Calls() {
		if len(c.Resources) > 0 && strings.HasPrefix(c.Resources[0], authorization.BackupsDomain+"/users/") {
			return c
		}
	}
	t.Fatalf("no user-backup authorization call recorded; calls=%v", fa.Calls())
	return mocks.AuthZReq{}
}

// Scheduler.Backup must resolve includeUsers, gate the selected users behind a
// backup-create authorization on backups/users/<id>, and surface them in the
// create-backup response.
func TestSchedulerCreateBackupIncludeUsers(t *testing.T) {
	t.Parallel()

	var (
		cls         = "Class-A"
		node        = "Node-A"
		backendName = "gcs"
		backupID    = "1"
		any         = mock.Anything
		ctx         = context.Background()
		path        = "dst/path"
		cresp       = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sReq        = &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
		sresp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
	)

	t.Run("resolves includeUsers and returns them in the response", func(t *testing.T) {
		req := BackupRequest{
			ID:           backupID,
			Include:      []string{cls},
			Backend:      backendName,
			IncludeUsers: []string{"ns1:*"},
		}
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		fs.userLister.users = []string{"ns1:alice", "ns1:bob", "ns2:carol"}
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
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
		fs.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(marshalMeta(backup.BackupDescriptor{Status: backup.Success}), nil)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &req)
		require.Nil(t, err)
		// ns2:carol must not leak in: only ns1:* was requested.
		assert.ElementsMatch(t, []string{"ns1:alice", "ns1:bob"}, resp.Users)
		assert.Equal(t, []string{cls}, resp.Classes)

		// Exporting credential material requires backup-create permission on
		// each selected user — mirroring the collection backup check.
		authz := userBackupAuthz(t, fs)
		assert.Equal(t, authorization.CREATE, authz.Verb)
		assert.ElementsMatch(t, authorization.BackupUsers("ns1:alice", "ns1:bob"), authz.Resources)
	})

	t.Run("duplicate includeUsers selector is rejected", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.userLister.users = []string{"ns1:alice"}
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		fs.selector.On("Backupable", ctx, mock.Anything).Return(nil)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &BackupRequest{
			ID:           backupID,
			Backend:      backendName,
			Include:      []string{cls},
			IncludeUsers: []string{"ns1:*", "ns1:*"},
		})
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("includeUsers naming a missing user is rejected", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		fs.userLister.users = []string{"ns1:alice"}
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		fs.selector.On("Backupable", ctx, mock.Anything).Return(nil)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &BackupRequest{
			ID:           backupID,
			Backend:      backendName,
			Include:      []string{cls},
			IncludeUsers: []string{"ns1:ghost"},
		})
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})

	t.Run("includeUsers matching no users is rejected", func(t *testing.T) {
		fs := newFakeScheduler(nil)
		// userLister.users left empty: nothing for the selector to match.
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
		fs.selector.On("Backupable", ctx, mock.Anything).Return(nil)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &BackupRequest{
			ID:           backupID,
			Backend:      backendName,
			Include:      []string{cls},
			IncludeUsers: []string{"ns1:*"},
		})
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no dynamic users match")
		assert.IsType(t, backup.ErrUnprocessable{}, err)
	})
}

// denyUserAuthorizer denies one exact resource and defers everything else to the
// embedded blanket fake, so Calls() still records the allowed authorizations.
type denyUserAuthorizer struct {
	*mocks.FakeAuthorizer
	denyResource string
}

func (d *denyUserAuthorizer) Authorize(ctx context.Context, pr *models.Principal, verb string, resources ...string) error {
	for _, r := range resources {
		if r == d.denyResource {
			return authzerrors.NewForbidden(pr, verb, resources...)
		}
	}
	return d.FakeAuthorizer.Authorize(ctx, pr, verb, resources...)
}

// Scheduler.Backup must record the resolved dynamic-user IDs on the global
// descriptor so the restore side can authorize them later. Ordinary backups (no
// includeUsers) must record no users, keeping the on-disk shape unchanged.
func TestSchedulerCreateBackupRecordsUsers(t *testing.T) {
	t.Parallel()

	var (
		cls         = "Class-A"
		node        = "Node-A"
		backendName = "gcs"
		backupID    = "1"
		any         = mock.Anything
		ctx         = context.Background()
		path        = "dst/path"
		cresp       = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sReq        = &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
		sresp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
	)

	setup := func(fs *fakeScheduler, req *BackupRequest) {
		fs.selector.On("ListClasses", ctx).Return([]string{cls})
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
		fs.backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(marshalMeta(backup.BackupDescriptor{Status: backup.Success}), nil)
	}

	t.Run("includeUsers are recorded on the global descriptor", func(t *testing.T) {
		req := BackupRequest{
			ID:           backupID,
			Include:      []string{cls},
			Backend:      backendName,
			IncludeUsers: []string{"ns1:*"},
		}
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		fs.userLister.users = []string{"ns1:alice", "ns1:bob", "ns2:carol"}
		setup(fs, &req)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &req)
		require.Nil(t, err)
		assert.ElementsMatch(t, []string{"ns1:alice", "ns1:bob"}, resp.Users)
		// ns2:carol must not leak in: only ns1:* was requested.
		assert.ElementsMatch(t, []string{"ns1:alice", "ns1:bob"}, fs.backend.glMeta.UserList())
	})

	t.Run("ordinary backup records no users", func(t *testing.T) {
		req := BackupRequest{
			ID:      backupID,
			Include: []string{cls},
			Backend: backendName,
		}
		fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
		setup(fs, &req)

		resp, err := fs.scheduler().Backup(ctx, &models.Principal{}, &req)
		require.Nil(t, err)
		assert.Empty(t, resp.Users)
		assert.Nil(t, fs.backend.glMeta.Users)
	})
}

// Scheduler.Restore must authorize each dynamic user recorded in the artefact with
// a backup-create permission before any participant work, stripping the namespace
// prefix when the restore strips it, and failing the whole restore if any user is
// denied. Artefacts that enumerate no users, and restores with users disabled, must
// not gate at all (preserving disaster-recovery and collection-only restores).
func TestSchedulerRestoreAuthorizesUsers(t *testing.T) {
	t.Parallel()

	var (
		cls         = "MyClass-A"
		node        = "Node-A"
		any         = mock.Anything
		backendName = "gcs"
		backupID    = "1"
		timePt      = time.Now().UTC()
		ctx         = context.Background()
		path        = "bucket/backups/" + backupID
		keyNode     = backupID + "/" + node
		cResp       = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq        = &StatusRequest{OpRestore, backupID, backendName, "", "", ""}
		sResp       = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
	)

	shardingStateBytes, _ := json.Marshal(&sharding.State{
		IndexID:  cls,
		Physical: map[string]sharding.Physical{"S1": {Name: "S1"}},
	})
	rawClassBytes, _ := json.Marshal(&models.Class{Class: cls})
	nodeMeta := backup.BackupDescriptor{
		ID:     backupID,
		Status: backup.Success,
		Classes: []backup.ClassDescriptor{{
			Name:          cls,
			Schema:        rawClassBytes,
			ShardingState: shardingStateBytes,
		}},
	}
	nodeMetaBytes, _ := json.Marshal(nodeMeta)

	userBackupCall := func(fa *mocks.FakeAuthorizer) (mocks.AuthZReq, bool) {
		for _, c := range fa.Calls() {
			if len(c.Resources) > 0 && strings.HasPrefix(c.Resources[0], authorization.BackupsDomain+"/users/") {
				return c, true
			}
		}
		return mocks.AuthZReq{}, false
	}

	tests := []struct {
		name         string
		users        []string
		option       string
		strip        bool
		denyResource string
		// wantAuthz lists the user resources expected on the recorded authz call,
		// empty when no user authz must be evoked.
		wantAuthz    []string
		wantDenied   bool
		wantDispatch bool
	}{
		{
			name:         "allowed users are authorized and restore proceeds",
			users:        []string{"ns1:alice", "ns1:bob"},
			option:       models.RestoreConfigUsersOptionsAll,
			wantAuthz:    authorization.BackupUsers("ns1:alice", "ns1:bob"),
			wantDispatch: true,
		},
		{
			name:         "one denied user fails the whole restore before dispatch",
			users:        []string{"ns1:alice", "ns1:bob"},
			option:       models.RestoreConfigUsersOptionsAll,
			denyResource: authorization.BackupsDomain + "/users/ns1:bob",
			wantDenied:   true,
		},
		{
			name:         "namespace is stripped before authorizing",
			users:        []string{"ns1:alice"},
			option:       models.RestoreConfigUsersOptionsAll,
			strip:        true,
			wantAuthz:    authorization.BackupUsers("alice"),
			wantDispatch: true,
		},
		{
			name:         "old/empty artefact is not gated and proceeds",
			users:        nil,
			option:       models.RestoreConfigUsersOptionsAll,
			wantDispatch: true,
		},
		{
			name:         "user restore disabled does not gate and proceeds",
			users:        []string{"ns1:alice"},
			option:       models.RestoreConfigUsersOptionsNoRestore,
			wantDispatch: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meta := backup.DistributedBackupDescriptor{
				ID:            backupID,
				StartedAt:     timePt,
				Version:       "1",
				ServerVersion: "1",
				Status:        backup.Success,
				Nodes:         map[string]*backup.NodeDescriptor{node: {Classes: []string{cls}}},
				Users:         tc.users,
			}
			metaBytes := marshalCoordinatorMeta(meta)

			fs := newFakeScheduler(newFakeNodeResolver([]string{node}))
			// Strip happens iff the target cluster is non-namespaced.
			fs.schema.namespacesEnabled = !tc.strip
			fa := mocks.NewMockAuthorizer()
			fs.auth = &denyUserAuthorizer{FakeAuthorizer: fa, denyResource: tc.denyResource}

			fs.backend.On("Initialize", ctx, mock.Anything).Return(nil)
			fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(metaBytes, nil)
			fs.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(metaBytes, nil)
			fs.backend.On("GetObject", ctx, keyNode, BackupFile).Return(nodeMetaBytes, nil)
			fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
			fs.backend.On("PutObject", mock.Anything, mock.Anything, GlobalRestoreFile, mock.AnythingOfType("[]uint8")).Return(nil)
			fs.client.On("CanCommit", any, node, any).Return(cResp, nil)
			fs.client.On("Commit", any, node, sReq).Return(nil)
			fs.client.On("Status", any, node, sReq).Return(sResp, nil)

			req := BackupRequest{
				ID:                backupID,
				Include:           []string{cls},
				Backend:           backendName,
				UserRestoreOption: tc.option,
			}
			s := fs.scheduler()
			resp, err := s.Restore(ctx, &models.Principal{}, &req, false)

			if tc.wantDenied {
				require.Error(t, err)
				assert.Nil(t, resp)
				assert.True(t, errors.As(err, &authzerrors.Forbidden{}), "expected forbidden, got %v", err)
				// No participant work may have started for a denied user.
				fs.client.AssertNotCalled(t, "CanCommit", any, node, any)
				_, ok := userBackupCall(fa)
				assert.False(t, ok, "denied user authz must not be recorded")
				return
			}

			require.Nil(t, err)
			require.NotNil(t, resp)
			// Drain the async restore so AssertExpectations is stable.
			for i := 0; i < 10; i++ {
				time.Sleep(time.Millisecond * 60)
				if i > 0 && s.restorer.lastOp.get().Status == "" {
					break
				}
			}

			call, ok := userBackupCall(fa)
			if len(tc.wantAuthz) == 0 {
				assert.False(t, ok, "no user authz expected; got %+v", call)
			} else {
				require.True(t, ok, "expected a user-backup authz call")
				assert.Equal(t, authorization.CREATE, call.Verb)
				assert.ElementsMatch(t, tc.wantAuthz, call.Resources)
			}
			if tc.wantDispatch {
				fs.client.AssertCalled(t, "CanCommit", any, node, any)
				// RestoreClass receives the strip derived from the target's
				// namespace state: strip iff the cluster is non-namespaced.
				assert.Equal(t, tc.strip, fs.schema.lastStripNamespaces)
			}
		})
	}
}

// classDesc builds a per-node class descriptor, optionally carrying aliases.
// makeUserSnapshot builds a real dynamic-user snapshot blob containing the
// given qualified ids, so validation exercises the exact apply-time
// strip-and-collide logic rather than a stand-in.
func makeUserSnapshot(t *testing.T, ids ...string) []byte {
	t.Helper()
	logger, _ := test.NewNullLogger()
	dbu, err := apikey.NewDBUser(t.TempDir(), true, logger)
	require.NoError(t, err)
	for _, id := range ids {
		require.NoError(t, dbu.CreateUser(id, "hash-"+id, "ident-"+id, "", namespacing.NamespaceFromQualified(id), time.Now()))
	}
	snap, err := dbu.Snapshot()
	require.NoError(t, err)
	return snap
}

func classDesc(t *testing.T, name string, aliases ...string) backup.ClassDescriptor {
	t.Helper()
	d := backup.ClassDescriptor{Name: name}
	if len(aliases) == 0 {
		return d
	}
	xs := make([]*models.Alias, len(aliases))
	for i, a := range aliases {
		xs[i] = &models.Alias{Alias: a}
	}
	b, err := json.Marshal(xs)
	require.NoError(t, err)
	d.Aliases = b
	d.AliasesIncluded = true
	return d
}

// TestValidateNamespaceStripping pins the fail-fast collision check for
// restores into a namespace-disabled cluster: entities that strip to the
// same identity must reject the whole restore before any node stages data.
// Everything is judged from the per-node descriptors — the payload nodes
// actually restore. Class and alias comparisons fold case; user snapshots
// are dry-run through the UserLister so the collision semantics are exactly
// the apply-time ones.
func TestValidateNamespaceStripping(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Real snapshot blobs, so validation exercises the exact apply-time
	// strip-and-collide logic rather than a stand-in.
	collidingUsers := makeUserSnapshot(t, "ns1:alice", "ns2:alice")
	cleanUsers := makeUserSnapshot(t, "ns1:alice", "ns2:bob")

	tests := []struct {
		name              string
		descriptors       func(t *testing.T) []backup.ClassDescriptor
		selected          []string
		userBlobs         [][]byte
		liveEntities      []string // live class/alias names served by the fake's ClassEqual
		liveClasses       []string // nil means ListClasses must not be called (it only serves stripped aliases)
		namespacesEnabled bool
		userRestoreOption string
		nilUserLister     bool
		wantErr           []string // substrings; empty means no error
	}{
		{
			name: "NamespacedTargetSkipsValidation",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns1:Movies"), classDesc(t, "ns2:Movies")}
			},
			selected:          []string{"ns1:Movies", "ns2:Movies"},
			namespacesEnabled: true,
		},
		{
			// A backup without qualified names must not even read the live
			// schema: plain restores keep their pre-existing semantics. The
			// unstubbed ListClasses mock panics if this regresses.
			name: "PlainBackupBypassesLiveSchemaRead",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "Movies"), classDesc(t, "Books")}
			},
			selected: []string{"Movies", "Books"},
		},
		{
			name: "TwoNamespacesSameClass",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns1:Movies"), classDesc(t, "ns2:Movies")}
			},
			selected: []string{"ns1:Movies", "ns2:Movies"},
			wantErr:  []string{"ns1:Movies", "ns2:Movies", `"Movies"`},
		},
		{
			name: "CaseVariantClassesCollide",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns1:MOVIES"), classDesc(t, "ns2:Movies")}
			},
			selected: []string{"ns1:MOVIES", "ns2:Movies"},
			wantErr:  []string{"ns1:MOVIES", "ns2:Movies"},
		},
		{
			name: "QualifiedCollidesWithUnqualified",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "Movies"), classDesc(t, "ns1:Movies")}
			},
			selected: []string{"Movies", "ns1:Movies"},
			wantErr:  []string{"[Movies ns1:Movies]"},
		},
		{
			name: "SingleNamespaceIsCollisionFree",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns1:Movies"), classDesc(t, "ns1:Books")}
			},
			selected: []string{"ns1:Movies", "ns1:Books"},
		},
		{
			// The request's include/exclude selection governs: a colliding
			// class left out of the restore must not block it.
			name: "UnselectedDescriptorsIgnored",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns1:Movies"), classDesc(t, "ns2:Movies")}
			},
			selected: []string{"ns1:Movies"},
		},
		{
			name: "StrippedNameTakenByLiveClass",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns1:Movies")}
			},
			selected:     []string{"ns1:Movies"},
			liveEntities: []string{"Movies"},
			wantErr:      []string{"already exists in the cluster"},
		},
		{
			name: "StrippedNameTakenByLiveCaseVariant",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns1:Movies")}
			},
			selected:     []string{"ns1:Movies"},
			liveEntities: []string{"MOVIES"},
			wantErr:      []string{`"MOVIES"`},
		},
		{
			// ClassEqual spans live classes AND live aliases — the same
			// predicate the RAFT gate rejects with — so a stripped class
			// landing on a live alias fails fast instead of partially
			// committing mid-restore. The class/alias distinction inside
			// the predicate is pinned at the gate's own test
			// (TestSchemaManager_PreApplyFilterRestoreClassCollision).
			name: "StrippedClassTakenByLiveAlias",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns1:Movies")}
			},
			selected:     []string{"ns1:Movies"},
			liveEntities: []string{"Movies"},
			wantErr:      []string{`already exists in the cluster as "Movies"`},
		},
		{
			// An unqualified backup class colliding with a live class keeps
			// the pre-existing lazy per-class RAFT rejection; only names
			// changed by stripping fail fast here.
			name: "UnqualifiedKeepsLazyPerClassSemantics",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "Movies"), classDesc(t, "ns1:Books")}
			},
			selected:     []string{"Movies", "ns1:Books"},
			liveEntities: []string{"Movies"},
		},
		{
			name: "AliasesMergeAcrossNamespaces",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{
					classDesc(t, "ns1:Movies", "ns1:Films"),
					classDesc(t, "ns2:Books", "ns2:Films"),
				}
			},
			selected:    []string{"ns1:Movies", "ns2:Books"},
			liveClasses: []string{},
			wantErr:     []string{"ns1:Films", "ns2:Films", `"Films"`},
		},
		{
			name: "CaseVariantAliasesCollide",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{
					classDesc(t, "ns1:Movies", "ns1:FILMS"),
					classDesc(t, "ns2:Books", "ns2:Films"),
				}
			},
			selected:    []string{"ns1:Movies", "ns2:Books"},
			liveClasses: []string{},
			wantErr:     []string{"ns1:FILMS", "ns2:Films"},
		},
		{
			name: "AliasCollidesWithClass",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{
					classDesc(t, "ns1:Movies"),
					classDesc(t, "ns2:Books", "ns2:Movies"),
				}
			},
			selected:    []string{"ns1:Movies", "ns2:Books"},
			liveClasses: []string{},
			wantErr:     []string{"ns2:Movies", "collides with backup class [ns1:Movies]"},
		},
		{
			// A stripped alias landing on a live class hard-errors in
			// CreateAlias only after its class's data is committed — the
			// one-namespace-at-a-time workflow hits this when an earlier
			// restore's class owns the name the alias strips to.
			name: "StrippedAliasTakenByLiveClass",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns2:Books", "ns2:Movies")}
			},
			selected:    []string{"ns2:Books"},
			liveClasses: []string{"Movies"},
			wantErr:     []string{"ns2:Movies", `already exists as class "Movies"`},
		},
		{
			name: "StrippedAliasTakenByLiveCaseVariantClass",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns2:Books", "ns2:movies")}
			},
			selected:    []string{"ns2:Books"},
			liveClasses: []string{"MOVIES"},
			wantErr:     []string{`already exists as class "MOVIES"`},
		},
		{
			// A stripped alias alone must trigger the live-schema read even
			// when no selected class name changes.
			name: "AliasStripAloneTriggersLiveCheck",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "Books", "ns2:Movies")}
			},
			selected:    []string{"Books"},
			liveClasses: []string{"Movies"},
			wantErr:     []string{"ns2:Movies", `already exists as class "Movies"`},
		},
		{
			name: "DistinctAliasesPass",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{
					classDesc(t, "ns1:Movies", "ns1:Films"),
					classDesc(t, "ns2:Books", "ns2:Novels"),
				}
			},
			selected:    []string{"ns1:Movies", "ns2:Books"},
			liveClasses: []string{},
		},
		{
			name: "AliasesNotIncludedSkipped",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				d := classDesc(t, "ns1:Movies", "ns1:Films")
				d.AliasesIncluded = false
				e := classDesc(t, "ns2:Books", "ns2:Films")
				e.AliasesIncluded = false
				return []backup.ClassDescriptor{d, e}
			},
			selected: []string{"ns1:Movies", "ns2:Books"},
		},
		{
			name: "MalformedAliasBlobErrors",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{
					{Name: "ns1:Movies", Aliases: []byte("{"), AliasesIncluded: true},
				}
			},
			selected: []string{"ns1:Movies"},
			wantErr:  []string{"unmarshal aliases", "ns1:Movies"},
		},
		{
			// The user snapshot is validated by dry-running the apply-time
			// strip, so implicit whole-cluster snapshots are covered even
			// though no user ids appear in any descriptor.
			name:      "UserSnapshotCollisionRejects",
			userBlobs: [][]byte{collidingUsers},
			wantErr:   []string{"dynamic users:", `"alice"`},
		},
		{
			name:      "CleanUserSnapshotPasses",
			userBlobs: [][]byte{cleanUsers},
		},
		{
			name:      "EachUserSnapshotValidated",
			userBlobs: [][]byte{cleanUsers, collidingUsers},
			wantErr:   []string{"dynamic users:"},
		},
		{
			name:              "UserOptOutSkipsUserCheck",
			userBlobs:         [][]byte{collidingUsers},
			userRestoreOption: models.RestoreConfigUsersOptionsNoRestore,
		},
		{
			// Participants apply a present user snapshot even when dynamic
			// users are disabled on the target (userLister nil), so the
			// collision must still fail fast.
			name:          "NilUserListerStillValidates",
			userBlobs:     [][]byte{collidingUsers},
			nilUserLister: true,
			wantErr:       []string{"dynamic users:", `"alice"`},
		},
		{
			name: "AllCollisionsReported",
			descriptors: func(t *testing.T) []backup.ClassDescriptor {
				return []backup.ClassDescriptor{classDesc(t, "ns1:Movies"), classDesc(t, "ns2:Movies")}
			},
			selected:  []string{"ns1:Movies", "ns2:Movies"},
			userBlobs: [][]byte{collidingUsers},
			wantErr:   []string{`"Movies"`, `"alice"`},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newFakeScheduler(nil)
			fs.schema.namespacesEnabled = tc.namespacesEnabled
			fs.schema.liveEntities = tc.liveEntities
			if tc.liveClasses != nil {
				fs.selector.On("ListClasses", mock.Anything).Return(tc.liveClasses)
			}
			s := fs.scheduler()
			if tc.nilUserLister {
				s.userLister = nil
			}
			var descriptors []backup.ClassDescriptor
			if tc.descriptors != nil {
				descriptors = tc.descriptors(t)
			}

			err := s.validateNamespaceStripping(ctx, descriptors, tc.userBlobs, tc.selected, tc.userRestoreOption)

			if len(tc.wantErr) == 0 {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range tc.wantErr {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

// TestRestoreNamespaceStrippingCollisionFailsFast proves the whole restore
// is rejected at the scheduler, before any participant node is contacted:
// no staging, no state mutation, terminal Unprocessable error.
func TestRestoreNamespaceStrippingCollisionFailsFast(t *testing.T) {
	t.Parallel()
	var (
		ctx         = context.Background()
		backendName = "gcs"
		backupID    = "stripping-collision"
		nodeName    = "Node-A"
		nodeKey     = backupID + "/" + nodeName
	)
	meta := backup.DistributedBackupDescriptor{
		ID:            backupID,
		StartedAt:     time.Now().UTC(),
		Version:       "1",
		ServerVersion: "1",
		Status:        backup.Success,
		Leader:        nodeName,
		Nodes: map[string]*backup.NodeDescriptor{
			nodeName: {Classes: []string{"ns1:Movies", "ns2:Movies"}},
		},
	}
	nodeMeta := backup.BackupDescriptor{
		ID:     backupID,
		Status: backup.Success,
		Classes: []backup.ClassDescriptor{
			{Name: "ns1:Movies"},
			{Name: "ns2:Movies"},
		},
	}
	nodeMetaBytes, err := json.Marshal(nodeMeta)
	require.NoError(t, err)

	fs := newFakeScheduler(newFakeNodeResolver([]string{nodeName}))
	fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
	fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
	fs.backend.On("GetObject", ctx, nodeKey, BackupFile).Return(nodeMetaBytes, nil)
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
	// ListClasses is deliberately unstubbed: class collisions are judged via
	// ClassEqual, so the selector must not be consulted here.

	s := fs.scheduler()
	resp, err := s.Restore(ctx, nil, &BackupRequest{
		ID:      backupID,
		Backend: backendName,
		Include: []string{"ns1:Movies", "ns2:Movies"},
	}, false)

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.IsType(t, backup.ErrUnprocessable{}, err)
	assert.Contains(t, err.Error(), "strip to the same name")
	fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
}

// TestRestoreSecondNamespaceAliasCollisionFailsFast pins the sequential
// one-namespace-at-a-time workflow: an earlier restore's class already owns
// the name a later namespace's alias strips to. Without fail-fast validation
// the second restore would commit its class via RAFT and only then fail in
// CreateAlias, leaving a partial commit behind a Failed status.
func TestRestoreSecondNamespaceAliasCollisionFailsFast(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		backupID = "second-namespace-alias-collision"
		nodeName = "Node-A"
		nodeKey  = backupID + "/" + nodeName
	)
	meta := backup.DistributedBackupDescriptor{
		ID:            backupID,
		StartedAt:     time.Now().UTC(),
		Version:       "1",
		ServerVersion: "1",
		Status:        backup.Success,
		Leader:        nodeName,
		Nodes: map[string]*backup.NodeDescriptor{
			nodeName: {Classes: []string{"ns2:Books"}},
		},
	}
	nodeMeta := backup.BackupDescriptor{
		ID:      backupID,
		Status:  backup.Success,
		Classes: []backup.ClassDescriptor{classDesc(t, "ns2:Books", "ns2:Movies")},
	}
	nodeMetaBytes, err := json.Marshal(nodeMeta)
	require.NoError(t, err)

	fs := newFakeScheduler(newFakeNodeResolver([]string{nodeName}))
	fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
	fs.backend.On("GetObject", ctx, backupID, GlobalBackupFile).Return(marshalCoordinatorMeta(meta), nil)
	fs.backend.On("GetObject", ctx, nodeKey, BackupFile).Return(nodeMetaBytes, nil)
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
	// "Movies" is live from the previous namespace's restore.
	fs.selector.On("ListClasses", mock.Anything).Return([]string{"Movies"})

	s := fs.scheduler()
	resp, err := s.Restore(ctx, nil, &BackupRequest{
		ID:      backupID,
		Backend: "gcs",
		Include: []string{"ns2:Books"},
	}, false)

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.IsType(t, backup.ErrUnprocessable{}, err)
	assert.Contains(t, err.Error(), `already exists as class "Movies"`)
	fs.client.AssertNotCalled(t, "CanCommit", mock.Anything, mock.Anything, mock.Anything)
}

// TestFetchSchemaFailsClosed pins the union path's error handling: a node
// meta that cannot be read must fail the fetch instead of silently returning
// a partial descriptor set, which would skip the missing node's classes at
// restore time and blind the strip validation.
func TestFetchSchemaFailsClosed(t *testing.T) {
	t.Parallel()
	var (
		ctx      = context.Background()
		backupID = "fetch-schema-fails-closed"
		nodeA    = "Node-A"
		nodeB    = "Node-B"
	)
	meta := backup.DistributedBackupDescriptor{
		ID:     backupID,
		Status: backup.Success,
		// No Leader: the pre-RAFT union path reads every node's meta.
		Nodes: map[string]*backup.NodeDescriptor{
			nodeA: {Classes: []string{"ClassA"}},
			nodeB: {Classes: []string{"ClassB"}},
		},
	}
	nodeMeta := backup.BackupDescriptor{
		ID:      backupID,
		Status:  backup.Success,
		Classes: []backup.ClassDescriptor{{Name: "ClassA"}},
	}
	nodeMetaBytes, err := json.Marshal(nodeMeta)
	require.NoError(t, err)

	fs := newFakeScheduler(newFakeNodeResolver([]string{nodeA, nodeB}))
	fs.backend.On("Initialize", mock.Anything, mock.Anything).Return(nil)
	fs.backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/" + backupID)
	fs.backend.On("GetObject", ctx, backupID+"/"+nodeA, BackupFile).Return(nodeMetaBytes, nil)
	fs.backend.On("GetObject", ctx, backupID+"/"+nodeB, BackupFile).Return(nil, ErrAny)
	// The node-meta read falls back to the pre-node-dir legacy layout on
	// error; deny it so the original failure surfaces.
	fs.backend.On("GetObject", ctx, backupID, BackupFile).Return(nil, backup.ErrNotFound{})

	s := fs.scheduler()
	schema, userBlobs, err := s.fetchSchema(ctx, "gcs", "", "", &meta)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetch meta of node")
	assert.Nil(t, schema)
	assert.Nil(t, userBlobs)
}
