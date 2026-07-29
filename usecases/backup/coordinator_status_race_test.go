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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// These tests pin the ordering contract between the status API and durable
// state: a coordinator must never report SUCCESS for a backup or restore
// before the corresponding global descriptor (backup_config.json /
// restore_config.json) has been uploaded to the backend. A client that acts
// on SUCCESS (e.g. immediately restoring a freshly created backup) would
// otherwise race the final metadata upload and be rejected with
// "invalid backup in scheduler ... status: STARTED".

// TestCoordinatorBackupSuccessOnlyAfterGlobalMetaUpload blocks the final
// upload of backup_config.json and asserts that the coordinator's status
// never reports SUCCESS while the upload is still in flight.
func TestCoordinatorBackupSuccessOnlyAfterGlobalMetaUpload(t *testing.T) {
	t.Parallel()
	var (
		backendName  = "s3"
		any          = mock.Anything
		backupID     = "1"
		ctx          = context.Background()
		nodes        = []string{"N1", "N2"}
		classes      = []string{"Class-A", "Class-B"}
		nodeResolver = newFakeNodeResolver(nodes)
		cresp        = &CanCommitResponse{Method: OpCreate, ID: backupID, Timeout: 1}
		sReq         = &StatusRequest{OpCreate, backupID, backendName, "", "", ""}
		sresp        = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpCreate}
	)

	fc := newFakeCoordinator(nodeResolver)
	fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
	fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)
	fc.client.On("CanCommit", any, nodes[0], any).Return(cresp, nil)
	fc.client.On("CanCommit", any, nodes[1], any).Return(cresp, nil)
	fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
	fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
	fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
	fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
	fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
	// node descriptor reads during size aggregation
	fc.backend.On("GetObject", any, any, BackupFile).
		Return(marshalMeta(backup.BackupDescriptor{Status: backup.Success}), nil)
	// status fallback read once the operation is no longer active in memory
	globalSuccess, err := json.Marshal(backup.DistributedBackupDescriptor{ID: backupID, Status: backup.Success})
	require.NoError(t, err)
	fc.backend.On("GetObject", any, backupID, GlobalBackupFile).Return(globalSuccess, nil)

	// PutObject is called twice for the global descriptor: once with STARTED
	// before the nodes commit, once with the final status. Block the final
	// upload to simulate a slow object store.
	releaseFinalPut := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFinalPut) }) }
	t.Cleanup(release)
	var puts atomic.Int32
	fc.backend.On("PutObject", any, backupID, GlobalBackupFile, any).
		Run(func(mock.Arguments) {
			if puts.Add(1) == 2 {
				<-releaseFinalPut
			}
		}).
		Return(nil).
		Twice()

	coordinator := *fc.coordinator()
	coordinator.backends = &fakeBackupBackendProvider{backend: fc.backend}
	req := newReq(classes, backendName, backupID)
	store := coordStore{objectStore{fc.backend, req.ID, "", "", ""}}
	require.NoError(t, coordinator.Backup(ctx, store, &req))

	// While the final descriptor upload is blocked, the status endpoint must
	// not report SUCCESS: the durable descriptor still says STARTED.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		st, err := coordinator.OnStatus(ctx, store, sReq)
		require.NoError(t, err)
		require.NotEqualf(t, backup.Success, st.Status,
			"status reported SUCCESS while the global metadata upload was still in flight")
		time.Sleep(5 * time.Millisecond)
	}

	// Once the upload completes, SUCCESS must become visible.
	release()
	<-fc.backend.doneChan
	assert.Eventually(t, func() bool {
		st, err := coordinator.OnStatus(ctx, store, sReq)
		return err == nil && st.Status == backup.Success
	}, 5*time.Second, 5*time.Millisecond, "status never reported SUCCESS after the metadata upload completed")
}

// TestCoordinatorRestoreSuccessOnlyAfterGlobalMetaUpload is the restore-side
// counterpart: the final upload of restore_config.json is blocked and the
// restoration status must not report SUCCESS while it is in flight.
func TestCoordinatorRestoreSuccessOnlyAfterGlobalMetaUpload(t *testing.T) {
	t.Parallel()
	var (
		backendName  = "s3"
		any          = mock.Anything
		backupID     = "1"
		ctx          = context.Background()
		nodes        = []string{"N1", "N2"}
		classes      = []string{"Class-A", "Class-B"}
		nodeResolver = newFakeNodeResolver(nodes)
		cresp        = &CanCommitResponse{Method: OpRestore, ID: backupID, Timeout: 1}
		sReq         = &StatusRequest{OpRestore, backupID, backendName, "", "", ""}
		sresp        = &StatusResponse{Status: backup.Success, ID: backupID, Method: OpRestore}
		desc         = &backup.DistributedBackupDescriptor{
			StartedAt:     time.Now().UTC(),
			ID:            backupID,
			Status:        backup.Success,
			Version:       Version,
			ServerVersion: config.ServerVersion,
			Nodes: map[string]*backup.NodeDescriptor{
				nodes[0]: {Classes: classes, Status: backup.Success},
				nodes[1]: {Classes: classes, Status: backup.Success},
			},
		}
	)

	fc := newFakeCoordinator(nodeResolver)
	fc.selector.On("Shards", ctx, classes[0]).Return(nodes, nil)
	fc.selector.On("Shards", ctx, classes[1]).Return(nodes, nil)
	fc.client.On("CanCommit", any, nodes[0], any).Return(cresp, nil)
	fc.client.On("CanCommit", any, nodes[1], any).Return(cresp, nil)
	fc.client.On("Commit", any, nodes[0], sReq).Return(nil)
	fc.client.On("Commit", any, nodes[1], sReq).Return(nil)
	fc.client.On("Status", any, nodes[0], sReq).Return(sresp, nil)
	fc.client.On("Status", any, nodes[1], sReq).Return(sresp, nil)
	fc.backend.On("HomeDir", any, any, backupID).Return("bucket/" + backupID)
	// cancellation pre-check before the initial restore_config.json upload;
	// later reads are served dynamically from the fake's stored descriptor
	fc.backend.On("GetObject", ctx, backupID, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})

	// PutObject for restore_config.json is called three times: TRANSFERRING,
	// FINALIZING, and the final status. Block the final upload.
	releaseFinalPut := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFinalPut) }) }
	t.Cleanup(release)
	var puts atomic.Int32
	fc.backend.On("PutObject", any, backupID, GlobalRestoreFile, any).
		Run(func(mock.Arguments) {
			if puts.Add(1) == 3 {
				<-releaseFinalPut
			}
		}).
		Return(nil).
		Times(3)

	coordinator := *fc.coordinator()
	store := coordStore{objectStore{fc.backend, backupID, "", "", ""}}
	req := newReq([]string{}, backendName, "")
	require.NoError(t, coordinator.Restore(ctx, store, &req, desc, nil))

	// While the final descriptor upload is blocked, the restoration status
	// must not report SUCCESS: the durable descriptor is not final yet.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		st, err := coordinator.OnStatus(ctx, store, sReq)
		require.NoError(t, err)
		require.NotEqualf(t, backup.Success, st.Status,
			"restoration status reported SUCCESS while the global metadata upload was still in flight")
		time.Sleep(5 * time.Millisecond)
	}

	// Once the upload completes, SUCCESS must become visible.
	release()
	<-fc.backend.doneChan
	assert.Eventually(t, func() bool {
		st, err := coordinator.OnStatus(ctx, store, sReq)
		return err == nil && st.Status == backup.Success
	}, 5*time.Second, 5*time.Millisecond, "restoration status never reported SUCCESS after the metadata upload completed")
}

// TestUploaderNodeMetaUploadFailureIsNotSuccess pins the node-level variant
// of the same bug class: when a node fails to upload its own backup.json,
// it must not report SUCCESS to the coordinator — the coordinator would
// otherwise mark the whole backup successful even though the node's
// descriptor is missing from the backend, making the backup unrestorable.
func TestUploaderNodeMetaUploadFailureIsNotSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backupID := "1"

	backend := newFakeBackend()
	backend.On("PutObject", mock.Anything, backupID, BackupFile, mock.Anything).Return(ErrAny)

	sourcer := &fakeSourcer{}
	ch := make(chan backup.ClassDescriptor)
	close(ch)
	sourcer.On("BackupDescriptors", mock.Anything, backupID, mock.Anything, mock.Anything).
		Return((<-chan backup.ClassDescriptor)(ch))

	logger, _ := test.NewNullLogger()
	var lastOp backupStat
	lastOp.renew(backupID, "bucket/"+backupID, "", "")
	store := nodeStore{objectStore{backend, backupID, "", "", "N1"}}
	u := newUploader(config.Backup{}, sourcer, nil, nil, nil, store, backupID, lastOp.set, logger)

	desc := backup.BackupDescriptor{ID: backupID, StartedAt: time.Now().UTC()}
	err := u.all(ctx, nil, &desc, nil, "", "")
	require.ErrorIs(t, err, ErrAny)
	assert.NotEqual(t, backup.Success, lastOp.get().Status,
		"node must not report SUCCESS when uploading its meta file failed")
}
