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
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/fsm"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

type backupper struct {
	node           string
	logger         logrus.FieldLogger
	sourcer        Sourcer
	rbacSourcer    fsm.Snapshotter
	dynUserSourcer fsm.Snapshotter
	backends       BackupBackendProvider
	// shardCoordinationChan is sync and coordinate operations
	shardSyncChan
}

func newBackupper(node string, logger logrus.FieldLogger, sourcer Sourcer, rbacSourcer fsm.Snapshotter, dynUserSourcer fsm.Snapshotter, backends BackupBackendProvider,
) *backupper {
	return &backupper{
		node:           node,
		logger:         logger,
		sourcer:        sourcer,
		rbacSourcer:    rbacSourcer,
		dynUserSourcer: dynUserSourcer,
		backends:       backends,
		shardSyncChan:  shardSyncChan{coordChan: make(chan interface{}, 5)},
	}
}

// Backup is called by the User
func (b *backupper) Backup(ctx context.Context,
	store nodeStore, id string, classes []string, overrideBucket, overridePath string,
) (*backup.CreateMeta, error) {
	// make sure there is no active backup
	req := Request{
		Method:  OpCreate,
		ID:      id,
		Classes: classes,
		Bucket:  overrideBucket,
		Path:    overridePath,
	}
	if _, err := b.backup(store, &req); err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	return &backup.CreateMeta{
		Path:   store.HomeDir(overrideBucket, overridePath),
		Status: backup.Started,
	}, nil
}

// Status returns status of a backup
// If the backup is still active the status is immediately returned
// If not it fetches the metadata file to get the status
func (b *backupper) Status(ctx context.Context, backend, bakID string,
) (*models.BackupCreateStatusResponse, error) {
	st, err := b.OnStatus(ctx, &StatusRequest{OpCreate, bakID, backend, "", ""}) // retrieved from store
	if err != nil {
		if errors.Is(err, errMetaNotFound) {
			err = backup.NewErrNotFound(err)
		} else {
			err = backup.NewErrUnprocessable(err)
		}
		return nil, err
	}
	// check if backup is still active
	status := string(st.Status)
	return &models.BackupCreateStatusResponse{
		ID:      bakID,
		Path:    st.Path,
		Status:  &status,
		Backend: backend,
	}, nil
}

func (b *backupper) OnStatus(ctx context.Context, req *StatusRequest) (reqState, error) {
	// check if backup is still active
	st := b.lastOp.get()
	if st.ID == req.ID {
		return st, nil // st contains path, which is the homedir, a combination of bucket and path
	}

	// The backup might have been already created.
	store, err := nodeBackend(b.node, b.backends, req.Backend, req.ID, req.Bucket, req.Path)
	if err != nil {
		return reqState{}, fmt.Errorf("no backup provider %q, did you enable the right module?", req.Backend)
	}

	meta, err := store.Meta(ctx, req.ID, store.bucket, store.path, false)
	if err != nil {
		path := fmt.Sprintf("%s/%s", req.ID, BackupFile)
		return reqState{}, fmt.Errorf("cannot get status while backing up: %w: %q: %w", errMetaNotFound, path, err)
	}
	if meta.Error != "" {
		return reqState{}, errors.New(meta.Error)
	}

	return reqState{
		Starttime: meta.StartedAt,
		ID:        req.ID,
		Path:      store.HomeDir(store.bucket, store.path),
		Status:    backup.Status(meta.Status),
	}, nil
}

// backup checks if the node is ready to back up (can commit phase)
//
// Moreover it starts a goroutine in the background which waits for the
// next instruction from the coordinator (second phase).
// It will start the backup as soon as it receives an ack, or abort otherwise
func (b *backupper) backup(store nodeStore, req *Request) (CanCommitResponse, error) {
	id := req.ID
	expiration := req.Duration
	if expiration > _TimeoutShardCommit {
		expiration = _TimeoutShardCommit
	}
	ret := CanCommitResponse{
		Method:  OpCreate,
		ID:      req.ID,
		Timeout: expiration,
	}

	// make sure there is no active backup
	if prevID := b.lastOp.renew(id, store.HomeDir(req.Bucket, req.Path), req.Bucket, req.Path); prevID != "" {
		return ret, fmt.Errorf("backup %s already in progress", prevID)
	}
	b.waitingForCoordinatorToCommit.Store(true) // is set to false by wait()
	// waits for ack from coordinator in order to processed with the backup
	f := func() {
		defer b.lastOp.reset()
		if err := b.waitForCoordinator(expiration, id); err != nil {
			b.logger.WithField("action", "create_backup").
				Error(err)
			b.lastAsyncError = err
			return

		}
		provider := newUploader(b.sourcer, b.rbacSourcer, b.dynUserSourcer, store, req.ID, b.lastOp.set, b.logger).
			withCompression(newZipConfig(req.Compression))

		result := backup.BackupDescriptor{
			StartedAt:     time.Now().UTC(),
			ID:            id,
			Classes:       make([]backup.ClassDescriptor, 0, len(req.Classes)),
			Version:       Version,
			ServerVersion: config.ServerVersion,
		}

		// the coordinator might want to abort the backup
		done := make(chan struct{})
		ctx := b.withCancellation(context.Background(), id, done, b.logger)
		defer close(done)

		logFields := logrus.Fields{"action": "create_backup", "backup_id": req.ID, "override_bucket": req.Bucket, "override_path": req.Path}
		if err := provider.all(ctx, req.Classes, &result, req.Bucket, req.Path); err != nil {
			b.logger.WithFields(logFields).Error(err)
			b.lastAsyncError = err

		} else {
			b.logger.WithFields(logFields).Info("backup completed successfully")
		}
		result.CompletedAt = time.Now().UTC()
	}
	enterrors.GoWrapper(f, b.logger)

	return ret, nil
}
