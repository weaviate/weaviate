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
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus"
)

type backupper struct {
	node     string
	logger   logrus.FieldLogger
	sourcer  Sourcer
	backends BackupBackendProvider
	// shardCoordinationChan is sync and coordinate operations
	shardSyncChan
}

func newBackupper(node string, logger logrus.FieldLogger, sourcer Sourcer, backends BackupBackendProvider,
) *backupper {
	return &backupper{
		node:          node,
		logger:        logger,
		sourcer:       sourcer,
		backends:      backends,
		shardSyncChan: shardSyncChan{coordChan: make(chan interface{}, 5)},
	}
}

// Backup is called by the User
func (b *backupper) Backup(ctx context.Context,
	store nodeStore, id string, classes []string,
) (*backup.CreateMeta, error) {
	// make sure there is no active backup
	req := Request{
		Method:  OpCreate,
		ID:      id,
		Classes: classes,
	}
	if _, err := b.backup(ctx, store, &req); err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	return &backup.CreateMeta{
		Path:   store.HomeDir(),
		Status: backup.Started,
	}, nil
}

// Status returns status of a backup
// If the backup is still active the status is immediately returned
// If not it fetches the metadata file to get the status
func (b *backupper) Status(ctx context.Context, backend, bakID string,
) (*models.BackupCreateStatusResponse, error) {
	st, err := b.OnStatus(ctx, &StatusRequest{OpCreate, bakID, backend})
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

func (b *backupper) OnStatus(ctx context.Context, req *StatusRequest) (reqStat, error) {
	// check if backup is still active
	st := b.lastOp.get()
	if st.ID == req.ID {
		return st, nil
	}

	// The backup might have been already created.
	store, err := nodeBackend(b.node, b.backends, req.Backend, req.ID)
	if err != nil {
		return reqStat{}, fmt.Errorf("no backup provider %q, did you enable the right module?", req.Backend)
	}

	meta, err := store.Meta(ctx)
	if err != nil {
		path := fmt.Sprintf("%s/%s", req.ID, BackupFile)
		return reqStat{}, fmt.Errorf("%w: %q: %v", errMetaNotFound, path, err)
	}

	return reqStat{
		Starttime: meta.StartedAt,
		ID:        req.ID,
		Path:      store.HomeDir(),
		Status:    backup.Status(meta.Status),
	}, nil
}

// Backup is called by the User
func (b *backupper) backup(ctx context.Context,
	store nodeStore, req *Request,
) (CanCommitResponse, error) {
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
	if prevID := b.lastOp.renew(id, store.HomeDir()); prevID != "" {
		return ret, fmt.Errorf("backup %s already in progress", prevID)
	}
	b.waitingForCoordinatorToCommit.Store(true) // is set to false by wait()

	go func() {
		defer b.lastOp.reset()
		if err := b.waitForCoordinator(expiration, id); err != nil {
			b.logger.WithField("action", "create_backup").
				Error(err)
			b.lastAsyncError = err
			return

		}
		provider := newUploader(b.sourcer, store, req.ID, b.lastOp.set)
		result := backup.BackupDescriptor{
			StartedAt:     time.Now().UTC(),
			ID:            id,
			Classes:       make([]backup.ClassDescriptor, 0, len(req.Classes)),
			Version:       Version,
			ServerVersion: config.ServerVersion,
		}
		if err := provider.all(context.Background(), req.Classes, &result); err != nil {
			b.logger.WithField("action", "create_backup").
				Error(err)
		}
		result.CompletedAt = time.Now().UTC()
	}()

	return ret, nil
}
