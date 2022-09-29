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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus"
)

const (
	_TimeoutShardCommit = 20 * time.Second
)

type reqStat struct {
	Starttime time.Time
	ID        string
	Status    backup.Status
	path      string
}

type backupStat struct {
	reqStat
	sync.Mutex
}

func (s *backupStat) get() reqStat {
	s.Lock()
	defer s.Unlock()
	return s.reqStat
}

// renew state if and only it is not in use
// it returns "" in case of success and current id in case of failure
func (s *backupStat) renew(id string, start time.Time, path string) string {
	s.Lock()
	defer s.Unlock()
	if s.reqStat.ID != "" {
		return s.reqStat.ID
	}
	s.reqStat.ID = id
	s.reqStat.path = path
	s.reqStat.Starttime = start
	s.reqStat.Status = backup.Started
	return ""
}

func (s *backupStat) reset() {
	s.Lock()
	s.reqStat.ID = ""
	s.reqStat.path = ""
	s.reqStat.Status = ""
	s.Unlock()
}

func (s *backupStat) set(st backup.Status) {
	s.Lock()
	s.reqStat.Status = st
	s.Unlock()
}

type backupper struct {
	logger     logrus.FieldLogger
	sourcer    Sourcer
	backends   BackupBackendProvider
	lastBackup backupStat

	waitingForCoodinatorToCommit atomic.Bool
	coordChan                    chan interface{}
	timeoutCommit                time.Duration
	// lastAsyncError used for debugging when no metadata is created
	lastAsyncError error
}

func newBackupper(logger logrus.FieldLogger, sourcer Sourcer, backends BackupBackendProvider,
) *backupper {
	return &backupper{
		logger:        logger,
		sourcer:       sourcer,
		backends:      backends,
		coordChan:     make(chan interface{}, 5),
		timeoutCommit: _TimeoutShardCommit,
	}
}

// Backup is called by the User
func (b *backupper) Backup(ctx context.Context,
	store objectStore, id string, classes []string,
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
		Path:   store.HomeDir(id),
		Status: backup.Started,
	}, nil
}

// Status returns status of a backup
// If the backup is still active the status is immediately returned
// If not it fetches the metadata file to get the status
func (b *backupper) Status(ctx context.Context, backend, bakID string,
) (*models.BackupCreateStatusResponse, error) {
	// check if backup is still active
	st := b.lastBackup.get()
	if st.ID == bakID {
		status := string(st.Status)
		return &models.BackupCreateStatusResponse{
			ID:      bakID,
			Path:    st.path,
			Status:  &status,
			Backend: backend,
		}, nil
	}

	// The backup might have been already created.
	store, err := b.objectStore(backend)
	if err != nil {
		err = fmt.Errorf("no backup provider %q, did you enable the right module?", backend)
		return nil, backup.NewErrUnprocessable(err)
	}

	meta, err := store.Meta(ctx, bakID)
	if err != nil {
		return nil, backup.NewErrNotFound(
			fmt.Errorf("backup status: get metafile %s/%s: %w", bakID, BackupFile, err))
	}

	status := string(meta.Status)

	return &models.BackupCreateStatusResponse{
		ID:      bakID,
		Path:    store.HomeDir(bakID),
		Status:  &status,
		Backend: backend,
	}, nil
}

func (b *backupper) objectStore(backend string) (objectStore, error) {
	caps, err := b.backends.BackupBackend(backend)
	if err != nil {
		return objectStore{}, err
	}
	return objectStore{caps}, nil
}

// Backup is called by the User
func (b *backupper) backup(ctx context.Context,
	store objectStore, req *Request,
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
	if prevID := b.lastBackup.renew(id, time.Now(), store.HomeDir(id)); prevID != "" {
		return ret, fmt.Errorf("backup %s already in progress", prevID)
	}
	b.waitingForCoodinatorToCommit.Store(true) // is set to false by wait()

	go func() {
		defer b.lastBackup.reset()
		if err := b.wait(expiration, id); err != nil {
			b.logger.WithField("action", "create_backup").
				Error(err)
			b.lastAsyncError = err
			return

		}
		provider := newUploader(b.sourcer, store, req.ID, b.lastBackup.set)
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

// wait for the coordinator to confirm or to abort previous operation
func (b *backupper) wait(d time.Duration, id string) error {
	defer b.waitingForCoodinatorToCommit.Store(false)
	if d == 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return fmt.Errorf("timed out waiting for coordinator to commit")
		case v, ok := <-b.coordChan:
			if !ok {
				continue
			}
			switch v := v.(type) {
			case AbortRequest:
				if v.ID == id {
					return fmt.Errorf("coordinator aborted operation")
				}
			case StatusRequest:
				if v.ID == id {
					return nil
				}
			}
		}
	}
}

// OnCommit will be triggered when the coordinator confirms the execution of a previous operation
func (b *backupper) OnCommit(ctx context.Context, req *StatusRequest) error {
	st := b.lastBackup.get()
	if st.ID == req.ID && b.waitingForCoodinatorToCommit.Load() {
		b.coordChan <- *req
		return nil
	}
	return fmt.Errorf("shard has abandoned backup opeartion")
}

// Abort tells a node to abort the previous backup operation
func (b *backupper) OnAbort(_ context.Context, req *AbortRequest) error {
	st := b.lastBackup.get()
	if st.ID == req.ID && b.waitingForCoodinatorToCommit.Load() {
		b.coordChan <- *req
		return nil
	}
	return nil
}
