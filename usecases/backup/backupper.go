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
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus"
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
}

func newBackupper(logger logrus.FieldLogger, sourcer Sourcer, backends BackupBackendProvider,
) *backupper {
	return &backupper{
		logger:   logger,
		sourcer:  sourcer,
		backends: backends,
	}
}

// Backup is called by the User
func (b *backupper) Backup(ctx context.Context,
	store objectStore, id string, classes []string,
) (*backup.CreateMeta, error) {
	// make sure there is no active backup
	if prevID := b.lastBackup.renew(id, time.Now(), store.HomeDir(id)); prevID != "" {
		err := fmt.Errorf("backup %s already in progress", prevID)
		return nil, backup.NewErrUnprocessable(err)
	}
	go func() {
		defer b.lastBackup.reset()
		provider := newUploader(b.sourcer, store, id, b.lastBackup.set)
		result := backup.BackupDescriptor{
			StartedAt:     time.Now().UTC(),
			ID:            id,
			Classes:       make([]backup.ClassDescriptor, 0, len(classes)),
			Version:       Version,
			ServerVersion: config.ServerVersion,
		}
		if err := provider.all(context.Background(), classes, &result); err != nil {
			b.logger.WithField("action", "create_backup").
				Error(err)
		}
		result.CompletedAt = time.Now().UTC()
	}()

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
		// TODO: do we need to remove models.BackupCreateMeta{classes, backend, ID}
		// classes are returned as part of createBackup
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
			fmt.Errorf("backup status: get metafile %s/%s: %w", bakID, MetaDataFilename, err))
	}

	status := string(meta.Status)

	// TODO: populate Error field if backup failed
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
