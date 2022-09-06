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
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/sirupsen/logrus"
)

// Version of backup structure
const Version = "1.0"

// TODO
// 1. maybe add node to the base path when initializing storage model
// the base path = "bucket/node/backupid" or somthing like that
// 4. error handling need to be implmented properly.
// Current error handling is not idiomatic and relays on string comparisons which makes testing very brittle.

type BackupStorageProvider interface {
	BackupStorage(storageName string) (modulecapabilities.SnapshotStorage, error)
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type schemaManger interface {
	RestoreClass(ctx context.Context,
		principal *models.Principal,
		d *backup.ClassDescriptor,
	) error
}

type RestoreStatus struct {
	Path        string
	StartedAt   time.Time
	CompletedAt time.Time
	Status      backup.Status
	Err         error
}

type Manager struct {
	logger     logrus.FieldLogger
	authorizer authorizer
	backupper  *backupper
	restorer   *restorer
	storages   BackupStorageProvider

	// TODO: keeping status in memory after restore has been done
	// is not a proper solution for communicating status to the user.
	// On app crash or restart this data will be lost
	// This should be regarded as workaround and should be fixed asap
	RestoreStatus sync.Map
}

func NewManager(
	logger logrus.FieldLogger,
	authorizer authorizer,
	schema schemaManger,
	sourcer Sourcer,
	storages BackupStorageProvider,
) *Manager {
	m := &Manager{
		logger:     logger,
		authorizer: authorizer,
		storages:   storages,
		backupper: newBackupper(logger,
			sourcer,
			storages),
		restorer: newRestorer(logger,
			sourcer,
			storages,
			schema,
		),
	}
	return m
}

type BackupRequest struct {
	// StorageType specify on which storage to store backups (gcs, s3, ..)
	StorageType string
	// ID is the backup ID
	ID string

	// Include is list of class which need to be backed up
	// The same class cannot appear in both Include and Exclude in the same request
	Include []string
	// Exclude means include all classes but those specified in Exclude
	// The same class cannot appear in both Include and Exclude in the same request
	Exclude []string
}

func (m *Manager) Backup(ctx context.Context, pr *models.Principal, req *BackupRequest,
) (*models.BackupCreateResponse, error) {
	path := fmt.Sprintf("backups/%s/%s", req.StorageType, req.ID)
	if err := m.authorizer.Authorize(pr, "add", path); err != nil {
		return nil, err
	}

	if err := validateID(req.ID); err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("invalid include and exclude"))
	}
	classes := req.Include
	if len(classes) == 0 {
		classes = m.backupper.sourcer.ListBackupable()
	}
	if classes = filter_classes(classes, req.Exclude); len(classes) == 0 {
		backup.NewErrUnprocessable(fmt.Errorf("empty class list"))
	}

	if err := m.backupper.sourcer.Backupable(ctx, classes); err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	store, err := m.objectStore(req.StorageType)
	if err != nil {
		err = fmt.Errorf("find storage provider %s", req.StorageType)
		return nil, backup.NewErrUnprocessable(err)
	}

	destPath := store.DestinationPath(req.ID)
	// there is no snapshot with given id on the storage, regardless of its state (valid or corrupted)
	_, err = store.Meta(ctx, req.ID)
	if err == nil {
		err = fmt.Errorf("backup %s already exists at %s", req.ID, destPath)
		return nil, backup.NewErrUnprocessable(err)
	}
	if _, ok := err.(backup.ErrNotFound); !ok {
		err = fmt.Errorf("backup %s already exists at %s", req.ID, destPath)
		return nil, backup.NewErrUnprocessable(err)
	}

	if err := store.Initialize(ctx, req.ID); err != nil {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("init uploader: %w", err))
	}
	if meta, err := m.backupper.Backup(ctx, store, req.ID, classes); err != nil {
		return nil, err
	} else {
		status := string(meta.Status)
		return &models.BackupCreateResponse{
			Classes:     classes,
			ID:          req.ID,
			StorageName: req.StorageType,
			Status:      &status,
			Path:        meta.Path,
		}, nil
	}
}

func (m *Manager) BackupStatus(ctx context.Context, principal *models.Principal,
	storageName, backupID string,
) (*models.BackupCreateStatusResponse, error) {
	path := fmt.Sprintf("backups/%s/%s", storageName, backupID)
	err := m.authorizer.Authorize(principal, "get", path)
	if err != nil {
		return nil, err
	}

	return m.backupper.Status(ctx, storageName, backupID)
}

// TODO validate meta data file

func (m *Manager) Restore(ctx context.Context, pr *models.Principal,
	req *BackupRequest,
) (*models.BackupRestoreResponse, error) {
	path := fmt.Sprintf("backups/%s/%s/restore", req.StorageType, req.ID)
	if err := m.authorizer.Authorize(pr, "restore", path); err != nil {
		return nil, err
	}
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("invalid include and exclude"))
	}

	store, err := m.objectStore(req.StorageType)
	if err != nil {
		err = fmt.Errorf("find storage provider %s", req.StorageType)
		return nil, backup.NewErrUnprocessable(err)
	}
	destPath := store.DestinationPath(req.ID)

	meta, err := store.Meta(ctx, req.ID)
	if err != nil {
		err = fmt.Errorf("find backup %s: %w", destPath, err)
		if _, ok := err.(backup.ErrNotFound); ok {
			return nil, backup.NewErrNotFound(err)
		}
		return nil, backup.NewErrUnprocessable(err)
	}
	if meta.Status != string(backup.Success) {
		err = fmt.Errorf("invalid backup %s status: %s", destPath, meta.Status)
		return nil, backup.NewErrNotFound(err)
	}
	if len(req.Include) > 0 {
		if first := meta.AllExists(req.Exclude); first != "" {
			return nil, backup.NewErrUnprocessable(fmt.Errorf("class %s doesn't exist", first))
		}
		meta.Include(req.Include)
	} else {
		meta.Exclude(req.Exclude)
	}

	status := string(backup.Started)
	classes := meta.List()
	returnData := &models.BackupRestoreResponse{
		Classes:     classes,
		ID:          req.ID,          // TODO remove since it's included in the path
		StorageName: req.StorageType, // TODO remove since it's included in the path
		Status:      &status,
		Path:        path,
	}
	go func() {
		var err error
		status := RestoreStatus{
			Path:      destPath,
			StartedAt: time.Now().UTC(),
			Status:    backup.Transferring,
			Err:       nil,
		}
		defer func() {
			status.CompletedAt = time.Now().UTC()
			if err == nil {
				status.Status = backup.Success
			} else {
				status.Err = err
				status.Status = backup.Failed
			}
			m.RestoreStatus.Store(basePath(req.StorageType, req.ID), status)
		}()
		err = m.restorer.restoreAll(context.Background(), pr, meta, store)
		if err != nil {
			m.logger.WithField("action", "restore").WithField("backup_id", meta.ID).Error(err)
		}
	}()

	return returnData, nil
}

func (m *Manager) RestorationStatus(ctx context.Context, principal *models.Principal, storageName, ID string,
) (_ RestoreStatus, err error) {
	ppath := fmt.Sprintf("backups/%s/%s/restore", storageName, ID)
	if err := m.authorizer.Authorize(principal, "get", ppath); err != nil {
		return RestoreStatus{}, err
	}
	if st := m.restorer.status(); st.ID == ID {
		return RestoreStatus{
			Path:      st.path,
			StartedAt: st.Starttime,
			Status:    st.Status,
		}, nil
	}
	ref := basePath(storageName, ID)
	istatus, ok := m.RestoreStatus.Load(ref)
	if !ok {
		return RestoreStatus{}, errors.Errorf("status not found: %s", ref)
	}
	return istatus.(RestoreStatus), nil
}

func validateID(snapshotID string) error {
	if snapshotID == "" {
		return fmt.Errorf("missing snapshotID value")
	}

	exp := regexp.MustCompile("^[a-z0-9_-]+$")
	if !exp.MatchString(snapshotID) {
		return fmt.Errorf("invalid characters for snapshotID. Allowed are lowercase, numbers, underscore, minus")
	}

	return nil
}

func (m *Manager) objectStore(storageName string) (objectStore, error) {
	caps, err := m.storages.BackupStorage(storageName)
	if err != nil {
		return objectStore{}, err
	}
	return objectStore{caps}, nil
}

// basePath of the backup
func basePath(storageType, backupID string) string {
	return fmt.Sprintf("%s/%s", storageType, backupID)
}

// exclude_classes return classes excluding excludes
// this is and in-place operation
func filter_classes(classes, excludes []string) []string {
	if len(excludes) == 0 {
		return classes
	}
	cs := classes[:0]
	xmap := make(map[string]struct{}, len(excludes))
	for _, c := range excludes {
		xmap[c] = struct{}{}
	}
	for _, c := range classes {
		if _, ok := xmap[c]; !ok {
			cs = append(cs, c)
		}
	}
	return cs
}
