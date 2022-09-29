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
// 1. maybe add node to the base path when initializing backup module
// the base path = "bucket/node/backupid" or somthing like that
// 2. error handling need to be implemented properly.
// Current error handling is not idiomatic and relays on string comparisons which makes testing very brittle.

type BackupBackendProvider interface {
	BackupBackend(backend string) (modulecapabilities.BackupBackend, error)
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
	backends   BackupBackendProvider

	// TODO: keeping status in memory after restore has been done
	// is not a proper solution for communicating status to the user.
	// On app crash or restart this data will be lost
	// This should be regarded as workaround and should be fixed asap
	restoreStatusMap sync.Map
}

func NewManager(
	logger logrus.FieldLogger,
	authorizer authorizer,
	schema schemaManger,
	sourcer Sourcer,
	backends BackupBackendProvider,
) *Manager {
	m := &Manager{
		logger:     logger,
		authorizer: authorizer,
		backends:   backends,
		backupper: newBackupper(logger,
			sourcer,
			backends),
		restorer: newRestorer(logger,
			sourcer,
			backends,
			schema,
		),
	}
	return m
}

type BackupRequest struct {
	// ID is the backup ID
	ID string
	// Backend specify on which backend to store backups (gcs, s3, ..)
	Backend string

	// Include is list of class which need to be backed up
	// The same class cannot appear in both Include and Exclude in the same request
	Include []string
	// Exclude means include all classes but those specified in Exclude
	// The same class cannot appear in both Include and Exclude in the same request
	Exclude []string
}

func (m *Manager) Backup(ctx context.Context, pr *models.Principal, req *BackupRequest,
) (*models.BackupCreateResponse, error) {
	path := fmt.Sprintf("backups/%s/%s", req.Backend, req.ID)
	if err := m.authorizer.Authorize(pr, "add", path); err != nil {
		return nil, err
	}
	store, err := backend(m.backends, req.Backend)
	if err != nil {
		err = fmt.Errorf("no backup backend %q, did you enable the right module?", req.Backend)
		return nil, backup.NewErrUnprocessable(err)
	}

	classes, err := m.validateBackupRequest(ctx, store, req)
	if err != nil {
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
			Classes: classes,
			ID:      req.ID,
			Backend: req.Backend,
			Status:  &status,
			Path:    meta.Path,
		}, nil
	}
}

func (m *Manager) Restore(ctx context.Context, pr *models.Principal,
	req *BackupRequest,
) (*models.BackupRestoreResponse, error) {
	path := fmt.Sprintf("backups/%s/%s/restore", req.Backend, req.ID)
	if err := m.authorizer.Authorize(pr, "restore", path); err != nil {
		return nil, err
	}
	store, err := backend(m.backends, req.Backend)
	if err != nil {
		err = fmt.Errorf("no backup backend %q, did you enable the right module?", req.Backend)
		return nil, backup.NewErrUnprocessable(err)
	}
	meta, err := m.validateRestoreRequst(ctx, store, req)
	if err != nil {
		return nil, err
	}
	cs := meta.List()
	if cls := m.restorer.AnyExists(cs); cls != "" {
		err := fmt.Errorf("cannot restore class %q because it already exists", cls)
		return nil, backup.NewErrUnprocessable(err)
	}
	status := string(backup.Started)
	destPath := store.HomeDir(req.ID)
	returnData := &models.BackupRestoreResponse{
		Classes: cs,
		ID:      req.ID,
		Backend: req.Backend,
		Status:  &status,
		Path:    destPath,
	}
	// make sure there is no active restore
	if prevID := m.restorer.lastStatus.renew(req.ID, time.Now(), destPath); prevID != "" {
		err := fmt.Errorf("restore %s already in progress", prevID)
		return nil, backup.NewErrUnprocessable(err)
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
			m.restoreStatusMap.Store(basePath(req.Backend, req.ID), status)
			m.restorer.lastStatus.reset()
		}()

		err = m.restorer.restoreAll(context.Background(), pr, meta, store)
		if err != nil {
			m.logger.WithField("action", "restore").WithField("backup_id", meta.ID).Error(err)
		}
	}()

	return returnData, nil
}

func (m *Manager) BackupStatus(ctx context.Context, principal *models.Principal,
	backend, backupID string,
) (*models.BackupCreateStatusResponse, error) {
	path := fmt.Sprintf("backups/%s/%s", backend, backupID)
	err := m.authorizer.Authorize(principal, "get", path)
	if err != nil {
		return nil, err
	}

	return m.backupper.Status(ctx, backend, backupID)
}

func (m *Manager) RestorationStatus(ctx context.Context, principal *models.Principal, backend, ID string,
) (_ RestoreStatus, err error) {
	ppath := fmt.Sprintf("backups/%s/%s/restore", backend, ID)
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
	ref := basePath(backend, ID)
	istatus, ok := m.restoreStatusMap.Load(ref)
	if !ok {
		err := errors.Errorf("status not found: %s", ref)
		return RestoreStatus{}, backup.NewErrNotFound(err)
	}
	return istatus.(RestoreStatus), nil
}

func (m *Manager) validateBackupRequest(ctx context.Context, store objectStore, req *BackupRequest) ([]string, error) {
	if err := validateID(req.ID); err != nil {
		return nil, err
	}
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		return nil, fmt.Errorf("malformed request: 'include' and 'exclude' cannot be both empty")
	}
	classes := req.Include
	if len(classes) == 0 {
		classes = m.backupper.sourcer.ListBackupable()
	}
	if classes = filterClasses(classes, req.Exclude); len(classes) == 0 {
		return nil, fmt.Errorf("empty class list: please choose from : %v", classes)
	}

	if err := m.backupper.sourcer.Backupable(ctx, classes); err != nil {
		return nil, err
	}
	destPath := store.HomeDir(req.ID)
	// there is no backup with given id on the backend, regardless of its state (valid or corrupted)
	_, err := store.Meta(ctx, req.ID)
	if err == nil {
		return nil, fmt.Errorf("backup %q already exists at %q", req.ID, destPath)
	}
	if _, ok := err.(backup.ErrNotFound); !ok {
		return nil, fmt.Errorf("check if backup %q exists at %q: %w", req.ID, destPath, err)
	}
	return classes, nil
}

func (m *Manager) validateRestoreRequst(ctx context.Context, store objectStore, req *BackupRequest) (*backup.BackupDescriptor, error) {
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		err := fmt.Errorf("malformed request: 'include' and 'exclude' cannot be both empty")
		return nil, backup.NewErrUnprocessable(err)
	}
	destPath := store.HomeDir(req.ID)
	meta, err := store.Meta(ctx, req.ID)
	if err != nil {
		err = fmt.Errorf("find backup %s: %w", destPath, err)
		nerr := backup.ErrNotFound{}
		if errors.As(err, &nerr) {
			return nil, backup.NewErrNotFound(err)
		}
		return nil, backup.NewErrUnprocessable(err)
	}
	if meta.ID != req.ID {
		err = fmt.Errorf("wrong backup file: expected %q got %q", req.ID, meta.ID)
		return nil, backup.NewErrUnprocessable(err)
	}
	if meta.Status != string(backup.Success) {
		err = fmt.Errorf("invalid backup %s status: %s", destPath, meta.Status)
		return nil, backup.NewErrUnprocessable(err)
	}
	if err := meta.Validate(); err != nil {
		err = fmt.Errorf("corrupted backup file: %w", err)
		return nil, backup.NewErrUnprocessable(err)
	}
	classes := meta.List()
	if len(req.Include) > 0 {
		if first := meta.AllExists(req.Include); first != "" {
			cs := meta.List()
			err = fmt.Errorf("class %s doesn't exist in the backup, but does have %v: ", first, cs)
			return nil, backup.NewErrUnprocessable(err)
		}
		meta.Include(req.Include)
	} else {
		meta.Exclude(req.Exclude)
	}
	if len(meta.Classes) == 0 {
		err = fmt.Errorf("empty class list: please choose from : %v", classes)
		return nil, backup.NewErrUnprocessable(err)
	}
	return meta, nil
}

func validateID(backupID string) error {
	if backupID == "" {
		return fmt.Errorf("missing backupID value")
	}

	exp := regexp.MustCompile("^[a-z0-9_-]+$")
	if !exp.MatchString(backupID) {
		return fmt.Errorf("invalid characters for backupID. Allowed are lowercase, numbers, underscore, minus")
	}

	return nil
}

func backend(provider BackupBackendProvider, backend string) (objectStore, error) {
	caps, err := provider.BackupBackend(backend)
	if err != nil {
		return objectStore{}, err
	}
	return objectStore{caps}, nil
}

// basePath of the backup
func basePath(backendType, backupID string) string {
	return fmt.Sprintf("%s/%s", backendType, backupID)
}

func filterClasses(classes, excludes []string) []string {
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
