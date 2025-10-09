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
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

var (
	errLocalBackendDBRO = errors.New("local filesystem backend is not viable for backing up a node cluster, try s3 or gcs")
	errIncludeExclude   = errors.New("malformed request: 'include' and 'exclude' cannot both contain values")
)

const (
	errMsgHigherVersion = "unable to restore backup as it was produced by a higher version"
)

// Scheduler assigns backup operations to coordinators.
type Scheduler struct {
	// deps
	logger     logrus.FieldLogger
	authorizer authorization.Authorizer
	backupper  *coordinator
	restorer   *coordinator
	backends   BackupBackendProvider
}

// NewScheduler creates a new scheduler with two coordinators
func NewScheduler(
	authorizer authorization.Authorizer,
	client client,
	sourcer Selector,
	backends BackupBackendProvider,
	nodeResolver NodeResolver,
	schema schemaManger,
	logger logrus.FieldLogger,
) *Scheduler {
	m := &Scheduler{
		logger:     logger,
		authorizer: authorizer,
		backends:   backends,
		backupper: newCoordinator(
			sourcer,
			client,
			schema,
			logger, nodeResolver),
		restorer: newCoordinator(
			sourcer,
			client,
			schema,
			logger, nodeResolver),
	}
	return m
}

func (s *Scheduler) CleanupUnfinishedBackups(ctx context.Context) {
	for _, backend := range s.backends.EnabledBackupBackends() {
		backups, err := backend.AllBackups(ctx)
		if err != nil {
			s.logger.
				WithField("action", "cleanup_unfinished_backups").
				Error(fmt.Errorf("get all backups: %w", err))
			continue
		}
		for _, bak := range backups {
			if backupNotCompleted(bak.Status, bak.Error) {
				bak.Status = backup.Cancelled
				bak.Error = "backup canceled due to node restart"
				// TODO: make compatible with override bucket/path?
				store, err := coordBackend(s.backends, backend.Name(), bak.ID, "", "")
				if err != nil {
					s.logger.WithField("action", "cleanup_unfinished_backups").
						Error(fmt.Errorf("init coordinator store: %w", err))
					continue
				}
				// TODO: make compatible with override bucket/path?
				if err := store.PutMeta(ctx, GlobalBackupFile, bak, "", ""); err != nil {
					s.logger.WithField("action", "cleanup_unfinished_backups").
						Error(fmt.Errorf("update meta file: %w", err))
					continue
				}
			}
		}
	}
}

func backupNotCompleted(status backup.Status, errorStr string) bool {
	return status == backup.Started ||
		status == backup.Transferred ||
		status == backup.Transferring ||
		strings.Contains(errorStr, "might be down")
}

func (s *Scheduler) Backup(ctx context.Context, pr *models.Principal, req *BackupRequest,
) (_ *models.BackupCreateResponse, err error) {
	defer func(begin time.Time) {
		logOperation(s.logger, "try_backup", req.ID, req.Backend, begin, err)
	}(time.Now())

	store, err := coordBackend(s.backends, req.Backend, req.ID, req.Bucket, req.Path)
	if err != nil {
		err = fmt.Errorf("no backup backend %q: %w, did you enable the right module?", req.Backend, err)
		return nil, backup.NewErrUnprocessable(err)
	}

	classes, err := s.validateBackupRequest(ctx, store, req)
	if err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	if err := s.authorizer.Authorize(ctx, pr, authorization.CREATE, authorization.Backups(classes...)...); err != nil {
		return nil, err
	}

	if err := store.Initialize(ctx, req.Bucket, req.Path); err != nil {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("init uploader: %w", err))
	}
	breq := Request{
		Method:      OpCreate,
		ID:          req.ID,
		Backend:     req.Backend,
		Classes:     classes,
		Compression: req.Compression,
		Bucket:      req.Bucket,
		Path:        req.Path,
	}
	if err := s.backupper.Backup(ctx, store, &breq); err != nil {
		return nil, backup.NewErrUnprocessable(err)
	} else {
		st := s.backupper.lastOp.get()
		status := string(st.Status)
		return &models.BackupCreateResponse{
			Classes: classes,
			ID:      req.ID,
			Backend: req.Backend,
			Status:  &status,
			Path:    st.Path, // The HomeDir, not the override path
			Bucket:  st.OverrideBucket,
		}, nil
	}
}

// Restore loads the backup and restores classes in temporary directories on the filesystem.
// The final backup restoration is orchestrated by the raft store.
func (s *Scheduler) Restore(ctx context.Context, pr *models.Principal,
	req *BackupRequest,
) (_ *models.BackupRestoreResponse, err error) {
	defer func(begin time.Time) {
		logOperation(s.logger, "try_restore", req.ID, req.Backend, begin, err)
	}(time.Now())

	store, err := coordBackend(s.backends, req.Backend, req.ID, req.Bucket, req.Path)
	if err != nil {
		err = fmt.Errorf("no backup backend %q: %w, did you enable the right module?", req.Backend, err)
		return nil, backup.NewErrUnprocessable(err)
	}
	meta, err := s.validateRestoreRequest(ctx, store, req)
	if err != nil {
		if errors.Is(err, errMetaNotFound) {
			return nil, backup.NewErrNotFound(err)
		}
		return nil, backup.NewErrUnprocessable(err)
	}

	if err := s.authorizer.Authorize(ctx, pr, authorization.CREATE, authorization.Backups(meta.Classes()...)...); err != nil {
		return nil, err
	}

	schema, err := s.fetchSchema(ctx, req.Backend, req.Bucket, req.Path, meta)
	if err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}
	status := string(backup.Started)
	data := &models.BackupRestoreResponse{
		Backend: req.Backend,
		ID:      req.ID,
		Path:    store.HomeDir(req.Bucket, req.Path),
		Classes: meta.Classes(),
	}

	rReq := Request{
		Method:            OpRestore,
		ID:                req.ID,
		Backend:           req.Backend,
		Compression:       req.Compression,
		Classes:           meta.Classes(),
		Bucket:            req.Bucket,
		Path:              req.Path,
		UserRestoreOption: req.UserRestoreOption,
		RbacRestoreOption: req.RbacRestoreOption,
	}
	err = s.restorer.Restore(ctx, store, &rReq, meta, schema)
	if err != nil {
		status = string(backup.Failed)
		data.Error = err.Error()
		return nil, backup.NewErrUnprocessable(err)
	}

	data.Status = &status
	return data, nil
}

func (s *Scheduler) BackupStatus(ctx context.Context, principal *models.Principal,
	backend, backupID, overrideBucket, overridePath string,
) (_ *Status, err error) {
	defer func(begin time.Time) {
		logOperation(s.logger, "backup_status", backupID, backend, begin, err)
	}(time.Now())
	store, err := coordBackend(s.backends, backend, backupID, overrideBucket, overridePath)
	if err != nil {
		err = fmt.Errorf("no backup provider %q: %w, did you enable the right module?", backend, err)
		return nil, backup.NewErrUnprocessable(err)
	}

	req := &StatusRequest{OpCreate, backupID, backend, store.bucket, store.path}
	st, err := s.backupper.OnStatus(ctx, store, req)
	if err != nil {
		return nil, backup.NewErrNotFound(err)
	}
	return st, nil
}

func (s *Scheduler) RestorationStatus(ctx context.Context, principal *models.Principal, backend, backupID, overrideBucket, overridePath string,
) (_ *Status, err error) {
	defer func(begin time.Time) {
		logOperation(s.logger, "restoration_status", backupID, backend, time.Now(), err)
	}(time.Now())
	store, err := coordBackend(s.backends, backend, backupID, overrideBucket, overridePath)
	if err != nil {
		err = fmt.Errorf("no backup provider %q: %w, did you enable the right module?", backend, err)
		return nil, backup.NewErrUnprocessable(err)
	}
	req := &StatusRequest{OpRestore, backupID, backend, overrideBucket, overridePath}
	st, err := s.restorer.OnStatus(ctx, store, req)
	if err != nil {
		return nil, backup.NewErrNotFound(err)
	}
	return st, nil
}

func (s *Scheduler) Cancel(ctx context.Context, principal *models.Principal, backend, backupID, overrideBucket, overridePath string,
) error {
	defer func(begin time.Time) {
		var err error
		logOperation(s.logger, "cancel_backup", backupID, backend, begin, err)
	}(time.Now())

	store, err := coordBackend(s.backends, backend, backupID, overrideBucket, overridePath)
	if err != nil {
		err = fmt.Errorf("no backup provider %q: %w, did you enable the right module?", backend, err)
		return backup.NewErrUnprocessable(err)
	}

	if err := validateID(backupID); err != nil {
		return err
	}

	if err := store.Initialize(ctx, overrideBucket, overridePath); err != nil {
		return backup.NewErrUnprocessable(fmt.Errorf("init uploader: %w", err))
	}

	meta, _ := store.Meta(ctx, GlobalBackupFile, overrideBucket, overridePath)
	if meta != nil {
		if err := s.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Backups(meta.Classes()...)...); err != nil {
			return err
		}
		// if existed meta and not in the next cases shall be cancellable
		switch meta.Status {
		case backup.Cancelled:
			return nil
		case backup.Success:
			return backup.NewErrUnprocessable(fmt.Errorf("backup %q already succeeded", backupID))
		default:
			// do nothing and continue the cancellation
		}
	}

	nodes, err := s.backupper.Nodes(ctx, &Request{
		Method:  OpCreate,
		Backend: backend,
		ID:      backupID,
		Classes: s.backupper.selector.ListClasses(ctx),
	})
	if err != nil {
		return err
	}
	s.backupper.abortAll(ctx,
		&AbortRequest{Method: OpCreate, ID: backupID, Backend: backend, Bucket: overrideBucket, Path: overridePath}, nodes)

	return nil
}

func (s *Scheduler) List(ctx context.Context, principal *models.Principal, backend string) (*models.BackupListResponse, error) {
	var err error
	defer func(begin time.Time) {
		logOperation(s.logger, "list_backup", "", backend, time.Now(), err)
	}(time.Now())

	backupBackend, err := s.backends.BackupBackend(backend)
	if err != nil {
		return nil, err
	}

	backups, err := backupBackend.AllBackups(ctx)
	if err != nil {
		return nil, err
	}

	response := make(models.BackupListResponse, len(backups))
	for i, b := range backups {
		response[i] = &models.BackupListResponseItems0{
			ID:      b.ID,
			Status:  string(b.Status),
			Classes: b.Classes(),
		}
	}

	return &response, nil
}

func coordBackend(provider BackupBackendProvider, backend, id, overrideBucket, overridePath string) (coordStore, error) {
	caps, err := provider.BackupBackend(backend)
	if err != nil {
		return coordStore{}, err
	}
	cs := coordStore{objectStore{backend: caps, backupId: id, bucket: overrideBucket, path: overridePath}}
	return cs, nil
}

func (s *Scheduler) validateBackupRequest(ctx context.Context, store coordStore, req *BackupRequest) ([]string, error) {
	if !store.backend.IsExternal() && s.backupper.nodeResolver.NodeCount() > 1 {
		return nil, errLocalBackendDBRO
	}

	if err := validateID(req.ID); err != nil {
		return nil, err
	}
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		return nil, errIncludeExclude
	}
	if dup := findDuplicate(req.Include); dup != "" {
		return nil, fmt.Errorf("class list 'include' contains duplicate: %s", dup)
	}
	classes := req.Include
	if len(classes) == 0 {
		classes = s.backupper.selector.ListClasses(ctx)
		// no classes exist in the DB
		if len(classes) == 0 {
			return nil, fmt.Errorf("no available classes to backup, there's nothing to do here")
		}
	}
	if classes = filterClasses(classes, req.Exclude); len(classes) == 0 {
		return nil, fmt.Errorf("empty class list: please choose from : %v", classes)
	}

	if err := s.backupper.selector.Backupable(ctx, classes); err != nil {
		return nil, err
	}
	if err := s.checkIfBackupExists(ctx, store, req); err != nil {
		return nil, err
	}
	return classes, nil
}

func (s *Scheduler) checkIfBackupExists(ctx context.Context, store coordStore, req *BackupRequest) error {
	destPath := store.HomeDir(req.Bucket, req.Path)
	// there is no backup with given id on the backend, regardless of its state (valid or corrupted)
	meta, err := store.Meta(ctx, GlobalBackupFile, req.Bucket, req.Path)
	if err == nil && meta.Status != backup.Cancelled {
		return fmt.Errorf("backup %q already exists at %q", req.ID, destPath)
	}

	if !errors.As(err, &backup.ErrNotFound{}) {
		return fmt.Errorf("check if backup %q exists at %q: %w", req.ID, destPath, err)
	}
	return nil
}

func (s *Scheduler) validateRestoreRequest(ctx context.Context, store coordStore, req *BackupRequest) (*backup.DistributedBackupDescriptor, error) {
	if !store.backend.IsExternal() && s.restorer.nodeResolver.NodeCount() > 1 {
		return nil, errLocalBackendDBRO
	}
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		return nil, errIncludeExclude
	}
	if dup := findDuplicate(req.Include); dup != "" {
		return nil, fmt.Errorf("class list 'include' contains duplicate: %s", dup)
	}
	destPath := store.HomeDir(req.Bucket, req.Path)
	meta, err := store.Meta(ctx, GlobalBackupFile, req.Bucket, req.Path)
	if err != nil {
		notFoundErr := backup.ErrNotFound{}
		if errors.As(err, &notFoundErr) {
			return nil, fmt.Errorf("backup id %q does not exist: %w: %w", req.ID, notFoundErr, errMetaNotFound)
		}
		return nil, fmt.Errorf("find backup %s: %w", destPath, err)
	}
	if meta.ID != req.ID {
		return nil, fmt.Errorf("wrong backup file: expected %q got %q", req.ID, meta.ID)
	}
	if meta.Status != backup.Success {
		return nil, fmt.Errorf("invalid backup in scheduler %s status: %s", destPath, meta.Status)
	}
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("corrupted backup file: %w", err)
	}
	if v := meta.Version; v[0] > Version[0] {
		return nil, fmt.Errorf("%s: %s > %s", errMsgHigherVersion, v, Version)
	}
	cs := meta.Classes()
	if len(req.Include) > 0 {
		if first := meta.AllExist(req.Include); first != "" {
			err = fmt.Errorf("class %s doesn't exist in the backup, but does have %v: ", first, cs)
			return nil, err
		}
		meta.Include(req.Include)
	} else {
		meta.Exclude(req.Exclude)
	}
	if meta.RemoveEmpty().Count() == 0 {
		return nil, fmt.Errorf("nothing left to restore: please choose from : %v", cs)
	}
	if len(req.NodeMapping) > 0 {
		meta.NodeMapping = req.NodeMapping
		meta.ApplyNodeMapping()
	}
	return meta, nil
}

// fetchSchema retrieves and returns the latest schema for all classes
// In pre-raft scenarios where schema may diverge, some guesswork is necessary
func (s *Scheduler) fetchSchema(
	ctx context.Context,
	backend string,
	overrideBucket string,
	overridePath string,
	req *backup.DistributedBackupDescriptor,
) ([]backup.ClassDescriptor, error) {
	f := func(node string) ([]backup.ClassDescriptor, error) {
		store, err := nodeBackend(node, s.backends, backend, req.ID, overrideBucket, overridePath)
		if err != nil {
			return nil, err
		}
		meta, err := store.Meta(ctx, req.ID, store.bucket, store.path, true)
		if err != nil {
			return nil, err
		}
		return meta.Classes, nil
	}

	if req.Leader != "" {
		return f(req.Leader) // raft version of the backup
	}

	// union
	m := make(map[string]backup.ClassDescriptor, 64)
	for k := range req.Nodes {
		xs, err := f(k)
		if err != nil {
			break
		}
		// guess the most up to date version
		for _, x := range xs {
			c, ok := m[x.Name]
			if !ok || len(x.ShardingState) > len(c.ShardingState) {
				m[x.Name] = x
				continue
			}
		}
	}
	xs := make([]backup.ClassDescriptor, len(m))
	i := 0
	for _, v := range m {
		xs[i] = v
		i++
	}
	return xs, nil
}

func logOperation(logger logrus.FieldLogger, name, id, backend string, begin time.Time, err error) {
	le := logger.WithField("action", name).
		WithField("backup_id", id).WithField("backend", backend).
		WithField("took", time.Since(begin))
	if err != nil {
		le.Error(err)
	} else {
		le.Info()
	}
}

// findDuplicate returns first duplicate if it is found, and "" otherwise
func findDuplicate(xs []string) string {
	m := make(map[string]struct{}, len(xs))
	for _, x := range xs {
		if _, ok := m[x]; ok {
			return x
		}
		m[x] = struct{}{}
	}
	return ""
}
