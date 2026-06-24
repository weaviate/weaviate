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
	"path"
	"slices"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

var (
	errLocalBackendDBRO = errors.New("local filesystem backend is not viable for backing up a node cluster, try s3 or gcs")
	errIncludeExclude   = errors.New("malformed request: 'include' and 'exclude' cannot both contain values")
)

const (
	errMsgHigherVersion = "unable to restore backup as it was produced by a higher version"
)

type AllBackupsOrder string

const (
	AllBackupsOrderAsc  AllBackupsOrder = "asc"
	AllBackupsOrderDesc AllBackupsOrder = "desc"
)

// Scheduler assigns backup operations to coordinators.
type Scheduler struct {
	// deps
	logger     logrus.FieldLogger
	authorizer authorization.Authorizer
	backupper  *coordinator
	restorer   *coordinator
	backends   BackupBackendProvider
	// nil when dynamic DB users are not enabled.
	userLister UserLister
}

// NewScheduler creates a new scheduler with two coordinators
func NewScheduler(
	authorizer authorization.Authorizer,
	client client,
	sourcer Selector,
	userLister UserLister,
	backends BackupBackendProvider,
	nodeResolver NodeResolver,
	schema schemaManger,
	logger logrus.FieldLogger,
) *Scheduler {
	m := &Scheduler{
		logger:     logger,
		authorizer: authorizer,
		backends:   backends,
		userLister: userLister,
		backupper: newCoordinator(
			sourcer,
			client,
			schema,
			logger, nodeResolver, backends,
		),
		restorer: newCoordinator(
			sourcer,
			client,
			schema,
			logger, nodeResolver, backends,
		),
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

	explicitInclude := len(req.Include) > 0

	if explicitInclude {
		// Copy Include because authorization.Backups uppercases its input in place.
		includeCopy := append([]string(nil), req.Include...)
		if err := s.authorizer.Authorize(ctx, pr, authorization.CREATE, authorization.Backups(includeCopy...)...); err != nil {
			return nil, err
		}
	}

	store, err := coordBackend(s.backends, req.Backend, req.ID, req.Bucket, req.Path)
	if err != nil {
		err = fmt.Errorf("no backup backend %q: %w, did you enable the right module?", req.Backend, err)
		return nil, backup.NewErrUnprocessable(err)
	}

	classes, users, err := s.validateBackupRequest(ctx, store, req)
	if err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	if !explicitInclude {
		classes, err = s.filterBackupableClasses(ctx, pr, authorization.CREATE, classes)
		if err != nil {
			return nil, err
		}
	}

	// Guard preserves backward compatibility: existing roles only have the
	// collection-scoped backup permission. Skipping the user-scoped check
	// when includeUsers wasn't set keeps ordinary backups working unchanged.
	if len(users) > 0 {
		if err := s.authorizer.Authorize(ctx, pr, authorization.CREATE, authorization.BackupUsers(users...)...); err != nil {
			return nil, err
		}
	}

	if err := store.Initialize(ctx, req.Bucket, req.Path); err != nil {
		return nil, fmt.Errorf("init uploader: %w", err)
	}
	breq := Request{
		Method:       OpCreate,
		ID:           req.ID,
		Backend:      req.Backend,
		Classes:      classes,
		Users:        users,
		Compression:  req.Compression,
		Bucket:       req.Bucket,
		Path:         req.Path,
		BaseBackupID: req.BaseBackupID,
	}
	if err := s.backupper.Backup(ctx, store, &breq); err != nil {
		return nil, err
	} else {
		st := s.backupper.lastOp.get()
		status := string(st.Status)
		return &models.BackupCreateResponse{
			Classes: classes,
			Users:   users,
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
	req *BackupRequest, overwriteAlais bool,
) (_ *models.BackupRestoreResponse, err error) {
	defer func(begin time.Time) {
		logOperation(s.logger, "try_restore", req.ID, req.Backend, begin, err)
	}(time.Now())

	explicitInclude := len(req.Include) > 0

	if explicitInclude {
		// Copy Include because authorization.Backups uppercases its input in place.
		includeCopy := append([]string(nil), req.Include...)
		if err := s.authorizer.Authorize(ctx, pr, authorization.CREATE, authorization.Backups(includeCopy...)...); err != nil {
			return nil, err
		}
	}

	if req.UserRestoreOption != models.RestoreConfigUsersOptionsNoRestore {
		if err := s.authorizer.Authorize(ctx, pr, authorization.CREATE, authorization.BackupUsers(req.IncludeUsers...)...); err != nil {
			return nil, err
		}
	}

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

	if !explicitInclude {
		allowed, err := s.filterBackupableClasses(ctx, pr, authorization.CREATE, meta.Classes())
		if err != nil {
			return nil, err
		}
		meta.Include(allowed)
	}

	schema, err := s.fetchSchema(ctx, req.Backend, req.Bucket, req.Path, meta)
	if err != nil {
		return nil, err
	}
	status := string(backup.Started)
	data := &models.BackupRestoreResponse{
		Backend: req.Backend,
		ID:      req.ID,
		Path:    store.HomeDir(req.Bucket, req.Path),
		Classes: meta.Classes(),
	}

	rReq := Request{
		Method:                OpRestore,
		NodeMapping:           req.NodeMapping,
		ID:                    req.ID,
		Backend:               req.Backend,
		Compression:           req.Compression,
		Classes:               meta.Classes(),
		Bucket:                req.Bucket,
		Path:                  req.Path,
		UserRestoreOption:     req.UserRestoreOption,
		RbacRestoreOption:     req.RbacRestoreOption,
		RestoreOverwriteAlias: overwriteAlais,
		ShouldStripNamespaces: req.ShouldStripNamespaces,
	}
	err = s.restorer.Restore(ctx, store, &rReq, meta, schema)
	if err != nil {
		status = string(backup.Failed)
		data.Error = err.Error()
		return nil, err
	}

	data.Status = &status
	return data, nil
}

// filterBackupableClasses returns the subset of classes the caller may act on
// with verb, narrowing the empty-Include operation instead of failing it whole.
// An empty result is Forbidden; any other authorizer error is Unprocessable.
func (s *Scheduler) filterBackupableClasses(ctx context.Context, pr *models.Principal, verb string, classes []string) ([]string, error) {
	allowed := make([]string, 0, len(classes))
	for _, c := range classes {
		if err := s.authorizer.Authorize(ctx, pr, verb, authorization.Backups(c)...); err != nil {
			if errors.As(err, &authzerrors.Forbidden{}) {
				continue
			}
			return nil, backup.NewErrUnprocessable(err)
		}
		allowed = append(allowed, c)
	}
	if len(allowed) == 0 {
		return nil, authzerrors.NewForbidden(pr, verb, authorization.Backups(classes...)...)
	}
	return allowed, nil
}

// authorizeBackupByID authorizes the caller against the classes recorded in the
// backup meta. A missing meta is a no-op (the 404 may leak the id); any other
// backend error fails closed.
func (s *Scheduler) authorizeBackupByID(ctx context.Context, principal *models.Principal, verb string,
	store coordStore, filename, overrideBucket, overridePath string,
) error {
	meta, err := store.Meta(ctx, filename, overrideBucket, overridePath)
	if err != nil {
		// A read concurrent with a write yields a partial file that fails to
		// unmarshal; treat it as not-found so a mid-write status poll retries.
		var syntaxErr *json.SyntaxError
		if errors.As(err, &backup.ErrNotFound{}) || errors.As(err, &syntaxErr) {
			return nil
		}
		return err
	}
	return s.authorizer.Authorize(ctx, principal, verb, authorization.Backups(meta.Classes()...)...)
}

const metaReadAttempts = 3

// metaWithRetry reads the backup meta, retrying briefly on a partial file mid-write
// (json.SyntaxError) so a class-scoped caller can resolve the real classes for a
// class-aware authz check. ErrNotFound and other errors return immediately.
func metaWithRetry(ctx context.Context, store coordStore, filename, overrideBucket, overridePath string,
) (*backup.DistributedBackupDescriptor, error) {
	var (
		meta *backup.DistributedBackupDescriptor
		err  error
	)
	for attempt := range metaReadAttempts {
		meta, err = store.Meta(ctx, filename, overrideBucket, overridePath)
		if err == nil {
			return meta, nil
		}
		var syntaxErr *json.SyntaxError
		if !errors.As(err, &syntaxErr) || attempt == metaReadAttempts-1 {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil, err
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

	if err := s.authorizeBackupByID(ctx, principal, authorization.READ, store, GlobalBackupFile, overrideBucket, overridePath); err != nil {
		return nil, err
	}

	req := &StatusRequest{OpCreate, backupID, backend, store.bucket, store.path, ""}
	st, err := s.backupper.OnStatus(ctx, store, req)
	if err != nil {
		if errors.Is(err, errMetaNotFound) {
			return nil, backup.NewErrNotFound(err)
		}
		return nil, err
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
	if err := s.authorizeBackupByID(ctx, principal, authorization.READ, store, GlobalRestoreFile, overrideBucket, overridePath); err != nil {
		return nil, err
	}
	req := &StatusRequest{OpRestore, backupID, backend, overrideBucket, overridePath, ""}
	st, err := s.restorer.OnStatus(ctx, store, req)
	if err != nil {
		if errors.Is(err, errMetaNotFound) {
			return nil, backup.NewErrNotFound(err)
		}
		return nil, err
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

	idErr := validateID(backupID)

	// Authorize before validating the id so an unpermitted caller gets 403, not a
	// hint about the id. Scope to the backup's classes when readable, else wildcard.
	var meta *backup.DistributedBackupDescriptor
	var classes []string
	if idErr == nil {
		if m, err := metaWithRetry(ctx, store, GlobalBackupFile, overrideBucket, overridePath); err == nil {
			meta = m
			classes = m.Classes()
		}
	}
	if err := s.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Backups(classes...)...); err != nil {
		return err
	}
	if idErr != nil {
		return backup.NewErrUnprocessable(idErr)
	}

	if err := store.Initialize(ctx, overrideBucket, overridePath); err != nil {
		return fmt.Errorf("init uploader: %w", err)
	}

	if meta != nil {
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

func (s *Scheduler) CancelRestore(ctx context.Context, principal *models.Principal, backend, backupID, overrideBucket, overridePath string,
) (err error) {
	defer func(begin time.Time) {
		logOperation(s.logger, "cancel_restore", backupID, backend, begin, err)
	}(time.Now())

	store, err := coordBackend(s.backends, backend, backupID, overrideBucket, overridePath)
	if err != nil {
		err = fmt.Errorf("no backup provider %q: %w, did you enable the right module?", backend, err)
		return backup.NewErrUnprocessable(err)
	}

	idErr := validateID(backupID)

	// Authorize before validating the id so an unpermitted caller gets 403, not a
	// hint about the id. Prefer the restore descriptor, else the backup descriptor;
	// if neither is readable, require wildcard DELETE.
	var meta *backup.DistributedBackupDescriptor
	var metaErr error
	var classes []string
	if idErr == nil {
		if meta, metaErr = metaWithRetry(ctx, store, GlobalRestoreFile, overrideBucket, overridePath); metaErr == nil {
			classes = meta.Classes()
		} else if backupMeta, err := metaWithRetry(ctx, store, GlobalBackupFile, overrideBucket, overridePath); err == nil {
			classes = backupMeta.Classes()
		}
	}
	if err := s.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Backups(classes...)...); err != nil {
		return err
	}
	if idErr != nil {
		return backup.NewErrUnprocessable(idErr)
	}

	if err := store.Initialize(ctx, overrideBucket, overridePath); err != nil {
		return fmt.Errorf("init uploader: %w", err)
	}

	if metaErr == nil {
		switch meta.Status {
		case backup.Cancelled, backup.Cancelling:
			// Cancellation already in progress or complete
			return nil
		case backup.Success:
			return backup.NewErrUnprocessable(fmt.Errorf("restore %q already succeeded", backupID))
		case backup.Finalizing:
			return backup.NewErrUnprocessable(fmt.Errorf("restore %q is applying schema changes and cannot be cancelled", backupID))
		default:
			// Transferring, Started - attempt to claim cancellation
		}

		// Attempt to claim cancellation by writing CANCELLING status first.
		// This acts as a distributed lock - the first coordinator to write CANCELLING wins.
		meta.Status = backup.Cancelling
		if err := store.PutMeta(ctx, GlobalRestoreFile, meta, overrideBucket, overridePath); err != nil {
			s.logger.WithField("action", "cancel_restore").
				WithField("backup_id", backupID).
				Warnf("failed to write cancelling status, another coordinator may be handling: %v", err)
			// Another coordinator may have won, let them handle it
			return nil
		}

		// Re-read to verify we won the race (another coordinator may have written simultaneously)
		verifyMeta, _ := store.Meta(ctx, GlobalRestoreFile, overrideBucket, overridePath)
		if verifyMeta != nil && verifyMeta.Status == backup.Cancelled {
			// Another coordinator already completed cancellation
			return nil
		}
		s.restorer.lastOp.set(backup.Cancelling)
	}

	// We've claimed cancellation (or meta was nil) - proceed with abort
	nodes, err := s.restorer.Nodes(ctx, &Request{
		Method:  OpRestore,
		Backend: backend,
		ID:      backupID,
		Classes: s.restorer.selector.ListClasses(ctx),
	})
	if err != nil {
		return err
	}
	s.restorer.abortAll(ctx,
		&AbortRequest{Method: OpRestore, ID: backupID, Backend: backend, Bucket: overrideBucket, Path: overridePath}, nodes)

	// Update coordinator's lastOp status to prevent stale reads from OnStatus()
	s.restorer.lastOp.set(backup.Cancelled)

	// Write final CANCELED status to restore_config.json
	if meta != nil {
		meta.Status = backup.Cancelled
		meta.Error = "restore canceled by user"
		meta.CompletedAt = time.Now().UTC()
		if err := store.PutMeta(ctx, GlobalRestoreFile, meta, overrideBucket, overridePath); err != nil {
			s.logger.WithField("action", "cancel_restore").
				WithField("backup_id", backupID).
				Errorf("failed to write canceled status to restore_config.json: %v", err)
			// Don't return error - cancellation signal has been sent to nodes
		}
	}

	return nil
}

func (s *Scheduler) List(ctx context.Context, principal *models.Principal, backend string, sortingOrder *string, includeBaseBackupID bool) (*models.BackupListResponse, error) {
	var err error
	defer func(begin time.Time) {
		logOperation(s.logger, "list_backup", "", backend, time.Now(), err)
	}(time.Now())

	backupBackend, err := s.backends.BackupBackend(backend, modulecapabilities.BackendUseCaseBackup)
	if err != nil {
		err = fmt.Errorf("no backup backend %q: %w, did you enable the right module?", backend, err)
		return nil, backup.NewErrUnprocessable(err)
	}

	backups, err := backupBackend.AllBackups(ctx)
	if err != nil {
		return nil, err
	}

	slices.SortFunc(backups, sortBackups(AllBackupsOrder(*sortingOrder)))

	response := make(models.BackupListResponse, 0, len(backups))
	for _, b := range backups {
		classes := b.Classes()
		if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(classes...)...); err != nil {
			if errors.As(err, &authzerrors.Forbidden{}) {
				continue
			}
			return nil, err
		}
		item := &models.BackupListResponseItems0{
			ID:          b.ID,
			Classes:     classes,
			Status:      string(b.Status),
			StartedAt:   strfmt.DateTime(b.StartedAt.UTC()),
			CompletedAt: strfmt.DateTime(b.CompletedAt.UTC()),
			Size:        float64(b.PreCompressionSizeBytes) / (1024 * 1024 * 1024), // Convert bytes to GiB,
		}
		// Base backup ID is sensitive and only populated for callers the
		// handler has confirmed as root.
		if includeBaseBackupID {
			item.IncrementalBaseBackupID = b.BaseBackupID
		}
		response = append(response, item)
	}

	return &response, nil
}

func sortBackups(order AllBackupsOrder) func(a, b *backup.DistributedBackupDescriptor) int {
	cmp := 1
	if order == AllBackupsOrderDesc {
		cmp = -1
	}

	return func(a, b *backup.DistributedBackupDescriptor) int {
		if a.StartedAt.Before(b.StartedAt) {
			return -cmp
		}
		if a.StartedAt.After(b.StartedAt) {
			return cmp
		}

		return 0
	}
}

func coordBackend(provider BackupBackendProvider, backend, id, overrideBucket, overridePath string) (coordStore, error) {
	caps, err := provider.BackupBackend(backend, modulecapabilities.BackendUseCaseBackup)
	if err != nil {
		return coordStore{}, err
	}
	cs := coordStore{objectStore{backend: caps, backupId: id, bucket: overrideBucket, path: overridePath}}
	return cs, nil
}

// validateBackupRequest resolves the request into concrete classes and
// users. users is empty unless includeUsers was supplied.
func (s *Scheduler) validateBackupRequest(ctx context.Context, store coordStore, req *BackupRequest) (classes, users []string, err error) {
	if !store.backend.IsExternal() && s.backupper.nodeResolver.NodeCount() > 1 {
		return nil, nil, errLocalBackendDBRO
	}

	if err := validateID(req.ID); err != nil {
		return nil, nil, err
	}
	if req.BaseBackupID != "" {
		if err := validateID(req.BaseBackupID); err != nil {
			return nil, nil, fmt.Errorf("base backup id: %w", err)
		}
		if req.ID == req.BaseBackupID {
			return nil, nil, fmt.Errorf("base backup cannot be the same as the new backup ID: %s", req.BaseBackupID)
		}
	}
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		return nil, nil, errIncludeExclude
	}

	if dup := findDuplicate(req.Include); dup != "" {
		return nil, nil, fmt.Errorf("class list 'include' contains duplicate: %s", dup)
	}

	// Get all available classes first for wildcard expansion
	allClasses := s.backupper.selector.ListClasses(ctx)
	if len(allClasses) == 0 {
		return nil, nil, fmt.Errorf("no available classes to backup, there's nothing to do here")
	}

	// Expand wildcards in Include list
	include := expandWildcards(req.Include, allClasses)

	// Expand wildcards in Exclude list
	exclude := expandWildcards(req.Exclude, allClasses)

	classes = include
	if len(classes) == 0 {
		classes = allClasses
	}
	if classes = filterClasses(classes, exclude); len(classes) == 0 {
		return nil, nil, fmt.Errorf("empty class list: please choose from : %v", allClasses)
	}

	if err := s.backupper.selector.Backupable(ctx, classes); err != nil {
		return nil, nil, err
	}

	users, err = s.resolveUsers(req.IncludeUsers)
	if err != nil {
		return nil, nil, err
	}

	if err := s.checkIfBackupExists(ctx, store, req); err != nil {
		return nil, nil, err
	}

	// validate base backup chain
	compressionType, err := CompressionTypeFromLevel(req.Level)
	if err != nil {
		return nil, nil, fmt.Errorf("get compression type: %w", err)
	}
	if _, err := resolveBaseBackupChain(ctx, req.BaseBackupID, req.Bucket, req.Path, compressionType, store.MetaForBackupID); err != nil {
		return nil, nil, fmt.Errorf("resolve base backup chain: %w", err)
	}

	return classes, users, nil
}

// resolveUsers expands includeUsers selectors. Empty input → nil (ordinary
// backup; whole-cluster snapshot is the participant's default).
func (s *Scheduler) resolveUsers(includeUsers []string) ([]string, error) {
	if len(includeUsers) == 0 {
		return nil, nil
	}
	if s.userLister == nil {
		return nil, errors.New("'includeUsers' was set but dynamic DB users are not enabled")
	}
	return resolveUserSelectors(includeUsers, s.userLister.ListAllUsers())
}

// resolveUserSelectors mirrors class-selector semantics: '*'/'?' wildcards,
// dedup, exact selectors must exist, and a non-empty list matching nothing
// errors. Absent includeUsers is the caller's job — not equivalent to "all".
func resolveUserSelectors(includeUsers, allUsers []string) ([]string, error) {
	if dup := findDuplicate(includeUsers); dup != "" {
		return nil, fmt.Errorf("user list 'includeUsers' contains duplicate: %s", dup)
	}

	users := expandWildcards(includeUsers, allUsers)

	known := make(map[string]struct{}, len(allUsers))
	for _, u := range allUsers {
		known[u] = struct{}{}
	}
	for _, u := range users {
		if _, ok := known[u]; !ok {
			return nil, fmt.Errorf("user %q in 'includeUsers' does not exist", u)
		}
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("no dynamic users match 'includeUsers' %v", includeUsers)
	}
	return users, nil
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
	// Check for duplicates in raw patterns early (before backend operations)
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
		return nil, fmt.Errorf("wrong backup file: restore request asked for %q but the descriptor at %q reports its ID as %q (someone placed metadata from a different backup into this slot, or the backup_config.json was overwritten by an aborted operation; remove the slot and retry with the original backup ID)",
			req.ID, path.Join(destPath, GlobalBackupFile), meta.ID)
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

	// Expand wildcards in Include list against backup's classes
	include := expandWildcards(req.Include, cs)

	// Expand wildcards in Exclude list against backup's classes
	exclude := expandWildcards(req.Exclude, cs)

	if len(include) > 0 {
		if first := meta.AllExist(include); first != "" {
			err = fmt.Errorf("class %s doesn't exist in the backup, but does have %v: ", first, cs)
			return nil, err
		}
		meta.Include(include)
	} else {
		meta.Exclude(exclude)
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

// matchesWildcard checks if a class name matches a wildcard pattern.
// Patterns support '*' (matches any sequence) and '?' (matches any single character).
func matchesWildcard(pattern, className string) bool {
	matched, err := path.Match(pattern, className)
	if err != nil {
		return false
	}
	return matched
}

// expandWildcards expands patterns (which may contain wildcards) against a list of candidate classes.
// Non-wildcard patterns are passed through as-is. Wildcard patterns are expanded to matching classes.
func expandWildcards(patterns, candidates []string) []string {
	if len(patterns) == 0 {
		return patterns
	}

	result := make([]string, 0, len(patterns))
	seen := make(map[string]struct{}, len(patterns))

	for _, pattern := range patterns {
		// Check if pattern contains wildcard characters
		if strings.ContainsAny(pattern, "*?") {
			// Expand wildcard pattern against candidates
			for _, candidate := range candidates {
				if matchesWildcard(pattern, candidate) {
					if _, exists := seen[candidate]; !exists {
						seen[candidate] = struct{}{}
						result = append(result, candidate)
					}
				}
			}
		} else {
			// Non-wildcard pattern - add as-is
			if _, exists := seen[pattern]; !exists {
				seen[pattern] = struct{}{}
				result = append(result, pattern)
			}
		}
	}

	return result
}
