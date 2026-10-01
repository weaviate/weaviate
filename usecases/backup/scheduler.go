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
	"maps"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
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
	// nil when RBAC is not enabled.
	roleLister RoleLister
	schema     schemaManger
	// The cluster's configured static API key users, empty when API keys are off.
	// The namespace-strip dry run needs them to tell a global static user from a
	// namespaced dynamic one. Build it with rbac.StaticAPIKeyUsers, so this list
	// is the same one the nodes strip with.
	staticAPIKeyUsers []string
	// The state-machine validators run the same check at apply time.
	namespaces usecasesNamespaces.Exister
}

// NewScheduler creates a new scheduler with two coordinators
func NewScheduler(
	authorizer authorization.Authorizer,
	client client,
	sourcer Selector,
	userLister UserLister,
	roleLister RoleLister,
	backends BackupBackendProvider,
	nodeResolver NodeResolver,
	schema schemaManger,
	staticAPIKeyUsers []string,
	rolesAndUsers rolesAndUsersRestorer,
	namespaces usecasesNamespaces.Exister,
	logger logrus.FieldLogger,
) *Scheduler {
	m := &Scheduler{
		logger:            logger,
		authorizer:        authorizer,
		backends:          backends,
		userLister:        userLister,
		roleLister:        roleLister,
		schema:            schema,
		staticAPIKeyUsers: staticAPIKeyUsers,
		namespaces:        namespaces,
		backupper: newCoordinator(
			sourcer,
			client,
			schema,
			logger, nodeResolver, backends, nil,
		),
		restorer: newCoordinator(
			sourcer,
			client,
			schema,
			logger, nodeResolver, backends, rolesAndUsers,
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

	var explicit []string
	if explicitInclude {
		// Copy Include because authorization.Backups uppercases its input in place.
		includeCopy := append([]string(nil), req.Include...)
		explicit = append(explicit, authorization.Backups(includeCopy...)...)
	}
	if len(req.IncludeUsers) > 0 {
		if ids, ok := allLiteralIDs(req.IncludeUsers); ok {
			explicit = append(explicit, authorization.BackupUsers(ids...)...)
		} else {
			explicit = append(explicit, authorization.BackupUsers()...)
		}
	}
	if len(req.IncludeRoles) > 0 {
		if ids, ok := allLiteralIDs(req.IncludeRoles); ok {
			explicit = append(explicit, authorization.BackupRoles(ids...)...)
		} else {
			explicit = append(explicit, authorization.BackupRoles()...)
		}
	}
	if err := s.authorizeResources(ctx, pr, authorization.CREATE, explicit); err != nil {
		return nil, err
	}

	store, err := coordBackend(s.backends, req.Backend, req.ID, req.Bucket, req.Path)
	if err != nil {
		err = fmt.Errorf("no backup backend %q: %w, did you enable the right module?", req.Backend, err)
		return nil, backup.NewErrUnprocessable(err)
	}

	selection, err := s.validateBackupRequest(ctx, store, pr, req)
	if err != nil {
		if errors.As(err, &authzerrors.Forbidden{}) {
			return nil, err
		}
		return nil, backup.NewErrUnprocessable(err)
	}

	// An omitted selector backs up the whole store. A caller who may not do that
	// gets a backup without it, as an omitted include narrows to the authorized
	// classes. This runs after validation, which resets both skip flags.
	if len(req.IncludeUsers) == 0 {
		allowed, err := s.authorizeWholeStore(ctx, pr, authorization.BackupUsers())
		if err != nil {
			return nil, err
		}
		if !allowed {
			selection.skipUsers = true
			s.logger.WithField("action", "try_backup").WithField("backup_id", req.ID).
				Warn("caller may not back up all dynamic users, backing up none: grant manage_backups on users to include them")
		}
	}
	if len(req.IncludeRoles) == 0 {
		allowed, err := s.authorizeWholeStore(ctx, pr, authorization.BackupRoles())
		if err != nil {
			return nil, err
		}
		if !allowed {
			selection.skipRoles = true
			s.logger.WithField("action", "try_backup").WithField("backup_id", req.ID).
				Warn("caller may not back up all roles, backing up none: grant manage_backups on roles to include them")
		}
	}

	// Narrowing can empty a class-less request that passed validation on its
	// default identity selections.
	if len(selection.classes) == 0 && selection.skipUsers && selection.skipRoles {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("backup selects no collections, users, or roles: available collections"))
	}

	if err := store.Initialize(ctx, req.Bucket, req.Path); err != nil {
		return nil, fmt.Errorf("init uploader: %w", err)
	}
	breq := Request{
		Method:       OpCreate,
		ID:           req.ID,
		Backend:      req.Backend,
		Classes:      selection.classes,
		Users:        selection.users,
		Roles:        selection.roles,
		SkipUsers:    selection.skipUsers,
		SkipRoles:    selection.skipRoles,
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
			Classes: selection.classes,
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

	store, err := coordBackend(s.backends, req.Backend, req.ID, req.Bucket, req.Path)
	if err != nil {
		err = fmt.Errorf("no backup backend %q: %w, did you enable the right module?", req.Backend, err)
		return nil, backup.NewErrUnprocessable(err)
	}
	meta, err := s.validateRestoreRequest(ctx, store, pr, req)
	if err != nil {
		if errors.Is(err, errMetaNotFound) {
			return nil, backup.NewErrNotFound(err)
		}
		if errors.As(err, &authzerrors.Forbidden{}) {
			return nil, err
		}
		return nil, backup.NewErrUnprocessable(err)
	}

	schema, userBlob, rbacBlob, err := s.fetchSchema(ctx, req.Backend, req.Bucket, req.Path, meta)
	if err != nil {
		return nil, err
	}
	// A node ignoring a skip flag can upload an excluded snapshot.
	// Applying that snapshot would replace this cluster's users or roles.
	if meta.SkipUsers && len(userBlob) > 0 {
		s.logger.WithField("action", "try_restore").WithField("backup_id", req.ID).
			Warn("discarding the user snapshot: 'includeUsers' matched no user at backup time, a participant uploaded one anyway")
	}
	if meta.SkipRoles && len(rbacBlob) > 0 {
		s.logger.WithField("action", "try_restore").WithField("backup_id", req.ID).
			Warn("discarding the RBAC snapshot: 'includeRoles' matched no role at backup time, a participant uploaded one anyway")
	}
	if meta.SkipUsers {
		userBlob = nil
	}
	if meta.SkipRoles {
		rbacBlob = nil
	}

	// A nil lister means that subsystem is disabled. Its blob stays empty, so
	// nothing is applied for it, and the restore proceeds.
	blobs := rolesAndUsersBlobs{}
	if req.RbacRestoreOption != models.RestoreConfigRolesOptionsNoRestore {
		if s.roleLister != nil {
			blobs.roles = rbacBlob
		} else if len(rbacBlob) > 0 {
			s.logSubsystemDisabled(req.ID, "roles", "RBAC")
		}
	}
	if req.UserRestoreOption != models.RestoreConfigUsersOptionsNoRestore {
		if s.userLister != nil {
			blobs.users = userBlob
		} else if len(userBlob) > 0 {
			s.logSubsystemDisabled(req.ID, "users", "dynamic user management")
		}
	}

	// The descriptor, never the request, names what a blob holds: restore
	// requests carry no includeUsers/includeRoles. This authorization check runs
	// before any step reads a blob. Later steps see only the blobs it authorized,
	// because their errors name the users and roles inside.
	if err := s.authorizeResources(ctx, pr, authorization.CREATE, s.restorePrincipalResources(blobs)); err != nil {
		return nil, err
	}

	if err := s.validateNamespaceStripping(ctx, schema, blobs.users, blobs.roles, meta.Classes(), req.UserRestoreOption, req.RbacRestoreOption); err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	if err := s.validateNamespaceReferences(blobs.users, blobs.roles, req.UserRestoreOption, req.RbacRestoreOption); err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}
	// Restore status and cancel authorize against what this restore applies.
	meta.SkipUsers = len(blobs.users) == 0
	meta.SkipRoles = len(blobs.roles) == 0

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
	}
	err = s.restorer.Restore(ctx, store, &rReq, meta, schema, blobs)
	if err != nil {
		status = string(backup.Failed)
		data.Error = err.Error()
		return nil, err
	}

	data.Status = &status
	return data, nil
}

// filterBackupableClasses returns the classes authorized for the requested action.
// Empty input requires no authorization. If no supplied class is authorized, it
// returns Forbidden. Other authorization errors become Unprocessable.
func (s *Scheduler) filterBackupableClasses(ctx context.Context, pr *models.Principal, verb string, classes []string) ([]string, error) {
	if len(classes) == 0 {
		return classes, nil
	}
	resources := make([]string, len(classes))
	for i, c := range classes {
		resources[i] = authorization.Backups(c)[0]
	}
	permitted, err := s.permittedBackupResources(ctx, pr, verb, resources)
	if err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}
	allowed := make([]string, 0, len(classes))
	for i, c := range classes {
		if permitted[resources[i]] {
			allowed = append(allowed, c)
		}
	}
	if len(allowed) == 0 {
		// Authorize writes one audit denial, for the wildcard resource the error names.
		if err := s.authorizer.Authorize(ctx, pr, verb, authorization.Backups()...); err != nil && !errors.As(err, &authzerrors.Forbidden{}) {
			return nil, backup.NewErrUnprocessable(err)
		}
		return nil, backupsForbidden(pr, verb)
	}
	return allowed, nil
}

// authorizeResources authorizes a backup operation's resources. Every
// users/roles check goes through here: no resources means nothing to
// authorize, and the rbac authorizer rejects a call carrying none, root
// included.
func (s *Scheduler) authorizeResources(ctx context.Context, pr *models.Principal, verb string, resources []string) error {
	if len(resources) == 0 {
		return nil
	}
	return s.authorizer.Authorize(ctx, pr, verb, resources...)
}

// authorizeWholeStore reports whether the caller may back up every user or
// every role. Forbidden returns false, so the backup is narrowed, not failed.
// Any other authorizer error becomes Unprocessable, as in filterBackupableClasses.
func (s *Scheduler) authorizeWholeStore(ctx context.Context, pr *models.Principal, wildcard []string) (bool, error) {
	err := s.authorizeResources(ctx, pr, authorization.CREATE, wildcard)
	switch {
	case err == nil:
		return true, nil
	case errors.As(err, &authzerrors.Forbidden{}):
		return false, nil
	default:
		return false, backup.NewErrUnprocessable(err)
	}
}

// allLiteralIDs returns an includeUsers or includeRoles list as sorted,
// deduplicated literal IDs. ok is false when any entry is a pattern; users and
// roles must treat that case identically, which is why the scan lives here.
func allLiteralIDs(include []string) (ids []string, ok bool) {
	ids = make([]string, 0, len(include))
	for _, id := range include {
		if isWildcard(id) {
			return nil, false
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return slices.Compact(ids), true
}

// restorePrincipalResources returns the users and roles resources a restore
// of blobs requires. Applying a snapshot replaces that subsystem's whole
// store, deleting every entry the snapshot does not carry, so each applied
// blob requires its kind wildcard regardless of the IDs the descriptor names.
func (s *Scheduler) restorePrincipalResources(blobs rolesAndUsersBlobs) []string {
	var resources []string
	if len(blobs.users) > 0 {
		resources = append(resources, authorization.BackupUsers()...)
	}
	if len(blobs.roles) > 0 {
		resources = append(resources, authorization.BackupRoles()...)
	}
	return resources
}

// descriptorPrincipalResources returns the users and roles resources that the
// content recorded in meta requires: none for a skipped subsystem, the named
// IDs, or the kind wildcard for an empty list. A nil meta means it could not
// be read; that requires both wildcards.
func (s *Scheduler) descriptorPrincipalResources(meta *backup.DistributedBackupDescriptor) []string {
	if meta == nil {
		return append(authorization.BackupUsers(), authorization.BackupRoles()...)
	}
	var resources []string
	if !meta.SkipUsers {
		resources = append(resources, authorization.BackupUsers(s.canonicalIDs(meta.UserList())...)...)
	}
	if !meta.SkipRoles {
		resources = append(resources, authorization.BackupRoles(s.canonicalIDs(meta.RoleList())...)...)
	}
	return resources
}

// canonicalIDs returns ids in the one form the checks compare: sorted,
// deduplicated, and stripped of their namespace qualifier when this cluster
// has namespaces disabled, since permissions here name IDs as they exist
// here. Users and roles must strip identically. Empty in, empty out; the
// resource builders map an empty list to the kind wildcard.
func (s *Scheduler) canonicalIDs(ids []string) []string {
	strip := !s.schema.NamespacesEnabled()
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if strip {
			id = namespacing.StripQualification(id)
		}
		out = append(out, id)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// permittedBackupResources returns which of resources pr may act on with verb,
// through FilterAuthorizedResources, which writes no denial to the audit log.
// adminlist refuses the whole list with Forbidden, which permits none of it.
func (s *Scheduler) permittedBackupResources(ctx context.Context, pr *models.Principal, verb string, resources []string) (map[string]bool, error) {
	permitted := make(map[string]bool, len(resources))
	if len(resources) == 0 {
		return permitted, nil
	}
	allowed, err := s.authorizer.FilterAuthorizedResources(ctx, pr, verb, resources...)
	if err != nil && !errors.As(err, &authzerrors.Forbidden{}) {
		return nil, err
	}
	for _, r := range allowed {
		permitted[r] = true
	}
	return permitted, nil
}

// authorizeBackupClasses authorizes verb on classes read from a backup
// descriptor and denies with backupsForbidden.
func (s *Scheduler) authorizeBackupClasses(ctx context.Context, pr *models.Principal, verb string, classes []string) error {
	err := s.authorizer.Authorize(ctx, pr, verb, authorization.Backups(classes...)...)
	if errors.As(err, &authzerrors.Forbidden{}) {
		return backupsForbidden(pr, verb)
	}
	return err
}

// backupsForbidden builds the Forbidden error for classes the caller did not
// name. It names the wildcard resource, since the caller may not see them, and
// the action a role grants, such as read_backups.
func backupsForbidden(pr *models.Principal, verb string) error {
	resource := authorization.Backups()[0]
	action := verb
	if perm, err := conv.PathToPermission(verb, resource); err == nil && perm.Action != nil {
		action = *perm.Action
	}
	return authzerrors.NewForbidden(pr, action, resource)
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
	if err := s.authorizeBackupClasses(ctx, principal, verb, meta.Classes()); err != nil {
		return err
	}
	return s.authorizeResources(ctx, principal, verb, s.descriptorPrincipalResources(meta))
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
		logOperation(s.logger, "restoration_status", backupID, backend, begin, err)
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
	// hint about the id. The check covers the backup's recorded content when its
	// descriptor is readable; without one it requires the wildcards.
	var meta *backup.DistributedBackupDescriptor
	var classes []string
	if idErr == nil {
		if m, err := metaWithRetry(ctx, store, GlobalBackupFile, overrideBucket, overridePath); err == nil {
			meta = m
			classes = m.Classes()
		}
	}
	if err := s.authorizeBackupClasses(ctx, principal, authorization.DELETE, classes); err != nil {
		return err
	}
	if err := s.authorizeResources(ctx, principal, authorization.DELETE, s.descriptorPrincipalResources(meta)); err != nil {
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
	var meta, authMeta *backup.DistributedBackupDescriptor
	var metaErr error
	var classes []string
	if idErr == nil {
		if meta, metaErr = metaWithRetry(ctx, store, GlobalRestoreFile, overrideBucket, overridePath); metaErr == nil {
			authMeta = meta
			classes = meta.Classes()
		} else if backupMeta, err := metaWithRetry(ctx, store, GlobalBackupFile, overrideBucket, overridePath); err == nil {
			authMeta = backupMeta
			classes = backupMeta.Classes()
		}
	}
	if err := s.authorizeBackupClasses(ctx, principal, authorization.DELETE, classes); err != nil {
		return err
	}
	if err := s.authorizeResources(ctx, principal, authorization.DELETE, s.descriptorPrincipalResources(authMeta)); err != nil {
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
		logOperation(s.logger, "list_backup", "", backend, begin, err)
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

	classes := make([][]string, len(backups))
	principals := make([][]string, len(backups))
	for i, b := range backups {
		classes[i] = b.Classes()
		principals[i] = s.descriptorPrincipalResources(b)
	}
	readable, err := s.canReadBackups(ctx, principal, classes, principals)
	if err != nil {
		return nil, err
	}

	response := make(models.BackupListResponse, 0, len(backups))
	for i, b := range backups {
		if !readable[i] {
			continue
		}
		item := &models.BackupListResponseItems0{
			ID:          b.ID,
			Classes:     classes[i],
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

// canReadBackups reports for each backup whether the caller may READ every
// collection it names, uppercasing those names in place as authorization.Backups
// does, and every users and roles resource in backupPrincipals. Each distinct
// resource is authorized once per listing, not once per backup. The trade-off
// is that it does not stop at the first denial and writes no audit record per denial.
func (s *Scheduler) canReadBackups(ctx context.Context, principal *models.Principal, backupClasses, backupPrincipals [][]string) ([]bool, error) {
	readable := make([]bool, len(backupClasses))
	// rbac.Manager rejects a call carrying no resources, so a listing with no
	// backups must not make one.
	if len(backupClasses) == 0 {
		return readable, nil
	}

	// rbac.Manager enforces one resource at a time, so check for a caller
	// holding backup READ outright before naming thousands of collections.
	// The probe uses AuthorizeSilent because a denial here is the ordinary route
	// to the per-resource check below, which re-raises any other error. The
	// blanket READ needs all three wildcards; otherwise a caller holding only
	// the collections wildcard would list backups carrying users and roles.
	wildcards := append(authorization.Backups(), append(authorization.BackupUsers(), authorization.BackupRoles()...)...)
	if err := s.authorizer.AuthorizeSilent(ctx, principal, authorization.READ, wildcards...); err == nil {
		// AuthorizeSilent writes no audit record and nothing else authorizes
		// this endpoint. Record the grant that permitted the whole listing.
		if err := s.authorizer.Authorize(ctx, principal, authorization.READ, wildcards...); err != nil {
			return nil, err
		}
		for i := range readable {
			readable[i] = true
		}
		return readable, nil
	}

	resources := make([][]string, len(backupClasses))
	named := make(map[string]struct{})
	for i, classes := range backupClasses {
		resources[i] = append(authorization.Backups(classes...), backupPrincipals[i]...)
		for _, resource := range resources[i] {
			named[resource] = struct{}{}
		}
	}

	// The adminlist authorizer answers a denied filter with Forbidden rather
	// than an empty result. Both mean the caller sees no backup at all.
	granted, err := s.authorizer.FilterAuthorizedResources(ctx, principal, authorization.READ,
		slices.Sorted(maps.Keys(named))...)
	if err != nil {
		if !errors.As(err, &authzerrors.Forbidden{}) {
			return nil, err
		}
		return readable, nil
	}
	mayRead := make(map[string]struct{}, len(granted))
	for _, resource := range granted {
		mayRead[resource] = struct{}{}
	}

	for i := range backupClasses {
		readable[i] = true
		for _, resource := range resources[i] {
			if _, ok := mayRead[resource]; !ok {
				readable[i] = false
				break
			}
		}
	}
	return readable, nil
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

// backupSelections carries resolved names and snapshot exclusions. Empty names
// keep the full backend snapshot unless the corresponding skip flag is set.
type backupSelections struct {
	classes, users, roles []string
	// Explicit empty lists and unmatched wildcards exclude the snapshot.
	skipUsers, skipRoles bool
}

// validateBackupRequest resolves the request into classes, users, and roles.
// An empty or wildcard Include selects from the classes pr may back up, and no
// error names another. Users and roles stay empty without includeUsers/includeRoles.
func (s *Scheduler) validateBackupRequest(ctx context.Context, store coordStore, pr *models.Principal, req *BackupRequest) (selections backupSelections, err error) {
	if !store.backend.IsExternal() && s.backupper.nodeResolver.NodeCount() > 1 {
		return selections, errLocalBackendDBRO
	}

	if err = validateID(req.ID); err != nil {
		return selections, err
	}
	if req.BaseBackupID != "" {
		if err = validateID(req.BaseBackupID); err != nil {
			return selections, fmt.Errorf("base backup id: %w", err)
		}
		if req.ID == req.BaseBackupID {
			return selections, fmt.Errorf("base backup cannot be the same as the new backup ID: %s", req.BaseBackupID)
		}
	}
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		return selections, errIncludeExclude
	}

	if dup := findDuplicate(req.Include); dup != "" {
		return selections, fmt.Errorf("class list 'include' contains duplicate: %s", dup)
	}

	candidates := s.backupper.selector.ListClasses(ctx)
	// The authorizer checked Include's pattern text, not every class a wildcard
	// matches, so a wildcard expands only over classes pr may back up.
	if len(req.Include) == 0 || slices.ContainsFunc(req.Include, isWildcard) {
		if candidates, err = s.filterBackupableClasses(ctx, pr, authorization.CREATE, candidates); err != nil {
			return selections, err
		}
	}

	// Expand wildcards in Include list
	include := expandWildcards(req.Include, candidates)

	// Expand wildcards in Exclude list
	exclude := expandWildcards(req.Exclude, candidates)

	classes := include
	if len(req.Include) == 0 {
		classes = candidates
	}
	classes = filterClasses(classes, exclude)
	if len(classes) > 0 {
		if err := s.backupper.selector.Backupable(ctx, classes); err != nil {
			return selections, err
		}
	}

	users, err := s.resolveUsers(req.IncludeUsers)
	if err != nil {
		return selections, err
	}
	selections.skipUsers = req.IncludeUsers != nil && len(users) == 0

	roles, err := s.resolveRoles(req.IncludeRoles)
	if err != nil {
		return selections, err
	}
	selections.skipRoles = req.IncludeRoles != nil && len(roles) == 0

	if len(classes) == 0 && len(users) == 0 && len(roles) == 0 {
		hasIdentities, err := s.hasDefaultIdentities(req)
		if err != nil {
			return selections, err
		}
		if !hasIdentities {
			if len(req.Include) > 0 && len(include) == 0 {
				return selections, fmt.Errorf("backup selects no collections, users, or roles: class list 'include' %v matches no class", req.Include)
			}
			return selections, fmt.Errorf("backup selects no collections, users, or roles: available collections: %v", candidates)
		}
	}

	if err = s.checkIfBackupExists(ctx, store, req); err != nil {
		return selections, err
	}

	// validate base backup chain
	compressionType, err := CompressionTypeFromLevel(req.Level)
	if err != nil {
		return selections, fmt.Errorf("get compression type: %w", err)
	}
	if _, err = resolveBaseBackupChain(ctx, req.BaseBackupID, time.Now().UTC(), req.Bucket, req.Path, compressionType, store.MetaForBackupID); err != nil {
		return selections, fmt.Errorf("resolve base backup chain: %w", err)
	}

	// The response does not report users or roles, so a selector that matched
	// nothing is only visible here.
	if selections.skipUsers && len(req.IncludeUsers) > 0 {
		s.logger.WithField("action", "try_backup").WithField("backup_id", req.ID).
			Warnf("'includeUsers' %v matches no dynamic user, backing up none", req.IncludeUsers)
	}
	if selections.skipRoles && len(req.IncludeRoles) > 0 {
		s.logger.WithField("action", "try_backup").WithField("backup_id", req.ID).
			Warnf("'includeRoles' %v matches no role, backing up none", req.IncludeRoles)
	}

	selections.classes = classes
	selections.users = users
	selections.roles = roles

	return
}

// hasDefaultIdentities checks omitted selectors without turning a full backend
// snapshot into a filtered one. Full RBAC snapshots include built-in roles.
func (s *Scheduler) hasDefaultIdentities(req *BackupRequest) (bool, error) {
	if req.IncludeUsers == nil && s.userLister != nil && len(s.userLister.ListAllUsers()) > 0 {
		return true, nil
	}
	if req.IncludeRoles == nil && s.roleLister != nil {
		roles, err := s.roleLister.ListAllRoles()
		if err != nil {
			return false, fmt.Errorf("list all roles: %w", err)
		}
		return len(roles) > 0, nil
	}
	return false, nil
}

// resolveUsers preserves nil and empty inputs without consulting the backend.
func (s *Scheduler) resolveUsers(includeUsers []string) ([]string, error) {
	if len(includeUsers) == 0 {
		return includeUsers, nil
	}
	if s.userLister == nil {
		return nil, errors.New("'includeUsers' was set but dynamic DB users are not enabled")
	}
	return resolveUserSelectors(includeUsers, s.userLister.ListAllUsers())
}

// resolveUserSelectors mirrors class-selector semantics: '*'/'?' wildcards,
// dedup, exact selectors must exist. Wildcards matching nothing yield an empty
// result, which the caller turns into "back up no users". The caller handles
// omitted selectors separately to preserve full backend snapshots.
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
	return users, nil
}

// resolveRoles preserves nil and empty inputs without consulting the backend.
func (s *Scheduler) resolveRoles(includeRoles []string) ([]string, error) {
	if len(includeRoles) == 0 {
		return includeRoles, nil
	}
	if s.roleLister == nil {
		return nil, errors.New("'includeRoles' was set but RBAC is not enabled")
	}
	allRoles, err := s.roleLister.ListAllRoles()
	if err != nil {
		return nil, fmt.Errorf("list all roles: %w", err)
	}
	return resolveRoleSelectors(includeRoles, allRoles)
}

// resolveRoleSelectors follows resolveUserSelectors: '*'/'?' wildcards, dedup,
// exact selectors must exist, and wildcards matching nothing yield an empty result.
//
// Built-in roles are the exception. Naming one explicitly is rejected, and
// wildcards expand over custom roles only, so '*' never picks up a built-in.
// Restore re-applies the built-ins from env and code either way.
func resolveRoleSelectors(includeRoles, allRoles []string) ([]string, error) {
	if dup := findDuplicate(includeRoles); dup != "" {
		return nil, fmt.Errorf("role list 'includeRoles' contains duplicate: %s", dup)
	}

	for _, r := range includeRoles {
		if slices.Contains(authorization.BuiltInRoles, r) {
			return nil, fmt.Errorf("role %q in 'includeRoles' is a built-in role and cannot be backed up", r)
		}
	}

	candidates := make([]string, 0, len(allRoles))
	for _, r := range allRoles {
		if slices.Contains(authorization.BuiltInRoles, r) {
			continue
		}
		candidates = append(candidates, r)
	}

	roles := expandWildcards(includeRoles, candidates)

	known := make(map[string]struct{}, len(candidates))
	for _, r := range candidates {
		known[r] = struct{}{}
	}
	for _, r := range roles {
		if _, ok := known[r]; !ok {
			return nil, fmt.Errorf("role %q in 'includeRoles' does not exist", r)
		}
	}
	return roles, nil
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

// validateRestoreRequest reads and checks the backup meta and narrows it to the
// classes to restore. An empty or wildcard Include selects from the classes pr
// may restore, and no error names another class.
func (s *Scheduler) validateRestoreRequest(ctx context.Context, store coordStore, pr *models.Principal, req *BackupRequest) (*backup.DistributedBackupDescriptor, error) {
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
	// Authorize before the checks below, whose errors describe the backup.
	cs := meta.Classes()
	if (len(req.Include) == 0 || slices.ContainsFunc(req.Include, isWildcard)) && len(cs) > 0 {
		if cs, err = s.filterBackupableClasses(ctx, pr, authorization.CREATE, cs); err != nil {
			return nil, err
		}
		meta.Include(cs)
	}
	if meta.ID != req.ID {
		return nil, fmt.Errorf("wrong backup file: restore request asked for %q but the descriptor at %q reports its ID as %q (someone placed metadata from a different backup into this slot, or the backup_config.json was overwritten by an aborted operation; remove the slot and retry with the original backup ID)",
			req.ID, path.Join(destPath, GlobalBackupFile), meta.ID)
	}
	if meta.Status != backup.Success {
		return nil, fmt.Errorf("invalid backup in scheduler %s status: %s", destPath, meta.Status)
	}
	if err := checkRestorableVersion(meta.Version, meta.ServerVersion); err != nil {
		return nil, err
	}
	if err := meta.Validate(); err != nil {
		return nil, fmt.Errorf("corrupted backup file: %w", err)
	}
	if v := meta.Version; v[0] > Version[0] {
		return nil, fmt.Errorf("%s: %s > %s", errMsgHigherVersion, v, Version)
	}

	// Base backups are only read after the restore has started staging data.
	// Resolve the chain upfront so a missing or invalid base is rejected before
	// any side effects begin.
	if _, err := resolveBaseBackupChain(ctx, meta.BaseBackupID, meta.StartedAt, req.Bucket, req.Path, meta.GetCompressionType(), store.MetaForBackupID); err != nil {
		return nil, fmt.Errorf("resolve base backup chain: %w", err)
	}

	if len(cs) == 0 {
		if len(req.Include) > 0 {
			return nil, fmt.Errorf("class-less backup cannot be restored with 'include': %v", req.Include)
		}
		if len(req.Exclude) > 0 {
			return nil, fmt.Errorf("class-less backup cannot be restored with 'exclude': %v", req.Exclude)
		}
		if req.UserRestoreOption == models.RestoreConfigUsersOptionsNoRestore &&
			req.RbacRestoreOption == models.RestoreConfigRolesOptionsNoRestore {
			return nil, errors.New("nothing to restore: backup has no collections and both users and roles restore options are 'noRestore'")
		}
		if len(req.NodeMapping) > 0 {
			meta.NodeMapping = req.NodeMapping
		}
		return meta, nil
	}

	// Expand wildcards in Include list against backup's classes
	include := expandWildcards(req.Include, cs)
	// An include list that expands to nothing must not fall through to every class.
	if len(req.Include) > 0 && len(include) == 0 {
		return nil, fmt.Errorf("class list 'include' %v matches no class in the backup", req.Include)
	}

	// Expand wildcards in Exclude list against backup's classes
	exclude := expandWildcards(req.Exclude, cs)

	if len(req.Include) > 0 {
		if len(include) == 0 {
			return nil, fmt.Errorf("class list 'include' %v matches no class in the backup", req.Include)
		}
		if first := meta.AllExist(include); first != "" {
			return nil, fmt.Errorf("class %s doesn't exist in the backup", first)
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
	}
	return meta, nil
}

// validateNamespaceStripping fails a restore into a namespace-disabled
// cluster when stripping the "<namespace>:" qualification would collide
// distinct backup entities (usecases/schema.Handler.RestoreClass and the
// dynamic-user snapshot restore strip at apply time).
//
// Everything is validated from the per-node descriptors (the payload nodes
// actually restore) filtered down to the selected classes.
func (s *Scheduler) validateNamespaceStripping(ctx context.Context, descriptors []backup.ClassDescriptor, userBlob, rbacBlob []byte, selectedClasses []string, userRestoreOption, rbacRestoreOption string) error {
	if s.schema.NamespacesEnabled() {
		return nil // restore does not strip namespaces
	}

	selected := make(map[string]struct{}, len(selectedClasses))
	for _, name := range selectedClasses {
		selected[name] = struct{}{}
	}

	type group struct {
		sources  []string
		short    string
		stripped bool
	}
	classes := make(map[string]*group, len(descriptors))
	aliases := make(map[string]*group, 8)
	collect := func(m map[string]*group, source, short string) *group {
		key := strings.ToLower(short)
		g, ok := m[key]
		if !ok {
			g = &group{short: short}
			m[key] = g
		}
		g.sources = append(g.sources, source)
		if short != source {
			g.stripped = true
		}
		return g
	}

	anyAliasStripped := false
	for i := range descriptors {
		d := &descriptors[i]
		if _, ok := selected[d.Name]; !ok {
			continue
		}
		collect(classes, d.Name, schema.UppercaseClassName(namespacing.StripQualification(d.Name)))
		if !d.AliasesIncluded || len(d.Aliases) == 0 {
			continue
		}
		var classAliases []*models.Alias
		if err := json.Unmarshal(d.Aliases, &classAliases); err != nil {
			return fmt.Errorf("unmarshal aliases of class %q: %w", d.Name, err)
		}
		for _, a := range classAliases {
			if collect(aliases, a.Alias, namespacing.StripQualification(a.Alias)).stripped {
				anyAliasStripped = true
			}
		}
	}

	var errs []string
	for _, g := range classes {
		if len(g.sources) > 1 {
			slices.Sort(g.sources)
			errs = append(errs, fmt.Sprintf("classes %v strip to the same name %q", g.sources, g.short))
		}
	}

	// A stripped class may also take the name of an entity already in the
	// cluster. Unqualified names are skipped: those keep the pre-existing lazy
	// per-class semantics, so namespace-free restores behave as before.
	for _, g := range classes {
		if !g.stripped {
			continue
		}
		if cur := s.schema.ClassEqual(g.short); cur != "" {
			slices.Sort(g.sources)
			errs = append(errs, fmt.Sprintf("classes %v strip to %q, which already exists in the cluster as %q", g.sources, g.short, cur))
		}
	}

	// A stripped alias landing on a live class errors in CreateAlias
	// after its class's data is already committed
	if anyAliasStripped {
		live := s.restorer.selector.ListClasses(ctx)
		liveByFold := make(map[string]string, len(live))
		for _, c := range live {
			liveByFold[strings.ToLower(c)] = c
		}
		for key, g := range aliases {
			if !g.stripped {
				continue
			}
			if cur, ok := liveByFold[key]; ok {
				slices.Sort(g.sources)
				errs = append(errs, fmt.Sprintf("aliases %v strip to %q, which already exists as class %q in the cluster", g.sources, g.short, cur))
			}
		}
	}

	// Without the checks below, two aliases stripping to the same name make
	// the later CreateAlias silently skip (or overwrite, with overwriteAlias),
	// and an alias stripping onto a restored class name fails that class
	// mid-restore after its data is already committed.
	for key, g := range aliases {
		if len(g.sources) > 1 {
			slices.Sort(g.sources)
			errs = append(errs, fmt.Sprintf("aliases %v strip to the same name %q", g.sources, g.short))
		}
		if cls, ok := classes[key]; ok && (g.stripped || cls.stripped) {
			slices.Sort(g.sources)
			errs = append(errs, fmt.Sprintf("aliases %v strip to %q, which collides with backup class %v", g.sources, g.short, cls.sources))
		}
	}

	// The user snapshot is opaque here; the dry-run reuses the exact
	// strip-and-collide logic the real restore runs, covering every id-keyed
	// field and both filtered (includeUsers) and whole-cluster snapshots.
	if userRestoreOption != models.RestoreConfigUsersOptionsNoRestore {
		if err := apikey.ValidateNamespaceStrip(userBlob); err != nil {
			errs = append(errs, fmt.Sprintf("dynamic users: %v", err))
		}
	}

	// casbin merges colliding rows on restore without reporting anything, so run
	// the RBAC snapshot through the same strip-and-collide logic the nodes apply.
	// This is the only check that runs before any node stages data. The same
	// strip runs again per node at apply time and refuses the blob there too.
	if s.roleLister != nil && rbacRestoreOption != models.RestoreConfigRolesOptionsNoRestore {
		if err := rbac.ValidateNamespaceStrip(rbacBlob, s.staticAPIKeyUsers); err != nil {
			errs = append(errs, fmt.Sprintf("roles: %v", err))
		}
	}

	if len(errs) == 0 {
		return nil
	}
	slices.Sort(errs)
	return fmt.Errorf("restoring into a cluster without namespaces strips namespace qualifications, which would cause name collisions: %s. Restore one namespace at a time using 'include'/'exclude', or remove the conflicting entities from the target cluster first", strings.Join(errs, "; "))
}

// validateNamespaceReferences rejects a restore whose blobs name a namespace that
// is missing or deleting on this cluster, before any node stages data. Suspended
// and resuming are accepted. The apply-time check is the one that counts; this one
// exists so the caller sees the error before any node does work.
func (s *Scheduler) validateNamespaceReferences(userBlob, rbacBlob []byte, userRestoreOption, rbacRestoreOption string) error {
	if !s.schema.NamespacesEnabled() {
		return nil // restore strips namespaces instead of resolving them
	}

	var errs []string
	if s.roleLister != nil && rbacRestoreOption != models.RestoreConfigRolesOptionsNoRestore {
		if err := rbac.RequireReferencedNamespacesExist(rbacBlob, s.staticAPIKeyUsers, s.namespaces); err != nil {
			errs = append(errs, fmt.Sprintf("roles: %v", err))
		}
	}

	if s.userLister != nil && userRestoreOption != models.RestoreConfigUsersOptionsNoRestore {
		if err := apikey.RequireReferencedNamespacesExist(userBlob, s.namespaces); err != nil {
			errs = append(errs, fmt.Sprintf("dynamic users: %v", err))
		}
	}

	if len(errs) == 0 {
		return nil
	}
	slices.Sort(errs)
	return fmt.Errorf("backup references namespaces that are missing or deleting on this cluster: %s. Create the namespaces first, or restore without 'rolesOptions'/'usersOptions'", strings.Join(errs, "; "))
}

// logSubsystemDisabled is the only signal that the restore skipped roles or
// users the request asked for because the subsystem is off on this cluster.
func (s *Scheduler) logSubsystemDisabled(backupID, artefact, subsystem string) {
	s.logger.WithField("action", "restore_roles_and_users").
		WithField("backup_id", backupID).
		Warnf("skipping %s from the backup: %s is disabled on this cluster", artefact, subsystem)
}

// fetchSchema retrieves and returns the latest schema for all classes
// In pre-raft scenarios where schema may diverge, some guesswork is necessary.
// It also returns the backup's user and role snapshots, each empty when the
// backup does not carry one.
//
// Only a descriptor that records a leader can carry those snapshots: the
// RbacBackups and UserBackups fields were added long after Leader was, so a
// descriptor without a leader predates both. The union below therefore reads
// classes alone.
func (s *Scheduler) fetchSchema(
	ctx context.Context,
	backend string,
	overrideBucket string,
	overridePath string,
	req *backup.DistributedBackupDescriptor,
) (_ []backup.ClassDescriptor, userBlob, rbacBlob []byte, _ error) {
	f := func(node string) (*backup.BackupDescriptor, error) {
		store, err := nodeBackend(node, s.backends, backend, req.ID, overrideBucket, overridePath)
		if err != nil {
			return nil, err
		}
		meta, err := store.Meta(ctx, req.ID, store.bucket, store.path)
		if err != nil {
			return nil, err
		}
		return meta, nil
	}

	if req.Leader != "" {
		meta, err := f(req.Leader) // raft version of the backup
		if err != nil {
			return nil, nil, nil, fmt.Errorf("fetch meta of node %q: %w", req.Leader, err)
		}
		return meta.Classes, meta.UserBackups, meta.RbacBackups, nil
	}

	// union
	m := make(map[string]backup.ClassDescriptor, 64)
	for k := range req.Nodes {
		meta, err := f(k)
		if err != nil {
			// Fail closed: a partial union would silently skip the missing
			// node's classes at restore time and blind the strip validation.
			return nil, nil, nil, fmt.Errorf("fetch meta of node %q: %w", k, err)
		}
		// guess the most up to date version
		for _, x := range meta.Classes {
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
	return xs, nil, nil, nil
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

// isWildcard reports whether expandWildcards expands pattern against candidates.
func isWildcard(pattern string) bool {
	return strings.ContainsAny(pattern, "*?")
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
		if isWildcard(pattern) {
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
