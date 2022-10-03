package backup

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/sirupsen/logrus"
)

// Scheduler assigns backup operations to coordinators.
type Scheduler struct {
	// deps
	logger     logrus.FieldLogger
	authorizer authorizer
	backupper  *coordinator
	restorer   *coordinator
	backends   BackupBackendProvider
}

// NewScheduler creates a new scheduler with two coordinators
func NewScheduler(
	authorizer authorizer,
	client client,
	sourcer selector,
	backends BackupBackendProvider,
	nodeResolver nodeResolver,
	logger logrus.FieldLogger,
) *Scheduler {
	m := &Scheduler{
		logger:     logger,
		authorizer: authorizer,
		backends:   backends,
		backupper: newCoordinator(
			sourcer,
			client,
			logger, nodeResolver),
		restorer: newCoordinator(
			sourcer,
			client,
			logger, nodeResolver),
	}
	return m
}

func (s *Scheduler) Backup(ctx context.Context, pr *models.Principal, req *BackupRequest,
) (*models.BackupCreateResponse, error) {
	path := fmt.Sprintf("backups/%s/%s", req.Backend, req.ID)
	if err := s.authorizer.Authorize(pr, "add", path); err != nil {
		return nil, err
	}
	store, err := coordBackend(s.backends, req.Backend, req.ID)
	if err != nil {
		err = fmt.Errorf("no backup backend %q, did you enable the right module?", req.Backend)
		return nil, backup.NewErrUnprocessable(err)
	}

	classes, err := s.validateBackupRequest(ctx, store, req)
	if err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	if err := store.Initialize(ctx); err != nil {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("init uploader: %w", err))
	}
	breq := Request{
		Method:  OpCreate,
		ID:      req.ID,
		Backend: req.Backend,
		Classes: classes,
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
			Path:    st.Path,
		}, nil
	}
}

func (s *Scheduler) Restore(ctx context.Context, pr *models.Principal,
	req *BackupRequest,
) (*models.BackupRestoreResponse, error) {
	return nil, backup.NewErrUnprocessable(fmt.Errorf("not implemented"))
}

func (m *Scheduler) BackupStatus(ctx context.Context, principal *models.Principal,
	backend, backupID string,
) (*models.BackupCreateStatusResponse, error) {
	return nil, backup.NewErrUnprocessable(fmt.Errorf("not implemented"))
}

func (m *Scheduler) RestorationStatus(ctx context.Context, principal *models.Principal, backend, ID string,
) (_ RestoreStatus, err error) {
	return RestoreStatus{}, backup.NewErrUnprocessable(fmt.Errorf("not implemented"))
}

func coordBackend(provider BackupBackendProvider, backend, id string) (coordStore, error) {
	caps, err := provider.BackupBackend(backend)
	if err != nil {
		return coordStore{}, err
	}
	return coordStore{objStore{b: caps, BasePath: id}}, nil
}

func (s *Scheduler) validateBackupRequest(ctx context.Context, store coordStore, req *BackupRequest) ([]string, error) {
	if err := validateID(req.ID); err != nil {
		return nil, err
	}
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		return nil, fmt.Errorf("malformed request: 'include' and 'exclude' cannot be both empty")
	}
	classes := req.Include
	if len(classes) == 0 {
		classes = s.backupper.selector.ListClasses(ctx)
	}
	if classes = filterClasses(classes, req.Exclude); len(classes) == 0 {
		return nil, fmt.Errorf("empty class list: please choose from : %v", classes)
	}

	if err := s.backupper.selector.Backupable(ctx, classes); err != nil {
		return nil, err
	}
	destPath := store.HomeDir()
	// there is no backup with given id on the backend, regardless of its state (valid or corrupted)
	_, err := store.GlobalMeta(ctx, req.ID)
	if err == nil {
		return nil, fmt.Errorf("backup %q already exists at %q", req.ID, destPath)
	}
	if _, ok := err.(backup.ErrNotFound); !ok {
		return nil, fmt.Errorf("check if backup %q exists at %q: %w", req.ID, destPath, err)
	}
	return classes, nil
}
