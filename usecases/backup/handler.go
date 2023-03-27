//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package backup

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

// Version of backup structure
const Version = "1.0"

// TODO error handling need to be implemented properly.
// Current error handling is not idiomatic and relays on string comparisons which makes testing very brittle.

var regExpID = regexp.MustCompile("^[a-z0-9_-]+$")

type BackupBackendProvider interface {
	BackupBackend(backend string) (modulecapabilities.BackupBackend, error)
}

type authorizer interface {
	Authorize(principal *models.Principal, verb, resource string) error
}

type schemaManger interface {
	RestoreClass(ctx context.Context, d *backup.ClassDescriptor) error
	NodeName() string
}

type nodeResolver interface {
	NodeHostname(nodeName string) (string, bool)
	NodeCount() int
}

type Status struct {
	Path        string
	StartedAt   time.Time
	CompletedAt time.Time
	Status      backup.Status
	Err         string
}

type Manager struct {
	node string
	// deps
	logger     logrus.FieldLogger
	authorizer authorizer
	backupper  *backupper
	restorer   *restorer
	backends   BackupBackendProvider
}

func NewManager(
	logger logrus.FieldLogger,
	authorizer authorizer,
	schema schemaManger,
	sourcer Sourcer,
	backends BackupBackendProvider,
) *Manager {
	node := schema.NodeName()
	m := &Manager{
		node:       node,
		logger:     logger,
		authorizer: authorizer,
		backends:   backends,
		backupper: newBackupper(node, logger,
			sourcer,
			backends),
		restorer: newRestorer(node, logger,
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
	store, err := nodeBackend(m.node, m.backends, req.Backend, req.ID)
	if err != nil {
		err = fmt.Errorf("no backup backend %q, did you enable the right module?", req.Backend)
		return nil, backup.NewErrUnprocessable(err)
	}

	classes, err := m.validateBackupRequest(ctx, store, req)
	if err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	if err := store.Initialize(ctx); err != nil {
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
	store, err := nodeBackend(m.node, m.backends, req.Backend, req.ID)
	if err != nil {
		err = fmt.Errorf("no backup backend %q, did you enable the right module?", req.Backend)
		return nil, backup.NewErrUnprocessable(err)
	}
	meta, err := m.validateRestoreRequest(ctx, store, req)
	if err != nil {
		return nil, err
	}
	cs := meta.List()
	if cls := m.restorer.AnyExists(cs); cls != "" {
		err := fmt.Errorf("cannot restore class %q because it already exists", cls)
		return nil, backup.NewErrUnprocessable(err)
	}
	rreq := Request{
		Method:  OpRestore,
		ID:      meta.ID,
		Backend: req.Backend,
		Classes: cs,
	}
	data, err := m.restorer.Restore(ctx, &rreq, meta, store)
	if err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	return data, nil
}

func (m *Manager) BackupStatus(ctx context.Context, principal *models.Principal,
	backend, backupID string,
) (*models.BackupCreateStatusResponse, error) {
	return m.backupper.Status(ctx, backend, backupID)
}

func (m *Manager) RestorationStatus(ctx context.Context, principal *models.Principal, backend, ID string,
) (_ Status, err error) {
	return m.restorer.status(backend, ID)
}

// OnCanCommit will be triggered when coordinator asks the node to participate
// in a distributed backup operation
func (m *Manager) OnCanCommit(ctx context.Context, req *Request) *CanCommitResponse {
	ret := &CanCommitResponse{Method: req.Method, ID: req.ID}
	store, err := nodeBackend(m.node, m.backends, req.Backend, req.ID)
	if err != nil {
		ret.Err = fmt.Sprintf("no backup backend %q, did you enable the right module?", req.Backend)
		return ret
	}

	switch req.Method {
	case OpCreate:
		if err := m.backupper.sourcer.Backupable(ctx, req.Classes); err != nil {
			ret.Err = err.Error()
			return ret
		}
		if err = store.Initialize(ctx); err != nil {
			ret.Err = fmt.Sprintf("init uploader: %v", err)
			return ret
		}
		res, err := m.backupper.backup(ctx, store, req)
		if err != nil {
			ret.Err = err.Error()
			return ret
		}
		ret.Timeout = res.Timeout
	case OpRestore:
		meta, _, err := m.restorer.validate(ctx, &store, req)
		if err != nil {
			ret.Err = err.Error()
			return ret
		}
		res, err := m.restorer.restore(ctx, req, meta, store)
		if err != nil {
			ret.Err = err.Error()
			return ret
		}
		ret.Timeout = res.Timeout
	default:
		ret.Err = fmt.Sprintf("unknown backup operation: %s", req.Method)
		return ret
	}

	return ret
}

// OnCommit will be triggered when the coordinator confirms the execution of a previous operation
func (m *Manager) OnCommit(ctx context.Context, req *StatusRequest) (err error) {
	switch req.Method {
	case OpCreate:
		return m.backupper.OnCommit(ctx, req)
	case OpRestore:
		return m.restorer.OnCommit(ctx, req)
	default:
		return fmt.Errorf("%w: %s", errUnknownOp, req.Method)
	}
}

// OnAbort will be triggered when the coordinator abort the execution of a previous operation
func (m *Manager) OnAbort(ctx context.Context, req *AbortRequest) error {
	switch req.Method {
	case OpCreate:
		return m.backupper.OnAbort(ctx, req)
	case OpRestore:
		return m.restorer.OnAbort(ctx, req)
	default:
		return fmt.Errorf("%w: %s", errUnknownOp, req.Method)

	}
}

func (m *Manager) OnStatus(ctx context.Context, req *StatusRequest) *StatusResponse {
	ret := StatusResponse{
		Method: req.Method,
		ID:     req.ID,
	}
	switch req.Method {
	case OpCreate:
		st, err := m.backupper.OnStatus(ctx, req)
		ret.Status = st.Status
		if err != nil {
			ret.Status = backup.Failed
			ret.Err = err.Error()
		}
	case OpRestore:
		st, err := m.restorer.status(req.Backend, req.ID)
		ret.Status = st.Status
		ret.Err = st.Err
		if err != nil {
			ret.Status = backup.Failed
			ret.Err = err.Error()
		} else if st.Err != "" {
			ret.Err = st.Err
		}
	default:
		ret.Status = backup.Failed
		ret.Err = fmt.Sprintf("%v: %s", errUnknownOp, req.Method)
	}

	return &ret
}

func (m *Manager) validateBackupRequest(ctx context.Context, store nodeStore, req *BackupRequest) ([]string, error) {
	if err := validateID(req.ID); err != nil {
		return nil, err
	}
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		return nil, fmt.Errorf("malformed request: 'include' and 'exclude' cannot both contain values")
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
	destPath := store.HomeDir()
	// there is no backup with given id on the backend, regardless of its state (valid or corrupted)
	_, err := store.Meta(ctx, req.ID, false)
	if err == nil {
		return nil, fmt.Errorf("backup %q already exists at %q", req.ID, destPath)
	}
	if _, ok := err.(backup.ErrNotFound); !ok {
		return nil, fmt.Errorf("check if backup %q exists at %q: %w", req.ID, destPath, err)
	}
	return classes, nil
}

func (m *Manager) validateRestoreRequest(ctx context.Context, store nodeStore, req *BackupRequest) (*backup.BackupDescriptor, error) {
	if len(req.Include) > 0 && len(req.Exclude) > 0 {
		err := fmt.Errorf("malformed request: 'include' and 'exclude' cannot both contain values")
		return nil, backup.NewErrUnprocessable(err)
	}
	meta, cs, err := m.restorer.validate(ctx, &store, &Request{ID: req.ID, Classes: req.Include})
	if err != nil {
		if errors.Is(err, errMetaNotFound) {
			return nil, backup.NewErrNotFound(err)
		}
		return nil, backup.NewErrUnprocessable(err)
	}
	meta.Exclude(req.Exclude)
	if len(meta.Classes) == 0 {
		err = fmt.Errorf("empty class list: please choose from : %v", cs)
		return nil, backup.NewErrUnprocessable(err)
	}
	return meta, nil
}

func validateID(backupID string) error {
	if !regExpID.MatchString(backupID) {
		return fmt.Errorf("invalid backup id: allowed characters are lowercase, 0-9, _, -")
	}
	return nil
}

func nodeBackend(node string, provider BackupBackendProvider, backend, id string) (nodeStore, error) {
	caps, err := provider.BackupBackend(backend)
	if err != nil {
		return nodeStore{}, err
	}
	return nodeStore{objStore{b: caps, BasePath: fmt.Sprintf("%s/%s", id, node)}}, nil
}

// basePath of the backup
func basePath(backendType, backupID string) string {
	return fmt.Sprintf("%s/%s", backendType, backupID)
}

func filterClasses(classes, excludes []string) []string {
	if len(excludes) == 0 {
		return classes
	}
	m := make(map[string]struct{}, len(classes))
	for _, c := range classes {
		m[c] = struct{}{}
	}
	for _, x := range excludes {
		delete(m, x)
	}
	if len(classes) != len(m) {
		classes = classes[:len(m)]
		i := 0
		for k := range m {
			classes[i] = k
			i++
		}
	}

	return classes
}
