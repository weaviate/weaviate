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
	"fmt"
	"regexp"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

// Version of backup structure
const (
	// Version > version1 support compression
	Version = "2.0"
	// version1 store plain files without compression
	version1 = "1.0"
)

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
	RestoreClass(ctx context.Context, d *backup.ClassDescriptor, nodeMapping map[string]string) error
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

type Handler struct {
	node string
	// deps
	logger     logrus.FieldLogger
	authorizer authorizer
	backupper  *backupper
	restorer   *restorer
	backends   BackupBackendProvider
}

func NewHandler(
	logger logrus.FieldLogger,
	authorizer authorizer,
	schema schemaManger,
	sourcer Sourcer,
	backends BackupBackendProvider,
) *Handler {
	node := schema.NodeName()
	m := &Handler{
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

// Compression is the compression configuration.
type Compression struct {
	// Level is one of DefaultCompression, BestSpeed, BestCompression
	Level CompressionLevel

	// ChunkSize represents the desired size for chunks between 1 - 512  MB
	// However, during compression, the chunk size might
	// slightly deviate from this value, being either slightly
	// below or above the specified size
	ChunkSize int

	// CPUPercentage desired CPU core utilization (1%-80%), default: 50%
	CPUPercentage int
}

// BackupRequest a transition request from API to Backend.
type BackupRequest struct {
	// Compression is the compression configuration.
	Compression

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

	// NodeMapping is a map of node name replacement where key is the old name and value is the new name
	// No effect if the map is empty
	NodeMapping map[string]string
}

// OnCanCommit will be triggered when coordinator asks the node to participate
// in a distributed backup operation
func (m *Handler) OnCanCommit(ctx context.Context, req *Request) *CanCommitResponse {
	ret := &CanCommitResponse{Method: req.Method, ID: req.ID}

	nodeName := m.node
	// If we are doing a restore and have a nodeMapping specified, ensure we use the "old" node name from the backup to retrieve/store the
	// backup information.
	if req.Method == OpRestore {
		for oldNodeName, newNodeName := range req.NodeMapping {
			if nodeName == newNodeName {
				nodeName = oldNodeName
				break
			}
		}
	}
	store, err := nodeBackend(nodeName, m.backends, req.Backend, req.ID)
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
func (m *Handler) OnCommit(ctx context.Context, req *StatusRequest) (err error) {
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
func (m *Handler) OnAbort(ctx context.Context, req *AbortRequest) error {
	switch req.Method {
	case OpCreate:
		return m.backupper.OnAbort(ctx, req)
	case OpRestore:
		return m.restorer.OnAbort(ctx, req)
	default:
		return fmt.Errorf("%w: %s", errUnknownOp, req.Method)

	}
}

func (m *Handler) OnStatus(ctx context.Context, req *StatusRequest) *StatusResponse {
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
