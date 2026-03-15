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

package export

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

var (
	regExpID = regexp.MustCompile(`^[a-z0-9_-]+$`)

	// ErrExportNotFound is returned when no export with the given ID exists.
	ErrExportNotFound = errors.New("export not found")

	// ErrExportAlreadyFinished is returned when trying to cancel an export
	// that has already completed (SUCCESS, FAILED, or CANCELED).
	ErrExportAlreadyFinished = errors.New("export has already finished")

	// ErrExportValidation is returned when the export request fails input
	// validation (invalid ID, unknown backend, non-existent class, etc.).
	ErrExportValidation = errors.New("export validation error")

	// ErrExportAlreadyExists is returned when attempting to create an export
	// with an ID that already has artifacts on the storage backend.
	ErrExportAlreadyExists = errors.New("export already exists")

	// ErrExportAlreadyActive is returned when attempting to start an export
	// while another export is already in progress.
	ErrExportAlreadyActive = errors.New("export already active")

	// errExportCanceled is passed as the cause to context.WithCancelCause
	// when an export is canceled via the Cancel endpoint.
	errExportCanceled = errors.New("export was canceled")
)

const exportMetadataFile = "export_metadata.json"

// BackendProvider provides access to storage backends for export.
type BackendProvider interface {
	BackupBackend(backend string) (modulecapabilities.BackupBackend, error)
}

// Selector selects shards and classes for export
type Selector interface {
	ListClasses(ctx context.Context) []string
	ShardOwnership(ctx context.Context, className string) (map[string][]string, error)
	ExportShardNames(className string) ([]string, bool, error)
	AcquireShardForExport(ctx context.Context, className, shardName string) (shard ShardLike, release func(), skipReason string, err error)
	IsMultiTenant(ctx context.Context, className string) bool
}

// ShardLike is an alias for db.ShardLike
type ShardLike = interface {
	Store() *lsmkv.Store
	Name() string
}

// Scheduler manages export operations.
// In multi-node mode the RAFT leader acts as the single coordinator and uses
// a two-phase commit protocol (prepare/commit/abort) across participant nodes.
type Scheduler struct {
	shutdownCtx  context.Context
	logger       logrus.FieldLogger
	authorizer   authorization.Authorizer
	rbacConfig   rbacconf.Config
	selector     Selector
	backends     BackendProvider
	client       ExportClient // nil for single-node
	nodeResolver NodeResolver // nil for single-node
	localNode    string       // from appState.Cluster.LocalName()
	participant  *Participant // local participant — always present

	// activeExport holds the context and cancel function for the active
	// single-node export. A nil pointer means no export is running.
	// Cancel() uses the context to check whether its cancel call won the
	// first-caller race (CancelCauseFunc is first-caller-wins).
	activeExport atomic.Pointer[singleNodeExport]
}

// singleNodeExport bundles the context and cancel function for an active
// single-node export so Cancel() can inspect the cause after calling cancel.
type singleNodeExport struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
}

// NewScheduler creates a new export scheduler.
// When client and nodeResolver are nil, operates in single-node mode.
// The shutdownCtx is canceled on graceful server shutdown.
func NewScheduler(
	shutdownCtx context.Context,
	authorizer authorization.Authorizer,
	rbacConfig rbacconf.Config,
	selector Selector,
	backends BackendProvider,
	logger logrus.FieldLogger,
	client ExportClient,
	nodeResolver NodeResolver,
	localNode string,
	participant *Participant,
) *Scheduler {
	if participant == nil {
		panic("export: scheduler requires a non-nil participant")
	}
	return &Scheduler{
		shutdownCtx:  shutdownCtx,
		logger:       logger,
		authorizer:   authorizer,
		rbacConfig:   rbacConfig,
		selector:     selector,
		backends:     backends,
		client:       client,
		nodeResolver: nodeResolver,
		localNode:    localNode,
		participant:  participant,
	}
}

// isMultiNode returns true if the scheduler is configured for multi-node operation
func (s *Scheduler) isMultiNode() bool {
	return s.client != nil && s.nodeResolver != nil
}

// Export starts a new export operation.
func (s *Scheduler) Export(ctx context.Context, principal *models.Principal, id, backend string, include, exclude []string, bucket, path string) (*models.ExportCreateResponse, error) {
	if !regExpID.MatchString(id) {
		return nil, fmt.Errorf("%w: invalid export id: '%v' allowed characters are lowercase, 0-9, _, -", ErrExportValidation, id)
	}
	if backend == "" {
		return nil, fmt.Errorf("%w: backend is required", ErrExportValidation)
	}

	classes, err := s.resolveClasses(ctx, include, exclude)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve classes: %w", ErrExportValidation, err)
	}

	classes = filter.New[string](s.authorizer, s.rbacConfig).Filter(
		ctx,
		s.logger,
		principal,
		classes,
		authorization.CREATE,
		func(class string) string {
			return authorization.Backups(class)[0]
		},
	)

	if len(classes) == 0 {
		return nil, fmt.Errorf("%w: no exportable classes", ErrExportValidation)
	}

	backendStore, err := s.backends.BackupBackend(backend)
	if err != nil {
		return nil, fmt.Errorf("%w: backend %s not available: %w", ErrExportValidation, backend, err)
	}

	if err := backendStore.Initialize(ctx, id, bucket, path); err != nil {
		return nil, fmt.Errorf("initialize backend: %w", err)
	}

	if err := s.checkIfExportExists(ctx, backendStore, id, bucket, path); err != nil {
		return nil, err
	}

	now := strfmt.DateTime(time.Now().UTC())
	homePath := backendStore.HomeDir(id, bucket, path)

	s.logger.WithField("action", "export").
		WithField("export_id", id).
		WithField("classes", classes).
		WithField("class_count", len(classes)).
		WithField("multi_node", s.isMultiNode()).
		Info("starting export")

	status := &models.ExportStatusResponse{
		ID:        id,
		Backend:   backend,
		Status:    string(export.Transferring),
		StartedAt: now,
		Classes:   classes,
	}

	if s.isMultiNode() {
		if err := s.performMultiNodeExport(ctx, backendStore, id, status, classes, bucket, path); err != nil {
			return nil, err
		}
	} else {
		exportCtx, cancel := context.WithCancelCause(s.shutdownCtx)
		sne := &singleNodeExport{ctx: exportCtx, cancel: cancel}
		if !s.activeExport.CompareAndSwap(nil, sne) {
			cancel(nil)
			return nil, fmt.Errorf("%w: an export is already in progress", ErrExportAlreadyActive)
		}

		// Safety net: clear activeExport on panic during the synchronous
		// phase so that a recovered panic does not permanently block future
		// exports.
		defer func() {
			if r := recover(); r != nil {
				s.activeExport.Store(nil)
				cancel(nil)
				panic(r) // re-panic after cleanup
			}
		}()

		// Resolve shards and write initial metadata synchronously so that
		// metadata exists on the backend before the API response is returned.
		// This prevents Cancel from getting a 404 when called right after Create.
		shards, err := s.writeInitialMetadata(ctx, backendStore, id, status, classes, bucket, path)
		if err != nil {
			s.activeExport.Store(nil)
			cancel(nil)
			return nil, err
		}

		enterrors.GoWrapper(func() {
			defer func() {
				// Safety net: ensure fields are cleared even on panic.
				s.activeExport.Store(nil)
				cancel(nil)
			}()
			s.performSingleNodeExport(exportCtx, cancel, backendStore, id, status, classes, shards, bucket, path)
		}, s.logger)
	}

	return &models.ExportCreateResponse{
		ID:        id,
		Backend:   backend,
		Path:      homePath,
		Status:    string(export.Started),
		StartedAt: now,
		Classes:   classes,
	}, nil
}

// Status retrieves the status of an export.
// In multi-node mode, assembles status from metadata's NodeAssignments +
// per-node status files. In single-node mode, reads the metadata file directly.
func (s *Scheduler) Status(ctx context.Context, principal *models.Principal, backend, id, bucket, path string) (*models.ExportStatusResponse, error) {
	if !regExpID.MatchString(id) {
		return nil, fmt.Errorf("%w: invalid export id: '%v' allowed characters are lowercase, 0-9, _, -", ErrExportValidation, id)
	}
	backendStore, err := s.backends.BackupBackend(backend)
	if err != nil {
		return nil, fmt.Errorf("%w: backend %s not available: %w", ErrExportValidation, backend, err)
	}

	if err := backendStore.Initialize(ctx, id, bucket, path); err != nil {
		return nil, fmt.Errorf("initialize backend: %w", err)
	}

	// Metadata is always written before the export starts.
	meta, err := s.getExportMetadata(ctx, backendStore, id, bucket, path)
	if err != nil {
		if errors.As(err, &backup.ErrNotFound{}) {
			return nil, ErrExportNotFound
		}
		return nil, fmt.Errorf("get export metadata: %w", err)
	}

	// Authorize using metadata classes.
	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(meta.Classes...)...); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
	}

	// Terminal metadata — return directly (both single-node and multi-node).
	switch meta.Status {
	case export.Success, export.Failed, export.Canceled:
		return s.statusFromMetadata(backendStore, id, bucket, path, meta)
	default:
	}

	if s.isMultiNode() {
		assembled, _, err := s.assembleStatusFromMetadata(ctx, backendStore, principal, id, bucket, path, meta)
		if err != nil {
			return nil, err
		}
		// Promote: if all nodes reached a terminal state, persist the final
		// metadata so future Status() calls (and Cancel()) see it directly
		// without re-assembling from per-node files.
		switch export.Status(assembled.Status) {
		case export.Success, export.Failed:
			promotedMeta := &ExportMetadata{
				ID:              meta.ID,
				Backend:         meta.Backend,
				StartedAt:       meta.StartedAt,
				CompletedAt:     time.Time(assembled.CompletedAt),
				Status:          export.Status(assembled.Status),
				Classes:         meta.Classes,
				NodeAssignments: meta.NodeAssignments,
				Error:           assembled.Error,
				ShardStatus:     assembled.ShardStatus,
			}
			if writeErr := writeExportMetadata(backendStore, id, bucket, path, promotedMeta, s.logger); writeErr != nil {
				s.logger.WithField("action", "export_status").
					WithField("export_id", id).
					Warnf("failed to promote assembled status to metadata: %v", writeErr)
			}
		default:
		}
		return assembled, nil
	}

	// Single-node: metadata is always present (written at export start).
	return s.statusFromMetadata(backendStore, id, bucket, path, meta)
}

// Cancel cancels a running export.
// Returns ErrExportNotFound if the export does not exist,
// or ErrExportAlreadyFinished if it has already completed.
//
// Note: Cancel does not remove artifacts (Parquet files, status files,
// metadata) already written to the backend. This is intentional — partial data
// is kept so operators can inspect what was exported before the cancellation
// and to avoid the complexity of distributed garbage collection across
// storage backends. The same applies to failed exports.
func (s *Scheduler) Cancel(ctx context.Context, principal *models.Principal, backend, id, bucket, path string) error {
	if !regExpID.MatchString(id) {
		return fmt.Errorf("%w: invalid export id: '%v' allowed characters are lowercase, 0-9, _, -", ErrExportValidation, id)
	}
	backendStore, err := s.backends.BackupBackend(backend)
	if err != nil {
		return fmt.Errorf("%w: backend %s not available: %w", ErrExportValidation, backend, err)
	}

	if err := backendStore.Initialize(ctx, id, bucket, path); err != nil {
		return fmt.Errorf("initialize backend: %w", err)
	}

	// Metadata is always written before the export starts.
	meta, err := s.getExportMetadata(ctx, backendStore, id, bucket, path)
	if err != nil {
		if errors.As(err, &backup.ErrNotFound{}) {
			return ErrExportNotFound
		}
		return fmt.Errorf("get export metadata: %w", err)
	}

	if err := s.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Backups(meta.Classes...)...); err != nil {
		return fmt.Errorf("authorization failed: %w", err)
	}

	// If terminal metadata already exists, the export is done.
	switch meta.Status {
	case export.Success, export.Failed, export.Canceled:
		return ErrExportAlreadyFinished
	default:
	}

	var abortErr error
	if s.isMultiNode() {
		assembled, allTerminal, assembleErr := s.assembleStatusFromMetadata(ctx, backendStore, principal, id, bucket, path, meta)
		if assembleErr == nil {
			switch export.Status(assembled.Status) {
			case export.Success:
				return ErrExportAlreadyFinished
			case export.Failed:
				// The assembled FAILED status may come from liveness checks
				// (node unreachable / not running) rather than actual completion.
				// If all nodes genuinely reported a terminal status, the export
				// is already done — do not overwrite FAILED with CANCELED.
				if allTerminal {
					return ErrExportAlreadyFinished
				}
				// Some nodes may still be running — proceed with best-effort
				// aborts so they get a cancellation signal.
			case export.Started, export.Transferring, export.Canceled:
			}
		}

		// Build node info from metadata for abort.
		nodes := make([]exportNodeInfo, 0, len(meta.NodeAssignments))
		for nodeName := range meta.NodeAssignments {
			ni := exportNodeInfo{
				req: &ExportRequest{ID: id, NodeName: nodeName},
			}
			if nodeName != s.localNode {
				host, ok := s.nodeResolver.NodeHostname(nodeName)
				if ok {
					ni.host = host
				}
			}
			nodes = append(nodes, ni)
		}
		if abortErr = s.abortAll(id, nodes); abortErr != nil {
			s.logger.WithField("action", "export_cancel").
				WithField("export_id", id).
				Errorf("best-effort abort encountered errors: %v", abortErr)
		}
	} else {
		sne := s.activeExport.Load()
		if sne == nil {
			return ErrExportAlreadyFinished
		}
		sne.cancel(errExportCanceled)

		if !errors.Is(context.Cause(sne.ctx), errExportCanceled) {
			return ErrExportAlreadyFinished
		}
	}

	cancelErrMsg := "export was canceled"
	if abortErr != nil {
		cancelErrMsg = fmt.Sprintf("export was canceled but some nodes could not be reached: %v", abortErr)
	}
	cancelMeta := &ExportMetadata{
		ID:              id,
		Backend:         backend,
		Status:          export.Canceled,
		Error:           cancelErrMsg,
		Classes:         meta.Classes,
		NodeAssignments: meta.NodeAssignments,
		StartedAt:       meta.StartedAt,
		CompletedAt:     time.Now().UTC(),
	}
	if err := writeExportMetadata(backendStore, id, bucket, path, cancelMeta, s.logger); err != nil {
		s.logger.WithField("action", "export_cancel").
			WithField("export_id", id).
			Errorf("failed to persist canceled metadata: %v", err)
		return fmt.Errorf("export canceled but failed to persist status: %w", err)
	}
	return nil
}

// statusFromMetadata builds an ExportStatusResponse from an ExportMetadata record.
// Used for terminal states (single-node and promoted multi-node).
func (s *Scheduler) statusFromMetadata(backend modulecapabilities.BackupBackend, id, bucket, path string, meta *ExportMetadata) (*models.ExportStatusResponse, error) {
	es := &models.ExportStatusResponse{
		ID:          meta.ID,
		Backend:     meta.Backend,
		Path:        backend.HomeDir(id, bucket, path),
		Status:      string(meta.Status),
		StartedAt:   strfmt.DateTime(meta.StartedAt),
		Classes:     meta.Classes,
		Error:       meta.Error,
		ShardStatus: meta.ShardStatus,
	}

	if !meta.CompletedAt.IsZero() {
		es.CompletedAt = strfmt.DateTime(meta.CompletedAt)
		es.TookInMs = meta.CompletedAt.Sub(meta.StartedAt).Milliseconds()
	}

	return es, nil
}

// assembleNodeStatuses computes the overall export status from already-resolved
// per-node statuses. It does NOT perform liveness checks — the caller is
// responsible for enriching nodeStatuses beforehand (e.g. overriding
// non-terminal nodes to Failed when unreachable).
func assembleNodeStatuses(
	meta *ExportMetadata,
	homePath string,
	nodeStatuses map[string]*NodeStatus,
) (*models.ExportStatusResponse, bool) {
	status := &models.ExportStatusResponse{
		ID:          meta.ID,
		Backend:     meta.Backend,
		Path:        homePath,
		Status:      string(export.Transferring),
		StartedAt:   strfmt.DateTime(meta.StartedAt),
		Classes:     meta.Classes,
		ShardStatus: make(map[string]map[string]models.ShardProgress),
	}

	allSuccess := true
	anyFailed := false
	allTerminal := true
	var lastCompleted time.Time

	for nodeName, nodeStatus := range nodeStatuses {
		if nodeStatus.CompletedAt.After(lastCompleted) {
			lastCompleted = nodeStatus.CompletedAt
		}

		effectiveShardStatus := export.ShardTransferring
		switch nodeStatus.Status {
		case export.Success:
			effectiveShardStatus = export.ShardSuccess
		case export.Failed:
			effectiveShardStatus = export.ShardFailed
			anyFailed = true
			allSuccess = false
			if status.Error == "" {
				status.Error = fmt.Sprintf("node %s failed: %s", nodeName, nodeStatus.Error)
			}
		default:
			allTerminal = false
			allSuccess = false
		}

		for className, shards := range meta.NodeAssignments[nodeName] {
			if status.ShardStatus[className] == nil {
				status.ShardStatus[className] = make(map[string]models.ShardProgress)
			}
			for _, shardName := range shards {
				sp := nodeStatus.ShardProgress[className][shardName]
				if sp == nil {
					sp = &ShardProgress{Status: effectiveShardStatus}
				} else if sp.Status != export.ShardSuccess && sp.Status != export.ShardFailed && sp.Status != export.ShardSkipped {
					sp.Status = effectiveShardStatus
				}
				status.ShardStatus[className][shardName] = models.ShardProgress{
					Status:          string(sp.Status),
					ObjectsExported: sp.ObjectsExported,
					Error:           sp.Error,
					SkipReason:      sp.SkipReason,
				}
			}
		}
	}

	if anyFailed {
		status.Status = string(export.Failed)
	} else if allSuccess {
		status.Status = string(export.Success)
	}

	if !lastCompleted.IsZero() && (allSuccess || anyFailed) {
		status.CompletedAt = strfmt.DateTime(lastCompleted)
		status.TookInMs = lastCompleted.Sub(meta.StartedAt).Milliseconds()
	}

	return status, allTerminal
}

// assembleStatusFromMetadata reads per-node status files from the configured
// backup backend and assembles overall status using the metadata's
// NodeAssignments. The returned allTerminal flag is true when every node has
// written a status file with a terminal status (Success or Failed),
// distinguishing genuine completions from liveness-inferred failures.
func (s *Scheduler) assembleStatusFromMetadata(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	principal *models.Principal,
	id, bucket, path string,
	meta *ExportMetadata,
) (_ *models.ExportStatusResponse, allTerminal bool, _ error) {
	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(meta.Classes...)...); err != nil {
		return nil, false, fmt.Errorf("authorization failed: %w", err)
	}

	homePath := backend.HomeDir(id, bucket, path)

	// Read per-node statuses and apply liveness overrides for non-terminal nodes.
	// The status file is read up to twice per node. The export goroutine writes
	// the terminal status and then clears its activeExport flag. An IsRunning
	// check that lands between those two events sees "not running" while the
	// status file still shows Transferring/Started. A single re-read is
	// sufficient because stopWriter blocks until the write completes before
	// clearAndRelease clears activeExport, so the file is guaranteed to be on
	// disk when IsRunning returns false.
	const maxStatusReads = 2
	nodeStatuses := make(map[string]*NodeStatus, len(meta.NodeAssignments))
	for nodeName := range meta.NodeAssignments {
		var nodeStatus *NodeStatus
		for attempt := range maxStatusReads {
			ns, err := readNodeStatus(ctx, backend, id, bucket, path, nodeName)
			if err != nil {
				if !errors.As(err, &backup.ErrNotFound{}) {
					return nil, false, fmt.Errorf("get status for node %s: %w", nodeName, err)
				}
				ns = &NodeStatus{
					NodeName:      nodeName,
					Status:        export.Transferring,
					ShardProgress: make(map[string]map[string]*ShardProgress),
				}
			}

			// For non-terminal nodes, verify liveness and override to Failed if unreachable.
			switch ns.Status {
			case export.Success, export.Failed:
				// Terminal — no liveness check needed.
				nodeStatus = ns
			default:
				host, alive := s.nodeResolver.NodeHostname(nodeName)
				if !alive {
					ns.Status = export.Failed
					if ns.Error == "" {
						ns.Error = fmt.Sprintf("node %s is no longer part of the cluster", nodeName)
					}
					nodeStatus = ns
				} else if s.client != nil {
					running, runErr := s.client.IsRunning(ctx, host, id)
					if runErr != nil || !running {
						if attempt < maxStatusReads-1 {
							// Re-read the status file — the goroutine may
							// have written the terminal status by now.
							continue
						}
						ns.Status = export.Failed
						if ns.Error == "" {
							ns.Error = fmt.Sprintf("node %s is no longer running export %s", nodeName, id)
						}
					}
					nodeStatus = ns
				} else {
					nodeStatus = ns
				}
			}
			break
		}

		nodeStatuses[nodeName] = nodeStatus
	}

	status, allTerminal := assembleNodeStatuses(meta, homePath, nodeStatuses)
	return status, allTerminal, nil
}

// performMultiNodeExport orchestrates export across multiple nodes using
// a two-phase commit protocol:
//  1. Prepare all nodes (reserve the export slot).
//  2. If all prepared successfully, commit all (start the export).
//  3. If any prepare fails, abort all previously prepared nodes.
func (s *Scheduler) performMultiNodeExport(ctx context.Context, backend modulecapabilities.BackupBackend, exportID string, status *models.ExportStatusResponse, classes []string, bucket, path string) error {
	// Build node assignments: node → className → []shardName
	nodeAssignments := make(map[string]map[string][]string)

	for _, className := range classes {
		ownership, err := s.selector.ShardOwnership(ctx, className)
		if err != nil {
			return fmt.Errorf("failed to get shard ownership for class %s: %w", className, err)
		}

		for node, shards := range ownership {
			if nodeAssignments[node] == nil {
				nodeAssignments[node] = make(map[string][]string)
			}
			nodeAssignments[node][className] = shards
		}
	}

	// Build per-node requests and resolve hostnames.
	allNodeNames := make([]string, 0, len(nodeAssignments))
	for node := range nodeAssignments {
		allNodeNames = append(allNodeNames, node)
	}

	nodes := make([]exportNodeInfo, 0, len(nodeAssignments))
	for node, classShards := range nodeAssignments {
		var siblings []string
		for _, n := range allNodeNames {
			if n != node {
				siblings = append(siblings, n)
			}
		}
		ni := exportNodeInfo{
			req: &ExportRequest{
				ID:              exportID,
				Backend:         status.Backend,
				Classes:         classes,
				Shards:          classShards,
				Bucket:          bucket,
				Path:            path,
				NodeName:        node,
				SiblingNodes:    siblings,
				StartedAt:       time.Time(status.StartedAt),
				NodeAssignments: nodeAssignments,
			},
		}
		if node != s.localNode {
			host, ok := s.nodeResolver.NodeHostname(node)
			if !ok {
				return fmt.Errorf("failed to resolve hostname for node %s", node)
			}
			ni.host = host
		}
		nodes = append(nodes, ni)
	}

	// Phase 1: Prepare all nodes.
	var prepared []exportNodeInfo
	for _, ni := range nodes {
		var err error
		if ni.host == "" {
			err = s.participant.Prepare(ctx, ni.req)
		} else {
			err = s.client.Prepare(ctx, ni.host, ni.req)
		}
		if err != nil {
			// Abort all previously prepared nodes.
			s.abortAll(exportID, prepared)
			return fmt.Errorf("prepare node %s: %w", ni.req.NodeName, err)
		}
		prepared = append(prepared, ni)
	}

	// Write initial metadata to the backend after all nodes are prepared.
	initialMeta := &ExportMetadata{
		ID:              exportID,
		Backend:         status.Backend,
		StartedAt:       time.Time(status.StartedAt),
		Status:          export.Started,
		Classes:         classes,
		NodeAssignments: nodeAssignments,
	}
	if err := writeExportMetadata(backend, exportID, bucket, path, initialMeta, s.logger); err != nil {
		s.abortAll(exportID, prepared)
		return fmt.Errorf("failed to write export metadata: %w", err)
	}

	// Phase 2: Commit all nodes.
	for _, ni := range prepared {
		var err error
		if ni.host == "" {
			err = s.participant.Commit(ctx, exportID)
		} else {
			err = s.client.Commit(ctx, ni.host, exportID)
		}
		if err != nil {
			s.abortAll(exportID, prepared)
			initialMeta.Status = export.Failed
			initialMeta.Error = fmt.Sprintf("commit node %s failed: %v", ni.req.NodeName, err)
			initialMeta.CompletedAt = time.Now().UTC()
			writeExportMetadata(backend, exportID, bucket, path, initialMeta, s.logger)
			return fmt.Errorf("commit node %s: %w", ni.req.NodeName, err)
		}
	}

	s.logger.WithField("action", "export").
		WithField("export_id", exportID).
		WithField("nodes", len(prepared)).
		Info("multi-node export committed on all nodes")

	return nil
}

// abortAll sends abort to all previously prepared nodes (best-effort).
// It uses a fresh context so abort requests reach participants even when the
// original request context has been canceled (e.g. client disconnect).
// Remote aborts are retried up to 3 times on error.
func (s *Scheduler) abortAll(exportID string, nodes []exportNodeInfo) error {
	const maxRetries = 3
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var returnErr error
	for _, ni := range nodes {
		if ni.req.NodeName == s.localNode {
			s.participant.Abort(exportID)
			continue
		}
		if ni.host == "" {
			s.logger.WithField("action", "export_abort").
				WithField("export_id", exportID).
				WithField("node", ni.req.NodeName).
				Warn("skipping abort: cannot resolve host for remote node")
			continue
		}
		var nodeErr error
		for attempt := range maxRetries {
			attemptErr := s.client.Abort(ctx, ni.host, exportID)
			if attemptErr == nil {
				nodeErr = nil
				break
			}
			s.logger.WithField("action", "export_abort").
				WithField("export_id", exportID).
				WithField("node", ni.req.NodeName).
				WithField("attempt", attempt+1).
				Errorf("abort failed: %v", attemptErr)
			nodeErr = fmt.Errorf("attempt %d: %w: %w", attempt+1, attemptErr, nodeErr)
			if attempt < maxRetries-1 {
				time.Sleep(500 * time.Millisecond)
			}
		}
		if nodeErr != nil {
			returnErr = fmt.Errorf("node %s: %w: %w", ni.req.NodeName, nodeErr, returnErr)
		}
	}
	return returnErr
}

// clearCancelState clears the active export pointer and calls cancel(nil).
// CancelCauseFunc is first-caller-wins: if Cancel() already called
// cancel(errExportCanceled), this cancel(nil) is a no-op and
// context.Cause will still return errExportCanceled.
func (s *Scheduler) clearCancelState(cancel context.CancelCauseFunc) {
	s.activeExport.Store(nil)
	cancel(nil)
}

// writeInitialMetadata resolves shard names and writes initial export metadata
// to the storage backend. It is called synchronously from Export() so that the
// metadata is available before the API response is returned, allowing Cancel()
// to find it immediately.
func (s *Scheduler) writeInitialMetadata(ctx context.Context, backend modulecapabilities.BackupBackend, exportID string, status *models.ExportStatusResponse, classes []string, bucket, path string) (map[string][]string, error) {
	shards := make(map[string][]string, len(classes))
	for _, className := range classes {
		shardNames, _, err := s.selector.ExportShardNames(className)
		if err != nil {
			return nil, fmt.Errorf("failed to get shards for class %s: %w", className, err)
		}
		shards[className] = shardNames
	}

	meta := &ExportMetadata{
		ID:              exportID,
		Backend:         status.Backend,
		StartedAt:       time.Time(status.StartedAt),
		Status:          export.Transferring,
		Classes:         classes,
		NodeAssignments: map[string]map[string][]string{s.localNode: shards},
	}
	if err := writeExportMetadata(backend, exportID, bucket, path, meta, s.logger); err != nil {
		return nil, fmt.Errorf("failed to write export metadata: %w", err)
	}

	return shards, nil
}

// performSingleNodeExport delegates to the participant's export logic, reusing
// the same per-shard code path as multi-node exports. Shards must already be
// resolved via writeInitialMetadata.
func (s *Scheduler) performSingleNodeExport(ctx context.Context, cancel context.CancelCauseFunc, backend modulecapabilities.BackupBackend, exportID string, status *models.ExportStatusResponse, classes []string, shards map[string][]string, bucket, path string) {
	req := &ExportRequest{
		ID:       exportID,
		Backend:  status.Backend,
		Classes:  classes,
		Shards:   shards,
		Bucket:   bucket,
		Path:     path,
		NodeName: s.localNode,
	}

	nodeStatus := &NodeStatus{
		NodeName:      req.NodeName,
		Status:        export.Transferring,
		ShardProgress: make(map[string]map[string]*ShardProgress),
		Version:       config.ServerVersion,
	}

	exportErr := s.participant.doExport(ctx, backend, req, nodeStatus)

	// Clear the active export pointer and call cancel(nil). Because
	// CancelCauseFunc is first-caller-wins, this establishes who writes
	// terminal metadata: if Cancel() called cancel(errExportCanceled) first,
	// context.Cause returns errExportCanceled and we skip our write.
	s.clearCancelState(cancel)

	if errors.Is(context.Cause(ctx), errExportCanceled) {
		// Cancel() won the first-caller race on the CancelCauseFunc,
		// so it owns the metadata write.
		if exportErr != nil {
			s.logger.WithField("action", "export").
				WithField("export_id", exportID).
				Error(exportErr)
		}
		return
	}

	if exportErr != nil {
		s.logger.WithField("action", "export").
			WithField("export_id", exportID).
			Error(exportErr)

		status.Status = string(export.Failed)
		status.Error = exportErr.Error()
		failMeta := &ExportMetadata{
			ID:              exportID,
			Backend:         status.Backend,
			StartedAt:       time.Time(status.StartedAt),
			CompletedAt:     time.Now().UTC(),
			Status:          export.Failed,
			Classes:         classes,
			NodeAssignments: map[string]map[string][]string{s.localNode: shards},
			Error:           exportErr.Error(),
		}
		writeExportMetadata(backend, exportID, bucket, path, failMeta, s.logger)
		return
	}

	status.Status = string(export.Success)

	successMeta := &ExportMetadata{
		ID:              exportID,
		Backend:         status.Backend,
		StartedAt:       time.Time(status.StartedAt),
		CompletedAt:     time.Now().UTC(),
		Status:          export.Success,
		Classes:         classes,
		NodeAssignments: map[string]map[string][]string{s.localNode: shards},
	}
	if err := writeExportMetadata(backend, exportID, bucket, path, successMeta, s.logger); err != nil {
		s.logger.WithField("action", "export").
			WithField("export_id", exportID).
			Error(err)

		status.Status = string(export.Failed)
		status.Error = fmt.Sprintf("failed to write metadata: %v", err)
		return
	}

	s.logger.WithField("action", "export").
		WithField("export_id", exportID).
		Info("export completed successfully")
}

// writeExportMetadata writes the given ExportMetadata to the storage backend
// with retries. It uses a fresh context with a timeout so the write succeeds
// even if the original context was canceled.
func writeExportMetadata(
	backend modulecapabilities.BackupBackend,
	exportID, bucket, path string,
	metadata *ExportMetadata,
	logger logrus.FieldLogger,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Ensure CompletedAt is always set for terminal metadata so the file
	// has a meaningful timestamp even when no node reported a completion
	// time (e.g. all nodes crashed or the export was canceled/shut down).
	if metadata.CompletedAt.IsZero() {
		switch metadata.Status {
		case export.Success, export.Failed, export.Canceled:
			metadata.CompletedAt = time.Now().UTC()
		case export.Started, export.Transferring:
			// Non-terminal — no CompletedAt needed.
		}
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	const maxRetries = 3
	for attempt := range maxRetries {
		_, err = backend.Write(ctx, exportID, exportMetadataFile, bucket, path, newBytesReadCloser(data))
		if err == nil {
			return nil
		}
		if attempt < maxRetries-1 {
			logger.WithField("action", "export_write_metadata").
				WithField("export_id", exportID).
				Warnf("metadata write attempt %d failed, retrying: %v", attempt+1, err)
			select {
			case <-ctx.Done():
				return fmt.Errorf("write metadata aborted after %d attempts: %w", attempt+1, ctx.Err())
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	return fmt.Errorf("write metadata after %d attempts: %w", maxRetries, err)
}

// readNodeStatus reads and unmarshals a node's status file from the storage backend.
func readNodeStatus(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path, nodeName string) (*NodeStatus, error) {
	key := fmt.Sprintf("node_%s_status.json", nodeName)
	data, err := backend.GetObject(ctx, exportID, key, bucket, path)
	if err != nil {
		return nil, fmt.Errorf("get node status: %w", err)
	}

	var status NodeStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return nil, fmt.Errorf("unmarshal node status: %w", err)
	}

	return &status, nil
}

// checkIfExportExists checks if an export already exists in the backend by
// looking for the metadata file. If it exists the export folder is considered
// occupied regardless of its status.
//
// This is intentional: export IDs are not reusable even after cancellation or
// failure. Each export attempt must use a unique ID. Allowing reuse would risk
// mixing artifacts from different runs in the same backend directory and make
// it ambiguous which metadata files belong to which attempt.
func (s *Scheduler) checkIfExportExists(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string) error {
	home := backend.HomeDir(exportID, bucket, path)

	_, err := s.getExportMetadata(ctx, backend, exportID, bucket, path)
	if err == nil {
		return fmt.Errorf("%w: export %q already exists at %q", ErrExportAlreadyExists, exportID, home)
	}
	if !errors.As(err, &backup.ErrNotFound{}) {
		return fmt.Errorf("check existing export: %w", err)
	}

	return nil
}

// getExportMetadata retrieves export metadata from the backend
func (s *Scheduler) getExportMetadata(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string) (*ExportMetadata, error) {
	data, err := backend.GetObject(ctx, exportID, exportMetadataFile, bucket, path)
	if err != nil {
		return nil, fmt.Errorf("get metadata: %w", err)
	}

	var meta ExportMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}

	return &meta, nil
}

// resolveClasses determines which classes to export.
// It silently ignores non-existent classes in include/exclude lists to avoid
// leaking class existence information before authorization.
func (s *Scheduler) resolveClasses(ctx context.Context, include, exclude []string) ([]string, error) {
	allClasses := s.selector.ListClasses(ctx)

	if len(include) > 0 && len(exclude) > 0 {
		return nil, fmt.Errorf("cannot specify both 'include' and 'exclude'")
	}

	classMap := make(map[string]bool, len(allClasses))
	for _, class := range allClasses {
		classMap[class] = true
	}

	if len(include) > 0 {
		result := make([]string, 0, len(include))
		for _, class := range include {
			if classMap[class] {
				result = append(result, class)
			}
		}
		return result, nil
	}

	if len(exclude) > 0 {
		excludeMap := make(map[string]bool, len(exclude))
		for _, class := range exclude {
			excludeMap[class] = true
		}

		result := make([]string, 0, len(allClasses))
		for _, class := range allClasses {
			if !excludeMap[class] {
				result = append(result, class)
			}
		}
		return result, nil
	}

	return allClasses, nil
}
