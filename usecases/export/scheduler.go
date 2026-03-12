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
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
)

var (
	regExpID = regexp.MustCompile(`^[a-z0-9_-]+$`)

	// ErrExportNotFound is returned when no export with the given ID exists.
	ErrExportNotFound = errors.New("export not found")

	// ErrExportAlreadyFinished is returned when trying to cancel an export
	// that has already completed (SUCCESS, FAILED, or CANCELED).
	ErrExportAlreadyFinished = errors.New("export has already finished")

	// errExportCanceled is passed as the cause to context.WithCancelCause
	// when an export is canceled via the Cancel endpoint.
	errExportCanceled = errors.New("export was canceled")
)

const (
	exportMetadataFile = "export_metadata.json"
	exportPlanFile     = "export_plan.json"
)

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
	selector     Selector
	backends     BackendProvider
	client       ExportClient // nil for single-node
	nodeResolver NodeResolver // nil for single-node
	localNode    string       // from appState.Cluster.LocalName()
	participant  *Participant // local participant — always present

	// cancelExport stores the cancel-cause function for the active single-node
	// export. A nil pointer means no export is running.
	cancelExport atomic.Pointer[context.CancelCauseFunc]
}

// NewScheduler creates a new export scheduler.
// When client and nodeResolver are nil, operates in single-node mode.
// The shutdownCtx is canceled on graceful server shutdown.
func NewScheduler(
	shutdownCtx context.Context,
	authorizer authorization.Authorizer,
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
		return nil, fmt.Errorf("invalid export id: '%v' allowed characters are lowercase, 0-9, _, -", id)
	}
	if backend == "" {
		return nil, fmt.Errorf("backend is required")
	}

	backendStore, err := s.backends.BackupBackend(backend)
	if err != nil {
		return nil, fmt.Errorf("backend %s not available: %w", backend, err)
	}

	if err := backendStore.Initialize(ctx, id, bucket, path); err != nil {
		return nil, fmt.Errorf("initialize backend: %w", err)
	}

	if err := s.checkIfExportExists(ctx, backendStore, id, bucket, path); err != nil {
		return nil, err
	}

	classes, err := s.resolveClasses(ctx, include, exclude)
	if err != nil {
		return nil, fmt.Errorf("resolve classes: %w", err)
	}

	if len(classes) == 0 {
		return nil, fmt.Errorf("no classes selected for export")
	}

	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(classes...)...); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
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
		if !s.cancelExport.CompareAndSwap(nil, &cancel) {
			cancel(nil)
			return nil, fmt.Errorf("an export is already in progress")
		}

		// Safety net: clear cancelExport on panic during the synchronous
		// phase so that a recovered panic does not permanently block future
		// exports.
		defer func() {
			if r := recover(); r != nil {
				s.cancelExport.Store(nil)
				cancel(nil)
				panic(r) // re-panic after cleanup
			}
		}()

		// Resolve shards and write the export plan synchronously so that
		// the plan exists on the backend before the API response is returned. This
		// prevents Cancel from getting a 404 when called right after Create.
		shards, err := s.prepareSingleNodePlan(ctx, backendStore, id, status, classes, bucket, path)
		if err != nil {
			s.cancelExport.Store(nil)
			cancel(nil)
			return nil, err
		}

		enterrors.GoWrapper(func() {
			defer func() {
				// Safety net: ensure fields are cleared even on panic.
				s.cancelExport.Store(nil)
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
// In multi-node mode, assembles status from S3 export plan + per-node status files.
// In single-node mode, reads the metadata file directly.
func (s *Scheduler) Status(ctx context.Context, principal *models.Principal, backend, id, bucket, path string) (*models.ExportStatusResponse, error) {
	backendStore, err := s.backends.BackupBackend(backend)
	if err != nil {
		return nil, fmt.Errorf("backend %s not available: %w", backend, err)
	}

	if err := backendStore.Initialize(ctx, id, bucket, path); err != nil {
		return nil, fmt.Errorf("initialize backend: %w", err)
	}

	// Both single-node and multi-node write a plan, so try to read it first.
	plan, planErr := s.getExportPlan(ctx, backendStore, id, bucket, path)
	if planErr != nil {
		return nil, fmt.Errorf("get export plan: %w", planErr)
	}

	// Authorize using plan classes.
	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(plan.Classes...)...); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
	}

	// Check if the export was canceled or has terminal metadata.
	meta, metaErr := s.getExportMetadata(ctx, backendStore, id, bucket, path)
	if metaErr != nil && !errors.As(metaErr, &backup.ErrNotFound{}) {
		return nil, fmt.Errorf("get export metadata: %w", metaErr)
	}
	if metaErr == nil {
		switch meta.Status {
		case export.Success, export.Failed, export.Canceled:
			return s.statusFromMetadata(backendStore, id, bucket, path, meta)
		default:
			// Non-terminal states are reconstructed from per-node status below.
		}
	}

	if s.isMultiNode() {
		assembled, _, err := s.assembleStatusFromPlan(ctx, backendStore, principal, id, bucket, path, plan)
		if err != nil {
			return nil, err
		}
		// Promote: if all nodes reached a terminal state, persist the final
		// metadata so future Status() calls (and Cancel()) see it directly
		// without re-assembling from per-node files.
		switch export.Status(assembled.Status) {
		case export.Success, export.Failed:
			// Ensure CompletedAt is set from the assembled result (derived from
			// per-node completion times) so writeMetadata doesn't fall back to
			// time.Now(), which would record the Status() call time instead.
			if time.Time(assembled.CompletedAt).IsZero() {
				assembled.CompletedAt = strfmt.DateTime(time.Now().UTC())
				s.logger.WithField("action", "export_status").
					WithField("export_id", id).
					Warn("assembled terminal status had no CompletedAt; falling back to now")
			}
			if writeErr := s.writeMetadata(backendStore, id, bucket, path, assembled); writeErr != nil {
				s.logger.WithField("action", "export_status").
					WithField("export_id", id).
					Warnf("failed to promote assembled status to metadata: %v", writeErr)
			}
		default:
			// Non-terminal or already canceled — nothing to promote.
		}
		return assembled, nil
	}

	// Single-node: metadata has the terminal status.
	if metaErr == nil {
		return s.statusFromMetadata(backendStore, id, bucket, path, meta)
	}
	// Metadata not found — the export is still running.
	return &models.ExportStatusResponse{
		ID:        plan.ID,
		Backend:   plan.Backend,
		Path:      backendStore.HomeDir(id, bucket, path),
		Status:    string(export.Transferring),
		StartedAt: strfmt.DateTime(plan.StartedAt),
		Classes:   plan.Classes,
	}, nil
}

// Cancel cancels a running export.
// Returns ErrExportNotFound if the export does not exist,
// or ErrExportAlreadyFinished if it has already completed.
func (s *Scheduler) Cancel(ctx context.Context, principal *models.Principal, backend, id, bucket, path string) error {
	backendStore, err := s.backends.BackupBackend(backend)
	if err != nil {
		return fmt.Errorf("backend %s not available: %w", backend, err)
	}

	if err := backendStore.Initialize(ctx, id, bucket, path); err != nil {
		return fmt.Errorf("initialize backend: %w", err)
	}

	// Both single-node and multi-node write a plan, so use it as the
	// primary source of truth for authorization and node assignments.
	plan, err := s.getExportPlan(ctx, backendStore, id, bucket, path)
	if err != nil {
		if errors.As(err, &backup.ErrNotFound{}) {
			return ErrExportNotFound
		}
		return fmt.Errorf("get export plan: %w", err)
	}

	if err := s.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Backups(plan.Classes...)...); err != nil {
		return fmt.Errorf("authorization failed: %w", err)
	}

	// If terminal metadata already exists, the export is done.
	meta, metaErr := s.getExportMetadata(ctx, backendStore, id, bucket, path)
	if metaErr == nil {
		switch meta.Status {
		case export.Success, export.Failed, export.Canceled:
			return ErrExportAlreadyFinished
		default:
		}
	}

	var abortErr error
	if s.isMultiNode() {
		// In multi-node mode no terminal metadata is written on success —
		// the overall status is computed from per-node status files.
		assembled, allTerminal, assembleErr := s.assembleStatusFromPlan(ctx, backendStore, principal, id, bucket, path, plan)
		if assembleErr == nil {
			switch export.Status(assembled.Status) {
			case export.Success:
				// All nodes completed successfully — nothing to cancel.
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

		// Build node info from plan for abort.
		nodes := make([]exportNodeInfo, 0, len(plan.NodeAssignments))
		for nodeName := range plan.NodeAssignments {
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
			// abortAll is best-effort (retries 3x per node). If it still
			// fails, those nodes are likely unreachable. Continue to write
			// CANCELED metadata so that Status() reflects the user's intent.
			s.logger.WithField("action", "export_cancel").
				WithField("export_id", id).
				Errorf("best-effort abort encountered errors: %v", abortErr)
		}
	} else {
		cancelPtr := s.cancelExport.Load()
		if cancelPtr == nil {
			// The export goroutine finished between the metadata check
			// above and now — the export is already done.
			return ErrExportAlreadyFinished
		}
		(*cancelPtr)(errExportCanceled)
	}

	cancelErr := "export was canceled"
	if abortErr != nil {
		cancelErr = fmt.Sprintf("export was canceled but some nodes could not be reached: %v", abortErr)
	}
	cancelStatus := &models.ExportStatusResponse{
		ID:          id,
		Backend:     backend,
		Status:      string(export.Canceled),
		Error:       cancelErr,
		Classes:     plan.Classes,
		StartedAt:   strfmt.DateTime(plan.StartedAt),
		CompletedAt: strfmt.DateTime(time.Now().UTC()),
	}
	if err := s.writeMetadata(backendStore, id, bucket, path, cancelStatus); err != nil {
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

// assembleStatusFromPlan reads per-node status files from the configured backup backend and assembles
// overall status. The returned allTerminal flag is true when every node in
// the plan has written a status file with a terminal status (Success or
// Failed), distinguishing genuine completions from liveness-inferred failures.
func (s *Scheduler) assembleStatusFromPlan(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	principal *models.Principal,
	id, bucket, path string,
	plan *ExportPlan,
) (_ *models.ExportStatusResponse, allTerminal bool, _ error) {
	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(plan.Classes...)...); err != nil {
		return nil, false, fmt.Errorf("authorization failed: %w", err)
	}

	status := &models.ExportStatusResponse{
		ID:          plan.ID,
		Backend:     plan.Backend,
		Path:        backend.HomeDir(id, bucket, path),
		Status:      string(export.Transferring),
		StartedAt:   strfmt.DateTime(plan.StartedAt),
		Classes:     plan.Classes,
		ShardStatus: make(map[string]map[string]models.ShardProgress),
	}

	allSuccess := true
	anyFailed := false
	allTerminal = true
	var lastCompleted time.Time

	for nodeName := range plan.NodeAssignments {
		nodeStatus, err := s.getNodeStatus(ctx, backend, id, bucket, path, nodeName)
		if err != nil {
			if !errors.As(err, &backup.ErrNotFound{}) {
				return nil, false, fmt.Errorf("get status for node %s: %w", nodeName, err)
			}
			// No status file yet — treat as non-terminal and check liveness below
			allTerminal = false
			nodeStatus = &NodeStatus{
				NodeName:      nodeName,
				Status:        export.Transferring,
				ShardProgress: make(map[string]map[string]*ShardProgress),
			}
		}

		if nodeStatus.CompletedAt.After(lastCompleted) {
			lastCompleted = nodeStatus.CompletedAt
		}

		// effectiveShardStatus defaults to Transferring and is overridden to
		// Failed when a node is unreachable or no longer running the export.
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
		case export.Started, export.Transferring, export.Canceled:
			// Non-terminal (Transferring/Started): verify the node is still running
			allTerminal = false
			allSuccess = false
			host, alive := s.nodeResolver.NodeHostname(nodeName)
			if !alive {
				anyFailed = true
				effectiveShardStatus = export.ShardFailed
				if status.Error == "" {
					status.Error = fmt.Sprintf("node %s is no longer part of the cluster", nodeName)
				}
			} else if s.client != nil {
				running, runErr := s.client.IsRunning(ctx, host, id)
				if runErr != nil || !running {
					anyFailed = true
					effectiveShardStatus = export.ShardFailed
					if status.Error == "" {
						status.Error = fmt.Sprintf("node %s is no longer running export %s", nodeName, id)
					}
				}
			}
		}
		// Record per-shard status: use node's ShardProgress when available,
		// but override non-terminal shards if the node is effectively failed.
		for className, shards := range plan.NodeAssignments[nodeName] {
			if status.ShardStatus[className] == nil {
				status.ShardStatus[className] = make(map[string]models.ShardProgress)
			}
			for _, shardName := range shards {
				// Node may not have reported progress for this shard yet
				// (e.g. no status file written, or shard not started).
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
		status.TookInMs = lastCompleted.Sub(plan.StartedAt).Milliseconds()
	}

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
			s.logger.WithField("action", "export").WithField("class", className).Errorf("shard ownership: %v", err)
			status.Status = string(export.Failed)
			status.Error = fmt.Sprintf("failed to get shard ownership for class %s: %v", className, err)
			s.writeMetadata(backend, exportID, bucket, path, status)
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
				ID:           exportID,
				Backend:      status.Backend,
				Classes:      classes,
				Shards:       classShards,
				Bucket:       bucket,
				Path:         path,
				NodeName:     node,
				SiblingNodes: siblings,
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

	// Write export plan to S3 after all nodes are prepared.
	plan := &ExportPlan{
		ID:              exportID,
		Backend:         status.Backend,
		Classes:         classes,
		NodeAssignments: nodeAssignments,
		StartedAt:       time.Time(status.StartedAt),
	}

	if err := s.writeExportPlan(ctx, backend, exportID, bucket, path, plan); err != nil {
		s.abortAll(exportID, prepared)
		status.Status = string(export.Failed)
		status.Error = fmt.Sprintf("failed to write export plan: %v", err)
		s.writeMetadata(backend, exportID, bucket, path, status)
		return fmt.Errorf("failed to write export plan: %w", err)
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
			status.Status = string(export.Failed)
			status.Error = fmt.Sprintf("commit node %s failed: %v", ni.req.NodeName, err)
			s.writeMetadata(backend, exportID, bucket, path, status)
			return fmt.Errorf("commit node %s: %w", ni.req.NodeName, err)
		}
	}

	status.Status = string(export.Started)

	if err := s.writeMetadata(backend, exportID, bucket, path, status); err != nil {
		s.logger.WithField("action", "export").
			WithField("export_id", exportID).
			Errorf("failed to write metadata: %v", err)
		return fmt.Errorf("failed to write export metadata: %w", err)
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

// clearCancelState clears the cancel-cause pointer and calls cancel(nil).
// The cancel(nil) is a no-op if Cancel() already called it with
// errExportCanceled. This must be called BEFORE writing terminal metadata
// so that Cancel() can no longer find a non-nil pointer after metadata is
// persisted.
func (s *Scheduler) clearCancelState(cancel context.CancelCauseFunc) {
	s.cancelExport.Store(nil)
	cancel(nil)
}

// prepareSingleNodePlan resolves shard names and writes the export plan using the configured backup backend.
// It is called synchronously from Export() so that the plan is available before
// the API response is returned, allowing Cancel() to find it immediately.
func (s *Scheduler) prepareSingleNodePlan(ctx context.Context, backend modulecapabilities.BackupBackend, exportID string, status *models.ExportStatusResponse, classes []string, bucket, path string) (map[string][]string, error) {
	shards := make(map[string][]string, len(classes))
	for _, className := range classes {
		shardNames, _, err := s.selector.ExportShardNames(className)
		if err != nil {
			return nil, fmt.Errorf("failed to get shards for class %s: %w", className, err)
		}
		shards[className] = shardNames
	}

	plan := &ExportPlan{
		ID:              exportID,
		Backend:         status.Backend,
		Classes:         classes,
		NodeAssignments: map[string]map[string][]string{s.localNode: shards},
		StartedAt:       time.Time(status.StartedAt),
	}
	if err := s.writeExportPlan(ctx, backend, exportID, bucket, path, plan); err != nil {
		return nil, fmt.Errorf("failed to write export plan: %w", err)
	}

	return shards, nil
}

// performSingleNodeExport delegates to the participant's export logic, reusing
// the same per-shard code path as multi-node exports. Shards must already be
// resolved via prepareSingleNodePlan.
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

	exportErr := s.participant.doExport(ctx, backend, req)

	// Clear the cancel state BEFORE writing metadata. This closes the race
	// window where Cancel() could find a non-nil cancelExport after the
	// terminal metadata has already been persisted.
	s.clearCancelState(cancel)

	if exportErr != nil {
		s.logger.WithField("action", "export").
			WithField("export_id", exportID).
			Error(exportErr)

		if errors.Is(context.Cause(ctx), errExportCanceled) {
			// Cancel() already writes the CANCELED metadata, so skip the
			// duplicate write here.
			return
		}
		status.Status = string(export.Failed)
		status.Error = exportErr.Error()
		s.writeMetadata(backend, exportID, bucket, path, status)
		return
	}

	status.Status = string(export.Success)

	if err := s.writeMetadata(backend, exportID, bucket, path, status); err != nil {
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

// exportShardData exports all objects from a single shard to a ParquetWriter.
// This is a standalone function shared by both the scheduler and the participant.
func exportShardData(ctx context.Context, shard ShardLike, writer *ParquetWriter, className string, logger logrus.FieldLogger) error {
	store := shard.Store()
	if store == nil {
		return fmt.Errorf("store not found for shard %s", shard.Name())
	}

	bucket := store.Bucket("objects")
	if bucket == nil {
		return fmt.Errorf("objects bucket not found for shard %s", shard.Name())
	}

	cursor := bucket.Cursor()
	defer cursor.Close()

	key, val := cursor.First()

	objectCount := 0
	logger.WithField("shard", shard.Name()).
		WithField("class", className).
		Debug("starting to iterate objects")

	for key != nil {
		objectCount++
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		obj, err := storobj.FromBinary(val)
		if err != nil {
			logger.WithField("action", "export").
				WithField("shard", shard.Name()).
				WithField("class", className).
				Warnf("failed to deserialize object, skipping: %v", err)
			key, val = cursor.Next()
			continue
		}

		if err := writer.WriteObject(obj); err != nil {
			return fmt.Errorf("write object to parquet: %w", err)
		}

		key, val = cursor.Next()
	}

	logger.WithField("shard", shard.Name()).
		WithField("class", className).
		WithField("object_count", objectCount).
		Info("completed shard iteration")

	return nil
}

// writeMetadata writes the export metadata file (single-node path).
// It uses a fresh context with a timeout so the write succeeds even if the
// original context was canceled (e.g. during graceful shutdown).
func (s *Scheduler) writeMetadata(backend modulecapabilities.BackupBackend, exportID, bucket, path string, status *models.ExportStatusResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	completedAt := time.Time(status.CompletedAt)
	if completedAt.IsZero() {
		completedAt = time.Now().UTC()
	}

	metadata := &ExportMetadata{
		ID:          status.ID,
		Backend:     status.Backend,
		StartedAt:   time.Time(status.StartedAt),
		CompletedAt: completedAt,
		Status:      export.Status(status.Status),
		Classes:     status.Classes,
		Error:       status.Error,
		ShardStatus: status.ShardStatus,
		Version:     config.ServerVersion,
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
			s.logger.WithField("action", "export_write_metadata").
				WithField("export_id", exportID).
				Warnf("metadata write attempt %d failed, retrying: %v", attempt+1, err)
			time.Sleep(500 * time.Millisecond)
		}
	}
	return fmt.Errorf("write metadata after %d attempts: %w", maxRetries, err)
}

// writeExportPlan writes the export plan to S3 (multi-node path)
func (s *Scheduler) writeExportPlan(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string, plan *ExportPlan) error {
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal export plan: %w", err)
	}

	_, err = backend.Write(ctx, exportID, exportPlanFile, bucket, path, newBytesReadCloser(data))
	return err
}

// getExportPlan reads the export plan from S3
func (s *Scheduler) getExportPlan(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string) (*ExportPlan, error) {
	data, err := backend.GetObject(ctx, exportID, exportPlanFile, bucket, path)
	if err != nil {
		return nil, fmt.Errorf("get export plan: %w", err)
	}

	var plan ExportPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, fmt.Errorf("unmarshal export plan: %w", err)
	}

	return &plan, nil
}

// getNodeStatus reads a node's status file from the configured backup backend.
func (s *Scheduler) getNodeStatus(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path, nodeName string) (*NodeStatus, error) {
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
// looking for any known artifact (metadata or plan). If either file exists the
// export folder is considered occupied regardless of its status.
//
// This is intentional: export IDs are not reusable even after cancellation or
// failure. Each export attempt must use a unique ID. Allowing reuse would risk
// mixing artifacts from different runs in the same backend directory and make
// it ambiguous which status/plan files belong to which attempt.
func (s *Scheduler) checkIfExportExists(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string) error {
	home := backend.HomeDir(exportID, bucket, path)

	// Check metadata file — written in both single-node and multi-node paths.
	_, err := s.getExportMetadata(ctx, backend, exportID, bucket, path)
	if err == nil {
		return fmt.Errorf("export %q already exists at %q", exportID, home)
	}
	if !errors.As(err, &backup.ErrNotFound{}) {
		return fmt.Errorf("check existing export: %w", err)
	}

	// Also check the plan file, which is written before metadata in both paths.
	_, err = s.getExportPlan(ctx, backend, exportID, bucket, path)
	if err == nil {
		return fmt.Errorf("export %q already exists at %q", exportID, home)
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

// resolveClasses determines which classes to export
func (s *Scheduler) resolveClasses(ctx context.Context, include, exclude []string) ([]string, error) {
	allClasses := s.selector.ListClasses(ctx)

	if len(include) > 0 && len(exclude) > 0 {
		return nil, fmt.Errorf("cannot specify both 'include' and 'exclude'")
	}

	if len(include) > 0 {
		classMap := make(map[string]bool)
		for _, class := range allClasses {
			classMap[class] = true
		}

		for _, class := range include {
			if !classMap[class] {
				return nil, fmt.Errorf("class %s does not exist", class)
			}
		}
		return include, nil
	}

	if len(exclude) > 0 {
		classMap := make(map[string]bool, len(allClasses))
		for _, class := range allClasses {
			classMap[class] = true
		}

		excludeMap := make(map[string]bool, len(exclude))
		for _, class := range exclude {
			if !classMap[class] {
				return nil, fmt.Errorf("class %s does not exist", class)
			}
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
