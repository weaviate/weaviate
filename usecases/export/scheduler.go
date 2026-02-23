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
	"io"
	"regexp"
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

var regExpID = regexp.MustCompile(`^[a-z0-9_-]+$`)

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
	GetShardsForClass(ctx context.Context, className string) ([]ShardLike, error)
	ListClasses(ctx context.Context) []string
	ShardOwnership(ctx context.Context, className string) (map[string][]string, error)
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
}

// NewScheduler creates a new export scheduler.
// When client and nodeResolver are nil, operates in single-node mode.
// The shutdownCtx is cancelled on graceful server shutdown.
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
	if client != nil && nodeResolver != nil && participant == nil {
		panic("export: multi-node scheduler requires a non-nil participant")
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
		enterrors.GoWrapper(func() {
			s.performSingleNodeExport(s.shutdownCtx, backendStore, id, status, classes, bucket, path)
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

	if s.isMultiNode() {
		plan, err := s.getExportPlan(ctx, backendStore, id, bucket, path)
		if err != nil {
			// Plan may not exist if the export failed before/during plan writing.
			// Fall back to reading the metadata file which may contain the FAILED state.
			meta, metaErr := s.getExportMetadata(ctx, backendStore, id, bucket, path)
			if metaErr != nil {
				return nil, fmt.Errorf("export %s not found: %w", id, err)
			}
			// Missing plan is always a failure — override the status and preserve
			// any error already recorded in the metadata.
			meta.Status = export.Failed
			if meta.Error == "" {
				meta.Error = fmt.Sprintf("export plan not found: %v", err)
			}
			return s.statusFromMetadata(backendStore, id, bucket, path, meta)
		}
		return s.assembleStatusFromPlan(ctx, backendStore, principal, id, bucket, path, plan)
	}

	meta, err := s.getExportMetadata(ctx, backendStore, id, bucket, path)
	if err != nil {
		return nil, fmt.Errorf("export %s not found: %w", id, err)
	}

	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(meta.Classes...)...); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
	}

	return s.statusFromMetadata(backendStore, id, bucket, path, meta)
}

// statusFromMetadata builds an ExportStatusResponse from an ExportMetadata record.
// Used for single-node exports and as a fallback when the multi-node plan is missing.
func (s *Scheduler) statusFromMetadata(backend modulecapabilities.BackupBackend, id, bucket, path string, meta *ExportMetadata) (*models.ExportStatusResponse, error) {
	es := &models.ExportStatusResponse{
		ID:        meta.ID,
		Backend:   meta.Backend,
		Path:      backend.HomeDir(id, bucket, path),
		Status:    string(meta.Status),
		StartedAt: strfmt.DateTime(meta.StartedAt),
		Classes:   meta.Classes,
		Error:     meta.Error,
	}

	if !meta.CompletedAt.IsZero() {
		es.TookInMs = meta.CompletedAt.Sub(meta.StartedAt).Milliseconds()
	}

	return es, nil
}

// assembleStatusFromPlan reads per-node status files from S3 and assembles overall status.
func (s *Scheduler) assembleStatusFromPlan(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	principal *models.Principal,
	id, bucket, path string,
	plan *ExportPlan,
) (*models.ExportStatusResponse, error) {
	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(plan.Classes...)...); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
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
	var lastCompleted time.Time

	for nodeName := range plan.NodeAssignments {
		nodeStatus, err := s.getNodeStatus(ctx, backend, id, bucket, path, nodeName)
		if err != nil {
			// No status file yet — treat as non-terminal and check liveness below
			nodeStatus = &NodeStatus{
				NodeName:      nodeName,
				Status:        export.Transferring,
				ShardProgress: make(map[string]map[string]*ShardProgress),
			}
		}

		if nodeStatus.CompletedAt.After(lastCompleted) {
			lastCompleted = nodeStatus.CompletedAt
		}

		effectiveStatus := nodeStatus.Status
		switch nodeStatus.Status {
		case export.Success:
		case export.Failed:
			anyFailed = true
			allSuccess = false
			if status.Error == "" {
				status.Error = fmt.Sprintf("node %s failed: %s", nodeName, nodeStatus.Error)
			}
		default:
			// Non-terminal (Transferring/Started): verify the node is still running
			allSuccess = false
			host, alive := s.nodeResolver.NodeHostname(nodeName)
			if !alive {
				anyFailed = true
				effectiveStatus = export.Failed
				if status.Error == "" {
					status.Error = fmt.Sprintf("node %s is no longer part of the cluster", nodeName)
				}
			} else if s.client != nil {
				running, runErr := s.client.IsRunning(ctx, host, id)
				if runErr != nil || !running {
					anyFailed = true
					effectiveStatus = export.Failed
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
					sp = &ShardProgress{Status: effectiveStatus}
				} else if sp.Status != export.Success && sp.Status != export.Failed {
					sp.Status = effectiveStatus
				}
				status.ShardStatus[className][shardName] = models.ShardProgress{
					Status:          string(sp.Status),
					ObjectsExported: sp.ObjectsExported,
					Error:           sp.Error,
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
		status.TookInMs = lastCompleted.Sub(plan.StartedAt).Milliseconds()
	}

	return status, nil
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
			s.logger.WithField("action", "export").WithField("class", className).Error(err)
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
	nodes := make([]exportNodeInfo, 0, len(nodeAssignments))
	for node, classShards := range nodeAssignments {
		ni := exportNodeInfo{
			req: &ExportRequest{
				ID:       exportID,
				Backend:  status.Backend,
				Classes:  classes,
				Shards:   classShards,
				Bucket:   bucket,
				Path:     path,
				NodeName: node,
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

	status.Status = string(export.Success)

	if err := s.writeMetadata(backend, exportID, bucket, path, status); err != nil {
		s.logger.WithField("action", "export").
			WithField("export_id", exportID).
			Error(err)
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
// original request context has been cancelled (e.g. client disconnect).
func (s *Scheduler) abortAll(exportID string, nodes []exportNodeInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, ni := range nodes {
		if ni.host == "" {
			s.participant.Abort(exportID)
		} else {
			s.client.Abort(ctx, ni.host, exportID)
		}
	}
}

// performSingleNodeExport runs the original single-node export path.
func (s *Scheduler) performSingleNodeExport(ctx context.Context, backend modulecapabilities.BackupBackend, exportID string, status *models.ExportStatusResponse, classes []string, bucket, path string) {
	for _, className := range classes {
		if err := s.exportClass(ctx, backend, exportID, bucket, path, className); err != nil {
			s.logger.WithField("action", "export").
				WithField("export_id", exportID).
				WithField("class", className).
				Error(err)

			status.Status = string(export.Failed)
			status.Error = fmt.Sprintf("failed to export class %s: %v", className, err)

			s.writeMetadata(backend, exportID, bucket, path, status)
			return
		}
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

// exportClass exports a single class to a Parquet file (single-node path)
func (s *Scheduler) exportClass(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string, className string) error {
	shards, err := s.selector.GetShardsForClass(ctx, className)
	if err != nil {
		return fmt.Errorf("get shards for class %s: %w", className, err)
	}

	s.logger.WithField("class", className).
		WithField("shard_count", len(shards)).
		Info("found shards for class")

	if len(shards) == 0 {
		s.logger.WithField("class", className).Warn("no shards found for class")
		return nil
	}

	pr, pw := io.Pipe()
	errChan := make(chan error, 1)

	enterrors.GoWrapper(func() {
		_, err := backend.Write(ctx, exportID, fmt.Sprintf("%s.parquet", className), bucket, path, pr)
		errChan <- err
	}, s.logger)

	writer, err := NewParquetWriter(pw)
	if err != nil {
		pw.CloseWithError(err)
		<-errChan
		return fmt.Errorf("create parquet writer: %w", err)
	}

	for _, shard := range shards {
		if err := exportShardData(ctx, shard, writer, className, s.logger); err != nil {
			_ = writer.Close()
			pw.CloseWithError(err)
			<-errChan
			return fmt.Errorf("export shard %s: %w", shard.Name(), err)
		}
	}

	if err := writer.Close(); err != nil {
		pw.CloseWithError(err)
		<-errChan
		return fmt.Errorf("close parquet writer: %w", err)
	}

	if err := pw.Close(); err != nil {
		return err
	}

	if err := <-errChan; err != nil {
		return fmt.Errorf("upload parquet file: %w", err)
	}

	s.logger.WithField("class", className).
		WithField("objects", writer.ObjectsWritten()).
		Info("class export completed")

	return nil
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
				Warn(err)
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
// original context was cancelled (e.g. during graceful shutdown).
func (s *Scheduler) writeMetadata(backend modulecapabilities.BackupBackend, exportID, bucket, path string, status *models.ExportStatusResponse) error {
	ctx := context.Background()

	metadata := &ExportMetadata{
		ID:          status.ID,
		Backend:     status.Backend,
		StartedAt:   time.Time(status.StartedAt),
		CompletedAt: time.Now().UTC(),
		Status:      export.Status(status.Status),
		Classes:     status.Classes,
		Error:       status.Error,
		Version:     config.ServerVersion,
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = backend.Write(ctx, exportID, exportMetadataFile, bucket, path, newBytesReadCloser(data))
	return err
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

// getNodeStatus reads a node's status file from S3
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

	// In multi-node mode also check the plan file, which is written before
	// metadata in the happy path.
	if s.isMultiNode() {
		_, err := s.getExportPlan(ctx, backend, exportID, bucket, path)
		if err == nil {
			return fmt.Errorf("export %q already exists at %q", exportID, home)
		}
		if !errors.As(err, &backup.ErrNotFound{}) {
			return fmt.Errorf("check existing export: %w", err)
		}
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
