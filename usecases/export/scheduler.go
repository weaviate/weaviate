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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
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
	GetShardsForClass(ctx context.Context, className string) ([]ShardLike, error)
	ListClasses(ctx context.Context) []string
	ShardOwnership(ctx context.Context, className string) (map[string][]string, error)
}

// ShardLike is an alias for db.ShardLike
type ShardLike = interface {
	Store() *lsmkv.Store
	Name() string
}

// Scheduler manages export operations
type Scheduler struct {
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
func NewScheduler(
	authorizer authorization.Authorizer,
	selector Selector,
	backends BackendProvider,
	logger logrus.FieldLogger,
	client ExportClient,
	nodeResolver NodeResolver,
	localNode string,
	participant *Participant,
) *Scheduler {
	return &Scheduler{
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

// Export starts a new export operation
func (s *Scheduler) Export(ctx context.Context, principal *models.Principal, id, backend string, include, exclude []string, bucket, path string) (*ExportStatus, error) {
	if id == "" {
		return nil, fmt.Errorf("export ID is required")
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

	status := &ExportStatus{
		ID:        id,
		Backend:   backend,
		Path:      backendStore.HomeDir(id, bucket, path),
		Status:    export.Started,
		StartedAt: time.Now().UTC(),
		Classes:   classes,
		Progress:  make(map[string]*ClassProgress),
	}

	for _, class := range classes {
		status.Progress[class] = &ClassProgress{
			Status: export.Started,
		}
	}

	enterrors.GoWrapper(func() {
		s.performExport(context.Background(), backendStore, id, status, classes, bucket, path)
	}, s.logger)

	return status, nil
}

// Status retrieves the status of an export.
// In multi-node mode, assembles status from S3 export plan + per-node status files.
// In single-node mode, reads the metadata file directly.
func (s *Scheduler) Status(ctx context.Context, principal *models.Principal, backend, id, bucket, path string) (*ExportStatus, error) {
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
			return nil, fmt.Errorf("export %s not found: %w", id, err)
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

	return &ExportStatus{
		ID:        meta.ID,
		Backend:   meta.Backend,
		Path:      backendStore.HomeDir(id, bucket, path),
		Status:    meta.Status,
		StartedAt: meta.StartedAt,
		Classes:   meta.Classes,
		Progress:  meta.Progress,
		Error:     meta.Error,
	}, nil
}

// assembleStatusFromPlan reads per-node status files from S3 and assembles overall status.
func (s *Scheduler) assembleStatusFromPlan(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	principal *models.Principal,
	id, bucket, path string,
	plan *ExportPlan,
) (*ExportStatus, error) {
	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(plan.Classes...)...); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
	}

	status := &ExportStatus{
		ID:        plan.ID,
		Backend:   plan.Backend,
		Path:      backend.HomeDir(id, bucket, path),
		Status:    export.Transferring,
		StartedAt: plan.StartedAt,
		Classes:   plan.Classes,
		Progress:  make(map[string]*ClassProgress),
	}

	// Initialize progress for all classes
	for _, class := range plan.Classes {
		status.Progress[class] = &ClassProgress{
			Status: export.Transferring,
		}
	}

	allSuccess := true
	anyFailed := false

	for nodeName := range plan.NodeAssignments {
		nodeStatus, err := s.getNodeStatus(ctx, backend, id, bucket, path, nodeName)
		if err != nil {
			// Missing status file means still in progress
			allSuccess = false
			continue
		}

		if nodeStatus.Status == export.Failed {
			anyFailed = true
			if status.Error == "" {
				status.Error = fmt.Sprintf("node %s failed: %s", nodeName, nodeStatus.Error)
			}
		}

		if nodeStatus.Status != export.Success {
			allSuccess = false
		}

		// Merge class progress from this node
		for className, cp := range nodeStatus.ClassProgress {
			if existing, ok := status.Progress[className]; ok {
				existing.ObjectsExported += cp.ObjectsExported
				if cp.Status == export.Failed {
					existing.Status = export.Failed
					existing.Error = cp.Error
				} else if cp.Status == export.Success && existing.Status != export.Failed {
					existing.Status = export.Success
				}
			}
		}
	}

	if anyFailed {
		status.Status = export.Failed
	} else if allSuccess {
		status.Status = export.Success
	}

	return status, nil
}

// performExport executes the export.
// In multi-node mode: writes plan to S3, fires requests to all nodes.
// In single-node mode: exports all shards locally.
func (s *Scheduler) performExport(ctx context.Context, backend modulecapabilities.BackupBackend, exportID string, status *ExportStatus, classes []string, bucket, path string) {
	s.logger.WithField("action", "export").
		WithField("export_id", exportID).
		WithField("classes", classes).
		WithField("class_count", len(classes)).
		WithField("multi_node", s.isMultiNode()).
		Info("starting export")

	status.Status = export.Transferring

	if s.isMultiNode() {
		s.performMultiNodeExport(ctx, backend, exportID, status, classes, bucket, path)
	} else {
		s.performSingleNodeExport(ctx, backend, exportID, status, classes, bucket, path)
	}
}

// performMultiNodeExport orchestrates export across multiple nodes.
func (s *Scheduler) performMultiNodeExport(ctx context.Context, backend modulecapabilities.BackupBackend, exportID string, status *ExportStatus, classes []string, bucket, path string) {
	// Build node assignments: node → className → []shardName
	nodeAssignments := make(map[string]map[string][]string)

	for _, className := range classes {
		ownership, err := s.selector.ShardOwnership(ctx, className)
		if err != nil {
			s.logger.WithError(err).WithField("class", className).Error("failed to get shard ownership")
			status.Status = export.Failed
			status.Error = fmt.Sprintf("failed to get shard ownership for class %s: %v", className, err)
			s.writeMetadata(ctx, backend, exportID, bucket, path, status)
			return
		}

		for node, shards := range ownership {
			if nodeAssignments[node] == nil {
				nodeAssignments[node] = make(map[string][]string)
			}
			nodeAssignments[node][className] = shards
		}
	}

	// Write export plan to S3
	plan := &ExportPlan{
		ID:              exportID,
		Backend:         status.Backend,
		Classes:         classes,
		NodeAssignments: nodeAssignments,
		StartedAt:       status.StartedAt,
	}

	if err := s.writeExportPlan(ctx, backend, exportID, bucket, path, plan); err != nil {
		s.logger.WithError(err).Error("failed to write export plan")
		status.Status = export.Failed
		status.Error = fmt.Sprintf("failed to write export plan: %v", err)
		s.writeMetadata(ctx, backend, exportID, bucket, path, status)
		return
	}

	// Fire-and-forget to all nodes
	for node, classShards := range nodeAssignments {
		// Build per-node shard map: className → []shardName
		req := &ExportRequest{
			ID:       exportID,
			Backend:  status.Backend,
			Classes:  classes,
			Shards:   classShards,
			Bucket:   bucket,
			Path:     path,
			NodeName: node,
		}

		if node == s.localNode {
			// Local node: call participant directly
			if err := s.participant.OnExecute(ctx, req); err != nil {
				s.logger.WithError(err).WithField("node", node).Error("failed to start local export")
			}
		} else {
			// Remote node: fire-and-forget HTTP
			host, ok := s.nodeResolver.NodeHostname(node)
			if !ok {
				s.logger.WithField("node", node).Error("failed to resolve node hostname")
				continue
			}
			if err := s.client.Execute(ctx, host, req); err != nil {
				s.logger.WithError(err).WithField("node", node).Error("failed to send export request to node")
			}
		}
	}

	s.logger.WithField("action", "export").
		WithField("export_id", exportID).
		WithField("nodes", len(nodeAssignments)).
		Info("multi-node export requests dispatched")
}

// performSingleNodeExport runs the original single-node export path.
func (s *Scheduler) performSingleNodeExport(ctx context.Context, backend modulecapabilities.BackupBackend, exportID string, status *ExportStatus, classes []string, bucket, path string) {
	for _, className := range classes {
		if err := s.exportClass(ctx, backend, exportID, bucket, path, status, className); err != nil {
			s.logger.WithField("action", "export_class").
				WithField("export_id", exportID).
				WithField("class", className).
				WithError(err).Error("failed to export class")

			status.Progress[className].Status = export.Failed
			status.Progress[className].Error = err.Error()
			status.Status = export.Failed
			status.Error = fmt.Sprintf("failed to export class %s: %v", className, err)

			s.writeMetadata(ctx, backend, exportID, bucket, path, status)
			return
		}
	}

	if err := s.writeMetadata(ctx, backend, exportID, bucket, path, status); err != nil {
		s.logger.WithField("action", "write_metadata").
			WithField("export_id", exportID).
			WithError(err).Error("failed to write metadata")

		status.Status = export.Failed
		status.Error = fmt.Sprintf("failed to write metadata: %v", err)
		return
	}

	status.Status = export.Success
	s.logger.WithField("action", "export").
		WithField("export_id", exportID).
		Info("export completed successfully")
}

// exportClass exports a single class to a Parquet file (single-node path)
func (s *Scheduler) exportClass(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string, status *ExportStatus, className string) error {
	progress := status.Progress[className]
	progress.Status = export.Transferring

	shards, err := s.selector.GetShardsForClass(ctx, className)
	if err != nil {
		return fmt.Errorf("get shards for class %s: %w", className, err)
	}

	s.logger.WithField("class", className).
		WithField("shard_count", len(shards)).
		Info("found shards for class")

	if len(shards) == 0 {
		s.logger.WithField("class", className).Warn("no shards found for class")
		progress.Status = export.Success
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

	progress.ObjectsExported = writer.ObjectsWritten()
	progress.Status = export.Success

	s.logger.WithField("class", className).
		WithField("objects", progress.ObjectsExported).
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
			logger.WithField("shard", shard.Name()).
				WithField("class", className).
				WithError(err).Warn("failed to deserialize object, skipping")
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

// writeMetadata writes the export metadata file (single-node path)
func (s *Scheduler) writeMetadata(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string, status *ExportStatus) error {
	metadata := &ExportMetadata{
		ID:          status.ID,
		Backend:     status.Backend,
		StartedAt:   status.StartedAt,
		CompletedAt: time.Now().UTC(),
		Status:      status.Status,
		Classes:     status.Classes,
		Progress:    status.Progress,
		Error:       status.Error,
		Version:     config.ServerVersion,
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	reader := io.NopCloser(bytes.NewReader(data))
	_, err = backend.Write(ctx, exportID, exportMetadataFile, bucket, path, reader)
	return err
}

// writeExportPlan writes the export plan to S3 (multi-node path)
func (s *Scheduler) writeExportPlan(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string, plan *ExportPlan) error {
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal export plan: %w", err)
	}

	reader := io.NopCloser(bytes.NewReader(data))
	_, err = backend.Write(ctx, exportID, exportPlanFile, bucket, path, reader)
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

// checkIfExportExists checks if an export already exists in the backend
func (s *Scheduler) checkIfExportExists(ctx context.Context, backend modulecapabilities.BackupBackend, exportID, bucket, path string) error {
	if s.isMultiNode() {
		if _, err := s.getExportPlan(ctx, backend, exportID, bucket, path); err == nil {
			return fmt.Errorf("export %q already exists at %q", exportID, backend.HomeDir(exportID, bucket, path))
		}
		return nil
	}

	meta, err := s.getExportMetadata(ctx, backend, exportID, bucket, path)
	if err != nil {
		return nil
	}

	if meta.Status == export.Success || meta.Status == export.Transferring || meta.Status == export.Started {
		return fmt.Errorf("export %q already exists at %q", exportID, backend.HomeDir(exportID, bucket, path))
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
		excludeMap := make(map[string]bool)
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
