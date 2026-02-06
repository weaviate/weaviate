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
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	exportMetadataFile = "export_metadata.json"
)

// BackendProvider provides access to export backends
type BackendProvider interface {
	// BackupBackend returns a backup backend by name
	BackupBackend(backend string) (BackendStore, error)
}

// BackendStore provides storage backend operations
type BackendStore interface {
	// HomeDir returns the base path for exports
	HomeDir(exportID, bucket, path string) string
	// Initialize initializes the backend storage
	Initialize(ctx context.Context, exportID, bucket, path string) error
	// PutObject uploads data to the backend
	PutObject(ctx context.Context, exportID, key, bucket, path string, data io.ReadCloser) (int64, error)
	// GetObject retrieves data from the backend
	GetObject(ctx context.Context, exportID, key, bucket, path string) ([]byte, error)
}

// Selector selects shards and classes for export
type Selector interface {
	// GetShardsForClass gets all shards for a specific class
	GetShardsForClass(ctx context.Context, className string) ([]ShardLike, error)
	// ListClasses returns all available classes
	ListClasses(ctx context.Context) []string
}

// ShardLike is an alias for db.ShardLike
type ShardLike = interface {
	Store() *lsmkv.Store
	Name() string
}

// Scheduler manages export operations
type Scheduler struct {
	logger     logrus.FieldLogger
	authorizer authorization.Authorizer
	selector   Selector
	backends   BackendProvider
}

// NewScheduler creates a new export scheduler
func NewScheduler(
	authorizer authorization.Authorizer,
	selector Selector,
	backends BackendProvider,
	logger logrus.FieldLogger,
) *Scheduler {
	return &Scheduler{
		logger:     logger,
		authorizer: authorizer,
		selector:   selector,
		backends:   backends,
	}
}

// Export starts a new export operation
func (s *Scheduler) Export(ctx context.Context, principal *models.Principal, id, backend string, include, exclude []string, bucket, path string) (*ExportStatus, error) {
	// Validate request
	if id == "" {
		return nil, fmt.Errorf("export ID is required")
	}
	if backend == "" {
		return nil, fmt.Errorf("backend is required")
	}

	// Get backend
	backendStore, err := s.backends.BackupBackend(backend)
	if err != nil {
		return nil, fmt.Errorf("backend %s not available: %w", backend, err)
	}

	// Initialize backend
	if err := backendStore.Initialize(ctx, id, bucket, path); err != nil {
		return nil, fmt.Errorf("initialize backend: %w", err)
	}

	// Check if export already exists
	if err := s.checkIfExportExists(ctx, backendStore, id, bucket, path); err != nil {
		return nil, err
	}

	// Resolve classes to export
	classes, err := s.resolveClasses(ctx, include, exclude)
	if err != nil {
		return nil, fmt.Errorf("resolve classes: %w", err)
	}

	if len(classes) == 0 {
		return nil, fmt.Errorf("no classes selected for export")
	}

	// Authorize - require READ permission on classes
	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(classes...)...); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
	}

	// Initialize status
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

	// Start export asynchronously
	enterrors.GoWrapper(func() {
		s.performExport(context.Background(), backendStore, id, status, classes, bucket, path)
	}, s.logger)

	return status, nil
}

// Status retrieves the status of an export by reading from S3
func (s *Scheduler) Status(ctx context.Context, principal *models.Principal, backend, id, bucket, path string) (*ExportStatus, error) {
	// Get backend
	backendStore, err := s.backends.BackupBackend(backend)
	if err != nil {
		return nil, fmt.Errorf("backend %s not available: %w", backend, err)
	}

	// Initialize backend
	if err := backendStore.Initialize(ctx, id, bucket, path); err != nil {
		return nil, fmt.Errorf("initialize backend: %w", err)
	}

	// Read metadata from S3
	meta, err := s.getExportMetadata(ctx, backendStore, id, bucket, path)
	if err != nil {
		return nil, fmt.Errorf("export %s not found: %w", id, err)
	}

	// Authorize - require READ permission on exported classes
	if err := s.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Backups(meta.Classes...)...); err != nil {
		return nil, fmt.Errorf("authorization failed: %w", err)
	}

	// Convert metadata to status
	status := &ExportStatus{
		ID:        meta.ID,
		Backend:   meta.Backend,
		Path:      backendStore.HomeDir(id, bucket, path),
		Status:    meta.Status,
		StartedAt: meta.StartedAt,
		Classes:   meta.Classes,
		Progress:  meta.Progress,
		Error:     meta.Error,
	}

	return status, nil
}

// checkIfExportExists checks if an export already exists in the backend
func (s *Scheduler) checkIfExportExists(ctx context.Context, backend BackendStore, exportID, bucket, path string) error {
	meta, err := s.getExportMetadata(ctx, backend, exportID, bucket, path)
	if err != nil {
		// Export doesn't exist, which is what we want
		return nil
	}

	// Export exists - check if it's in a final state
	if meta.Status == export.Success || meta.Status == export.Transferring || meta.Status == export.Started {
		return fmt.Errorf("export %q already exists at %q", exportID, backend.HomeDir(exportID, bucket, path))
	}

	// If export failed, allow retry
	return nil
}

// getExportMetadata retrieves export metadata from the backend
func (s *Scheduler) getExportMetadata(ctx context.Context, backend BackendStore, exportID, bucket, path string) (*ExportMetadata, error) {
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

// performExport executes the actual export work
func (s *Scheduler) performExport(ctx context.Context, backend BackendStore, exportID string, status *ExportStatus, classes []string, bucket, path string) {
	s.logger.WithFields(logrus.Fields{
		"action":      "export",
		"export_id":   exportID,
		"classes":     classes,
		"class_count": len(classes),
	}).Info("starting export")

	status.Status = export.Transferring

	// Export each class
	for _, className := range classes {
		if err := s.exportClass(ctx, backend, exportID, bucket, path, status, className); err != nil {
			s.logger.WithFields(logrus.Fields{
				"action":    "export_class",
				"export_id": exportID,
				"class":     className,
			}).WithError(err).Error("failed to export class")

			status.Progress[className].Status = export.Failed
			status.Progress[className].Error = err.Error()
			status.Status = export.Failed
			status.Error = fmt.Sprintf("failed to export class %s: %v", className, err)

			// Write metadata with error
			s.writeMetadata(ctx, backend, exportID, bucket, path, status)
			return
		}
	}

	// Write metadata file
	if err := s.writeMetadata(ctx, backend, exportID, bucket, path, status); err != nil {
		s.logger.WithFields(logrus.Fields{
			"action":    "write_metadata",
			"export_id": exportID,
		}).WithError(err).Error("failed to write metadata")

		status.Status = export.Failed
		status.Error = fmt.Sprintf("failed to write metadata: %v", err)
		return
	}

	status.Status = export.Success
	s.logger.WithFields(logrus.Fields{
		"action":    "export",
		"export_id": exportID,
	}).Info("export completed successfully")
}

// exportClass exports a single class to a Parquet file
func (s *Scheduler) exportClass(ctx context.Context, backend BackendStore, exportID, bucket, path string, status *ExportStatus, className string) error {
	progress := status.Progress[className]
	progress.Status = export.Transferring

	// Get shards for this class
	shards, err := s.selector.GetShardsForClass(ctx, className)
	if err != nil {
		return fmt.Errorf("get shards for class %s: %w", className, err)
	}

	s.logger.WithFields(logrus.Fields{
		"class":       className,
		"shard_count": len(shards),
	}).Info("found shards for class")

	if len(shards) == 0 {
		s.logger.WithFields(logrus.Fields{
			"class": className,
		}).Warn("no shards found for class")
		progress.Status = export.Success
		return nil
	}

	// Create a pipe for streaming data to S3
	pr, pw := io.Pipe()
	errChan := make(chan error, 1)

	// Upload to S3 in background
	enterrors.GoWrapper(func() {
		_, err := backend.PutObject(ctx, exportID, fmt.Sprintf("%s.parquet", className), bucket, path, pr)
		errChan <- err
	}, s.logger)

	// Create Parquet writer
	writer, err := NewParquetWriter(pw)
	if err != nil {
		pw.Close()
		return fmt.Errorf("create parquet writer: %w", err)
	}

	// Write all objects from all shards
	for _, shard := range shards {
		if err := s.exportShard(ctx, shard, writer, className); err != nil {
			writer.Close()
			pw.Close()
			return fmt.Errorf("export shard %s: %w", shard.Name(), err)
		}
	}

	// Close writer (flushes to pipe)
	if err := writer.Close(); err != nil {
		pw.Close()
		return fmt.Errorf("close parquet writer: %w", err)
	}

	// Close pipe writer (signals upload completion)
	pw.Close()

	// Wait for upload to complete
	if err := <-errChan; err != nil {
		return fmt.Errorf("upload parquet file: %w", err)
	}

	progress.ObjectsExported = writer.ObjectsWritten()
	progress.Status = export.Success

	s.logger.WithFields(logrus.Fields{
		"class":   className,
		"objects": progress.ObjectsExported,
	}).Info("class export completed")

	return nil
}

// exportShard exports all objects from a single shard
func (s *Scheduler) exportShard(ctx context.Context, shard ShardLike, writer *ParquetWriter, className string) error {
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

	// Start from beginning
	key, val := cursor.First()

	objectCount := 0
	s.logger.WithFields(logrus.Fields{
		"shard": shard.Name(),
		"class": className,
	}).Debug("starting to iterate objects")

	for key != nil {
		objectCount++
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Deserialize object
		obj, err := storobj.FromBinary(val)
		if err != nil {
			s.logger.WithFields(logrus.Fields{
				"shard": shard.Name(),
				"class": className,
			}).WithError(err).Warn("failed to deserialize object, skipping")
			key, val = cursor.Next()
			continue
		}

		// Write to Parquet
		if err := writer.WriteObject(obj); err != nil {
			return fmt.Errorf("write object to parquet: %w", err)
		}

		// Move to next
		key, val = cursor.Next()
	}

	s.logger.WithFields(logrus.Fields{
		"shard":        shard.Name(),
		"class":        className,
		"object_count": objectCount,
	}).Info("completed shard iteration")

	return nil
}

// writeMetadata writes the export metadata file
func (s *Scheduler) writeMetadata(ctx context.Context, backend BackendStore, exportID, bucket, path string, status *ExportStatus) error {
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
	_, err = backend.PutObject(ctx, exportID, exportMetadataFile, bucket, path, reader)
	return err
}

// resolveClasses determines which classes to export
func (s *Scheduler) resolveClasses(ctx context.Context, include, exclude []string) ([]string, error) {
	allClasses := s.selector.ListClasses(ctx)

	if len(include) > 0 && len(exclude) > 0 {
		return nil, fmt.Errorf("cannot specify both 'include' and 'exclude'")
	}

	if len(include) > 0 {
		// Validate all included classes exist
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
		// Remove excluded classes
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

	// No filter - export all classes
	return allClasses, nil
}
