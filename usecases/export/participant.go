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
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

// Participant handles export requests on a single node.
// It exports its assigned shards directly to S3 and writes status files.
type Participant struct {
	selector Selector
	backends BackendProvider
	logger   logrus.FieldLogger
}

// NewParticipant creates a new export participant
func NewParticipant(
	selector Selector,
	backends BackendProvider,
	logger logrus.FieldLogger,
) *Participant {
	return &Participant{
		selector: selector,
		backends: backends,
		logger:   logger,
	}
}

// OnExecute handles an export request from the coordinator.
// It fires off an async goroutine to export assigned shards and returns immediately.
func (p *Participant) OnExecute(ctx context.Context, req *ExportRequest) error {
	backendStore, err := p.backends.BackupBackend(req.Backend)
	if err != nil {
		return fmt.Errorf("backend %s not available: %w", req.Backend, err)
	}

	if err := backendStore.Initialize(ctx, req.ID, req.Bucket, req.Path); err != nil {
		return fmt.Errorf("initialize backend: %w", err)
	}

	p.logger.WithField("action", "export_participant").
		WithField("export_id", req.ID).
		WithField("node", req.NodeName).
		WithField("classes", req.Classes).
		Info("participant starting export")

	enterrors.GoWrapper(func() {
		p.executeExport(context.Background(), backendStore, req)
	}, p.logger)

	return nil
}

// executeExport performs the actual export work for this node's assigned shards.
func (p *Participant) executeExport(ctx context.Context, backend modulecapabilities.BackupBackend, req *ExportRequest) {
	nodeStatus := &NodeStatus{
		NodeName:      req.NodeName,
		Status:        export.Transferring,
		ClassProgress: make(map[string]*ClassProgress),
	}

	for _, className := range req.Classes {
		shardNames, ok := req.Shards[className]
		if !ok || len(shardNames) == 0 {
			continue
		}
		nodeStatus.ClassProgress[className] = &ClassProgress{
			Status: export.Transferring,
		}
	}

	for _, className := range req.Classes {
		shardNames, ok := req.Shards[className]
		if !ok || len(shardNames) == 0 {
			continue
		}

		if err := p.exportClassShards(ctx, backend, req, className, shardNames, nodeStatus); err != nil {
			p.logger.WithField("action", "export_participant").
				WithField("export_id", req.ID).
				WithField("node", req.NodeName).
				WithField("class", className).
				WithError(err).Error("failed to export class shards")

			nodeStatus.ClassProgress[className].Status = export.Failed
			nodeStatus.ClassProgress[className].Error = err.Error()
			nodeStatus.Status = export.Failed
			nodeStatus.Error = fmt.Sprintf("failed to export class %s: %v", className, err)
			p.writeNodeStatus(ctx, backend, req, nodeStatus)
			return
		}
	}

	nodeStatus.Status = export.Success
	nodeStatus.CompletedAt = time.Now().UTC()
	p.writeNodeStatus(ctx, backend, req, nodeStatus)

	p.logger.WithField("action", "export_participant").
		WithField("export_id", req.ID).
		WithField("node", req.NodeName).
		Info("participant export completed successfully")
}

// exportClassShards exports specific shards of a class to individual Parquet files.
func (p *Participant) exportClassShards(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	className string,
	shardNames []string,
	nodeStatus *NodeStatus,
) error {
	// Get all shards for the class (we need the ShardLike handles)
	allShards, err := p.selector.GetShardsForClass(ctx, className)
	if err != nil {
		return fmt.Errorf("get shards for class %s: %w", className, err)
	}

	// Build lookup map
	shardMap := make(map[string]ShardLike, len(allShards))
	for _, s := range allShards {
		shardMap[s.Name()] = s
	}

	var totalObjects int64
	for _, shardName := range shardNames {
		shard, ok := shardMap[shardName]
		if !ok {
			return fmt.Errorf("shard %s not found on this node for class %s", shardName, className)
		}

		objects, err := p.exportShardToFile(ctx, backend, req, className, shardName, shard)
		if err != nil {
			return fmt.Errorf("export shard %s: %w", shardName, err)
		}
		totalObjects += objects

		// Update incremental progress
		nodeStatus.ClassProgress[className].ObjectsExported = totalObjects
		p.writeNodeStatus(ctx, backend, req, nodeStatus)
	}

	nodeStatus.ClassProgress[className].Status = export.Success
	nodeStatus.ClassProgress[className].ObjectsExported = totalObjects
	return nil
}

// exportShardToFile exports a single shard to a Parquet file: {ClassName}_{ShardName}.parquet
func (p *Participant) exportShardToFile(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	className, shardName string,
	shard ShardLike,
) (int64, error) {
	pr, pw := io.Pipe()
	errChan := make(chan error, 1)

	fileName := fmt.Sprintf("%s_%s.parquet", className, shardName)

	enterrors.GoWrapper(func() {
		_, err := backend.Write(ctx, req.ID, fileName, req.Bucket, req.Path, pr)
		errChan <- err
	}, p.logger)

	writer, err := NewParquetWriter(pw)
	if err != nil {
		pw.CloseWithError(err)
		<-errChan
		return 0, fmt.Errorf("create parquet writer: %w", err)
	}

	if err := exportShardData(ctx, shard, writer, className, p.logger); err != nil {
		_ = writer.Close()
		pw.CloseWithError(err)
		<-errChan
		return 0, fmt.Errorf("export shard %s: %w", shardName, err)
	}

	if err := writer.Close(); err != nil {
		pw.CloseWithError(err)
		<-errChan
		return 0, fmt.Errorf("close parquet writer: %w", err)
	}

	if err := pw.Close(); err != nil {
		return 0, err
	}

	if err := <-errChan; err != nil {
		return 0, fmt.Errorf("upload parquet file: %w", err)
	}

	p.logger.WithField("class", className).
		WithField("shard", shardName).
		WithField("objects", writer.ObjectsWritten()).
		WithField("file", fileName).
		Info("shard export completed")

	return writer.ObjectsWritten(), nil
}

// writeNodeStatus writes the node status file to S3.
func (p *Participant) writeNodeStatus(ctx context.Context, backend modulecapabilities.BackupBackend, req *ExportRequest, status *NodeStatus) {
	key := fmt.Sprintf("node_%s_status.json", status.NodeName)
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		p.logger.WithError(err).Error("failed to marshal node status")
		return
	}

	reader := io.NopCloser(bytes.NewReader(data))
	if _, err := backend.Write(ctx, req.ID, key, req.Bucket, req.Path, reader); err != nil {
		p.logger.WithError(err).Error("failed to write node status to S3")
	}
}
