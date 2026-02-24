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
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

const reservationTimeout = 30 * time.Second

// Participant handles export requests on a single node.
// It exports its assigned shards directly to S3 and writes status files.
//
// The two-phase commit protocol works as follows:
//  1. Prepare: reserves the export slot (atomic CAS). A background timer
//     auto-aborts after reservationTimeout if Commit is not called.
//  2. Commit: cancels the timer and starts the actual export work.
//  3. Abort: releases the reservation immediately.
type Participant struct {
	shutdownCtx context.Context
	selector    Selector
	backends    BackendProvider
	logger      logrus.FieldLogger

	// mu guards preparedReq, abortTimer, and cancelExport, which are set
	// during Prepare/Commit and consumed during Commit/Abort.
	mu           sync.Mutex
	preparedReq  *ExportRequest
	abortTimer   *time.Timer
	cancelExport context.CancelFunc
	// this stays set from the moment Prepare() reserves the slot until Commit() or Abort() releases it. Used for IsRunning() checks.
	activeExport string
}

// NewParticipant creates a new export participant.
// The shutdownCtx is cancelled on graceful server shutdown, allowing in-flight
// exports to detect the shutdown and write a failed status before exiting.
func NewParticipant(
	shutdownCtx context.Context,
	selector Selector,
	backends BackendProvider,
	logger logrus.FieldLogger,
) *Participant {
	return &Participant{
		shutdownCtx: shutdownCtx,
		selector:    selector,
		backends:    backends,
		logger:      logger,
	}
}

// Prepare reserves the export slot for the given request. If no Commit
// arrives within reservationTimeout the reservation is automatically released.
func (p *Participant) Prepare(_ context.Context, req *ExportRequest) error {
	f := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		if req == nil {
			return fmt.Errorf("request cannot be nil")
		}

		if req.ID == "" {
			return fmt.Errorf("export ID cannot be empty")
		}

		if p.activeExport != "" {
			return fmt.Errorf("active export %q already in progress", p.activeExport)
		}

		p.activeExport = req.ID

		p.preparedReq = req
		p.abortTimer = time.AfterFunc(reservationTimeout, func() {
			p.mu.Lock()
			defer p.mu.Unlock()

			if p.preparedReq == nil {
				return // Already committed or aborted — no-op.
			}
			p.logger.WithField("export_id", req.ID).
				Warn("export reservation timed out, auto-aborting")
			p.clearAndRelease()
		})

		return nil
	}
	if err := f(); err != nil {
		return err
	}

	p.logger.WithField("action", "export_participant").
		WithField("export_id", req.ID).
		WithField("node", req.NodeName).
		Info("participant prepared for export")

	return nil
}

// Commit starts the actual export. Must be called after a successful Prepare.
func (p *Participant) Commit(ctx context.Context, exportID string) error {
	if exportID == "" {
		return fmt.Errorf("export ID cannot be empty")
	}

	var req *ExportRequest
	var backendStore modulecapabilities.BackupBackend
	var exportCtx context.Context
	f := func() (errRet error) {
		p.mu.Lock()
		defer func() {
			if errRet != nil {
				p.clearAndRelease()
			}
			p.mu.Unlock()
		}()

		timer := p.abortTimer
		if timer == nil {
			errRet = fmt.Errorf("timer is nil. No export prepared")
			return errRet
		}
		timer.Stop()

		if p.activeExport != exportID {
			errRet = fmt.Errorf("active export ID mismatch: expected %q, got %q", p.activeExport, exportID)
			return errRet
		}

		req = p.preparedReq
		if req == nil {
			errRet = fmt.Errorf("no export prepared")
			return errRet
		}
		if req.ID != exportID {
			errRet = fmt.Errorf("export ID mismatch: expected %q, got %q", req.ID, exportID)
			return errRet
		}
		backendStore2, err := p.backends.BackupBackend(req.Backend)
		if err != nil {
			errRet = fmt.Errorf("backend %s not available: %w", req.Backend, err)
			return errRet
		}
		backendStore = backendStore2

		if err := backendStore.Initialize(ctx, req.ID, req.Bucket, req.Path); err != nil {
			errRet = fmt.Errorf("initialize backend: %w", err)
			return errRet
		}

		p.preparedReq = nil
		p.abortTimer = nil

		exportCtx2, cancel := context.WithCancel(p.shutdownCtx)
		p.cancelExport = cancel
		exportCtx = exportCtx2

		return nil
	}
	if err := f(); err != nil {
		return err
	}

	p.logger.WithField("action", "export_participant").
		WithField("export_id", req.ID).
		WithField("node", req.NodeName).
		WithField("classes", req.Classes).
		Info("participant starting export")

	enterrors.GoWrapper(func() {
		p.executeExport(exportCtx, backendStore, req)
	}, p.logger)

	return nil
}

// Abort cancels a prepared or running export.
// If the export is still in the prepared state, the reservation is released.
// If the export has already been committed, the running export is cancelled.
func (p *Participant) Abort(exportID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.activeExport != exportID {
		return
	}

	if p.cancelExport != nil {
		// Export is running — cancel it. The goroutine will detect context
		// cancellation, write a failed status, and call clearAndRelease()
		// via its defer.
		p.cancelExport()
		p.cancelExport = nil
		p.logger.WithField("action", "export_participant").
			WithField("export_id", exportID).
			Info("participant aborted running export")
	} else {
		// Still in prepared state — full cleanup.
		p.clearAndRelease()
		p.logger.WithField("action", "export_participant").
			WithField("export_id", exportID).
			Info("participant aborted export reservation")
	}
}

// clearAndRelease is called by the reservation timer. It only releases the
// slot if preparedReq is still set — if Commit or Abort already consumed it,
// the timer is a no-op.
func (p *Participant) clearAndRelease() {
	p.preparedReq = nil
	if p.abortTimer != nil {
		p.abortTimer.Stop()
	}
	p.abortTimer = nil
	if p.cancelExport != nil {
		p.cancelExport()
	}
	p.cancelExport = nil
	p.activeExport = ""
}

// IsRunning reports whether the given export is currently running on this node.
func (p *Participant) IsRunning(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeExport == "" {
		return false
	}
	return p.activeExport == id
}

func (p *Participant) executeExport(ctx context.Context, backend modulecapabilities.BackupBackend, req *ExportRequest) {
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.clearAndRelease()
	}()
	nodeStatus := &NodeStatus{
		NodeName:      req.NodeName,
		Status:        export.Transferring,
		ShardProgress: make(map[string]map[string]*ShardProgress),
	}

	for _, className := range req.Classes {
		shardNames, ok := req.Shards[className]
		if !ok || len(shardNames) == 0 {
			continue
		}
		nodeStatus.ShardProgress[className] = make(map[string]*ShardProgress)
		for _, shardName := range shardNames {
			nodeStatus.ShardProgress[className][shardName] = &ShardProgress{
				Status: export.Transferring,
			}
		}
	}

	for _, className := range req.Classes {
		shardNames, ok := req.Shards[className]
		if !ok || len(shardNames) == 0 {
			continue
		}

		if err := p.exportClassShards(ctx, backend, req, className, shardNames, nodeStatus); err != nil {
			p.logger.WithField("action", "export").
				WithField("export_id", req.ID).
				WithField("node", req.NodeName).
				WithField("class", className).
				Error(err)

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
	// TODO: This needs to be adapted to MT
	allShards, err := p.selector.GetShardsForClass(ctx, className)
	if err != nil {
		return fmt.Errorf("get shards for class %s: %w", className, err)
	}

	// Build lookup map
	shardMap := make(map[string]ShardLike, len(allShards))
	for _, s := range allShards {
		shardMap[s.Name()] = s
	}

	for _, shardName := range shardNames {
		shard, ok := shardMap[shardName]
		if !ok {
			return fmt.Errorf("shard %s not found on this node for class %s", shardName, className)
		}

		objects, err := p.exportShardToFile(ctx, backend, req, className, shardName, shard)
		if err != nil {
			nodeStatus.ShardProgress[className][shardName].Status = export.Failed
			nodeStatus.ShardProgress[className][shardName].Error = err.Error()
			return fmt.Errorf("export shard %s: %w", shardName, err)
		}

		// Update incremental progress
		nodeStatus.ShardProgress[className][shardName].Status = export.Success
		nodeStatus.ShardProgress[className][shardName].ObjectsExported = objects
		p.writeNodeStatus(ctx, backend, req, nodeStatus)
	}

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
// It uses a fresh context with a timeout so the write succeeds even if the
// original context was cancelled (e.g. during graceful shutdown).
func (p *Participant) writeNodeStatus(_ context.Context, backend modulecapabilities.BackupBackend, req *ExportRequest, status *NodeStatus) {
	ctx := context.Background()

	key := fmt.Sprintf("node_%s_status.json", status.NodeName)
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		p.logger.WithField("action", "export").WithField("node", status.NodeName).Error(err)
		return
	}

	if _, err := backend.Write(ctx, req.ID, key, req.Bucket, req.Path, newBytesReadCloser(data)); err != nil {
		p.logger.WithField("action", "export").WithField("node", status.NodeName).Error(err)
	}
}
