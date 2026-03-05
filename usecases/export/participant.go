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
	"runtime"
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

	if err := p.doExport(ctx, backend, req); err != nil {
		p.logger.WithField("action", "export_participant").
			WithField("export_id", req.ID).
			WithField("node", req.NodeName).
			Error(err)
		return
	}

	p.logger.WithField("action", "export_participant").
		WithField("export_id", req.ID).
		WithField("node", req.NodeName).
		Info("participant export completed successfully")
}

// doExport performs the actual export of all classes/shards in the request.
// It writes per-node status files and returns an error if any class fails.
// Used by both the multi-node participant path and the single-node scheduler.
func (p *Participant) doExport(ctx context.Context, backend modulecapabilities.BackupBackend, req *ExportRequest) error {
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

	stopWriter := p.startNodeStatusWriter(backend, req, nodeStatus)
	defer stopWriter()

	for _, className := range req.Classes {
		shardNames, ok := req.Shards[className]
		if !ok || len(shardNames) == 0 {
			continue
		}

		if err := p.exportClassShards(ctx, backend, req, className, shardNames, nodeStatus); err != nil {
			nodeStatus.SetFailed(className, err)
			return fmt.Errorf("export class %s: %w", className, err)
		}
	}

	nodeStatus.SetSuccess()
	return nil
}

// exportClassShards exports specific shards of a class to individual Parquet files.
// It uses AcquireShardForExport to handle MT tenants (activating COLD tenants if
// auto-activation is enabled) and bounded concurrency.
func (p *Participant) exportClassShards(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	className string,
	shardNames []string,
	nodeStatus *NodeStatus,
) error {
	isMT := p.selector.IsMultiTenant(ctx, className)

	eg := enterrors.NewErrorGroupWrapper(p.logger)
	eg.SetLimit(runtime.GOMAXPROCS(0))

	for _, shardName := range shardNames {
		eg.Go(func() error {
			shard, release, err := p.selector.AcquireShardForExport(ctx, className, shardName)
			if err != nil {
				nodeStatus.SetShardProgress(className, shardName, export.Failed, 0, err.Error())
				return fmt.Errorf("acquire shard %s: %w", shardName, err)
			}

			if shard == nil {
				// Tenant is COLD and auto-activation is disabled — skip.
				nodeStatus.SetShardProgress(className, shardName, export.Skipped, 0, "")
				return nil
			}
			defer release()

			objects, err := p.exportShardToFile(ctx, backend, req, className, shardName, shard, isMT)
			if err != nil {
				nodeStatus.SetShardProgress(className, shardName, export.Failed, 0, err.Error())
				return fmt.Errorf("export shard %s: %w", shardName, err)
			}

			nodeStatus.SetShardProgress(className, shardName, export.Success, objects, "")
			return nil
		}, shardName)
	}

	return eg.Wait()
}

// exportShardToFile exports a single shard to a Parquet file: {ClassName}_{ShardName}.parquet
func (p *Participant) exportShardToFile(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	className, shardName string,
	shard ShardLike,
	isMT bool,
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

	writer.SetFileMetadata("collection", className)
	if isMT {
		writer.SetFileMetadata("tenant", shardName)
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

// startNodeStatusWriter launches a background goroutine that periodically
// snapshots nodeStatus under mu and writes it to S3. The returned stop function
// triggers one final flush and blocks until the write completes.
func (p *Participant) startNodeStatusWriter(
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	nodeStatus *NodeStatus,
) (stop func()) {
	done := make(chan struct{}) // closed when the goroutine exits. Blocks until status is fully flushed on stop.
	quit := make(chan struct{})
	var once sync.Once

	key := fmt.Sprintf("node_%s_status.json", nodeStatus.NodeName)

	flush := func() {
		nodeStatus.mu.Lock()
		data, err := json.Marshal(nodeStatus)
		nodeStatus.mu.Unlock()
		if err != nil {
			p.logger.WithField("action", "export").WithField("node", nodeStatus.NodeName).Error(err)
			return
		}
		writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := backend.Write(writeCtx, req.ID, key, req.Bucket, req.Path, newBytesReadCloser(data)); err != nil {
			p.logger.WithField("action", "export").WithField("node", nodeStatus.NodeName).Error(err)
		}
	}

	enterrors.GoWrapper(func() {
		defer close(done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				flush()
			case <-quit:
				flush()
				return
			}
		}
	}, p.logger)

	return func() {
		once.Do(func() {
			close(quit)
			<-done
		})
	}
}
