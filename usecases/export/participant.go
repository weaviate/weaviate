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
	"runtime"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

const (
	reservationTimeout    = 30 * time.Second
	defaultStatusInterval = 30 * time.Second
)

// Participant handles export requests on a single node.
// It exports its assigned shards directly to S3 and writes status files.
//
// The two-phase commit protocol works as follows:
//  1. Prepare: reserves the export slot (atomic CAS). A background timer
//     auto-aborts after reservationTimeout if Commit is not called.
//  2. Commit: cancels the timer and starts the actual export work.
//  3. Abort: releases the reservation immediately.
type Participant struct {
	shutdownCtx    context.Context
	selector       Selector
	backends       BackendProvider
	logger         logrus.FieldLogger
	statusInterval time.Duration // interval for status writes and sibling checks; 0 uses defaultStatusInterval

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
// The shutdownCtx is canceled on graceful server shutdown, allowing in-flight
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
// If the export has already been committed, the running export is canceled.
func (p *Participant) Abort(exportID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.activeExport != exportID {
		return
	}

	if p.cancelExport != nil {
		// Export is running — cancel it. The goroutine will detect context
		// cancellation, write a failed status, and call clearAndRelease()
		// via its defer. We intentionally leave cancelExport non-nil so
		// that concurrent or repeated Abort calls still take this branch
		// instead of the "prepared" branch below.
		p.cancelExport()
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
// It uses an N-worker pool pattern: a single producer goroutine walks all
// shards depth-first and submits scanJobs to workers via a shared channel.
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
				Status: export.ShardTransferring,
			}
		}
	}

	stopWriter := p.startNodeStatusWriter(ctx, backend, req, nodeStatus)
	defer stopWriter()

	numWorkers := runtime.GOMAXPROCS(0)
	jobCh := make(chan scanJob, numWorkers)

	// Start N workers that process scan jobs.
	var workerWg sync.WaitGroup
	for range numWorkers {
		workerWg.Add(1)
		enterrors.GoWrapper(func() {
			defer workerWg.Done()
			for job := range jobCh {
				job.execute()
			}
		}, p.logger)
	}

	// cleanupErr collects the first error from any cleanup goroutine
	// (scan failure, write failure, upload failure). Written under
	// cleanupErrOnce so only the first error wins.
	// failFastCancel cancels all in-flight work (scan workers, shard
	// acquisition, range submission) so we don't waste effort after a
	// failure.
	failFastCtx, failFastCancel := context.WithCancel(ctx)
	defer failFastCancel()

	var cleanupErr error
	var cleanupErrOnce sync.Once
	setCleanupErr := func(err error) {
		cleanupErrOnce.Do(func() {
			cleanupErr = err
			failFastCancel()
		})
	}

	// Depth-first walk: submit all range jobs for all shards.
	var cleanupWg sync.WaitGroup
	err := p.submitJobs(failFastCtx, jobCh, &cleanupWg, setCleanupErr, backend, req, nodeStatus)
	close(jobCh)
	workerWg.Wait()
	// Wait for all per-shard cleanup goroutines (writer flush, shard release)
	// to finish. This must happen after workers are done so that all scan
	// results have been produced before cleanup closes the rows channels.
	cleanupWg.Wait()

	if err != nil {
		return err
	}
	if cleanupErr != nil {
		return cleanupErr
	}

	nodeStatus.SetSuccess()
	return nil
}

// submitJobs walks classes → shards → ranges depth-first, submitting scanJobs
// to jobCh. For each shard it sets up a Parquet writer pipeline and a cleanup
// goroutine that waits for all range jobs to complete before flushing.
// cleanupWg tracks the spawned cleanup goroutines; the caller must wait on it
// after workers have drained jobCh.
func (p *Participant) submitJobs(
	ctx context.Context,
	jobCh chan<- scanJob,
	cleanupWg *sync.WaitGroup,
	setCleanupErr func(error),
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	nodeStatus *NodeStatus,
) error {
	for _, className := range req.Classes {
		shardNames, ok := req.Shards[className]
		if !ok || len(shardNames) == 0 {
			continue
		}

		isMT := p.selector.IsMultiTenant(ctx, className)

		for _, shardName := range shardNames {
			if err := ctx.Err(); err != nil {
				nodeStatus.SetFailed(className, err)
				return fmt.Errorf("export class %s: %w", className, err)
			}

			shard, release, skipReason, err := p.selector.AcquireShardForExport(ctx, className, shardName)
			if err != nil {
				nodeStatus.SetShardProgress(className, shardName, export.ShardFailed, 0, err.Error(), "")
				nodeStatus.SetFailed(className, err)
				return fmt.Errorf("acquire shard %s/%s: %w", className, shardName, err)
			}

			if shard == nil {
				nodeStatus.SetShardProgress(className, shardName, export.ShardSkipped, 0, "", skipReason)
				continue
			}

			if err := p.submitShardJobs(ctx, jobCh, cleanupWg, setCleanupErr, backend, req, className, shardName, shard, release, isMT, nodeStatus); err != nil {
				nodeStatus.SetFailed(className, err)
				return err
			}
		}
	}

	return nil
}

// submitShardJobs sets up the writer pipeline for a single shard, computes key
// ranges, and submits scanJobs to jobCh. It spawns a cleanup goroutine
// (tracked by cleanupWg) that waits for all range jobs to complete, flushes
// the writer pipeline, and releases the shard.
func (p *Participant) submitShardJobs(
	ctx context.Context,
	jobCh chan<- scanJob,
	cleanupWg *sync.WaitGroup,
	setCleanupErr func(error),
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	className, shardName string,
	shard ShardLike,
	release func(),
	isMT bool,
	nodeStatus *NodeStatus,
) error {
	// Validate store/bucket before starting the writer pipeline so we can
	// return early without needing to tear down goroutines.
	store := shard.Store()
	if store == nil {
		release()
		err := fmt.Errorf("store not found for shard %s/%s", className, shardName)
		nodeStatus.SetShardProgress(className, shardName, export.ShardFailed, 0, err.Error(), "")
		return err
	}
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		release()
		err := fmt.Errorf("objects bucket not found for shard %s/%s", className, shardName)
		nodeStatus.SetShardProgress(className, shardName, export.ShardFailed, 0, err.Error(), "")
		return err
	}
	ranges := computeRanges(bucket)

	pw, writer, writerDone, scanCtx, scanCancel, rowsCh, uploadDone, writerErrFn, err := p.startShardWriter(ctx, backend, req, className, shardName, isMT)
	if err != nil {
		release()
		nodeStatus.SetShardProgress(className, shardName, export.ShardFailed, 0, err.Error(), "")
		return fmt.Errorf("start shard writer %s/%s: %w", className, shardName, err)
	}

	// Thread-safe scan error collector for this shard.
	var scanMu sync.Mutex
	var scanErr error
	setScanErr := func(err error) {
		scanMu.Lock()
		if scanErr == nil {
			scanErr = err
		}
		scanMu.Unlock()
	}

	// Submit range jobs, tracked by a per-shard WaitGroup.
	// If ranges is nil (validation failed above), no jobs are submitted and
	// the cleanup goroutine will handle the error immediately.
	var shardWg sync.WaitGroup
	submitted := true
	for _, r := range ranges {
		shardWg.Add(1)
		select {
		case jobCh <- scanJob{
			ctx:        scanCtx,
			bucket:     bucket,
			keyRange:   r,
			rowsCh:     rowsCh,
			wg:         &shardWg,
			setScanErr: setScanErr,
		}:
		case <-ctx.Done():
			// Undo the Add for this unsubmitted range. Already-submitted
			// jobs will complete and call Done() themselves. The cleanup
			// goroutine waits on shardWg before closing rowsCh, so there
			// is no close-while-sending race.
			shardWg.Done()
			setScanErr(ctx.Err())
			submitted = false
			break
		}
		if !submitted {
			break
		}
	}

	// Cleanup goroutine: waits for all range jobs to complete, flushes the
	// writer pipeline, and releases the shard. This is the single place that
	// closes rowsCh — always after shardWg.Wait() — preventing send-on-closed
	// races.
	cleanupWg.Add(1)
	enterrors.GoWrapper(func() {
		defer cleanupWg.Done()
		shardWg.Wait()
		scanCancel()
		close(rowsCh)
		<-writerDone

		scanMu.Lock()
		shardScanErr := scanErr
		scanMu.Unlock()

		// Check for writer-side errors (e.g. WriteRow failure).
		if wErr := writerErrFn(); wErr != nil && shardScanErr == nil {
			shardScanErr = fmt.Errorf("write row to parquet: %w", wErr)
		}

		if shardScanErr != nil {
			setCleanupErr(shardScanErr)
			_ = writer.Close()
			pw.CloseWithError(shardScanErr)
			<-uploadDone
			nodeStatus.SetShardProgress(className, shardName, export.ShardFailed, 0, shardScanErr.Error(), "")
			release()
			return
		}

		if err := writer.Close(); err != nil {
			setCleanupErr(err)
			pw.CloseWithError(err)
			<-uploadDone
			nodeStatus.SetShardProgress(className, shardName, export.ShardFailed, 0, err.Error(), "")
			release()
			return
		}

		if err := pw.Close(); err != nil {
			setCleanupErr(err)
			<-uploadDone
			nodeStatus.SetShardProgress(className, shardName, export.ShardFailed, 0, err.Error(), "")
			release()
			return
		}

		// Check for upload errors (e.g. S3 failure).
		if uploadErr := <-uploadDone; uploadErr != nil {
			setCleanupErr(uploadErr)
			nodeStatus.SetShardProgress(className, shardName, export.ShardFailed, 0, uploadErr.Error(), "")
			release()
			return
		}

		p.logger.WithField("class", className).
			WithField("shard", shardName).
			WithField("objects", writer.ObjectsWritten()).
			Info("shard export completed")

		nodeStatus.SetShardProgress(className, shardName, export.ShardSuccess, writer.ObjectsWritten(), "", "")
		release()
	}, p.logger)

	// If we couldn't submit all ranges due to context cancellation,
	// propagate the error so submitJobs stops processing further shards.
	if !submitted {
		return ctx.Err()
	}

	return nil
}

// startShardWriter sets up the Parquet writer pipeline for a single shard.
// It returns the pipe writer, parquet writer, a channel that closes when the
// upload goroutine finishes, a cancellable context for scan workers, and the
// rows channel that scan workers should write to.
func (p *Participant) startShardWriter(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	className, shardName string,
	isMT bool,
) (
	_ *io.PipeWriter, _ *ParquetWriter, _ <-chan struct{},
	_ context.Context, _ context.CancelFunc, _ chan []ParquetRow,
	_ <-chan error, // uploadDone
	_ func() error, // writerErr accessor (safe to call after <-writerDone)
	setupErr error,
) {
	pr, pw := io.Pipe()

	fileName := fmt.Sprintf("%s_%s.parquet", className, shardName)

	uploadDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := backend.Write(ctx, req.ID, fileName, req.Bucket, req.Path, pr)
		uploadDone <- err
	}, p.logger)

	writer, err := NewParquetWriter(pw)
	if err != nil {
		pw.CloseWithError(err)
		<-uploadDone
		return nil, nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("create parquet writer: %w", err)
	}

	writer.SetFileMetadata("collection", className)
	if isMT {
		writer.SetFileMetadata("tenant", shardName)
	}

	parallelism := runtime.GOMAXPROCS(0)
	rowsCh := make(chan []ParquetRow, parallelism)

	// scanCtx is canceled when the writer hits an error, so scan workers
	// stop early instead of blocking on channel sends.
	scanCtx, scanCancel := context.WithCancel(ctx)

	// Writer goroutine: single consumer drains rowsCh into ParquetWriter.
	// writerErr is written only inside this goroutine and read only after
	// <-writerDone, so no mutex is needed.
	var writerErr error
	writerDone := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(writerDone)
		for batch := range rowsCh {
			for i := range batch {
				if err := writer.WriteRow(batch[i]); err != nil {
					writerErr = err
					scanCancel()
					// Drain rowsCh so scan workers don't block on sends.
					for range rowsCh {
					}
					return
				}
			}
		}
	}, p.logger)

	writerErrFn := func() error { return writerErr }

	return pw, writer, writerDone, scanCtx, scanCancel, rowsCh, uploadDone, writerErrFn, nil
}

// siblingHasFailed checks whether any sibling node has reported a Failed
// status by reading their status files from the storage backend. If a sibling
// has failed it returns the sibling name and its error string so the caller
// can surface an actionable message. Not-found errors are silently ignored
// (sibling may not have written its status yet). Other backend errors are
// logged at warn level but do not trigger cancellation to avoid false
// positives during transient outages.
func (p *Participant) siblingHasFailed(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
) (failedSibling string, siblingErr string, failed bool) {
	for _, nodeName := range req.SiblingNodes {
		readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		key := fmt.Sprintf("node_%s_status.json", nodeName)
		data, err := backend.GetObject(readCtx, req.ID, key, req.Bucket, req.Path)
		cancel()
		if err != nil {
			var errNotFound backup.ErrNotFound
			if !errors.As(err, &errNotFound) {
				p.logger.WithField("action", "export_sibling_check").
					WithField("export_id", req.ID).
					WithField("sibling", nodeName).
					Warn(fmt.Errorf("read sibling status: %w", err))
			}
			continue
		}

		var siblingStatus NodeStatus
		if err := json.Unmarshal(data, &siblingStatus); err != nil {
			p.logger.WithField("action", "export_sibling_check").WithField("sibling", nodeName).
				Error(fmt.Errorf("unmarshal sibling status: %w", err))
			continue
		}

		if siblingStatus.Status == export.Failed {
			p.logger.WithField("action", "export_sibling_check").
				WithField("export_id", req.ID).
				WithField("sibling", nodeName).
				WithField("sibling_error", siblingStatus.Error).
				Warn("sibling node failed, canceling local export")
			return nodeName, siblingStatus.Error, true
		}
	}

	return "", "", false
}

// startNodeStatusWriter launches a background goroutine that periodically
// snapshots nodeStatus under mu and writes it to S3. It also checks sibling
// nodes' status and cancels the local export if any sibling has failed.
// The returned stop function triggers one final flush and blocks until the
// write completes.
func (p *Participant) startNodeStatusWriter(
	exportCtx context.Context,
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
			p.logger.WithField("action", "export").WithField("node", nodeStatus.NodeName).
				Error(fmt.Errorf("marshal node status: %w", err))
			return
		}
		writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := backend.Write(writeCtx, req.ID, key, req.Bucket, req.Path, newBytesReadCloser(data)); err != nil {
			p.logger.WithField("action", "export").WithField("node", nodeStatus.NodeName).
				Error(fmt.Errorf("write node status: %w", err))
		}
	}

	enterrors.GoWrapper(func() {
		defer close(done)
		interval := p.statusInterval
		if interval == 0 {
			interval = defaultStatusInterval
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				flush()
				if failedSibling, siblingErr, failed := p.siblingHasFailed(exportCtx, backend, req); failed {
					nodeStatus.mu.Lock()
					nodeStatus.Error = fmt.Sprintf("sibling node %q failed: %s", failedSibling, siblingErr)
					nodeStatus.mu.Unlock()

					p.mu.Lock()
					if p.cancelExport != nil {
						p.cancelExport()
					}
					p.mu.Unlock()
				}
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
