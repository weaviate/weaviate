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
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/export"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	reservationTimeout          = 30 * time.Second
	defaultStatusFlushInterval  = 10 * time.Second
	defaultSiblingCheckInterval = 1 * time.Minute
)

// Participant handles export requests on a single node.
// It exports its assigned shards directly to S3 and writes status files.
//
// The two-phase commit protocol works as follows:
//  1. Prepare: reserves the export slot (atomic CAS) and starts snapshotting
//     all shards in the background. This anchors the point-in-time to the
//     Prepare phase. A background timer auto-aborts after reservationTimeout
//     if Commit is not called.
//  2. Commit: waits for the snapshot goroutine to finish, then starts the
//     scan phase that reads the snapshots and uploads parquet files.
//  3. Abort: releases the reservation immediately, canceling any in-flight
//     snapshot goroutine.
type Participant struct {
	shutdownCtx          context.Context
	shutdownCancel       context.CancelFunc
	selector             Selector
	backends             BackendProvider
	logger               logrus.FieldLogger
	statusFlushInterval  time.Duration // 0 uses defaultStatusFlushInterval
	siblingCheckInterval time.Duration // 0 uses defaultSiblingCheckInterval

	// Deps for best-effort sibling abort on failure.
	client       ExportClient
	nodeResolver NodeResolver
	localNode    string

	// exportWg tracks in-flight export goroutines so Shutdown can wait for
	// them to finish their cleanup (final status flush, sibling abort, metadata
	// promotion) before the server process exits.
	exportWg sync.WaitGroup

	// mu guards preparedReq, abortTimer, cancelExport, and pending, which
	// are set during Prepare/Commit and consumed during Commit/Abort.
	mu           sync.Mutex
	preparedReq  *ExportRequest
	abortTimer   *time.Timer
	cancelExport context.CancelFunc
	pending      *pendingSnapshot // set during Prepare, consumed by Commit
	// this stays set from the moment Prepare() reserves the slot until Commit() or Abort() releases it. Used for IsRunning() checks.
	activeExport string
}

// NewParticipant creates a new export participant.
// Call StartShutdown to signal in-flight exports to stop, then Shutdown to
// wait for them to drain.
// client and nodeResolver enable best-effort sibling aborts on failure.
func NewParticipant(
	selector Selector,
	backends BackendProvider,
	logger logrus.FieldLogger,
	client ExportClient,
	nodeResolver NodeResolver,
	localNode string,
) *Participant {
	if client == nil {
		panic("export: participant requires a non-nil client")
	}
	if nodeResolver == nil {
		panic("export: participant requires a non-nil nodeResolver")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Participant{
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
		selector:       selector,
		backends:       backends,
		logger:         logger,
		client:         client,
		nodeResolver:   nodeResolver,
		localNode:      localNode,
	}
}

// StartShutdown signals in-flight exports to stop. It returns immediately;
// call Shutdown to wait for exports to finish their cleanup.
func (p *Participant) StartShutdown() {
	p.shutdownCancel()
}

// Shutdown waits for any in-flight export goroutine to finish its cleanup
// (final status flush, sibling abort, metadata promotion). The caller should
// call StartShutdown first to signal exports to stop, then call Shutdown to
// wait for them to drain. The provided context bounds how long we wait.
func (p *Participant) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		p.exportWg.Wait()
		close(done)
	}, p.logger)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Prepare reserves the export slot for the given request. If no Commit
// arrives within reservationTimeout the reservation is automatically released.
func (p *Participant) Prepare(_ context.Context, req *ExportRequest) error {
	f := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		if req == nil {
			return fmt.Errorf("%w: request cannot be nil", ErrExportValidation)
		}

		if req.ID == "" {
			return fmt.Errorf("%w: export ID cannot be empty", ErrExportValidation)
		}

		if p.activeExport != "" {
			return fmt.Errorf("%w: export %q already in progress", ErrExportAlreadyActive, p.activeExport)
		}

		p.activeExport = req.ID

		p.preparedReq = req

		// Start snapshotting in the background so the point-in-time is
		// anchored to the Prepare phase. Commit blocks on ps.done before
		// proceeding.
		//
		// NOTE: Prepare returns success before the snapshot completes.
		// This is intentional — it allows the coordinator to Prepare all
		// nodes concurrently and overlap their snapshot work with the
		// metadata write. The tradeoff is that a snapshot failure is only
		// discovered at Commit time, after other nodes may have already
		// committed. Those nodes are then aborted. Making Prepare
		// synchronous would be simpler but would serialize snapshotting
		// across the 2PC phases, increasing the chance of hitting the
		// reservation timeout on large deployments.
		snapshotCtx, snapshotCancel := context.WithCancel(p.shutdownCtx)
		ps := &pendingSnapshot{
			done:   make(chan struct{}),
			cancel: snapshotCancel,
		}
		p.pending = ps

		enterrors.GoWrapper(func() {
			defer close(ps.done)
			ps.snapshots, ps.skipped, ps.err = p.snapshotAllShards(snapshotCtx, req)
		}, p.logger)

		p.abortTimer = time.AfterFunc(reservationTimeout, func() {
			wasSet := func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()

				if p.preparedReq == nil {
					return false // Already committed or aborted — no-op.
				}
				p.clearAndRelease()
				return true
			}()

			if wasSet {
				p.logger.WithField("export_id", req.ID).
					Warn("export reservation timed out, auto-aborting")
			}
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

	// Peek at the prepared request and pending snapshot under a short lock
	// to get what we need for the blocking operations below. We don't
	// consume the request yet — that happens in the critical section.
	p.mu.Lock()
	req := p.preparedReq
	pending := p.pending
	p.mu.Unlock()

	// Initialize the backend outside the lock — this may involve network
	// I/O (S3 bucket verification, directory creation) and must not block
	// Abort/IsRunning callers. If initialization fails, backendStore stays
	// nil and the main critical section below will handle the error with
	// proper slot cleanup via clearAndRelease.
	var backendStore modulecapabilities.BackupBackend
	var backendErr error
	if req != nil && req.ID == exportID {
		backendStore, backendErr = p.backends.BackupBackend(req.Backend)
		if backendErr == nil {
			if backendErr = backendStore.Initialize(ctx, req.ID, req.Bucket, req.Path); backendErr != nil {
				backendStore = nil
			}
		}
	}

	// Wait for snapshots started during Prepare. Only wait if the IDs
	// match — mismatched commits should fail fast in the critical section
	// rather than blocking on an unrelated snapshot goroutine.
	var snapshots []shardSnapshot
	var skipped []skippedShard
	if pending != nil && req != nil && req.ID == exportID {
		select {
		case <-pending.done:
			snapshots = pending.snapshots
			skipped = pending.skipped
			if pending.err != nil {
				p.cleanupSnapshots(snapshots)
				p.mu.Lock()
				p.clearAndRelease()
				p.mu.Unlock()
				return fmt.Errorf("snapshot phase failed: %w", pending.err)
			}
		case <-ctx.Done():
			// Cancel the snapshot goroutine and clean up in the
			// background so we don't leave snapshots on disk until
			// the abort timer fires.
			pending.cancel()
			enterrors.GoWrapper(func() {
				<-pending.done
				p.cleanupSnapshots(pending.snapshots)
			}, p.logger)
			return ctx.Err()
		}
	}

	var exportCtx context.Context
	f := func() (errRet error) {
		p.mu.Lock()
		defer func() {
			if errRet != nil {
				// On error, clean up snapshots we took ownership of.
				// clearAndRelease may also try to clean up via the pending
				// field, but we nil it below. os.RemoveAll on an
				// already-removed dir is a no-op.
				p.pending = nil
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

		if p.preparedReq == nil {
			errRet = fmt.Errorf("no export prepared")
			return errRet
		}
		if p.preparedReq.ID != exportID {
			errRet = fmt.Errorf("export ID mismatch: expected %q, got %q", p.preparedReq.ID, exportID)
			return errRet
		}
		if p.preparedReq != req {
			errRet = fmt.Errorf("export request was replaced during backend initialization (abort+re-prepare race)")
			return errRet
		}

		if backendStore == nil {
			if backendErr != nil {
				errRet = fmt.Errorf("initialize backend: %w", backendErr)
			} else {
				errRet = fmt.Errorf("backend initialization was not attempted (state changed during init)")
			}
			return errRet
		}

		p.preparedReq = nil
		p.abortTimer = nil
		p.pending = nil // ownership transferred to executeExport

		exportCtx2, cancel := context.WithCancel(p.shutdownCtx)
		p.cancelExport = cancel
		exportCtx = exportCtx2

		return nil
	}
	err := f()
	if err != nil {
		// Critical section failed — clean up snapshots we own.
		p.cleanupSnapshots(snapshots)
		return err
	}

	p.logger.WithField("action", "export_participant").
		WithField("export_id", req.ID).
		WithField("node", req.NodeName).
		WithField("classes", req.Classes).
		Info("participant starting export")

	p.exportWg.Add(1)
	enterrors.GoWrapper(func() {
		defer p.exportWg.Done()
		p.executeExport(exportCtx, backendStore, req, snapshots, skipped)
	}, p.logger)

	return nil
}

// Abort cancels a prepared or running export.
// If the export is still in the prepared state, the reservation is released.
// If the export has already been committed, the running export is canceled.
func (p *Participant) Abort(exportID string) {
	var wasRunning bool
	func() {
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
			wasRunning = true
		} else {
			// Still in prepared state — full cleanup.
			p.clearAndRelease()
		}
	}()

	if wasRunning {
		p.logger.WithField("action", "export_participant").
			WithField("export_id", exportID).
			Info("participant aborted running export")
	} else {
		p.logger.WithField("action", "export_participant").
			WithField("export_id", exportID).
			Info("participant aborted export reservation")
	}
}

// clearAndRelease is called by the reservation timer, Abort, or Commit
// error paths. It releases the slot and cancels any in-flight work.
// If a pending snapshot goroutine is still running, it is canceled and a
// background goroutine waits for it to finish before cleaning up its
// snapshot directories. os.RemoveAll on an already-removed directory is
// a no-op, so double cleanup from both Commit and this path is safe.
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

	if ps := p.pending; ps != nil {
		p.pending = nil
		ps.cancel()
		enterrors.GoWrapper(func() {
			<-ps.done // wait for goroutine to finish
			p.cleanupSnapshots(ps.snapshots)
		}, p.logger)
	}

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

// abortSiblings sends best-effort, fire-and-forget abort requests to all
// sibling nodes participating in the same export. This is called when the
// local export fails so siblings stop quickly instead of waiting for the
// next periodic sibling-health check.
func (p *Participant) abortSiblings(exportID string, req *ExportRequest) {
	if len(req.SiblingNodes) == 0 {
		return
	}

	nodes := make([]exportNodeInfo, 0, len(req.SiblingNodes))
	for _, nodeName := range req.SiblingNodes {
		if nodeName == p.localNode {
			continue
		}
		ni := exportNodeInfo{
			req: &ExportRequest{ID: exportID, NodeName: nodeName},
		}
		if host, ok := p.nodeResolver.NodeHostname(nodeName); ok {
			ni.host = host
		}
		nodes = append(nodes, ni)
	}
	if len(nodes) == 0 {
		return
	}

	p.logger.WithField("action", "export_abort_siblings").
		WithField("export_id", exportID).
		WithField("siblings", len(nodes)).
		Info("notifying sibling nodes to abort")

	enterrors.GoWrapper(func() {
		if err := abortRemoteNodes(p.client, p.logger, exportID, nodes); err != nil {
			p.logger.WithField("action", "export_abort_siblings").
				WithField("export_id", exportID).
				Warnf("best-effort sibling abort encountered errors: %v", err)
		}
	}, p.logger)
}

func (p *Participant) executeExport(ctx context.Context, backend modulecapabilities.BackupBackend, req *ExportRequest, snapshots []shardSnapshot, skipped []skippedShard) {
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.clearAndRelease()
	}()

	nodeStatus := &NodeStatus{
		NodeName:      req.NodeName,
		Status:        export.Transferring,
		ShardProgress: make(map[string]map[string]*ShardProgress),
		Version:       config.ServerVersion,
	}

	if err := p.doExport(ctx, backend, req, nodeStatus, snapshots, skipped); err != nil {
		p.logger.WithField("action", "export_participant").
			WithField("export_id", req.ID).
			WithField("node", req.NodeName).
			Error(err)
		// Best-effort: notify sibling nodes to abort so they stop quickly
		// instead of waiting for the next periodic sibling-health check.
		p.abortSiblings(req.ID, req)
	} else {
		p.logger.WithField("action", "export_participant").
			WithField("export_id", req.ID).
			WithField("node", req.NodeName).
			Info("participant export completed successfully")
	}

	// After the final status flush (stopWriter in doExport), try to promote
	// metadata if this is the last node to finish.
	if len(req.SiblingNodes) > 0 {
		p.tryPromoteMetadata(backend, req, nodeStatus)
	}
}

// tryPromoteMetadata checks whether all nodes (own + siblings) have reached a
// terminal status and, if so, writes the final export metadata file. This
// promotes the multi-node export to a terminal state without requiring a
// Status() API call.
// NOTE: this may race with Scheduler.Status which does the same promotion.
// Both paths assemble from the same per-node status files so the result is
// identical — last writer wins with the same data.
func (p *Participant) tryPromoteMetadata(backend modulecapabilities.BackupBackend, req *ExportRequest, ownStatus *NodeStatus) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build metadata from the request to use as input for assembleNodeStatuses.
	meta := &ExportMetadata{
		ID:              req.ID,
		Backend:         req.Backend,
		Classes:         req.Classes,
		NodeAssignments: req.NodeAssignments,
		StartedAt:       req.StartedAt,
	}

	// Read all node statuses (own snapshot + siblings).
	nodeStatuses := map[string]*NodeStatus{req.NodeName: ownStatus.SyncAndSnapshot()}
	for _, sibling := range req.SiblingNodes {
		ns, err := readNodeStatus(ctx, backend, req.ID, req.Bucket, req.Path, sibling)
		if err != nil {
			// Sibling status not available — cannot determine if all are terminal.
			return
		}
		if ns.Status != export.Success && ns.Status != export.Failed {
			// Sibling still running.
			return
		}
		nodeStatuses[sibling] = ns
	}

	// Assemble using shared logic (same as Status endpoint).
	assembled, allTerminal := assembleNodeStatuses(meta, backend.HomeDir(req.ID, req.Bucket, req.Path), nodeStatuses)
	if !allTerminal {
		// There can be a race if a sibling wrote its status after we read it
		// but before we assembled. The next Status() call will promote it.
		return
	}

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
	if err := writeExportMetadata(backend, req.ID, req.Bucket, req.Path, promotedMeta, p.logger); err != nil {
		p.logger.WithField("export_id", req.ID).
			Warnf("last-node promotion: failed to write metadata: %v", err)
	}
}

// shardSnapshot holds the result of snapshotting a single shard's objects bucket.
type shardSnapshot struct {
	className string
	shardName string
	isMT      bool
	dir       string // snapshot directory path
	strategy  string // bucket strategy for NewSnapshotBucket
}

// skippedShard records a shard that was skipped during the snapshot phase,
// along with the reason. This is kept separate from nodeStatus so that
// snapshotAllShards can run without access to the NodeStatus (which belongs
// to the Commit/doExport phase).
type skippedShard struct {
	className  string
	shardName  string
	skipReason string
}

// pendingSnapshot tracks an in-flight snapshot goroutine started during
// Prepare. The goroutine writes its results and closes done; consumers
// must wait on done before reading snapshots, skipped, or err.
type pendingSnapshot struct {
	done   chan struct{}
	cancel context.CancelFunc

	// Written by goroutine before closing done; read only after done is closed.
	snapshots []shardSnapshot
	skipped   []skippedShard
	err       error
}

// doExport performs the actual export. Snapshots were already created during
// the Prepare phase and are passed in; this method records skipped shards
// in nodeStatus and runs the scan phase.
//
// The scan phase opens each snapshot as a read-only bucket, computes key
// ranges, and submits parallel scan jobs to an N-worker pool. Cleanup
// goroutines shut down snapshot buckets and remove snapshot directories
// once scanning completes.
func (p *Participant) doExport(ctx context.Context, backend modulecapabilities.BackupBackend, req *ExportRequest, nodeStatus *NodeStatus, snapshots []shardSnapshot, skipped []skippedShard) error {
	stopWriter := p.startNodeStatusWriter(ctx, backend, req, nodeStatus)
	defer stopWriter()

	// Record shards that were skipped during the snapshot phase.
	for _, s := range skipped {
		nodeStatus.SetShardProgress(s.className, s.shardName, export.ShardSkipped, "", s.skipReason)
	}

	// Ensure all snapshot directories are removed even if the scan phase
	// fails or panics. Snapshots that are successfully processed by cleanup
	// goroutines will already have their directories removed; RemoveAll on
	// a non-existent path is a no-op.
	defer p.cleanupSnapshots(snapshots)

	// Scan all snapshots in parallel using an N-worker pool.
	parallelism := runtime.GOMAXPROCS(0) * 2
	jobCh := make(chan scanJob, parallelism)

	// Start N workers that process scan jobs.
	var workerWg sync.WaitGroup
	for range parallelism {
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
	// failFastCancel cancels all in-flight work (scan workers, range
	// submission) so we don't waste effort after a failure.
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

	// Depth-first walk: submit all range jobs for all snapshots.
	var cleanupWg sync.WaitGroup
	submitErr := p.submitSnapshotJobs(failFastCtx, jobCh, &cleanupWg, setCleanupErr, backend, req, nodeStatus, parallelism, snapshots)
	close(jobCh)
	workerWg.Wait()
	// Wait for all per-shard cleanup goroutines (snapshot shutdown, dir
	// removal) to finish. This must happen after workers are done so that
	// all scan results have been produced before cleanup runs.
	cleanupWg.Wait()

	// submitSnapshotJobs errors (context cancellation, snapshot open) take
	// precedence over cleanupErr (writer flush, upload) because cleanup
	// failures are typically a downstream consequence of the root cause.
	if submitErr != nil {
		nodeStatus.SetNodeError(submitErr.Error())
		return submitErr
	}
	if cleanupErr != nil {
		nodeStatus.SetNodeError(cleanupErr.Error())
		return cleanupErr
	}

	nodeStatus.SetSuccess()
	return nil
}

// snapshotAllShards snapshots all shards for all classes in one batch per
// class. The selector handles locking, tenant status checks, and the
// active-vs-disk decision internally.
//
// Skip information is returned separately so that the caller (doExport) can
// record it in the NodeStatus, which is not available during the Prepare
// phase when this method runs.
func (p *Participant) snapshotAllShards(
	ctx context.Context,
	req *ExportRequest,
) ([]shardSnapshot, []skippedShard, error) {
	var snapshots []shardSnapshot
	var skipped []skippedShard

	for _, className := range req.Classes {
		shardNames, ok := req.Shards[className]
		if !ok || len(shardNames) == 0 {
			continue
		}

		isMT := p.selector.IsMultiTenant(ctx, className)

		results, err := p.selector.SnapshotShards(ctx, className, shardNames, req.ID)
		if err != nil {
			// Clean up any snapshots created before the error in this batch.
			for _, r := range results {
				if r.SnapshotDir != "" {
					os.RemoveAll(r.SnapshotDir)
				}
			}
			return snapshots, skipped, fmt.Errorf("snapshot class %s: %w", className, err)
		}

		for _, r := range results {
			if r.SkipReason != "" {
				skipped = append(skipped, skippedShard{
					className:  className,
					shardName:  r.ShardName,
					skipReason: r.SkipReason,
				})
				continue
			}
			if r.SnapshotDir == "" {
				skipped = append(skipped, skippedShard{
					className:  className,
					shardName:  r.ShardName,
					skipReason: "no data",
				})
				continue
			}
			snapshots = append(snapshots, shardSnapshot{
				className: className,
				shardName: r.ShardName,
				isMT:      isMT,
				dir:       r.SnapshotDir,
				strategy:  r.Strategy,
			})
		}
	}

	return snapshots, skipped, nil
}

// cleanupSnapshots removes all snapshot directories. Used on error paths
// when the scan phase will not run.
func (p *Participant) cleanupSnapshots(snapshots []shardSnapshot) {
	for _, snap := range snapshots {
		if err := os.RemoveAll(snap.dir); err != nil {
			p.logger.WithField("class", snap.className).
				WithField("shard", snap.shardName).
				WithField("path", snap.dir).
				Warnf("failed to remove snapshot directory: %v", err)
		}
	}
}

// submitSnapshotJobs opens each pre-created snapshot as a read-only bucket,
// computes key ranges, and submits scanJobs to the worker pool. A cleanup
// goroutine per shard shuts down the snapshot bucket, removes the directory,
// and updates shard status.
func (p *Participant) submitSnapshotJobs(
	ctx context.Context,
	jobCh chan<- scanJob,
	cleanupWg *sync.WaitGroup,
	setCleanupErr func(error),
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	nodeStatus *NodeStatus,
	parallelism int,
	snapshots []shardSnapshot,
) error {
	for _, snap := range snapshots {
		if err := ctx.Err(); err != nil {
			nodeStatus.SetFailed(snap.className, err)
			return fmt.Errorf("export class %s: %w", snap.className, err)
		}

		nodeStatus.SetShardProgress(snap.className, snap.shardName, export.ShardTransferring, "", "")

		if err := p.submitShardJobs(ctx, jobCh, cleanupWg, setCleanupErr,
			backend, req, snap, nodeStatus, parallelism); err != nil {
			return err
		}
	}
	return nil
}

// submitShardJobs opens a snapshot bucket, computes key ranges, and submits
// scanJobs to jobCh. Each scanJob owns its own writer pipeline (one parquet
// file per range). A cleanup goroutine (tracked by cleanupWg) waits for all
// range jobs to complete, then shuts down the snapshot bucket and removes the
// snapshot directory.
func (p *Participant) submitShardJobs(
	ctx context.Context,
	jobCh chan<- scanJob,
	cleanupWg *sync.WaitGroup,
	setCleanupErr func(error),
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	snap shardSnapshot,
	nodeStatus *NodeStatus,
	parallelism int,
) error {
	// CalcCountNetAdditions is cheap here: the hard-linked .cna files from
	// the source bucket already contain the precomputed counts, so each
	// segment just reads 12 bytes from disk. The count is needed by
	// computeRanges to split the key space for parallel scanning.
	snapshotBucket, err := lsmkv.NewSnapshotBucket(ctx, snap.dir,
		p.logger, lsmkv.WithStrategy(snap.strategy),
		lsmkv.WithCalcCountNetAdditions(true))
	if err != nil {
		os.RemoveAll(snap.dir)
		err = fmt.Errorf("open snapshot bucket for %s/%s: %w", snap.className, snap.shardName, err)
		nodeStatus.SetShardProgress(snap.className, snap.shardName, export.ShardFailed, err.Error(), "")
		return err
	}

	ranges := computeRanges(snapshotBucket, parallelism)

	writerCfg := &rangeWriterConfig{
		backend:   backend,
		req:       req,
		className: snap.className,
		shardName: snap.shardName,
		isMT:      snap.isMT,
		logger:    p.logger,
		onFlush: func(n int64) {
			nodeStatus.AddShardExported(snap.className, snap.shardName, n)
		},
	}

	// Thread-safe error collector for this shard. On the first error we
	// also call setCleanupErr which triggers failFastCancel, canceling the
	// context shared by all scan jobs across all shards so the entire
	// export stops quickly.
	var shardErr error
	var shardErrOnce sync.Once
	setErr := func(err error) {
		shardErrOnce.Do(func() {
			shardErr = err
			setCleanupErr(err)
		})
	}

	// Submit range jobs, tracked by a per-shard WaitGroup.
	var shardWg sync.WaitGroup
	var submitErr error
rangeloop:
	for i, r := range ranges {
		shardWg.Add(1)
		select {
		case jobCh <- scanJob{
			ctx:        ctx,
			bucket:     snapshotBucket,
			keyRange:   r,
			rangeIndex: i,
			writerCfg:  writerCfg,
			wg:         &shardWg,
			setErr:     setErr,
		}:
		case <-ctx.Done():
			setErr(ctx.Err())
			shardWg.Done()
			submitErr = ctx.Err()
			break rangeloop
		}
	}

	// Cleanup goroutine: waits for all range jobs, shuts down the snapshot
	// bucket, removes the snapshot directory, and updates shard status.
	// The written count lives in the shard's atomic counter and is synced
	// into ObjectsExported by SyncAndSnapshot.
	cleanupWg.Add(1)
	enterrors.GoWrapper(func() {
		defer cleanupWg.Done()
		shardWg.Wait()

		if err := snapshotBucket.Shutdown(context.Background()); err != nil {
			p.logger.WithField("class", snap.className).
				WithField("shard", snap.shardName).
				Warnf("failed to shutdown snapshot bucket: %v", err)
		}
		os.RemoveAll(snap.dir)

		if shardErr != nil {
			nodeStatus.SetShardProgress(snap.className, snap.shardName, export.ShardFailed, shardErr.Error(), "")
			return
		}

		p.logger.WithField("class", snap.className).
			WithField("shard", snap.shardName).
			WithField("objects", nodeStatus.GetShardWritten(snap.className, snap.shardName)).
			Info("shard export completed")

		nodeStatus.SetShardProgress(snap.className, snap.shardName, export.ShardSuccess, "", "")
	}, p.logger)

	return submitErr
}

// siblingHasFailed checks whether any sibling node has failed or become
// unreachable by reading status files and performing liveness checks.
func (p *Participant) siblingHasFailed(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
) (failedSibling string, siblingErr string, failed bool) {
	for _, nodeName := range req.SiblingNodes {
		if reason, ok := p.hasSiblingDied(ctx, backend, req, nodeName); ok {
			p.logSiblingFailed(req.ID, nodeName, reason)
			return nodeName, reason, true
		}
	}
	return "", "", false
}

// hasSiblingDied returns (reason, true) if the sibling has failed or is
// unreachable. It reads the status file, then falls back to a liveness
// check for non-terminal or missing statuses. If IsRunning returns false,
// the status file is re-read once to handle the race where the node wrote
// its terminal status just before clearing activeExport.
func (p *Participant) hasSiblingDied(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	nodeName string,
) (reason string, failed bool) {
	if reason, failed, terminal := p.checkSiblingStatus(ctx, backend, req, nodeName); terminal {
		return reason, failed
	}

	// Non-terminal or missing — check liveness.
	host, alive := p.nodeResolver.NodeHostname(nodeName)
	if !alive {
		return fmt.Sprintf("node %s is no longer part of the cluster", nodeName), true
	}

	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	running, err := p.client.IsRunning(checkCtx, host, req.ID)
	cancel()

	// Our own context is done — don't blame the sibling for our cancellation.
	if ctx.Err() != nil {
		return "", false
	}

	if err == nil && running {
		return "", false
	}

	// Transient error reaching sibling — log but treat as inconclusive. The
	// next tick or cluster membership change will catch a real failure.
	if err != nil {
		p.logger.WithField("action", "export_sibling_liveness").
			WithField("export_id", req.ID).
			WithField("sibling", nodeName).
			Warnf("IsRunning check failed: %v", err)
		return "", false
	}

	// err == nil && !running — sibling explicitly confirmed it is not running
	// this export. Re-read status: the node may have written terminal status
	// just before clearing activeExport (stopWriter flushes before clearAndRelease).
	if reason, failed, terminal := p.checkSiblingStatus(ctx, backend, req, nodeName); terminal {
		return reason, failed
	}

	return fmt.Sprintf("node %s is no longer running export %s", nodeName, req.ID), true
}

// checkSiblingStatus reads the sibling's status file and returns one of
// three outcomes: terminal failure (reason, true, true), terminal success
// ("", false, true), or non-terminal/missing ("", false, false).
func (p *Participant) checkSiblingStatus(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	nodeName string,
) (reason string, failed bool, terminal bool) {
	ns := p.readSiblingStatus(ctx, backend, req, nodeName)
	if ns == nil {
		return "", false, false
	}
	switch ns.Status {
	case export.Failed, export.Canceled:
		return ns.Error, true, true
	case export.Success:
		return "", false, true
	default:
		// non-terminal states
		return "", false, false
	}
}

// readSiblingStatus reads a sibling's status file from the backend.
// Returns nil if the file does not exist or cannot be parsed.
func (p *Participant) readSiblingStatus(
	ctx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	nodeName string,
) *NodeStatus {
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	key := fmt.Sprintf("node_%s_status.json", nodeName)
	data, err := backend.GetObject(readCtx, req.ID, key, req.Bucket, req.Path)
	if err != nil {
		var errNotFound backup.ErrNotFound
		if !errors.As(err, &errNotFound) {
			p.logger.WithField("action", "export_sibling_check").
				WithField("export_id", req.ID).
				WithField("sibling", nodeName).
				Warnf("read sibling status: %v", err)
		}
		return nil
	}

	var status NodeStatus
	if err := json.Unmarshal(data, &status); err != nil {
		p.logger.WithField("action", "export_sibling_check").
			WithField("sibling", nodeName).
			Errorf("unmarshal sibling status: %v", err)
		return nil
	}
	return &status
}

func (p *Participant) logSiblingFailed(exportID, nodeName, reason string) {
	p.logger.WithField("action", "export_sibling_check").
		WithField("export_id", exportID).
		WithField("sibling", nodeName).
		WithField("reason", reason).
		Warn("sibling node failed, canceling local export")
}

// startNodeStatusWriter launches two background goroutines: one that
// periodically flushes nodeStatus to the storage backend, and one that
// checks sibling node health and cancels the export if a sibling has
// failed or become unreachable. The returned stop function triggers a
// final flush and blocks until both goroutines exit.
func (p *Participant) startNodeStatusWriter(
	exportCtx context.Context,
	backend modulecapabilities.BackupBackend,
	req *ExportRequest,
	nodeStatus *NodeStatus,
) (stop func()) {
	quit := make(chan struct{})
	var wg sync.WaitGroup
	var once sync.Once

	key := fmt.Sprintf("node_%s_status.json", nodeStatus.NodeName)

	flush := func() {
		snap := nodeStatus.SyncAndSnapshot()
		data, err := json.Marshal(snap)
		if err != nil {
			p.logger.WithField("action", "export").WithField("node", nodeStatus.NodeName).
				Errorf("marshal node status: %v", err)
			return
		}
		writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := backend.Write(writeCtx, req.ID, key, req.Bucket, req.Path, newBytesReadCloser(data)); err != nil {
			p.logger.WithField("action", "export").WithField("node", nodeStatus.NodeName).
				Errorf("write node status: %v", err)
		}
	}

	flushInterval := p.statusFlushInterval
	if flushInterval == 0 {
		flushInterval = defaultStatusFlushInterval
	}
	siblingInterval := p.siblingCheckInterval
	if siblingInterval == 0 {
		siblingInterval = defaultSiblingCheckInterval
	}

	// Goroutine 1: periodic status flush.
	//
	// This intentionally does NOT select on exportCtx.Done(). On cancellation
	// the goroutine must stay alive until stopWriter() closes quit, so it can
	// perform the final flush with the terminal status (Failed/Canceled).
	// The cost is at most a few unnecessary intermediate flushes between
	// context cancellation and stopWriter() — negligible (small JSON PUTs).
	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		ticker := time.NewTicker(flushInterval)
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

	// Goroutine 2: periodic sibling liveness check.
	if len(req.SiblingNodes) > 0 {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			ticker := time.NewTicker(siblingInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if failedSibling, siblingErr, failed := p.siblingHasFailed(exportCtx, backend, req); failed {
						nodeStatus.SetNodeError(fmt.Sprintf("sibling node %q failed: %s", failedSibling, siblingErr))

						func() {
							p.mu.Lock()
							defer p.mu.Unlock()
							if p.cancelExport != nil {
								p.cancelExport()
							}
						}()
						return
					}
				case <-exportCtx.Done():
					return
				case <-quit:
					return
				}
			}
		}, p.logger)
	}

	return func() {
		once.Do(func() {
			close(quit)
			wg.Wait()
		})
	}
}
