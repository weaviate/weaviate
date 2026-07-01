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

// Package selfrecovery triggers automatic SELF_RECOVERY replication ops
// for shards whose local directories are missing at node startup. Wired
// only into the startup path so scale-out empty replicas aren't
// misread as data loss; the file copy and state machine live in the
// existing replication FSM + consumer.
package selfrecovery

import (
	"container/list"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	replicationtypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// ErrSelfRecoveryCancelled marks a CANCELLED op as terminal so runOne won't retry.
var ErrSelfRecoveryCancelled = errors.New("self-recovery op was cancelled")

// ErrSelfRecoveryShardNotInSchema maps to 404 in the REST handler.
var ErrSelfRecoveryShardNotInSchema = errors.New("shard not in local schema")

// ErrSelfRecoveryShardAlreadyLive maps to 409 in the REST handler.
var ErrSelfRecoveryShardAlreadyLive = errors.New("shard already has a live local directory; /restart is only valid while the shard is RECOVERING")

// RaftEntryPoint is the subset of *cluster.Raft used by the orchestrator.
type RaftEntryPoint interface {
	RegisterSelfRecovery(ctx context.Context, sourceNode, collection, shard, targetNode string) (strfmt.UUID, error)
	GetReplicationDetailsByReplicationId(ctx context.Context, uuid strfmt.UUID) (api.ReplicationDetailsResponse, error)
	GetReplicationDetailsByCollectionAndShard(ctx context.Context, collection, shard string) ([]api.ReplicationDetailsResponse, error)
	CancelReplication(ctx context.Context, uuid strfmt.UUID) error
}

type SchemaReader interface {
	ShardReplicas(class, shard string) (nodes []string, err error)
}

// PathResolver maps (collection, shard) to the local on-disk dir.
type PathResolver interface {
	ShardPath(collection, shard string) string
}

type ShardRef struct {
	Collection string
	Shard      string
}

// Orchestrator coordinates per-shard SELF_RECOVERY work on a single node.
type Orchestrator struct {
	raft                   RaftEntryPoint
	schema                 SchemaReader
	pathResolver           PathResolver
	clientFactory          copier.FileReplicationServiceClientFactory
	nodeSelector           cluster.NodeSelector
	nodeName               string
	enabled                bool
	concurrency            int
	maintenanceModeEnabled func() bool // nil-safe; treated as off when nil
	onRecoveryComplete     func(ctx context.Context, collection, shard string) error
	logger                 logrus.FieldLogger
	pollInterval           time.Duration
	probeTimeout           time.Duration
	probeBackoffMin        time.Duration
	probeBackoffMax        time.Duration
	restartTimeout         time.Duration
	vanishedGracePeriod    time.Duration
	emptyFallbackHook      func(ShardRef) // nil unless overridden in tests
	metrics                *Metrics

	// Worker pool draining an unbounded pending queue (never dropped). Close
	// abandons un-started items; they're re-discovered on the next startup.
	poolOnce       sync.Once
	queueMu        sync.Mutex
	queueCond      *sync.Cond
	pending        *list.List
	closed         bool
	workerWg       sync.WaitGroup
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// rng shuffles peer order; guarded by rngMu (concurrent workers).
	rngMu sync.Mutex
	rng   *rand.Rand

	// shardLocks serialises state-mutating ops per (collection, shard)
	// across runOne and Restart. Keyed "collection/shard"; never shrinks.
	shardLocks sync.Map
}

type submission struct {
	ctx context.Context
	ref ShardRef
	// captured at submit time so the classification can't flip if RAFT
	// bootstrap completes mid-probe
	fromBootstrap bool
}

type Config struct {
	Raft          RaftEntryPoint
	Schema        SchemaReader
	PathResolver  PathResolver
	ClientFactory copier.FileReplicationServiceClientFactory
	NodeSelector  cluster.NodeSelector
	NodeName      string
	Enabled       bool
	Concurrency   int
	// MaintenanceModeEnabled, when non-nil and true, makes Submit a no-op.
	MaintenanceModeEnabled func() bool
	// OnRecoveryComplete promotes the in-memory wrapper after empty-fallback
	// materialises an empty live dir (the op path doesn't need it).
	OnRecoveryComplete func(ctx context.Context, collection, shard string) error
	Logger             logrus.FieldLogger
	PollInterval       time.Duration // FSM poll cadence; 5s if zero
	ProbeTimeout       time.Duration // single ProbeShardData RPC; 5s if zero
}

func New(cfg Config) *Orchestrator {
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	probeTimeout := cfg.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = 5 * time.Second
	}
	logger := cfg.Logger
	if logger == nil {
		logger = logrus.NewEntry(logrus.New())
	}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	return &Orchestrator{
		raft:                   cfg.Raft,
		schema:                 cfg.Schema,
		pathResolver:           cfg.PathResolver,
		clientFactory:          cfg.ClientFactory,
		nodeSelector:           cfg.NodeSelector,
		nodeName:               cfg.NodeName,
		enabled:                cfg.Enabled,
		concurrency:            cfg.Concurrency,
		maintenanceModeEnabled: cfg.MaintenanceModeEnabled,
		onRecoveryComplete:     cfg.OnRecoveryComplete,
		logger:                 logger.WithField("component", "self_recovery"),
		pollInterval:           pollInterval,
		probeTimeout:           probeTimeout,
		probeBackoffMin:        5 * time.Second,
		probeBackoffMax:        5 * time.Minute,
		restartTimeout:         30 * time.Second,
		vanishedGracePeriod:    10 * time.Second,
		metrics:                GlobalMetrics(),
		rng:                    rand.New(rand.NewSource(cryptoSeed())),
		shutdownCtx:            shutdownCtx,
		shutdownCancel:         shutdownCancel,
	}
}

// cryptoSeed seeds the peer-shuffle math/rand so node startups pick
// independent peer orderings without relying on time/pid.
func cryptoSeed() int64 {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return time.Now().UnixNano() ^ int64(os.Getpid())
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

// Submit queues recovery (never drops); false = won't run (off/maintenance/
// shutdown), on which a wrapper-installing caller MUST fall back to normal init.
func (o *Orchestrator) Submit(ctx context.Context, ref ShardRef, fromBootstrap bool) bool {
	if !o.enabled {
		return false
	}
	if o.maintenanceModeEnabled != nil && o.maintenanceModeEnabled() {
		o.logger.WithFields(logrus.Fields{
			"event":      "self_recovery.skipped_maintenance_mode",
			"collection": ref.Collection,
			"shard":      ref.Shard,
		}).Info("self-recovery skipped: node is in maintenance mode")
		return false
	}
	o.poolOnce.Do(o.initPool)
	o.queueMu.Lock()
	defer o.queueMu.Unlock()
	if o.closed {
		return false
	}
	o.pending.PushBack(submission{ctx: ctx, ref: ref, fromBootstrap: fromBootstrap})
	o.queueCond.Signal()
	return true
}

// Enabled reports whether the SELF_RECOVERY feature flag is on.
func (o *Orchestrator) Enabled() bool {
	return o.enabled
}

// SubmitRecovery is the primitive-typed entry point for callers that
// can't import this package without a cycle. See Submit.
func (o *Orchestrator) SubmitRecovery(ctx context.Context, collection, shard string, fromBootstrap bool) bool {
	return o.Submit(ctx, ShardRef{Collection: collection, Shard: shard}, fromBootstrap)
}

// Restart cancels any in-flight SELF_RECOVERY op for the shard, waits for
// it to go terminal so the copier won't race the rmrf, erases
// "<shard>.recovering/", then resubmits. Bounded by restartTimeout.
// Rejects with ErrSelfRecoveryShardAlreadyLive when the live dir exists,
// ErrSelfRecoveryShardNotInSchema when the shard isn't in the schema.
func (o *Orchestrator) Restart(parentCtx context.Context, ref ShardRef) error {
	if o.schema != nil {
		if _, err := o.schema.ShardReplicas(ref.Collection, ref.Shard); err != nil {
			return fmt.Errorf("restart recovery: shard %s/%s: %w",
				ref.Collection, ref.Shard,
				errors.Join(ErrSelfRecoveryShardNotInSchema, err))
		}
	}
	unlock := o.lockShard(ref)
	defer unlock()
	if o.pathResolver != nil {
		livePath := o.pathResolver.ShardPath(ref.Collection, ref.Shard)
		if _, err := os.Stat(livePath); err == nil {
			return fmt.Errorf("restart recovery for %s/%s: %w (cancel any in-flight op via POST /replication/replicate/{id}/cancel)",
				ref.Collection, ref.Shard, ErrSelfRecoveryShardAlreadyLive)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("restart recovery: stat live dir %q: %w", livePath, err)
		}
	}

	ctx, cancel := context.WithTimeout(parentCtx, o.restartTimeout)
	defer cancel()

	cancelled, err := o.cancelInflightSelfRecoveryOps(ctx, ref)
	if err != nil {
		return fmt.Errorf("restart recovery: cancel in-flight op(s): %w", err)
	}

	for _, uuid := range cancelled {
		if err := o.waitForOpTerminal(ctx, uuid); err != nil {
			return fmt.Errorf("restart recovery: wait for op %s to settle: %w", uuid, err)
		}
	}

	if o.pathResolver != nil {
		recoveryPath := o.pathResolver.ShardPath(ref.Collection, ref.Shard) + api.RecoveryFolderSuffix
		if err := os.RemoveAll(recoveryPath); err != nil {
			return fmt.Errorf("restart recovery: remove %q: %w", recoveryPath, err)
		}
	}

	o.logger.WithFields(logrus.Fields{
		"event":         "self_recovery.restart",
		"collection":    ref.Collection,
		"shard":         ref.Shard,
		"cancelled_ops": cancelled,
	}).Warn("operator restarted self-recovery from scratch")

	// WithoutCancel so the recovery outlives the HTTP-bound parentCtx
	if !o.Submit(context.WithoutCancel(parentCtx), ref, false) && o.Enabled() {
		// resubmit refused (maintenance/shutdown); shard stays RECOVERING
		return errors.New("restart recovery: re-submission refused (node in maintenance mode or shutting down); retry shortly")
	}
	return nil
}

// RestartRecovery is the primitive-typed entry point for callers that
// can't import this package without a cycle.
func (o *Orchestrator) RestartRecovery(ctx context.Context, collection, shard string) error {
	return o.Restart(ctx, ShardRef{Collection: collection, Shard: shard})
}

// cancelInflightSelfRecoveryOps cancels every non-terminal SELF_RECOVERY
// op on the shard targeting this node, returning the cancelled UUIDs.
func (o *Orchestrator) cancelInflightSelfRecoveryOps(ctx context.Context, ref ShardRef) ([]strfmt.UUID, error) {
	if o.raft == nil {
		return nil, nil
	}
	ops, err := o.raft.GetReplicationDetailsByCollectionAndShard(ctx, ref.Collection, ref.Shard)
	if err != nil {
		if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var cancelled []strfmt.UUID
	for _, op := range ops {
		if op.TransferType != api.SELF_RECOVERY.String() {
			continue
		}
		if op.TargetNodeId != o.nodeName {
			continue
		}
		state := api.ShardReplicationState(op.Status.State)
		if state == api.READY || state == api.CANCELLED {
			continue
		}
		if err := o.raft.CancelReplication(ctx, op.Uuid); err != nil {
			return cancelled, fmt.Errorf("cancel op %s: %w", op.Uuid, err)
		}
		// runOne ticks CompletedTotal{cancelled} on observing CANCELLED;
		// don't double-count here
		cancelled = append(cancelled, op.Uuid)
	}
	return cancelled, nil
}

// HasInflightReplicationOp reports whether any non-terminal replication op
// targets the shard on this node. The startup hook checks it first so a
// node restarting mid scale-out COPY doesn't register a duplicate
// SELF_RECOVERY op and clobber the resumed COPY. Treat errors as "skip".
func (o *Orchestrator) HasInflightReplicationOp(ctx context.Context, collection, shard string) (bool, error) {
	if o.raft == nil {
		return false, nil
	}
	ops, err := o.raft.GetReplicationDetailsByCollectionAndShard(ctx, collection, shard)
	if err != nil {
		if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
			return false, nil
		}
		return false, err
	}
	for _, op := range ops {
		if op.TargetNodeId != o.nodeName {
			continue
		}
		switch api.ShardReplicationState(op.Status.State) {
		case api.READY, api.CANCELLED:
		default:
			return true, nil
		}
	}
	return false, nil
}

// HasInflightSelfRecoveryOp reports a non-terminal SELF_RECOVERY op targeting
// this shard on this node — the crash-restart resume case (live dir stays missing).
func (o *Orchestrator) HasInflightSelfRecoveryOp(ctx context.Context, collection, shard string) (bool, error) {
	if o.raft == nil {
		return false, nil
	}
	ops, err := o.raft.GetReplicationDetailsByCollectionAndShard(ctx, collection, shard)
	if err != nil {
		if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
			return false, nil
		}
		return false, err
	}
	for _, op := range ops {
		if op.TransferType != api.SELF_RECOVERY.String() {
			continue
		}
		if op.TargetNodeId != o.nodeName {
			continue
		}
		switch api.ShardReplicationState(op.Status.State) {
		case api.READY, api.CANCELLED:
		default:
			return true, nil
		}
	}
	return false, nil
}

// waitForOpTerminal polls the FSM until the op reaches READY or CANCELLED.
// A vanished op is terminal, but with a grace sleep so a still-running
// consumer goroutine can observe the cancellation.
func (o *Orchestrator) waitForOpTerminal(ctx context.Context, uuid strfmt.UUID) error {
	if o.raft == nil {
		return nil
	}
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		details, err := o.raft.GetReplicationDetailsByReplicationId(ctx, uuid)
		if err != nil {
			if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
				if !sleepCtx(ctx, o.vanishedGracePeriod) {
					return ctx.Err()
				}
				return nil
			}
			// transient (e.g. leader change) — keep polling
		} else {
			switch api.ShardReplicationState(details.Status.State) {
			case api.READY, api.CANCELLED:
				return nil
			case api.REGISTERED, api.HYDRATING, api.FINALIZING, api.INTEGRATING, api.DEHYDRATING:
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// CleanupOrphanRecoveryDirs removes "<shard>.recovering/" dirs whose live
// "<shard>/" sibling exists; in-flight recoveries (no sibling) are kept.
func (o *Orchestrator) CleanupOrphanRecoveryDirs(rootDataPath string) ([]string, error) {
	const suffix = api.RecoveryFolderSuffix
	if rootDataPath == "" {
		return nil, errors.New("cleanup orphan recovery dirs: empty root data path")
	}
	collections, err := os.ReadDir(rootDataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read data root %q: %w", rootDataPath, err)
	}
	var removed []string
	for _, c := range collections {
		if !c.IsDir() {
			continue
		}
		collDir := filepath.Join(rootDataPath, c.Name())
		shards, err := os.ReadDir(collDir)
		if err != nil {
			o.logger.WithError(err).WithField("dir", collDir).Warn("cleanup: cannot read collection dir")
			continue
		}
		for _, s := range shards {
			if !s.IsDir() || !strings.HasSuffix(s.Name(), suffix) {
				continue
			}
			recoveryDir := filepath.Join(collDir, s.Name())
			liveDir := filepath.Join(collDir, strings.TrimSuffix(s.Name(), suffix))
			if _, err := os.Stat(liveDir); err != nil {
				continue // no sibling: in-flight recovery to resume
			}
			if err := os.RemoveAll(recoveryDir); err != nil {
				o.logger.WithError(err).WithField("dir", recoveryDir).Warn("cleanup: failed to remove orphan recovery dir")
				continue
			}
			o.logger.WithField("dir", recoveryDir).Info("cleanup: removed orphan recovery dir")
			removed = append(removed, recoveryDir)
		}
	}
	return removed, nil
}

// AcceptEmpty is the operator escape hatch for the catastrophic-wipe case:
// erases "<shard>.recovering/", creates an empty "<shard>/", and promotes
// the wrapper so the shard is serviceable. Does NOT cancel in-flight RAFT
// ops — operator should cancel first.
func (o *Orchestrator) AcceptEmpty(ctx context.Context, ref ShardRef) (string, error) {
	if o.pathResolver == nil {
		return "", errors.New("accept-empty: no PathResolver configured")
	}
	// refuse unknown shards so the endpoint can't create arbitrary paths
	if o.schema != nil {
		if _, err := o.schema.ShardReplicas(ref.Collection, ref.Shard); err != nil {
			return "", fmt.Errorf("accept-empty: shard %s/%s: %w",
				ref.Collection, ref.Shard,
				errors.Join(ErrSelfRecoveryShardNotInSchema, err))
		}
	}
	unlock := o.lockShard(ref)
	defer unlock()
	livePath := o.pathResolver.ShardPath(ref.Collection, ref.Shard)
	recoveryPath := livePath + api.RecoveryFolderSuffix

	if _, err := os.Stat(recoveryPath); err == nil {
		if err := os.RemoveAll(recoveryPath); err != nil {
			return "", fmt.Errorf("remove recovery dir %q: %w", recoveryPath, err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("stat recovery dir %q: %w", recoveryPath, err)
	}
	if err := os.MkdirAll(livePath, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %q: %w", livePath, err)
	}
	if err := diskio.Fsync(filepath.Dir(livePath)); err != nil {
		return "", fmt.Errorf("fsync parent of %q: %w", livePath, err)
	}
	if o.onRecoveryComplete != nil {
		if err := o.onRecoveryComplete(ctx, ref.Collection, ref.Shard); err != nil {
			return "", fmt.Errorf("accept-empty: promote in-memory wrapper for %s/%s: %w",
				ref.Collection, ref.Shard, err)
		}
	}
	if o.metrics != nil {
		o.metrics.AcceptEmptyTotal.Inc()
	}
	o.logger.WithFields(logrus.Fields{
		"event":      "self_recovery.accept_empty",
		"collection": ref.Collection,
		"shard":      ref.Shard,
		"path":       livePath,
	}).Warn("operator accepted empty shard; recovery aborted")
	return livePath, nil
}

// runOne is the per-shard worker: probe peers, act on the decision, and
// back off & retry on transient errors up to maxAttempts. On give-up the
// shard is left in RECOVERING for operator recovery via the endpoints.
func (o *Orchestrator) runOne(ctx context.Context, ref ShardRef, fromBootstrap bool) {
	unlock := o.lockShard(ref)
	defer unlock()
	// bind per-op ctx to shutdownCtx so probes/polls bail when Close fires
	ctx, cancel := mergedCtx(ctx, o.shutdownCtx)
	defer cancel()

	logger := o.logger.WithFields(logrus.Fields{
		"event":      "self_recovery.started",
		"collection": ref.Collection,
		"shard":      ref.Shard,
	})
	logger.Info("starting self-recovery for shard")

	startedAt := time.Now()
	if o.metrics != nil {
		o.metrics.InProgress.Inc()
		defer o.metrics.InProgress.Dec()
	}

	const maxAttempts = 10
	attempts := 0
	backoff := o.probeBackoffMin

	retryAfterBackoff := func() bool {
		attempts++
		if !sleepCtx(ctx, backoff) {
			return false
		}
		backoff = nextBackoff(backoff, o.probeBackoffMax)
		return true
	}

	for {
		if ctx.Err() != nil {
			return
		}
		if attempts >= maxAttempts {
			logger.WithField("attempts", attempts).Error("self-recovery exhausted retries; giving up. " +
				"Shard stays in RECOVERING — use POST /debug/self-recovery/restart to retry from scratch, " +
				"or POST /debug/self-recovery/accept-empty to accept an empty shard")
			if o.metrics != nil {
				o.metrics.GiveupTotal.Inc()
			}
			o.recordOutcome("failure", "failure", startedAt)
			return
		}

		decision, err := o.probeAndDecide(ctx, ref)
		if err != nil {
			logger.WithError(err).Warn("self-recovery probe failed; will retry")
			if !retryAfterBackoff() {
				return
			}
			continue
		}

		switch decision.action {
		case actionRegisterOp:
			done, retry := o.handleRegisterDecision(ctx, ref, decision, startedAt, logger)
			if done {
				return
			}
			if retry && !retryAfterBackoff() {
				return
			}
		case actionEmptyFallback:
			o.handleEmptyFallback(ctx, ref, decision, startedAt, fromBootstrap, logger)
			return
		case actionRetry:
			logger.WithField("retry_in", backoff.String()).Debug("self-recovery: peers unreachable, will retry")
			if !retryAfterBackoff() {
				return
			}
		}
	}
}

// recordOutcome ticks completed_total and duration_seconds. Nil-safe.
func (o *Orchestrator) recordOutcome(completedResult, durationResult string, startedAt time.Time) {
	if o.metrics == nil {
		return
	}
	o.metrics.CompletedTotal.WithLabelValues(completedResult).Inc()
	o.metrics.DurationSeconds.WithLabelValues(durationResult).Observe(time.Since(startedAt).Seconds())
}

// handleRegisterDecision registers a SELF_RECOVERY op and polls it to
// terminal. done=true means nothing more to attempt (READY/cancelled/
// force-deleted); retry=true means back off and probe again.
func (o *Orchestrator) handleRegisterDecision(ctx context.Context, ref ShardRef, decision probeDecision,
	startedAt time.Time, logger logrus.FieldLogger,
) (done, retry bool) {
	if o.metrics != nil {
		o.metrics.StartedTotal.WithLabelValues(decision.sourceNode).Inc()
	}
	err := o.registerAndPoll(ctx, ref, decision.sourceNode)
	if err == nil {
		o.recordOutcome("success", "success", startedAt)
		logger.WithFields(logrus.Fields{
			"event":       "self_recovery.completed",
			"source_node": decision.sourceNode,
			"duration_ms": time.Since(startedAt).Milliseconds(),
		}).Info("self-recovery completed")
		return true, false
	}
	// force-delete and operator cancel are terminal; retrying would
	// re-register a fresh op and negate them
	switch {
	case errors.Is(err, replicationtypes.ErrReplicationOperationNotFound):
		logger.WithError(err).WithField("source_node", decision.sourceNode).
			Info("self-recovery op was force-deleted; abandoning")
		o.recordOutcome("cancelled", "cancelled", startedAt)
		return true, false
	case errors.Is(err, ErrSelfRecoveryCancelled):
		logger.WithError(err).WithField("source_node", decision.sourceNode).
			Info("self-recovery op cancelled; abandoning")
		o.recordOutcome("cancelled", "cancelled", startedAt)
		return true, false
	default:
		logger.WithError(err).WithField("source_node", decision.sourceNode).
			Warn("self-recovery register/poll failed; will retry")
		return false, true
	}
}

// handleEmptyFallback materialises an empty live shard dir, promotes the
// wrapper, and records the outcome. fromBootstrap selects the gentler
// log/metric treatment (all-peers-empty during bootstrap most likely
// means a class added during downtime, not data loss).
func (o *Orchestrator) handleEmptyFallback(ctx context.Context, ref ShardRef, decision probeDecision,
	startedAt time.Time, fromBootstrap bool, logger logrus.FieldLogger,
) {
	if err := o.emptyFallback(ref); err != nil {
		logger.WithError(err).Error("self-recovery empty-fallback failed")
		o.recordOutcome("failure", "failure", startedAt)
		return
	}
	// a failed promote would strand the shard in RECOVERING; record
	// failure rather than misreport success
	if o.onRecoveryComplete != nil {
		if err := o.onRecoveryComplete(ctx, ref.Collection, ref.Shard); err != nil {
			logger.WithError(err).Error("self-recovery: promote after empty-fallback failed")
			o.recordOutcome("failure", "failure", startedAt)
			return
		}
	}
	if o.metrics != nil {
		if fromBootstrap {
			o.metrics.NoDataDuringBootstrapTotal.Inc()
		} else {
			o.metrics.NoDataEmptyTotal.Inc()
		}
	}
	o.recordOutcome("empty_fallback", "empty_fallback", startedAt)

	fallbackFields := logrus.Fields{
		"event":        "self_recovery.empty_fallback",
		"probed_peers": decision.probedPeers,
		"duration_ms":  time.Since(startedAt).Milliseconds(),
		"collection":   ref.Collection,
		"shard":        ref.Shard,
		"action_taken": "created_empty_shard",
	}
	if fromBootstrap {
		logger.WithFields(fallbackFields).
			Info("no peer has data for shard during RAFT bootstrap; treating as fresh class")
	} else {
		fallbackFields["recoverable"] = false
		fallbackFields["operator_note"] = "if data is recoverable from backup, restore now"
		logger.WithFields(fallbackFields).
			Warn("no peer has data for shard; created empty shard")
	}
	if o.emptyFallbackHook != nil {
		o.emptyFallbackHook(ref)
	}
}

type recoveryAction int

const (
	actionRegisterOp recoveryAction = iota
	actionEmptyFallback
	actionRetry
)

type probeDecision struct {
	action      recoveryAction
	sourceNode  string
	probedPeers []string
}

func (o *Orchestrator) probeAndDecide(ctx context.Context, ref ShardRef) (probeDecision, error) {
	replicas, err := o.schema.ShardReplicas(ref.Collection, ref.Shard)
	if err != nil {
		return probeDecision{}, fmt.Errorf("read shard replicas: %w", err)
	}

	peers := make([]string, 0, len(replicas))
	for _, n := range replicas {
		if n != o.nodeName {
			peers = append(peers, n)
		}
	}
	if len(peers) == 0 {
		// only us per schema: nothing to recover from
		return probeDecision{action: actionEmptyFallback, probedPeers: nil}, nil
	}

	// shuffle so recovering nodes don't all pick the same data-bearing peer
	o.rngMu.Lock()
	o.rng.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	o.rngMu.Unlock()

	type probeResult struct {
		peer       string
		hasData    bool
		definitive bool
		err        error
	}
	results := make([]probeResult, len(peers))
	var wg sync.WaitGroup
	for i, peer := range peers {
		i, peer := i, peer
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			h, d, e := o.probePeer(ctx, peer, ref)
			results[i] = probeResult{peer: peer, hasData: h, definitive: d, err: e}
		}, o.logger)
	}
	wg.Wait()

	var (
		probedPeers        = make([]string, 0, len(results))
		anyDefinitiveEmpty bool
		anyUnreachable     bool
		firstSource        string
	)
	for _, r := range results {
		probedPeers = append(probedPeers, r.peer)
		if r.err != nil {
			anyUnreachable = true
			if o.metrics != nil {
				o.metrics.UnreachablePeerTotal.WithLabelValues(r.peer).Inc()
			}
			o.logger.WithError(r.err).WithFields(logrus.Fields{
				"event":      "self_recovery.peer_probe",
				"collection": ref.Collection,
				"shard":      ref.Shard,
				"peer":       r.peer,
				"result":     "unreachable",
			}).Debug("peer probe failed")
			continue
		}
		if r.hasData && firstSource == "" {
			firstSource = r.peer
		}
		if r.definitive && !r.hasData {
			anyDefinitiveEmpty = true
		}
	}

	if firstSource != "" {
		return probeDecision{
			action:      actionRegisterOp,
			sourceNode:  firstSource,
			probedPeers: probedPeers,
		}, nil
	}
	if anyUnreachable {
		// don't create empty when probes were inconclusive
		return probeDecision{action: actionRetry, probedPeers: probedPeers}, nil
	}
	if anyDefinitiveEmpty {
		return probeDecision{action: actionEmptyFallback, probedPeers: probedPeers}, nil
	}
	return probeDecision{action: actionRetry, probedPeers: probedPeers}, nil
}

// probePeer reports whether peer has data for the shard. definitive=true
// means the peer answered; err != nil means transport/timeout (retry).
func (o *Orchestrator) probePeer(ctx context.Context, peer string, ref ShardRef) (hasData bool, definitive bool, err error) {
	addr := o.nodeSelector.NodeAddress(peer)
	if addr == "" {
		return false, false, fmt.Errorf("no address for peer %q", peer)
	}
	port, err := o.nodeSelector.NodeGRPCPort(peer)
	if err != nil {
		return false, false, fmt.Errorf("get gRPC port for peer %q: %w", peer, err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, o.probeTimeout)
	defer cancel()

	client, err := o.clientFactory(probeCtx, net.JoinHostPort(addr, fmt.Sprintf("%d", port)))
	if err != nil {
		return false, false, fmt.Errorf("connect to peer %q: %w", peer, err)
	}

	resp, err := client.ProbeShardData(probeCtx, &protocol.ProbeShardDataRequest{
		IndexName: ref.Collection,
		ShardName: ref.Shard,
	})
	if err != nil {
		// NotFound = definitive no-data; Unavailable = peer busy (transient)
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.NotFound:
				return false, true, nil
			case codes.Unavailable:
				// Peer recovering this same shard has no data: definitive-empty so an
				// all-fresh formation breaks the deadlock instead of retrying forever.
				if isPeerRecoveringErr(err) {
					return false, true, nil
				}
				return false, false, fmt.Errorf("peer %q unavailable: %w", peer, err)
			default:
			}
		}
		// rolling-upgrade fallback for peers without typed codes
		if isShardAbsentErr(err) {
			return false, true, nil
		}
		return false, false, fmt.Errorf("probe shard data on peer %q: %w", peer, err)
	}
	return resp.GetHasData(), true, nil
}

// isPeerRecoveringErr reports whether a peer's probe error means it is
// itself recovering the shard (so it has no data to copy).
func isPeerRecoveringErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "is recovering")
}

// isShardAbsentErr is the rolling-upgrade fallback for peers not yet
// sending typed gRPC codes. Match only shard-specific phrasings — a bare
// "not found" would misclassify e.g. "file X not found".
func isShardAbsentErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "incoming list files get shard is nil"):
		return true
	case strings.Contains(msg, "shard is nil"):
		return true
	case strings.Contains(msg, "shard not found"):
		return true
	}
	return false
}

// registerAndPoll registers a SELF_RECOVERY op and polls the FSM until
// terminal: nil on READY, error on CANCELLED or poll failure,
// ErrReplicationOperationNotFound if the op vanished (force-deleted).
func (o *Orchestrator) registerAndPoll(ctx context.Context, ref ShardRef, sourceNode string) error {
	uuid, err := o.raft.RegisterSelfRecovery(ctx, sourceNode, ref.Collection, ref.Shard, o.nodeName)
	if err != nil {
		return fmt.Errorf("register self-recovery op: %w", err)
	}

	o.logger.WithFields(logrus.Fields{
		"event":       "self_recovery.op_registered",
		"collection":  ref.Collection,
		"shard":       ref.Shard,
		"source_node": sourceNode,
		"op_uuid":     uuid,
	}).Info("self-recovery op registered; polling for completion")

	// tolerate transient not-founds (e.g. leader change) before concluding
	// the op was force-deleted
	const notFoundThreshold = 3
	notFoundCount := 0

	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			details, err := o.raft.GetReplicationDetailsByReplicationId(ctx, uuid)
			if err != nil {
				if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
					notFoundCount++
					if notFoundCount >= notFoundThreshold {
						return fmt.Errorf("self-recovery op %s vanished from FSM (force-deleted upstream): %w", uuid, replicationtypes.ErrReplicationOperationNotFound)
					}
				}
				continue // transient (e.g. leader change) — keep polling
			}
			notFoundCount = 0
			switch api.ShardReplicationState(details.Status.State) {
			case api.READY:
				return nil
			case api.CANCELLED:
				return fmt.Errorf("self-recovery op %s: %w", uuid, ErrSelfRecoveryCancelled)
			case api.REGISTERED, api.HYDRATING, api.FINALIZING, api.INTEGRATING, api.DEHYDRATING:
			}
		}
	}
}

// emptyFallback creates an empty live shard dir; reached only when all
// probed peers definitively reported no data.
func (o *Orchestrator) emptyFallback(ref ShardRef) error {
	if o.pathResolver == nil {
		return errors.New("empty-fallback: no PathResolver configured")
	}
	dir := o.pathResolver.ShardPath(ref.Collection, ref.Shard)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", dir, err)
	}
	if err := diskio.Fsync(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("fsync parent of %q: %w", dir, err)
	}
	return nil
}

// initPool spawns the worker pool on first Submit; each worker pops one item,
// so concurrency alone (no queue cap) bounds in-flight recoveries.
func (o *Orchestrator) initPool() {
	n := 1
	if o.concurrency > 0 {
		n = o.concurrency
	}
	// under queueMu so a Close racing the first Submit can't data-race these
	// writes; bail if Close already won
	o.queueMu.Lock()
	if o.closed {
		o.queueMu.Unlock()
		return
	}
	o.pending = list.New()
	o.queueCond = sync.NewCond(&o.queueMu)
	o.workerWg.Add(n)
	o.queueMu.Unlock()
	for i := 0; i < n; i++ {
		enterrors.GoWrapper(func() {
			defer o.workerWg.Done()
			for {
				o.queueMu.Lock()
				for o.pending.Len() == 0 && !o.closed {
					o.queueCond.Wait()
				}
				if o.closed {
					o.queueMu.Unlock()
					return
				}
				front := o.pending.Front()
				o.pending.Remove(front)
				o.queueMu.Unlock()
				sub := front.Value.(submission)
				o.runOne(sub.ctx, sub.ref, sub.fromBootstrap)
			}
		}, o.logger)
	}
}

// Close cancels shutdownCtx and waits for workers (bounded by ctx). Un-started
// items are abandoned, re-discovered next startup. Idempotent; safe pre-init.
func (o *Orchestrator) Close(ctx context.Context) error {
	o.queueMu.Lock()
	if o.closed {
		o.queueMu.Unlock()
		return nil
	}
	o.closed = true
	if o.queueCond != nil {
		o.queueCond.Broadcast()
	}
	o.queueMu.Unlock()
	o.shutdownCancel()

	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		o.workerWg.Wait()
		close(done)
	}, o.logger)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// mergedCtx returns a context cancelled when EITHER parent or sibling is.
func mergedCtx(parent, sibling context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(sibling, cancel)
	return ctx, func() { stop(); cancel() }
}

// lockShard acquires the per-shard mutex and returns a release closure.
func (o *Orchestrator) lockShard(ref ShardRef) func() {
	key := ref.Collection + "/" + ref.Shard
	m, _ := o.shardLocks.LoadOrStore(key, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
