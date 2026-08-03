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

package rest

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// dropVectorIndexEnqueuer implements schema.DropVectorIndexEnqueuer. It submits
// the background cleanup distributed task and reports whether one is in flight,
// using the cluster DTM client + sharding state. Lives in the REST wiring layer
// so it can reuse buildUnitMaps/buildUnitSpecs.
type dropVectorIndexEnqueuer struct {
	clusterService clusterDropTaskClient
	schemaState    schemaStateQuerier
	logger         logrus.FieldLogger // nil-safe: only used for skip warnings
	// finalizer removes dropped VectorConfig entries directly — the escape
	// for MT collections with ZERO tenants, where no cleanup task can ever
	// exist to drive the finalize. Installed post-construction
	// (SetFinalizer): the schema manager does not exist yet when the
	// enqueuer is built. Nil-safe.
	finalizer dropVectorFinalizer
}

// dropVectorFinalizer is the slice of the schema finalizer the enqueuer uses.
type dropVectorFinalizer interface {
	RemoveDroppedVectorConfig(ctx context.Context, collection string, targets []string) error
}

// clusterDropTaskClient is the slice of the cluster service the enqueuer uses.
type clusterDropTaskClient interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
	AddDistributedTaskWithGroups(ctx context.Context, namespace, taskID string,
		taskPayload any, unitSpecs []distributedtask.UnitSpec) error
}

// schemaStateQuerier provides leader-consistent schema reads for a collection:
// the sharding state (units must be built from the current tenant statuses, not
// this node's eventually-consistent local view — a tenant activated moments ago
// could still read COLD locally and be skipped) and the class (targets are
// re-validated as still marked dropped right before submitting the destructive
// cleanup task). cluster.Raft satisfies it.
type schemaStateQuerier interface {
	QueryShardingState(class string) (*sharding.State, uint64, error)
	QueryReadOnlyClasses(classes ...string) (map[string]versioned.Class, error)
}

func newDropVectorIndexEnqueuer(clusterService clusterDropTaskClient, schemaState schemaStateQuerier, logger logrus.FieldLogger) *dropVectorIndexEnqueuer {
	return &dropVectorIndexEnqueuer{clusterService: clusterService, schemaState: schemaState, logger: logger}
}

// SetFinalizer installs the direct-finalize hook (see the field doc). Must be
// called before ClusterService.Open, alongside the rest of the wiring.
func (e *dropVectorIndexEnqueuer) SetFinalizer(f dropVectorFinalizer) {
	e.finalizer = f
}

// logInfo logs an enqueue-path decision (nil-safe like every logger use here).
func (e *dropVectorIndexEnqueuer) logInfo(collection, msg string) {
	if e.logger != nil {
		e.logger.WithField("collection", collection).Info(msg)
	}
}

// warnSkippedPayload surfaces an undecodable active-task payload instead of
// silently skipping it (the skip itself is deliberate fail-open behavior).
// Package-level so no skip site can silently re-inline a divergent copy.
func warnSkippedPayload(logger logrus.FieldLogger, where, taskID string, err error) {
	if logger != nil {
		logger.WithField("task", taskID).
			Warnf("drop-vector %s: skipping active task with unparseable payload: %v", where, err)
	}
}

// ListDistributedTasks exposes the cluster task list for the reconcile loop
// (one fetch per round) and its startup readiness probe.
func (e *dropVectorIndexEnqueuer) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return e.clusterService.ListDistributedTasks(ctx)
}

// HasActiveDrop reports whether a non-terminal drop task already covers
// targetVector on collection.
func (e *dropVectorIndexEnqueuer) HasActiveDrop(ctx context.Context, collection, targetVector string) (bool, error) {
	tasks, err := e.clusterService.ListDistributedTasks(ctx)
	if err != nil {
		return false, err
	}
	return activeDropCovers(tasks, collection, targetVector, e.logger), nil
}

// activeDropCovers reports whether a non-terminal drop task in tasks covers
// targetVector on collection. Shared by HasActiveDrop and the reconcile loop
// (which fetches the task list once per round instead of once per marker).
func activeDropCovers(tasks map[string][]*distributedtask.Task, collection, targetVector string,
	logger logrus.FieldLogger,
) bool {
	return db.ActiveDropCovers(tasks[db.DropVectorIndexNamespace], collection, targetVector, logger)
}

// EnqueueDropVectorIndex submits a fresh cleanup task with one unit per
// (shard, replica) grouped by shard. Shards already cleaned by this drop's
// earlier tasks get no unit.
func (e *dropVectorIndexEnqueuer) EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string) error {
	tasks, err := e.clusterService.ListDistributedTasks(ctx)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: list tasks for %q: %w", collection, err)
	}
	return e.EnqueueDropVectorIndexWithTasks(ctx, collection, targets, tasks)
}

// EnqueueDropVectorIndexWithTasks is EnqueueDropVectorIndex against an
// already-fetched task list, so the reconcile loop pays ONE ListDistributedTasks
// per round instead of one per marker. A slightly stale list is safe: the
// AddTask-apply guard (CheckConflict) re-proves the coverage claim against the
// FSM's live records, and any rejection is retried by the next round.
func (e *dropVectorIndexEnqueuer) EnqueueDropVectorIndexWithTasks(ctx context.Context, collection string,
	targets []string, tasks map[string][]*distributedtask.Task,
) error {
	// Re-validate against the leader-consistent class: the marker commit and this
	// enqueue are not atomic, and reconciliation may run off a stale local schema
	// snapshot. A target that is no longer marked dropped (class deleted and
	// re-created, or already finalized elsewhere) must not get a cleanup task —
	// that task would strip a live vector.
	targets, err := e.stillDroppedTargets(collection, targets)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: verify targets for %q: %w", collection, err)
	}
	if len(targets) == 0 {
		return nil // nothing (still) marked dropped — no-op
	}

	state, _, err := e.schemaState.QueryShardingState(collection)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: sharding state for %q: %w", collection, err)
	}
	if state == nil {
		return fmt.Errorf("drop-vector enqueue: no sharding state for collection %q", collection)
	}
	shardOwnership := activeShardOwnership(state)
	if len(shardOwnership) == 0 {
		// A non-MT collection always has shards, so an empty map there is a
		// real problem.
		if !state.PartitioningEnabled {
			return fmt.Errorf("drop-vector enqueue: no shards for collection %q", collection)
		}
		if len(state.Physical) == 0 {
			// ZERO tenants (never created, or all deleted after the marker
			// landed): no cleanup task can ever exist, so nothing would
			// drive the finalize — remove the entries directly. There is no
			// data to strip, and the FSM removal gate explicitly allows the
			// empty-shard-set case for exactly this reason.
			if e.finalizer == nil {
				e.logInfo(collection, "drop-vector enqueue: collection has no tenants but no finalizer is wired; the marker stays")
				return nil
			}
			if err := e.finalizer.RemoveDroppedVectorConfig(ctx, collection, targets); err != nil {
				return fmt.Errorf("drop-vector enqueue: finalize tenant-less collection %q: %w", collection, err)
			}
			e.logInfo(collection, "drop-vector enqueue: collection has no tenants; dropped vector entries removed directly")
			return nil
		}
		// Tenants exist but none is active: the marker is already applied —
		// a no-op success, not an error. Reconciliation re-enqueues once a
		// tenant is activated.
		e.logInfo(collection, "drop-vector enqueue: all tenants inactive; the marker stays until a tenant is activated")
		return nil
	}

	epoch, cleaned := db.EpochAndInheritedCoverage(collection, targets, state, tasks, e.logger)
	shardOwnership = withoutCleanedShards(shardOwnership, cleaned)
	shardOwnership, deferredShards := capShardOwnership(shardOwnership, maxShardsPerDropRound)
	if deferredShards > 0 {
		e.logInfo(collection, fmt.Sprintf(
			"drop-vector enqueue: round capped at %d shards, %d deferred to follow-up rounds (coverage chains via cleaned shards)",
			maxShardsPerDropRound, deferredShards))
	}
	if len(shardOwnership) == 0 {
		// Inherited coverage is always incomplete (a complete chain mints a
		// fresh epoch with no inheritance), so an emptied ownership map means
		// every ACTIVE shard is cleaned and the remainder is inactive.
		e.logInfo(collection, "drop-vector enqueue: all active shards already cleaned; the marker stays until the remaining shards' tenants are activated")
		return nil
	}

	// Known limitation (matches the reindex model): a unit is emitted per
	// (shard, replica) with no liveness filter, and the DTM never reassigns a
	// claimed unit. A permanently-removed replica leaves its unit non-terminal and
	// the task STARTED until an operator removes the node; no data is at risk (the
	// marker stays and reconciliation resumes after the topology is repaired).
	_, unitToShard, unitToNode := buildUnitMaps(shardOwnership)
	specs := buildUnitSpecs(shardOwnership)

	// Pass the payload struct, not pre-marshaled bytes: the cluster layer
	// json.Marshals taskPayload itself (bytes would be double-encoded into a JSON
	// string and fail to decode in CheckConflict / the provider).
	payload := db.DropVectorIndexTaskPayload{
		Collection: collection,
		Targets:    targets,
		// One op identity per drop EPOCH, not per round: every round of the
		// same drop shares the op, so a round re-arming after an interrupted
		// strip (deactivated tenant, failed round) finds the recorded
		// pending set and RESUMES from it — the arm is idempotent
		// (HasPendingSnapshot) and load-time recovery preserves progress. A
		// fresh epoch (new drop of a re-created name, expired records) means
		// a fresh op and a full clean.
		OpID:          epoch,
		UnitToNode:    unitToNode,
		UnitToShard:   unitToShard,
		DropEpochID:   epoch,
		CleanedShards: cleaned,
	}

	// Fresh task ID per submission so a re-trigger after a FAILED run is a new
	// task version. The ConflictDetector rejects a duplicate against an active
	// task, the backstop for the HasActiveDrop check race.
	taskID := uuid.NewString()
	return e.clusterService.AddDistributedTaskWithGroups(ctx, db.DropVectorIndexNamespace, taskID, payload, specs)
}

// maxShardsPerDropRound bounds one cleanup round. Units scale with
// shards × replicas and ride a single RAFT AddTask entry (payload maps plus a
// FSM Unit struct each): an unbounded MT collection would put tens of MB on
// the log — replicated, snapshotted, resident on every node — and arming
// serializes a memtable flush + sidecar open per shard before the first
// drain. The remainder chains through follow-up rounds via CleanedShards
// inheritance; finalize still requires a single task covering everyone,
// which the LAST batch satisfies (its units plus the inherited cleaned set).
var maxShardsPerDropRound = 1000 // var: tests shrink it to pin batching

// dropVectorNudgeDelay spaces a nudge-triggered round past the finishing
// task's SWAPPING→FINISHED ack window (normally well under a second); var so
// tests can shrink it.
var dropVectorNudgeDelay = 3 * time.Second

// capShardOwnership deterministically (sorted shard names) keeps at most max
// DISTINCT shards of the node→shards ownership map, reporting how many shards
// were deferred to later rounds. A kept shard keeps ALL its replicas — units
// are per (shard, replica) and a shard's group completes only when every
// replica's unit does.
func capShardOwnership(ownership map[string][]string, max int) (map[string][]string, int) {
	distinct := map[string]struct{}{}
	for _, shards := range ownership {
		for _, shard := range shards {
			distinct[shard] = struct{}{}
		}
	}
	if len(distinct) <= max {
		return ownership, 0
	}
	names := make([]string, 0, len(distinct))
	for name := range distinct {
		names = append(names, name)
	}
	sort.Strings(names)
	keep := make(map[string]struct{}, max)
	for _, name := range names[:max] {
		keep[name] = struct{}{}
	}
	capped := make(map[string][]string, len(ownership))
	for node, shards := range ownership {
		var kept []string
		for _, shard := range shards {
			if _, ok := keep[shard]; ok {
				kept = append(kept, shard)
			}
		}
		if len(kept) > 0 {
			capped[node] = kept
		}
	}
	return capped, len(names) - max
}

// withoutCleanedShards strips already-cleaned shards from the ownership map,
// removing nodes left with no shards.
func withoutCleanedShards(ownership map[string][]string, cleaned []string) map[string][]string {
	if len(cleaned) == 0 {
		return ownership
	}
	skip := make(map[string]struct{}, len(cleaned))
	for _, shard := range cleaned {
		skip[shard] = struct{}{}
	}
	result := make(map[string][]string, len(ownership))
	for node, shards := range ownership {
		var kept []string
		for _, shard := range shards {
			if _, ok := skip[shard]; !ok {
				kept = append(kept, shard)
			}
		}
		if len(kept) > 0 {
			result[node] = kept
		}
	}
	return result
}

// stillDroppedTargets filters targets to those still present and marked dropped
// in the leader-consistent class. A missing class means nothing to clean.
func (e *dropVectorIndexEnqueuer) stillDroppedTargets(collection string, targets []string) ([]string, error) {
	vclasses, err := e.schemaState.QueryReadOnlyClasses(collection)
	if err != nil {
		return nil, err
	}
	return droppedTargetsIn(vclasses[collection].Class, targets), nil
}

// droppedTargetsIn filters targets to those present and marked dropped in
// class. A nil class means nothing to clean.
func droppedTargetsIn(class *models.Class, targets []string) []string {
	if class == nil {
		return nil
	}
	var still []string
	for _, target := range targets {
		if cfg, ok := class.VectorConfig[target]; ok && modelsext.IsVectorIndexDropped(cfg) {
			still = append(still, target)
		}
	}
	return still
}

// activeShardOwnership builds node -> shard-names from a sharding state, limited
// to shards with locally loaded data: for multi-tenant collections only HOT/ACTIVE
// tenants (a cleanup unit on a deactivated tenant's shard would load it and
// prematurely remove its files; such tenants are picked up by a later
// reconciliation once active); for non-MT collections all shards. Shard lists are
// sorted per node for determinism.
func activeShardOwnership(state *sharding.State) map[string][]string {
	result := make(map[string][]string)
	for shardName, shard := range state.Physical {
		if state.PartitioningEnabled {
			switch entschema.ActivityStatus(shard.Status) {
			case models.TenantActivityStatusHOT, models.TenantActivityStatusACTIVE:
				// Loaded — include.
			default:
				continue
			}
		}
		for _, node := range shard.BelongsToNodes {
			if node != "" {
				result[node] = append(result[node], shardName)
			}
		}
	}
	for _, shards := range result {
		sort.Strings(shards)
	}
	return result
}

// LiveOpIDs returns the op IDs a sidecar sweep must treat as live: ops of
// ACTIVE drop-vector tasks, plus ops of terminal tasks whose targets are still
// marked dropped in the schema — a terminal round's recorded pending set is the
// next round's resume point, so sweeping it would restart the strip from
// scratch. Once the marker leaves the schema (finalize, or a finalize plus a
// re-create that revived the name) the op has no next round and the sweep
// collects it. Returns a non-nil (possibly empty) set on success; empty means
// "no live drop, sweep all".
func (e *dropVectorIndexEnqueuer) LiveOpIDs(ctx context.Context) (map[string]struct{}, error) {
	tasks, err := e.clusterService.ListDistributedTasks(ctx)
	if err != nil {
		return nil, err
	}
	live := map[string]struct{}{}
	// Every round of one drop shares (collection, targets), and terminal
	// records are deliberately retained while the marker is pending, so the
	// per-collection leader read is resolved once per call — not once per
	// record (QueryReadOnlyClasses is a serial RPC that ignores ctx, so an
	// unbounded per-record fan-out could hold the caller's sweep well past
	// its deadline on a partitioned leader).
	classes := map[string]classLookup{}
	for _, task := range tasks[db.DropVectorIndexNamespace] {
		p, err := db.DecodeDropVectorIndexTaskPayload(task.Payload)
		if err != nil {
			warnSkippedPayload(e.logger, "live-op-ids", task.ID, err)
			continue
		}
		if e.opStillNeeded(task, p, classes) {
			live[p.OpID] = struct{}{}
		}
	}
	return live, nil
}

// classLookup memoizes one leader class read within a single LiveOpIDs call,
// including a failed read (readErr) so a partitioned leader costs one RPC per
// collection, not one per retained record.
type classLookup struct {
	class   *models.Class
	readErr bool
}

// opStillNeeded reports whether a task's edit op must survive a sidecar sweep:
// always for an active task; for a terminal one, while any of its targets is
// still marked dropped — the marker means another round is coming, and the op's
// pending set is that round's resume point. A superseded old-epoch op alongside
// a NEW drop's marker also stays until that marker leaves the schema too: it
// strips the same target name, which is idempotent, and the transformer's
// dropped-target check fences it the moment the name goes live. Fails open on
// a leader read error:
// liveness feeds a destructive sweep, and "keep" is the reversible direction.
func (e *dropVectorIndexEnqueuer) opStillNeeded(
	task *distributedtask.Task, p *db.DropVectorIndexTaskPayload, classes map[string]classLookup,
) bool {
	if task.Status.IsActive() {
		return true
	}
	lookup, ok := classes[p.Collection]
	if !ok {
		vclasses, err := e.schemaState.QueryReadOnlyClasses(p.Collection)
		if err != nil {
			if e.logger != nil {
				e.logger.WithField("collection", p.Collection).
					Warnf("drop-vector: live-op-ids: leader class read failed; keeping this collection's terminal ops (fail open): %v", err)
			}
			lookup = classLookup{readErr: true}
		} else {
			lookup = classLookup{class: vclasses[p.Collection].Class}
		}
		classes[p.Collection] = lookup
	}
	if lookup.readErr {
		return true
	}
	return len(droppedTargetsIn(lookup.class, p.Targets)) > 0
}

var _ schema.DropVectorIndexEnqueuer = (*dropVectorIndexEnqueuer)(nil)

// dropVectorReconcileClient is the enqueuer slice reconciliation uses. The
// task list is fetched once per round and shared across every marker check.
type dropVectorReconcileClient interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
	EnqueueDropVectorIndexWithTasks(ctx context.Context, collection string, targets []string,
		tasks map[string][]*distributedtask.Task) error
}

// reconcileDroppedVectorIndexes enqueues cleanup for every "none" marker with no
// in-flight task — recovery for a crash between marker apply and enqueue, an
// upgrade with pre-existing markers, or a restore. Idempotent: the active-task
// check + the ConflictDetector dedupe across nodes running it at startup.
func reconcileDroppedVectorIndexes(ctx context.Context, classes []*models.Class,
	enq dropVectorReconcileClient, logger logrus.FieldLogger,
) {
	tasks, err := enq.ListDistributedTasks(ctx)
	if err != nil {
		logger.Warnf("drop-vector reconcile: listing tasks failed (round skipped): %v", err)
		return
	}
	for _, class := range classes {
		if class == nil {
			continue
		}
		for name, cfg := range class.VectorConfig {
			if !modelsext.IsVectorIndexDropped(cfg) {
				continue
			}
			if activeDropCovers(tasks, class.Class, name, logger) {
				continue
			}
			if err := enq.EnqueueDropVectorIndexWithTasks(ctx, class.Class, []string{name}, tasks); err != nil {
				logger.WithField("collection", class.Class).WithField("vector", name).
					Warnf("drop-vector reconcile: enqueue failed: %v", err)
			}
		}
	}
}

// schemaLister returns the local schema snapshot (eventually-consistent is fine
// for an idempotent safety net); *schema.Manager satisfies it.
type schemaLister interface {
	GetSchemaSkipAuth() entschema.Schema
}

// runDropVectorIndexReconciliation waits (bounded) for the cluster task store to
// be readable — so submits don't hit an unelected leader — then runs
// reconcileDroppedVectorIndexes periodically until ctx is cancelled. Launch in a
// goroutine.
func runDropVectorIndexReconciliation(ctx context.Context, lister schemaLister,
	enq dropVectorReconcileClient, logger logrus.FieldLogger, interval time.Duration,
	isLeader func() bool, nudge <-chan struct{},
) {
	const attempts = 30
	for i := 0; i < attempts; i++ {
		if ctx.Err() != nil {
			return
		}
		// Probe the DTM read path; success means the leader is reachable.
		if _, err := enq.ListDistributedTasks(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}

	for {
		// Read the schema AFTER the readiness wait (and fresh each round): at
		// startup the local schema is restored by the same background open the
		// probe waits for, so an early read would see an empty/stale snapshot and
		// silently skip markers — and this is the sole recovery path for every
		// "reconciliation retries" deferral. Each round is panic-contained: this
		// goroutine is that sole recovery path, so one bad round must not kill the
		// loop until restart.
		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("drop-vector reconcile: round panicked (loop continues): %v", r)
				}
			}()
			// Leader-only: every node runs this loop, but a round submits full
			// unit maps — N nodes racing the same marker append N-1 losing
			// multi-MB payloads to the RAFT log before CheckConflict rejects
			// them at apply. Followers stay warm and take over on election.
			if isLeader != nil && !isLeader() {
				return
			}
			sch := lister.GetSchemaSkipAuth()
			if sch.Objects != nil && len(sch.Objects.Classes) > 0 {
				reconcileDroppedVectorIndexes(ctx, sch.Objects.Classes, enq, logger)
			}
		}()
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		case <-nudge:
			// A round just ended with shards still uncovered (batch chain) or
			// failed (tenant deactivated mid-strip): follow up now instead of
			// idling a full interval. The nudge fires from OnTaskCompleted,
			// i.e. while the task is usually still SWAPPING (the scheduler
			// finalizes AFTER the completion callbacks) — an immediate round
			// would see it as active, skip the marker, and waste the nudge.
			// Wait out the ack window first.
			select {
			case <-ctx.Done():
				return
			case <-time.After(dropVectorNudgeDelay):
			}
		}
	}
}
