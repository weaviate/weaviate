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
	"strings"
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

// logInfo logs an enqueue-path decision (nil-safe like every logger use here).
func (e *dropVectorIndexEnqueuer) logInfo(collection, msg string) {
	if e.logger != nil {
		e.logger.WithField("collection", collection).Info(msg)
	}
}

// warnSkippedPayload surfaces an undecodable active-task payload instead of
// silently skipping it (the skip itself is deliberate fail-open behavior).
func (e *dropVectorIndexEnqueuer) warnSkippedPayload(where, taskID string, err error) {
	if e.logger != nil {
		e.logger.WithField("task", taskID).
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
	for _, task := range tasks[db.DropVectorIndexNamespace] {
		if !task.Status.IsActive() {
			continue
		}
		p, err := db.DecodeDropVectorIndexTaskPayload(task.Payload)
		if err != nil {
			if logger != nil {
				logger.WithField("task", task.ID).
					Warnf("drop-vector has-active-drop: skipping active task with unparseable payload: %v", err)
			}
			continue
		}
		if !strings.EqualFold(p.Collection, collection) {
			continue
		}
		for _, t := range p.Targets {
			// Exact match: target vector names are case-sensitive identifiers.
			if t == targetVector {
				return true
			}
		}
	}
	return false
}

// EnqueueDropVectorIndex submits a fresh cleanup task with one unit per
// (shard, replica) grouped by shard. Shards already cleaned by this drop's
// earlier tasks get no unit.
func (e *dropVectorIndexEnqueuer) EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string) error {
	// Re-validate against the leader-consistent class: the marker commit and this
	// enqueue are not atomic, and reconciliation may run off a stale local schema
	// snapshot. A target that is no longer marked dropped (class deleted and
	// re-created, or already finalized elsewhere) must not get a cleanup task —
	// that task would strip a live vector.
	targets, markerEpoch, err := e.stillDroppedTargets(collection, targets)
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
		// All-cold MT collection: no active shard to strip now, and the marker is
		// already applied — a no-op success, not an error. Reconciliation re-enqueues
		// once tenants are active. A non-MT collection always has shards, so an empty
		// map there is a real problem.
		if state.PartitioningEnabled {
			e.logInfo(collection, "drop-vector enqueue: no active tenant to clean; the marker stays until a tenant is activated")
			return nil
		}
		return fmt.Errorf("drop-vector enqueue: no shards for collection %q", collection)
	}

	epoch, cleaned, err := e.epochAndInheritedCoverage(ctx, collection, targets, state, markerEpoch)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: coverage inheritance for %q: %w", collection, err)
	}
	shardOwnership = withoutCleanedShards(shardOwnership, cleaned)
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
		Collection:    collection,
		Targets:       targets,
		OpID:          uuid.NewString(),
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

// epochAndInheritedCoverage returns the payload epoch for a new task of this
// drop and the cleaned-shard set accumulated by its completed earlier tasks.
// The marker's generation token IS the epoch: minted with the marker itself,
// it identifies the drop regardless of which task records survived, so records
// of a previous drop of a re-created name can never be inherited (they carry
// the previous token). A pre-token marker ("" — written by an older node) gets
// a fresh unique epoch per task and inherits nothing: pre-chain semantics,
// never stale coverage.
func (e *dropVectorIndexEnqueuer) epochAndInheritedCoverage(ctx context.Context,
	collection string, targets []string, state *sharding.State, markerEpoch string,
) (string, []string, error) {
	if markerEpoch == "" {
		return uuid.NewString(), nil, nil
	}
	tasks, err := e.clusterService.ListDistributedTasks(ctx)
	if err != nil {
		return "", nil, err
	}
	covered := map[string]struct{}{}
	for _, task := range tasks[db.DropVectorIndexNamespace] {
		if !task.Status.IsCompleted() {
			continue
		}
		p, err := db.DecodeDropVectorIndexTaskPayload(task.Payload)
		if err != nil {
			e.warnSkippedPayload("coverage-inheritance", task.ID, err)
			continue
		}
		if p.DropEpochID != markerEpoch || !strings.EqualFold(p.Collection, collection) || !sameTargetSet(p.Targets, targets) {
			continue
		}
		for shard := range p.CoveredShards() {
			covered[shard] = struct{}{}
		}
	}
	// Prune to current shards: deleted tenants would otherwise accumulate in
	// every subsequent payload forever.
	cleaned := make([]string, 0, len(covered))
	for shard := range covered {
		if _, ok := state.Physical[shard]; ok {
			cleaned = append(cleaned, shard)
		}
	}
	sort.Strings(cleaned)
	return markerEpoch, cleaned, nil
}

// sameTargetSet reports whether two target lists contain the same names with
// the same multiplicities (exact case — target vector names are case-sensitive
// identifiers).
func sameTargetSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, t := range a {
		counts[t]++
	}
	for _, t := range b {
		counts[t]--
		if counts[t] < 0 {
			return false
		}
	}
	return true
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

// shardsNotIn returns the state's shards absent from the given set, sorted.
// stillDroppedTargets filters targets to those still present and marked
// dropped in the leader-consistent class, and returns their shared drop epoch
// (the marker's generation token; "" for markers written by pre-token nodes).
// A missing class means nothing to clean. Multi-target enqueues must share one
// epoch — every current caller is single-target.
func (e *dropVectorIndexEnqueuer) stillDroppedTargets(collection string, targets []string) ([]string, string, error) {
	vclasses, err := e.schemaState.QueryReadOnlyClasses(collection)
	if err != nil {
		return nil, "", err
	}
	class := vclasses[collection].Class
	if class == nil {
		return nil, "", nil
	}
	var still []string
	var epoch string
	for _, target := range targets {
		cfg, ok := class.VectorConfig[target]
		if !ok || !modelsext.IsVectorIndexDropped(cfg) {
			continue
		}
		targetEpoch := modelsext.DropEpochID(cfg)
		if len(still) > 0 && targetEpoch != epoch {
			return nil, "", fmt.Errorf("targets %v span different drop epochs; enqueue them separately", targets)
		}
		still = append(still, target)
		epoch = targetEpoch
	}
	return still, epoch, nil
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

// LiveOpIDs returns the op IDs of drop-vector tasks that are still active
// (non-terminal). Wired into the DB so a shard load can sweep an orphaned op — one
// whose task has finished or been removed — instead of re-arming it. Returns a
// non-nil (possibly empty) set on success; empty means "no active drop, sweep all".
func (e *dropVectorIndexEnqueuer) LiveOpIDs(ctx context.Context) (map[string]struct{}, error) {
	tasks, err := e.clusterService.ListDistributedTasks(ctx)
	if err != nil {
		return nil, err
	}
	live := map[string]struct{}{}
	for _, task := range tasks[db.DropVectorIndexNamespace] {
		if !task.Status.IsActive() {
			continue
		}
		p, err := db.DecodeDropVectorIndexTaskPayload(task.Payload)
		if err != nil {
			e.warnSkippedPayload("live-op-ids", task.ID, err)
			continue
		}
		live[p.OpID] = struct{}{}
	}
	return live, nil
}

var _ schema.DropVectorIndexEnqueuer = (*dropVectorIndexEnqueuer)(nil)

// dropVectorReconcileClient is the enqueuer slice reconciliation uses. The
// task list is fetched once per round and shared across every marker check.
type dropVectorReconcileClient interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
	EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string) error
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
			if err := enq.EnqueueDropVectorIndex(ctx, class.Class, []string{name}); err != nil {
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
			sch := lister.GetSchemaSkipAuth()
			if sch.Objects != nil && len(sch.Objects.Classes) > 0 {
				reconcileDroppedVectorIndexes(ctx, sch.Objects.Classes, enq, logger)
			}
		}()
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
