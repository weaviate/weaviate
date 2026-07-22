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
	"os"
	"sort"
	"strconv"
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
	// finalizer removes the schema marker when accumulated coverage spans
	// every shard and there is nothing left to enqueue (the DTM cannot run a
	// unit-less task). Nil-safe: without it the marker just stays.
	finalizer dropVectorMarkerFinalizer
}

// dropVectorMarkerFinalizer is the slice of the schema finalizer the enqueuer
// uses for coverage-complete direct finalization.
type dropVectorMarkerFinalizer interface {
	RemoveDroppedVectorConfig(ctx context.Context, collection string, targets []string) error
}

// SetFinalizer installs the marker finalizer. Must run during single-threaded
// startup (the enqueuer is constructed before the schema manager exists),
// before the reconcile loop starts or REST serving begins.
func (e *dropVectorIndexEnqueuer) SetFinalizer(f dropVectorMarkerFinalizer) {
	e.finalizer = f
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

// warnSkippedPayload surfaces an undecodable active-task payload instead of
// silently skipping it (the skip itself is deliberate fail-open behavior).
func (e *dropVectorIndexEnqueuer) warnSkippedPayload(where, taskID string, err error) {
	if e.logger != nil {
		e.logger.WithField("task", taskID).
			Warnf("drop-vector %s: skipping active task with unparseable payload: %v", where, err)
	}
}

// HasActiveDrop reports whether a non-terminal drop task already covers
// targetVector on collection.
func (e *dropVectorIndexEnqueuer) HasActiveDrop(ctx context.Context, collection, targetVector string) (bool, error) {
	tasks, err := e.clusterService.ListDistributedTasks(ctx)
	if err != nil {
		return false, err
	}
	for _, task := range tasks[db.DropVectorIndexNamespace] {
		if !task.Status.IsActive() {
			continue
		}
		p, err := db.DecodeDropVectorIndexTaskPayload(task.Payload)
		if err != nil {
			e.warnSkippedPayload("has-active-drop", task.ID, err)
			continue
		}
		if !strings.EqualFold(p.Collection, collection) {
			continue
		}
		for _, t := range p.Targets {
			// Exact match: target vector names are case-sensitive identifiers.
			if t == targetVector {
				return true, nil
			}
		}
	}
	return false, nil
}

// EnqueueDropVectorIndex submits a fresh cleanup task with one unit per
// (shard, replica) grouped by shard. Shards already cleaned by this epoch's
// earlier tasks get no unit; with coverage complete and nothing left to
// enqueue, the marker is finalized directly.
func (e *dropVectorIndexEnqueuer) EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string, freshEpoch bool) error {
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
		// All-cold MT collection: no active shard to strip now, and the marker is
		// already applied — a no-op success, not an error. Reconciliation re-enqueues
		// once tenants are active. A non-MT collection always has shards, so an empty
		// map there is a real problem.
		if state.PartitioningEnabled {
			if e.logger != nil {
				e.logger.WithField("collection", collection).
					Info("drop-vector enqueue: no active tenant to clean; the marker stays until a tenant is activated")
			}
			return nil
		}
		return fmt.Errorf("drop-vector enqueue: no shards for collection %q", collection)
	}

	epoch, cleaned, err := e.epochAndInheritedCoverage(ctx, collection, targets, freshEpoch)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: coverage inheritance for %q: %w", collection, err)
	}
	shardOwnership = withoutCleanedShards(shardOwnership, cleaned)
	if len(shardOwnership) == 0 {
		if uncovered := shardsNotIn(state, cleaned); len(uncovered) > 0 {
			if e.logger != nil {
				e.logger.WithField("collection", collection).WithField("shards", uncovered).
					Info("drop-vector enqueue: all active shards already cleaned; the marker stays until the remaining shards' tenants are activated")
			}
			return nil
		}
		// Coverage spans every shard with nothing left to enqueue: finalize
		// directly. The FSM removal gate re-verifies against the task records.
		if e.finalizer == nil {
			return nil
		}
		if e.logger != nil {
			e.logger.WithField("collection", collection).
				Info("drop-vector enqueue: accumulated coverage spans all shards; finalizing the marker")
		}
		return e.finalizer.RemoveDroppedVectorConfig(ctx, collection, targets)
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

// epochAndInheritedCoverage resolves the drop epoch and the cleaned-shard set
// accumulated by completed tasks of that epoch. freshEpoch mints a new epoch
// with no inheritance; otherwise the newest matching task decides the epoch,
// and a missing/pre-epoch chain (older nodes, records aged past the task TTL)
// starts fresh — costing one full re-clean, never correctness.
func (e *dropVectorIndexEnqueuer) epochAndInheritedCoverage(ctx context.Context,
	collection string, targets []string, freshEpoch bool,
) (string, []string, error) {
	if freshEpoch {
		return uuid.NewString(), nil, nil
	}
	tasks, err := e.clusterService.ListDistributedTasks(ctx)
	if err != nil {
		return "", nil, err
	}
	var newest *db.DropVectorIndexTaskPayload
	var newestAt time.Time
	matching := make(map[*distributedtask.Task]*db.DropVectorIndexTaskPayload)
	for _, task := range tasks[db.DropVectorIndexNamespace] {
		p, err := db.DecodeDropVectorIndexTaskPayload(task.Payload)
		if err != nil {
			e.warnSkippedPayload("coverage-inheritance", task.ID, err)
			continue
		}
		if !strings.EqualFold(p.Collection, collection) || !sameTargetSet(p.Targets, targets) {
			continue
		}
		matching[task] = p
		if newest == nil || task.StartedAt.After(newestAt) {
			newest, newestAt = p, task.StartedAt
		}
	}
	if newest == nil || newest.DropEpochID == "" {
		return uuid.NewString(), nil, nil
	}
	covered := map[string]struct{}{}
	for task, p := range matching {
		if p.DropEpochID != newest.DropEpochID || !completedTaskStatus(task.Status) {
			continue
		}
		for shard := range p.CoveredShards() {
			covered[shard] = struct{}{}
		}
	}
	cleaned := make([]string, 0, len(covered))
	for shard := range covered {
		cleaned = append(cleaned, shard)
	}
	sort.Strings(cleaned)
	return newest.DropEpochID, cleaned, nil
}

// completedTaskStatus matches tasks whose every unit succeeded (mirrors the
// provider gate's completedStatus).
func completedTaskStatus(s distributedtask.TaskStatus) bool {
	return s == distributedtask.TaskStatusSwapping || s == distributedtask.TaskStatusFinished
}

// sameTargetSet reports whether two target lists contain the same names
// (exact case — target vector names are case-sensitive identifiers).
func sameTargetSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, t := range a {
		set[t] = struct{}{}
	}
	for _, t := range b {
		if _, ok := set[t]; !ok {
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
func shardsNotIn(state *sharding.State, set []string) []string {
	in := make(map[string]struct{}, len(set))
	for _, shard := range set {
		in[shard] = struct{}{}
	}
	var out []string
	for shard := range state.Physical {
		if _, ok := in[shard]; !ok {
			out = append(out, shard)
		}
	}
	sort.Strings(out)
	return out
}

// stillDroppedTargets filters targets to those still present and marked dropped
// in the leader-consistent class. A missing class means nothing to clean.
func (e *dropVectorIndexEnqueuer) stillDroppedTargets(collection string, targets []string) ([]string, error) {
	vclasses, err := e.schemaState.QueryReadOnlyClasses(collection)
	if err != nil {
		return nil, err
	}
	class := vclasses[collection].Class
	if class == nil {
		return nil, nil
	}
	var still []string
	for _, target := range targets {
		if cfg, ok := class.VectorConfig[target]; ok && modelsext.IsVectorIndexDropped(cfg) {
			still = append(still, target)
		}
	}
	return still, nil
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

// reconcileDroppedVectorIndexes enqueues cleanup for every "none" marker with no
// in-flight task — recovery for a crash between marker apply and enqueue, an
// upgrade with pre-existing markers, or a restore. Idempotent: HasActiveDrop +
// the ConflictDetector dedupe across nodes running it at startup.
func reconcileDroppedVectorIndexes(ctx context.Context, classes []*models.Class,
	enq schema.DropVectorIndexEnqueuer, logger logrus.FieldLogger,
) {
	for _, class := range classes {
		if class == nil {
			continue
		}
		for name, cfg := range class.VectorConfig {
			if !modelsext.IsVectorIndexDropped(cfg) {
				continue
			}
			active, err := enq.HasActiveDrop(ctx, class.Class, name)
			if err != nil {
				logger.WithField("collection", class.Class).WithField("vector", name).
					Warnf("drop-vector reconcile: HasActiveDrop failed: %v", err)
				continue
			}
			if active {
				continue
			}
			if err := enq.EnqueueDropVectorIndex(ctx, class.Class, []string{name}, false); err != nil {
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

// dropVectorReconcileInterval is how often reconciliation re-checks for "none"
// markers without a live task after the startup pass — the pickup path for
// tenants that were inactive when a task finalized deferred, for markers left by
// a FAILED task, and for backup restores; without it those wait for a restart.
const dropVectorReconcileInterval = 15 * time.Minute

// dropVectorReconcileIntervalFromEnv returns the reconcile interval, overridable
// via DROP_VECTOR_INDEX_RECONCILE_INTERVAL_SECONDS (faster marker convergence
// after restores or tenant activations; each round costs leader reads). Invalid
// or non-positive values fall back to the default with a warning.
func dropVectorReconcileIntervalFromEnv(logger logrus.FieldLogger) time.Duration {
	raw := os.Getenv("DROP_VECTOR_INDEX_RECONCILE_INTERVAL_SECONDS")
	if raw == "" {
		return dropVectorReconcileInterval
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		logger.Warnf("drop-vector reconcile: invalid DROP_VECTOR_INDEX_RECONCILE_INTERVAL_SECONDS %q, using default %s",
			raw, dropVectorReconcileInterval)
		return dropVectorReconcileInterval
	}
	return time.Duration(seconds) * time.Second
}

// runDropVectorIndexReconciliation waits (bounded) for the cluster task store to
// be readable — so submits don't hit an unelected leader — then runs
// reconcileDroppedVectorIndexes periodically until ctx is cancelled. Launch in a
// goroutine.
func runDropVectorIndexReconciliation(ctx context.Context, lister schemaLister,
	enq schema.DropVectorIndexEnqueuer, logger logrus.FieldLogger, interval time.Duration,
) {
	const attempts = 30
	for i := 0; i < attempts; i++ {
		if ctx.Err() != nil {
			return
		}
		// Probe the DTM read path; success means the leader is reachable.
		if _, err := enq.HasActiveDrop(ctx, "", ""); err == nil {
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
