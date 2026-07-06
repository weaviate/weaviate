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

func newDropVectorIndexEnqueuer(clusterService clusterDropTaskClient, schemaState schemaStateQuerier) *dropVectorIndexEnqueuer {
	return &dropVectorIndexEnqueuer{clusterService: clusterService, schemaState: schemaState}
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
			continue
		}
		if !strings.EqualFold(p.Collection, collection) {
			continue
		}
		for _, t := range p.Targets {
			// Case-insensitive, matching CheckConflict so pre-check and FSM agree.
			if strings.EqualFold(t, targetVector) {
				return true, nil
			}
		}
	}
	return false, nil
}

// EnqueueDropVectorIndex submits a fresh cleanup task (fresh task + op ID) with
// one unit per (shard, replica) grouped by shard, so each replica node strips
// its own objects bucket.
func (e *dropVectorIndexEnqueuer) EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string) error {
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
			return nil
		}
		return fmt.Errorf("drop-vector enqueue: no shards for collection %q", collection)
	}

	_, unitToShard, unitToNode := buildUnitMaps(shardOwnership)
	specs := buildUnitSpecs(shardOwnership)

	// Pass the payload struct, not pre-marshaled bytes: the cluster layer
	// json.Marshals taskPayload itself (bytes would be double-encoded into a JSON
	// string and fail to decode in CheckConflict / the provider).
	payload := db.DropVectorIndexTaskPayload{
		Collection:  collection,
		Targets:     targets,
		OpID:        uuid.NewString(),
		UnitToNode:  unitToNode,
		UnitToShard: unitToShard,
	}

	// Fresh task ID per submission so a re-trigger after a FAILED run is a new
	// task version. The ConflictDetector rejects a duplicate against an active
	// task, the backstop for the HasActiveDrop check race.
	taskID := uuid.NewString()
	return e.clusterService.AddDistributedTaskWithGroups(ctx, db.DropVectorIndexNamespace, taskID, payload, specs)
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
		for name, cfg := range class.VectorConfig {
			if strings.EqualFold(name, target) && modelsext.IsVectorIndexDropped(cfg) {
				still = append(still, target)
				break
			}
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

// runDropVectorIndexReconciliationAtStartup waits (bounded) for the cluster task
// store to be readable — so submits don't hit an unelected leader — then runs
// reconcileDroppedVectorIndexes once. Launch in a goroutine; cancellable via ctx.
func runDropVectorIndexReconciliationAtStartup(ctx context.Context, lister schemaLister,
	enq schema.DropVectorIndexEnqueuer, logger logrus.FieldLogger,
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

	// Read the schema AFTER the readiness wait: at startup the local schema is
	// restored by the same background open the probe waits for, so an early read
	// would see an empty/stale snapshot and silently skip markers — and this is
	// the sole recovery path for every "reconciliation retries" deferral.
	sch := lister.GetSchemaSkipAuth()
	if sch.Objects == nil || len(sch.Objects.Classes) == 0 {
		return
	}
	reconcileDroppedVectorIndexes(ctx, sch.Objects.Classes, enq, logger)
}
