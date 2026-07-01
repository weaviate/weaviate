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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/schema"
)

// dropVectorIndexEnqueuer implements schema.DropVectorIndexEnqueuer. It submits
// the Phase-2 cleanup distributed task and reports whether one is in flight,
// using the cluster DTM client + sharding state. Lives in the REST wiring layer
// so it can reuse buildUnitMaps/buildUnitSpecs.
type dropVectorIndexEnqueuer struct {
	clusterService clusterDropTaskClient
	ownership      shardOwnershipLister
}

// clusterDropTaskClient is the slice of the cluster service the enqueuer uses.
type clusterDropTaskClient interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
	AddDistributedTaskWithGroups(ctx context.Context, namespace, taskID string,
		taskPayload any, unitSpecs []distributedtask.UnitSpec) error
}

// shardOwnershipLister returns node -> shard-names for a collection (non-HOT MT
// tenants excluded — their cleanup is deferred to a later reconciliation once they
// are active), and reports multi-tenancy so the enqueuer can tell an all-cold MT
// collection (empty, expected) from a shard-less one (an error). *db.DB satisfies
// it; narrowed for testability.
type shardOwnershipLister interface {
	ShardReplicaOwnershipActive(ctx context.Context, className string) (map[string][]string, error)
	IsMultiTenant(ctx context.Context, className string) bool
}

func newDropVectorIndexEnqueuer(clusterService clusterDropTaskClient, ownership shardOwnershipLister) *dropVectorIndexEnqueuer {
	return &dropVectorIndexEnqueuer{clusterService: clusterService, ownership: ownership}
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
		var p db.DropVectorIndexTaskPayload
		if err := json.Unmarshal(task.Payload, &p); err != nil {
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
	shardOwnership, err := e.ownership.ShardReplicaOwnershipActive(ctx, collection)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: shard ownership for %q: %w", collection, err)
	}
	if len(shardOwnership) == 0 {
		// All-cold MT collection: no active shard to strip now, and the marker is
		// already applied — a no-op success, not an error. Reconciliation re-enqueues
		// once tenants are active. A non-MT collection always has shards, so an empty
		// map there is a real problem.
		if e.ownership.IsMultiTenant(ctx, collection) {
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
		var p db.DropVectorIndexTaskPayload
		if err := json.Unmarshal(task.Payload, &p); err != nil {
			continue
		}
		if p.OpID != "" {
			live[p.OpID] = struct{}{}
		}
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
	sch := lister.GetSchemaSkipAuth()
	if sch.Objects == nil || len(sch.Objects.Classes) == 0 {
		return
	}

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
	reconcileDroppedVectorIndexes(ctx, sch.Objects.Classes, enq, logger)
}
