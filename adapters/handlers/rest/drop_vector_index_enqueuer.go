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

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/usecases/schema"
)

// dropVectorIndexEnqueuer implements schema.DropVectorIndexEnqueuer. It submits
// the Phase-2 cleanup distributed task and reports whether one is in flight,
// using the cluster DTM client + sharding state. Lives in the REST wiring layer
// so it can reuse buildUnitMaps/buildUnitSpecs and ShardReplicaOwnership.
type dropVectorIndexEnqueuer struct {
	clusterService clusterDropTaskClient
	db             *db.DB
}

// clusterDropTaskClient is the slice of the cluster service the enqueuer uses.
type clusterDropTaskClient interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
	AddDistributedTaskWithGroups(ctx context.Context, namespace, taskID string,
		taskPayload any, unitSpecs []distributedtask.UnitSpec) error
}

func newDropVectorIndexEnqueuer(clusterService clusterDropTaskClient, database *db.DB) *dropVectorIndexEnqueuer {
	return &dropVectorIndexEnqueuer{clusterService: clusterService, db: database}
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
			if t == targetVector {
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
	shardOwnership, err := e.db.ShardReplicaOwnership(ctx, collection)
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: shard ownership for %q: %w", collection, err)
	}
	if len(shardOwnership) == 0 {
		return fmt.Errorf("drop-vector enqueue: no shards for collection %q", collection)
	}

	_, unitToShard, unitToNode := buildUnitMaps(shardOwnership)
	specs := buildUnitSpecs(shardOwnership)

	opID := uuid.NewString()
	payload, err := json.Marshal(&db.DropVectorIndexTaskPayload{
		Collection:  collection,
		Targets:     targets,
		OpID:        opID,
		UnitToNode:  unitToNode,
		UnitToShard: unitToShard,
	})
	if err != nil {
		return fmt.Errorf("drop-vector enqueue: marshal payload: %w", err)
	}

	// Fresh task ID per submission so a re-trigger after a FAILED run is a new
	// task version (C4). The ConflictDetector rejects a duplicate against an
	// active task, the backstop for the HasActiveDrop check race.
	taskID := uuid.NewString()
	return e.clusterService.AddDistributedTaskWithGroups(ctx, db.DropVectorIndexNamespace, taskID, payload, specs)
}

var _ schema.DropVectorIndexEnqueuer = (*dropVectorIndexEnqueuer)(nil)
