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

package db

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects"
)

type batchQueue struct {
	objects       []*storobj.Object
	originalIndex []int
}

func (db *DB) BatchPutObjects(ctx context.Context, objs objects.BatchObjects,
	repl *additional.ReplicationProperties, schemaVersion uint64,
) (objects.BatchObjects, error) {
	objectByClass := make(map[string]batchQueue)
	indexByClass := make(map[string]*Index)

	// Only check memory here if async indexing is disabled. With async indexing,
	// hnsw.AddBatch checks the allocation for single-vector HNSW indexes when the
	// queue worker inserts the batch. Enqueuing is therefore not blocked by the
	// index's memory demand. Everything else this call allocates is unchecked on
	// that path.
	if !db.AsyncIndexingEnabled {
		if err := db.memMonitor.CheckAlloc(estimateBatchMemory(objs)); err != nil {
			db.logger.Errorf("memory pressure: cannot process batch: %v", err)
			return nil, fmt.Errorf("cannot process batch: %w", err)
		}
	}

	for _, item := range objs {
		if item.Err != nil {
			// item has a validation error or another reason to ignore
			continue
		}
		queue := objectByClass[item.Object.Class]
		vectors, multiVectors, err := dto.GetVectors(item.Object.Vectors)
		if err != nil {
			return nil, fmt.Errorf("cannot process batch: cannot get vectors: %w", err)
		}
		queue.objects = append(queue.objects, storobj.FromObject(item.Object, item.Object.Vector, vectors, multiVectors))
		queue.originalIndex = append(queue.originalIndex, item.OriginalIndex)
		objectByClass[item.Object.Class] = queue
	}

	if err := db.schemaReader.WaitForUpdate(ctx, schemaVersion); err != nil {
		return nil, err
	}

	// wrapped by func to acquire and safely release indexLock only for duration of loop
	func() {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()

		for class, queue := range objectByClass {
			index, ok := db.indices[indexID(schema.ClassName(class))]
			if !ok {
				msg := fmt.Sprintf("could not find index for class %v. It might have been deleted in the meantime", class)
				db.logger.Warn(msg)
				for _, origIdx := range queue.originalIndex {
					if origIdx >= len(objs) {
						db.logger.Errorf(
							"batch add queue index out of bounds. len(objs) == %d, queue.originalIndex == %d",
							len(objs), origIdx)
						break
					}
					objs[origIdx].Err = errors.New(msg)
				}
				continue
			}
			index.dropIndex.RLock()
			indexByClass[class] = index
		}
	}()

	// safely release remaining locks (in case of panic)
	defer func() {
		for _, index := range indexByClass {
			if index != nil {
				index.dropIndex.RUnlock()
			}
		}
	}()

	for class, index := range indexByClass {
		queue := objectByClass[class]
		errs := index.putObjectBatch(ctx, queue.objects, repl, schemaVersion)
		index.metrics.BatchCount(len(queue.objects))
		index.metrics.BatchCountBytes(estimateStorBatchMemory(queue.objects))

		// remove index from map to skip releasing its lock in defer
		indexByClass[class] = nil
		index.dropIndex.RUnlock()
		for i, err := range errs {
			if err != nil {
				objs[queue.originalIndex[i]].Err = err
			}
		}
	}

	return objs, nil
}

func (db *DB) AddBatchReferences(ctx context.Context, references objects.BatchReferences,
	repl *additional.ReplicationProperties, schemaVersion uint64,
) (objects.BatchReferences, error) {
	refByClass := make(map[schema.ClassName]objects.BatchReferences)
	indexByClass := make(map[schema.ClassName]*Index)

	for _, item := range references {
		if item.Err != nil {
			// item has a validation error or another reason to ignore
			continue
		}
		refByClass[item.From.Class] = append(refByClass[item.From.Class], item)
	}

	if err := db.schemaReader.WaitForUpdate(ctx, schemaVersion); err != nil {
		return nil, err
	}

	// wrapped by func to acquire and safely release indexLock only for duration of loop
	func() {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()

		for class, queue := range refByClass {
			index, ok := db.indices[indexID(class)]
			if !ok {
				for _, item := range queue {
					references[item.OriginalIndex].Err = fmt.Errorf("could not find index for class %v. It might have been deleted in the meantime", class)
				}
				continue
			}
			index.dropIndex.RLock()
			indexByClass[class] = index
		}
	}()

	// safely release remaining locks (in case of panic)
	defer func() {
		for _, index := range indexByClass {
			if index != nil {
				index.dropIndex.RUnlock()
			}
		}
	}()

	for class, index := range indexByClass {
		queue := refByClass[class]
		errs := index.AddReferencesBatch(ctx, queue, repl, schemaVersion)
		// remove index from map to skip releasing its lock in defer
		indexByClass[class] = nil
		index.dropIndex.RUnlock()
		for i, err := range errs {
			if err != nil {
				references[queue[i].OriginalIndex].Err = err
			}
		}
	}

	return references, nil
}

func (db *DB) BatchDeleteObjects(ctx context.Context, params objects.BatchDeleteParams,
	deletionTime time.Time, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) (objects.BatchDeleteResult, error) {
	if err := db.schemaReader.WaitForUpdate(ctx, schemaVersion); err != nil {
		return objects.BatchDeleteResult{}, err
	}

	start := time.Now()
	// get index for a given class
	className := params.ClassName
	idx := db.GetIndex(className)
	if idx == nil {
		return objects.BatchDeleteResult{}, errors.Errorf("cannot find index for class %v", className)
	}

	limit := db.config.QueryMaximumResults

	// findUUIDs asks each shard for limit+1 matches. One over the limit separates
	// "more matches than limit" from "exactly limit" without resolving them all.
	shardDocIDs, err := idx.findUUIDs(ctx, params.Filters, tenant, repl, perShardResolveLimit(limit))
	if err != nil {
		return objects.BatchDeleteResult{}, errors.Wrapf(err, "cannot find objects")
	}

	plan := planShardDeletes(shardDocIDs, limit)
	toDelete, matches := plan.toDelete, plan.matches

	if plan.cappedShards > 0 {
		db.logger.WithFields(logrus.Fields{
			"action":        "batch_delete_objects_capped",
			"class":         className,
			"tenant":        tenant,
			"limit":         limit,
			"capped_shards": plan.cappedShards,
		}).Infof("batch delete resolved the maximum on %d shard(s): more objects match than one call deletes, repeat the request until matches is 0", plan.cappedShards)
	}

	db.logger.WithFields(logrus.Fields{
		"action":  "batch_delete_objects_post_find_ids",
		"params":  params,
		"tenant":  tenant,
		"matches": matches,
		"dry_run": params.DryRun,
		"took":    time.Since(start),
	}).Debugf("batch delete: identified %v objects to delete", matches)

	if err := db.memMonitor.CheckAlloc(memwatch.EstimateObjectDeleteMemory() * matches); err != nil {
		db.logger.Errorf("memory pressure: cannot process batch delete object: %v", err)
		return objects.BatchDeleteResult{}, fmt.Errorf("cannot process batch delete object: %w", err)
	}

	// delete the DocIDs in given shards
	deletedObjects, err := idx.batchDeleteObjects(ctx, toDelete, deletionTime, params.DryRun, repl, schemaVersion, tenant)
	if err != nil {
		return objects.BatchDeleteResult{}, errors.Wrapf(err, "cannot delete objects")
	}

	result := objects.BatchDeleteResult{
		Matches:      matches,
		Limit:        limit,
		DeletionTime: deletionTime,
		DryRun:       params.DryRun,
		Objects:      deletedObjects,
	}

	db.logger.WithFields(logrus.Fields{
		"action":  "batch_delete_objects_completed",
		"params":  params,
		"tenant":  tenant,
		"matches": matches,
		"took":    time.Since(start),
		"dry_run": params.DryRun,
	}).Debugf("batch delete completed in %s", time.Since(start))
	return result, nil
}

// perShardResolveLimit is how many matches a shard resolves for one batch delete, given
// QUERY_MAXIMUM_RESULTS. It is one over the limit so the reply can tell "more matches
// than limit" from "exactly limit". A limit of zero or less means no cap.
//
// The clamp keeps the result inside int32, which the cluster-internal find request
// narrows to, and keeps limit+1 from wrapping into a negative that every reader down
// the line takes for "no cap".
func perShardResolveLimit(limit int64) int {
	if limit <= 0 {
		return 0
	}
	return int(min(limit, math.MaxInt32-1)) + 1
}

// shardDeletePlan is what one batch delete call does with the UUIDs the shards resolved.
type shardDeletePlan struct {
	// toDelete holds the UUIDs to delete per shard, at most limit across all of them.
	// A shard with nothing to delete is left out.
	toDelete map[string][]strfmt.UUID
	// matches is the count the reply publishes: the exact number of matching objects
	// while it is at or below limit, and limit+1 when more match than one call deletes.
	matches int64
	// cappedShards is how many shards resolved as many matches as they were asked for.
	cappedShards int
}

func planShardDeletes(shardDocIDs map[string][]strfmt.UUID, limit int64) shardDeletePlan {
	plan := shardDeletePlan{toDelete: map[string][]strfmt.UUID{}}
	perShard := int64(perShardResolveLimit(limit))

	for shardName, docIDs := range shardDocIDs {
		resolved := int64(len(docIDs))
		if perShard > 0 && resolved >= perShard {
			plan.cappedShards++
		}
		if takes := min(resolved, limit-plan.matches); takes > 0 {
			plan.toDelete[shardName] = docIDs[:takes]
		}
		plan.matches += resolved
	}

	if limit > 0 && plan.matches > limit {
		plan.matches = limit + 1
	}
	return plan
}

func estimateBatchMemory(objs objects.BatchObjects) int64 {
	var sum int64
	for _, item := range objs {
		sum += memwatch.EstimateObjectMemory(item.Object)
	}

	return sum
}

func estimateStorBatchMemory(objs []*storobj.Object) int64 {
	var sum int64
	for _, item := range objs {
		sum += memwatch.EstimateStorObjectMemory(item)
	}

	return sum
}
