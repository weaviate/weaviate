//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
)

type batchQueue struct {
	objects       []*storobj.Object
	originalIndex []int
}

func (db *DB) BatchPutObjects(ctx context.Context, objs objects.BatchObjects,
	repl *additional.ReplicationProperties,
) (objects.BatchObjects, error) {
	objectByClass := make(map[string]batchQueue)
	indexByClass := make(map[string]*Index)

	for _, item := range objs {
		if item.Err != nil {
			// item has a validation error or another reason to ignore
			continue
		}
		queue := objectByClass[item.Object.Class]
		queue.objects = append(queue.objects, storobj.FromObject(item.Object, item.Vector))
		queue.originalIndex = append(queue.originalIndex, item.OriginalIndex)
		objectByClass[item.Object.Class] = queue
	}

	db.indexLock.RLock()
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
				objs[origIdx].Err = fmt.Errorf(msg)
			}
			continue
		}
		index.dropIndex.RLock()
		indexByClass[class] = index
	}
	db.indexLock.RUnlock()

	for class, queue := range objectByClass {
		index, ok := indexByClass[class]
		if !ok {
			continue
		}
		errs := index.putObjectBatch(ctx, queue.objects, repl)
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
	repl *additional.ReplicationProperties,
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

	db.indexLock.RLock()
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
	db.indexLock.RUnlock()

	for class, queue := range refByClass {
		index, ok := indexByClass[class]
		if !ok {
			continue
		}
		errs := index.addReferencesBatch(ctx, queue, repl)
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
	repl *additional.ReplicationProperties,
) (objects.BatchDeleteResult, error) {
	// get index for a given class
	idx := db.GetIndex(params.ClassName)
	if idx == nil {
		return objects.BatchDeleteResult{}, errors.Errorf("cannot find index for class %v", params.ClassName)
	}
	// find all DocIDs in all shards that match the filter
	shardDocIDs, err := idx.findDocIDs(ctx, params.Filters)
	if err != nil {
		return objects.BatchDeleteResult{}, errors.Wrapf(err, "cannot find objects")
	}
	// prepare to be deleted list of DocIDs from all shards
	toDelete := map[string][]uint64{}
	limit := db.config.QueryMaximumResults

	matches := int64(0)
	for shardName, docIDs := range shardDocIDs {
		docIDsLength := int64(len(docIDs))
		if matches <= limit {
			if matches+docIDsLength <= limit {
				toDelete[shardName] = docIDs
			} else {
				toDelete[shardName] = docIDs[:limit-matches]
			}
		}
		matches += docIDsLength
	}
	// delete the DocIDs in given shards
	deletedObjects, err := idx.batchDeleteObjects(ctx, toDelete, params.DryRun, repl)
	if err != nil {
		return objects.BatchDeleteResult{}, errors.Wrapf(err, "cannot delete objects")
	}

	result := objects.BatchDeleteResult{
		Matches: matches,
		Limit:   db.config.QueryMaximumResults,
		DryRun:  params.DryRun,
		Objects: deletedObjects,
	}
	return result, nil
}
