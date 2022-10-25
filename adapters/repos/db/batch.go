//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

type batchQueue struct {
	objects       []*storobj.Object
	originalIndex []int
}

func (db *DB) BatchPutObjects(ctx context.Context, objects objects.BatchObjects) (objects.BatchObjects, error) {
	byIndex := map[string]batchQueue{}
	db.indexLock.Lock()
	defer db.indexLock.Unlock()

	for _, item := range objects {
		for _, index := range db.indices {
			if index.Config.ClassName != schema.ClassName(item.Object.Class) {
				continue
			}

			if item.Err != nil {
				// item has a validation error or another reason to ignore
				continue
			}

			queue := byIndex[index.ID()]
			object := storobj.FromObject(item.Object, item.Vector)
			queue.objects = append(queue.objects, object)
			queue.originalIndex = append(queue.originalIndex, item.OriginalIndex)
			byIndex[index.ID()] = queue
		}
	}

	for indexID, queue := range byIndex {
		errs := db.indices[indexID].putObjectBatch(ctx, queue.objects)
		for index, err := range errs {
			if err != nil {
				objects[queue.originalIndex[index]].Err = err
			}
		}
	}

	return objects, nil
}

func (db *DB) AddBatchReferences(ctx context.Context, references objects.BatchReferences) (objects.BatchReferences, error) {
	byIndex := map[string]objects.BatchReferences{}
	db.indexLock.Lock()
	defer db.indexLock.Unlock()
	for _, item := range references {
		for _, index := range db.indices {
			if item.Err != nil {
				// item has a validation error or another reason to ignore
				continue
			}

			if index.Config.ClassName != item.From.Class {
				continue
			}

			queue := byIndex[index.ID()]
			queue = append(queue, item)
			byIndex[index.ID()] = queue
		}
	}

	for indexID, queue := range byIndex {
		errs := db.indices[indexID].addReferencesBatch(ctx, queue)
		for index, err := range errs {
			if err != nil {
				references[queue[index].OriginalIndex].Err = err
			}
		}
	}

	return references, nil
}

func (db *DB) BatchDeleteObjects(ctx context.Context, params objects.BatchDeleteParams) (objects.BatchDeleteResult, error) {
	// get index for a given class
	idx := db.GetIndex(params.ClassName)
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
	deletedObjects, err := idx.batchDeleteObjects(ctx, toDelete, params.DryRun)
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
