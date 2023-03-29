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

func (db *DB) BatchPutObjects(ctx context.Context, objects objects.BatchObjects,
	repl *additional.ReplicationProperties,
) (objects.BatchObjects, error) {
	byIndex := map[*Index]batchQueue{}
	indexById := make(map[int]*Index, len(objects))
	db.indexLock.RLock()

	for i, item := range objects {
		for _, index := range db.indices {
			if index.Config.ClassName != schema.ClassName(item.Object.Class) {
				continue
			}

			if item.Err != nil {
				// item has a validation error or another reason to ignore
				continue
			}
			_, ok := byIndex[index]
			if !ok { // only lock index once
				index.indexLock.RLock()
			}
			indexById[i] = index
		}
	}
	db.indexLock.RUnlock()
	for i, item := range objects {
		if item.Err != nil {
			// item has a validation error or another reason to ignore
			continue
		}
		index := indexById[i]
		queue := byIndex[index]
		object := storobj.FromObject(item.Object, item.Vector)
		queue.objects = append(queue.objects, object)
		queue.originalIndex = append(queue.originalIndex, item.OriginalIndex)
		byIndex[index] = queue

	}

	for index, queue := range byIndex {
		errs := index.putObjectBatch(ctx, queue.objects, repl)
		index.indexLock.RUnlock()
		for i, err := range errs {
			if err != nil {
				objects[queue.originalIndex[i]].Err = err
			}
		}
	}

	return objects, nil
}

func (db *DB) AddBatchReferences(ctx context.Context, references objects.BatchReferences,
	repl *additional.ReplicationProperties,
) (objects.BatchReferences, error) {
	byIndex := map[*Index]objects.BatchReferences{}
	db.indexLock.RLock()
	for _, item := range references {
		for _, index := range db.indices {
			if item.Err != nil {
				// item has a validation error or another reason to ignore
				continue
			}

			if index.Config.ClassName != item.From.Class {
				continue
			}
			queue, ok := byIndex[index]
			if !ok { // only lock index once
				index.indexLock.RLock()
			}
			queue = append(queue, item)
			byIndex[index] = queue
		}
	}
	db.indexLock.RUnlock()

	for index, queue := range byIndex {
		errs := index.addReferencesBatch(ctx, queue, repl)
		index.indexLock.RUnlock()
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
