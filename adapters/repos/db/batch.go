//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"

	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/kinds"
)

type batchQueue struct {
	objects       []*storobj.Object
	originalIndex []int
}

func (db *DB) BatchPutThings(ctx context.Context, things kinds.BatchThings) (kinds.BatchThings, error) {
	byIndex := map[string]batchQueue{}
	for _, item := range things {
		for _, index := range db.indices {
			if index.Config.Kind != kind.Thing || index.Config.ClassName != schema.ClassName(item.Thing.Class) {
				continue
			}

			if item.Err != nil {
				// item has a validation error or another reason to ignore
				continue
			}

			queue := byIndex[index.ID()]
			queue.objects = append(queue.objects, storobj.FromThing(item.Thing, item.Vector))
			queue.originalIndex = append(queue.originalIndex, item.OriginalIndex)
			byIndex[index.ID()] = queue
		}
	}

	for indexID, queue := range byIndex {
		errs := db.indices[indexID].putObjectBatch(ctx, queue.objects)
		for index, err := range errs {
			things[queue.originalIndex[index]].Err = err
		}
	}

	return things, nil
}

func (db *DB) BatchPutActions(ctx context.Context, actions kinds.BatchActions) (kinds.BatchActions, error) {
	byIndex := map[string]batchQueue{}
	for _, item := range actions {
		for _, index := range db.indices {
			if index.Config.Kind != kind.Action || index.Config.ClassName != schema.ClassName(item.Action.Class) {
				continue
			}

			if item.Err != nil {
				// item has a validation error or another reason to ignore
				continue
			}

			queue := byIndex[index.ID()]
			queue.objects = append(queue.objects, storobj.FromAction(item.Action, item.Vector))
			queue.originalIndex = append(queue.originalIndex, item.OriginalIndex)
			byIndex[index.ID()] = queue
		}
	}

	for indexID, queue := range byIndex {
		errs := db.indices[indexID].putObjectBatch(ctx, queue.objects)
		for index, err := range errs {
			actions[queue.originalIndex[index]].Err = err
		}
	}

	return actions, nil
}

func (db *DB) AddBatchReferences(ctx context.Context, references kinds.BatchReferences) (kinds.BatchReferences, error) {
	byIndex := map[string]kinds.BatchReferences{}
	for _, item := range references {
		for _, index := range db.indices {
			if index.Config.Kind != item.From.Kind ||
				index.Config.ClassName != item.From.Class {
				continue
			}

			if item.Err != nil {
				// item has a validation error or another reason to ignore
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
			references[queue[index].OriginalIndex].Err = err
		}
	}

	return references, nil
}
