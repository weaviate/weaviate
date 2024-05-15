//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

// MergeObjects updates a batch of objects with the new properties provided.
// The update is executed as a patch, updating each property provided, and
// leaving the rest of the properties unchanged.
//
// If an object, or an object property does not exist, the update for that
// individual object will fail, but the remaining valid objects will proceed
// to be patched.
func (b *BatchManager) MergeObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	err := b.authorizer.Authorize(principal, "merge", "batch/objects")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return b.mergeObjects(ctx, principal, objects, repl)
}

func (b *BatchManager) mergeObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	if len(objects) == 0 {
		return nil, fmt.Errorf("cannot be empty, need at least one object for batching")
	}

	res, err := b.prepareObjectsMerge(ctx, principal, objects, repl)

	beforePersistence := time.Now()
	defer b.metrics.BatchOp("total_persistence_level", beforePersistence.UnixNano())
	if res, err = b.vectorRepo.BatchMergeObjects(ctx, res, repl); err != nil {
		return nil, NewErrInternal("batch objects: %#v", err)
	}

	resp := make(BatchObjects, len(objects))
	for i := range objects {
		resp[i] = BatchObject{
			OriginalIndex: res[i].OriginalIndex,
			Err:           res[i].Err,
			Object:        objects[i],
			UUID:          objects[i].ID,
		}
	}

	return resp, nil
}

func (b *BatchManager) prepareObjectsMerge(ctx context.Context, principal *models.Principal,
	updates []*models.Object, repl *additional.ReplicationProperties,
) (BatchMergeDocuments, error) {
	prevObjs, propsToDelete, errs := b.validateMerge(ctx, principal, updates, repl)
	docs := make(BatchMergeDocuments, len(updates))

	now := time.Now().UnixMilli()

	for i := range prevObjs {
		cls, id := updates[i].Class, updates[i].ID
		primitive, refs := splitPrimitiveAndRefs(updates[i].Properties.(map[string]interface{}), cls, id)
		objWithVec, err := mergeObjectSchemaAndVectorize(ctx, cls, prevObjs[i].Properties,
			primitive, principal, prevObjs[i].Vector, updates[i].Vector, prevObjs[i].Vectors,
			updates[i].Vectors, updates[i].ID, merger{
				schemaManager:   b.schemaManager,
				modulesProvider: b.modulesProvider,
				findObject:      b.findObject,
				logger:          b.logger,
			})
		if err != nil {
			return nil, &Error{"merge and vectorize", StatusInternalServerError, err}
		}
		mergeDoc := &MergeDocument{
			Class:              cls,
			ID:                 id,
			PrimitiveSchema:    primitive,
			References:         refs,
			Vector:             objWithVec.Vector,
			Vectors:            objWithVec.Vectors,
			UpdateTime:         now,
			PropertiesToDelete: propsToDelete[i],
		}

		if objWithVec.Additional != nil {
			mergeDoc.AdditionalProperties = objWithVec.Additional
		}

		docs[i] = &BatchMergeDocument{
			MergeDocument: mergeDoc,
			OriginalIndex: i,
			Err:           errs[i],
		}
	}
	return docs, nil
}

func (b *BatchManager) validateMerge(ctx context.Context, principal *models.Principal,
	updates []*models.Object, repl *additional.ReplicationProperties,
) ([]*models.Object, [][]string, []error) {
	var (
		previousObjs  = make([]*models.Object, len(updates))
		propsToDelete = make([][]string, len(updates))
		errs          = make([]error, len(updates))

		v = &objectMergeValidator{
			config:            b.config,
			authorizer:        b.authorizer,
			vectorRepo:        b.vectorRepo,
			autoSchemaManager: b.autoSchemaManager,
			schemaManager:     b.schemaManager,
		}
	)

	for i, obj := range updates {
		prevObj, delProps, err := v.validateObjectMerge(ctx, principal, obj, repl)
		if err != nil {
			errs[i] = err
			continue
		}
		previousObjs[i], propsToDelete[i] = prevObj, delProps
	}

	return previousObjs, propsToDelete, errs
}
