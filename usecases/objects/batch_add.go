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

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

// AddObjects Class Instances in batch to the connected DB
func (b *BatchManager) AddObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	err := b.authorizer.Authorize(principal, "create", "batch/objects")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	before := time.Now()
	b.metrics.BatchInc()
	defer b.metrics.BatchOp("total_uc_level", before.UnixNano())
	defer b.metrics.BatchDec()

	return b.addObjects(ctx, principal, objects, fields, repl)
}

func (b *BatchManager) addObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	beforePreProcessing := time.Now()
	if err := b.validateObjectForm(objects); err != nil {
		return nil, NewErrInvalidUserInput("invalid param 'objects': %v", err)
	}

	batchObjects := b.validateAndGetVector(ctx, principal, objects, repl)
	b.metrics.BatchOp("total_preprocessing", beforePreProcessing.UnixNano())

	var (
		res BatchObjects
		err error
	)

	beforePersistence := time.Now()
	defer b.metrics.BatchOp("total_persistence_level", beforePersistence.UnixNano())
	if res, err = b.vectorRepo.BatchPutObjects(ctx, batchObjects, repl); err != nil {
		return nil, NewErrInternal("batch objects: %#v", err)
	}

	return res, nil
}

func (b *BatchManager) validateAndGetVector(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties,
) BatchObjects {
	now := unixNow()
	batchObjects := make(BatchObjects, len(objects))

	objectsPerClass := make(map[string][]*models.Object)
	originalIndexPerClass := make(map[string][]int)

	for i, obj := range objects {
		object := &models.Object{}
		if err := b.autoSchemaManager.autoSchema(ctx, principal, obj, true); err != nil {
			batchObjects[i].Err = err
		}

		if obj.ID == "" {
			// Generate UUID for the new object
			uid, err := generateUUID()
			object.ID = uid
			batchObjects[i].Err = err
		} else {
			if _, err := uuid.Parse(obj.ID.String()); err != nil {
				batchObjects[i].Err = err
			}
			object.ID = obj.ID
		}

		if batchObjects[i].Err != nil {
			batchObjects[i].OriginalIndex = i
			batchObjects[i].Object = obj
			continue
		}

		object.LastUpdateTimeUnix = now
		object.CreationTimeUnix = now

		object.Class = obj.Class
		object.Vector = obj.Vector
		object.Tenant = obj.Tenant
		object.Properties = obj.Properties
		if object.Properties == nil {
			object.Properties = map[string]interface{}{}
		}

		batchObjects[i].UUID = object.ID

		// no need to send this to the vectorizer if there is already a vector
		if obj.Vector != nil {
			batchObjects[i].OriginalIndex = i
			batchObjects[i].Object = object
			continue
		}

		if objectsPerClass[obj.Class] == nil {
			objectsPerClass[obj.Class] = make([]*models.Object, 0)
			originalIndexPerClass[obj.Class] = make([]int, 0)
		}
		objectsPerClass[obj.Class] = append(objectsPerClass[obj.Class], object)
		originalIndexPerClass[obj.Class] = append(originalIndexPerClass[obj.Class], i)
	}

	for className, objectsForClass := range objectsPerClass {
		class, err := b.schemaManager.GetClass(ctx, principal, className)
		if err != nil {
			for i, obj := range objectsForClass {
				origIndex := originalIndexPerClass[className][i]
				batchObjects[origIndex].Err = err
				batchObjects[origIndex].OriginalIndex = origIndex
				batchObjects[origIndex].Object = obj
			}
			continue
		}
		if class == nil {
			for i, obj := range objectsForClass {
				origIndex := originalIndexPerClass[className][i]
				batchObjects[origIndex].Err = fmt.Errorf("class '%v' not present in schema", obj.Class)
				batchObjects[origIndex].OriginalIndex = origIndex
				batchObjects[origIndex].Object = obj
			}
			continue

		}

		validator := validation.New(b.vectorRepo.Exists, b.config, repl)
		for i, obj := range objectsForClass {
			err = validator.Object(ctx, class, obj, nil)
			if err != nil {
				origIndex := originalIndexPerClass[className][i]
				batchObjects[origIndex].Err = err
				batchObjects[origIndex].OriginalIndex = origIndex
				batchObjects[origIndex].Object = obj
			}
		}
		errorsPerObj, err := b.modulesProvider.BatchUpdateVector(ctx, class, objectsForClass, b.findObject, b.logger)
		if err != nil {
			for i, obj := range objectsForClass {
				origIndex := originalIndexPerClass[className][i]
				batchObjects[origIndex].Err = err
				batchObjects[origIndex].OriginalIndex = origIndex
				batchObjects[origIndex].Object = obj
			}
		}
		for i, obj := range objectsForClass {
			origIndex := originalIndexPerClass[className][i]
			batchObjects[origIndex].OriginalIndex = origIndex
			batchObjects[origIndex].Object = obj
			batchObjects[origIndex].UUID = obj.ID
			for j, err := range errorsPerObj {
				if i == j {
					batchObjects[origIndex].Err = err
				}
			}

		}
	}

	return batchObjects
}

func (b *BatchManager) validateObjectForm(classes []*models.Object) error {
	if len(classes) == 0 {
		return fmt.Errorf("cannot be empty, need at least one object for batching")
	}

	return nil
}

func unixNow() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
