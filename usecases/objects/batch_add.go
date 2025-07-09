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
	"errors"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/versioned"

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

var errEmptyObjects = NewErrInvalidUserInput("invalid param 'objects': cannot be empty, need at least one object for batching")

// AddObjects Class Instances in batch to the connected DB
func (b *BatchManager) AddObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	ctx = classcache.ContextWithClassCache(ctx)

	classesShards := make(map[string][]string)
	for _, obj := range objects {
		obj.Class = schema.UppercaseClassName(obj.Class)
		classesShards[obj.Class] = append(classesShards[obj.Class], obj.Tenant)
	}
	knownClasses := map[string]versioned.Class{}

	// whole request fails if permissions for any collection are not present
	for className, shards := range classesShards {
		// we don't leak any info that someone who inserts data does not have anyway
		vClass, err := b.schemaManager.GetCachedClassNoAuth(ctx, className)
		if err != nil {
			return nil, err
		}
		knownClasses[className] = vClass[className]

		if err := b.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.ShardsData(className, shards...)...); err != nil {
			return nil, err
		}

		if err := b.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.ShardsData(className, shards...)...); err != nil {
			return nil, err
		}
	}

	return b.addObjects(ctx, principal, objects, repl, knownClasses)
}

// AddObjectsGRPCAfterAuth bypasses the authentication in the REST endpoint as GRPC has its own checking
func (b *BatchManager) AddObjectsGRPCAfterAuth(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (BatchObjects, error) {
	return b.addObjects(ctx, principal, objects, repl, fetchedClasses)
}

func (b *BatchManager) addObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (BatchObjects, error) {
	ctx = classcache.ContextWithClassCache(ctx)

	before := time.Now()
	b.metrics.BatchInc()
	defer b.metrics.BatchOp("total_uc_level", before.UnixNano())
	defer b.metrics.BatchDec()

	beforePreProcessing := time.Now()
	if len(objects) == 0 {
		return nil, errEmptyObjects
	}

	var maxSchemaVersion uint64
	batchObjects, maxSchemaVersion := b.validateAndGetVector(ctx, principal, objects, repl, fetchedClasses)
	schemaVersion, tenantCount, err := b.autoSchemaManager.autoTenants(ctx, principal, objects, fetchedClasses)
	if err != nil {
		return nil, fmt.Errorf("auto create tenants: %w", err)
	}
	if schemaVersion > maxSchemaVersion {
		maxSchemaVersion = schemaVersion
	}

	b.metrics.BatchTenants(tenantCount)
	b.metrics.BatchObjects(len(objects))
	b.metrics.BatchOp("total_preprocessing", beforePreProcessing.UnixNano())

	var res BatchObjects

	beforePersistence := time.Now()
	defer b.metrics.BatchOp("total_persistence_level", beforePersistence.UnixNano())

	// Ensure that the local schema has caught up to the version we used to validate
	if err := b.schemaManager.WaitForUpdate(ctx, maxSchemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", maxSchemaVersion, err)
	}
	if res, err = b.vectorRepo.BatchPutObjects(ctx, batchObjects, repl, maxSchemaVersion); err != nil {
		return nil, NewErrInternal("batch objects: %#v", err)
	}

	return res, nil
}

func (b *BatchManager) validateAndGetVector(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (BatchObjects, uint64) {
	var (
		now          = time.Now().UnixNano() / int64(time.Millisecond)
		batchObjects = make(BatchObjects, len(objects))

		objectsPerClass       = make(map[string][]*models.Object)
		originalIndexPerClass = make(map[string][]int)
		validator             = validation.New(b.vectorRepo.Exists, b.config, repl)
	)

	// validate each object and sort by class (==vectorizer)
	var maxSchemaVersion uint64
	for i, obj := range objects {
		batchObjects[i].OriginalIndex = i

		if obj.Class == "" {
			batchObjects[i].Err = errors.New("object has an empty class")
			continue
		}

		schemaVersion, err := b.autoSchemaManager.autoSchema(ctx, principal, true, fetchedClasses, obj)
		if err != nil {
			batchObjects[i].Err = err
		}
		if schemaVersion > maxSchemaVersion {
			maxSchemaVersion = schemaVersion
		}

		if obj.ID == "" {
			// Generate UUID for the new object
			uid, err := generateUUID()
			obj.ID = uid
			batchObjects[i].Err = err
		} else {
			if _, err := uuid.Parse(obj.ID.String()); err != nil {
				batchObjects[i].Err = err
			}
		}
		if obj.Properties == nil {
			obj.Properties = map[string]interface{}{}
		}
		obj.CreationTimeUnix = now
		obj.LastUpdateTimeUnix = now
		batchObjects[i].Object = obj
		batchObjects[i].UUID = obj.ID
		if batchObjects[i].Err != nil {
			continue
		}

		if len(fetchedClasses) == 0 || fetchedClasses[obj.Class].Class == nil {
			batchObjects[i].Err = fmt.Errorf("class '%v' not present in schema", obj.Class)
			continue
		}
		class := fetchedClasses[obj.Class].Class

		if err := validator.Object(ctx, class, obj, nil); err != nil {
			batchObjects[i].Err = err
			continue
		}

		if objectsPerClass[obj.Class] == nil {
			objectsPerClass[obj.Class] = make([]*models.Object, 0)
			originalIndexPerClass[obj.Class] = make([]int, 0)
		}
		objectsPerClass[obj.Class] = append(objectsPerClass[obj.Class], obj)
		originalIndexPerClass[obj.Class] = append(originalIndexPerClass[obj.Class], i)
	}

	for className, objectsForClass := range objectsPerClass {
		class := fetchedClasses[className]
		errorsPerObj, err := b.modulesProvider.BatchUpdateVector(ctx, class.Class, objectsForClass, b.findObject, b.logger)
		if err != nil {
			for i := range objectsForClass {
				origIndex := originalIndexPerClass[className][i]
				batchObjects[origIndex].Err = err
			}
		}
		for i, err := range errorsPerObj {
			origIndex := originalIndexPerClass[className][i]
			batchObjects[origIndex].Err = err
		}
	}

	return batchObjects, maxSchemaVersion
}
