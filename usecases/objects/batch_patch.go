//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// PatchObjects performs partial updates on multiple objects in batch.
// This implements RFC 7396 JSON Merge Patch semantics.
func (b *BatchManager) PatchObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	ctx = classcache.ContextWithClassCache(ctx)

	before := time.Now()
	b.metrics.BatchInc()
	defer b.metrics.BatchOp("total_uc_level", before.UnixNano())
	defer b.metrics.BatchDec()

	if len(objects) == 0 {
		return nil, errEmptyObjects
	}

	// Collect classes for authorization
	classesShards := make(map[string][]string)
	for _, obj := range objects {
		obj.Class = schema.UppercaseClassName(obj.Class)
		cls, _ := b.resolveAlias(obj.Class)
		obj.Class = cls
		classesShards[obj.Class] = append(classesShards[obj.Class], obj.Tenant)
	}
	knownClasses := map[string]versioned.Class{}

	// Check permissions for all classes
	for className, shards := range classesShards {
		vClass, err := b.schemaManager.GetCachedClassNoAuth(ctx, className)
		if err != nil {
			return nil, err
		}
		knownClasses[className] = vClass[className]

		if err := b.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.ShardsData(className, shards...)...); err != nil {
			return nil, err
		}
	}

	// Validate and prepare merge documents
	beforePreProcessing := time.Now()
	batchObjects, maxSchemaVersion := b.validateAndPrepareMerge(ctx, principal, objects, repl, knownClasses)
	b.metrics.BatchObjects(len(objects))
	b.metrics.BatchOp("total_preprocessing", beforePreProcessing.UnixNano())

	beforePersistence := time.Now()
	defer b.metrics.BatchOp("total_persistence_level", beforePersistence.UnixNano())

	// Wait for schema to catch up
	if err := b.schemaManager.WaitForUpdate(ctx, maxSchemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", maxSchemaVersion, err)
	}

	// Perform batch merge in database
	res, err := b.vectorRepo.BatchPatchObjects(ctx, batchObjects, repl, maxSchemaVersion)
	if err != nil {
		return nil, NewErrInternal("batch patch objects: %#v", err)
	}

	return res, nil
}

// validateAndPrepareMerge validates each object and prepares MergeDocument for each
func (b *BatchManager) validateAndPrepareMerge(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (BatchObjects, uint64) {
	var (
		now          = time.Now().UnixNano() / int64(time.Millisecond)
		batchObjects = make(BatchObjects, len(objects))

		objectsPerClass         = make(map[string][]*models.Object)
		originalIndexPerClass   = make(map[string][]int)
		existingObjectsPerClass = make(map[string][]*models.Object)
	)

	var maxSchemaVersion uint64
	for i, obj := range objects {
		batchObjects[i].OriginalIndex = i
		batchObjects[i].Object = obj
		batchObjects[i].UUID = obj.ID

		// Basic validation
		if obj.Class == "" {
			batchObjects[i].Err = errors.New("object has an empty class")
			continue
		}
		if obj.ID == "" {
			batchObjects[i].Err = errors.New("object has an empty id")
			continue
		}

		// Memory pressure check
		if b.allocChecker != nil {
			if err := b.allocChecker.CheckAlloc(memwatch.EstimateObjectMemory(obj)); err != nil {
				b.logger.WithError(err).Errorf("memory pressure: cannot process patch object %s/%s", obj.Class, obj.ID)
				batchObjects[i].Err = err
				continue
			}
		}

		// Auto schema
		schemaVersion, err := b.autoSchemaManager.autoSchema(ctx, principal, false, fetchedClasses, obj)
		if err != nil {
			batchObjects[i].Err = err
			continue
		}
		if schemaVersion > maxSchemaVersion {
			maxSchemaVersion = schemaVersion
		}

		// Validate class exists
		if len(fetchedClasses) == 0 || fetchedClasses[obj.Class].Class == nil {
			batchObjects[i].Err = fmt.Errorf("class '%v' not present in schema", obj.Class)
			continue
		}

		// Fetch existing object
		existingObj, err := b.vectorRepo.Object(ctx, obj.Class, obj.ID, nil, additional.Properties{}, repl, obj.Tenant)
		if err != nil {
			switch {
			case errors.As(err, &ErrMultiTenancy{}):
				batchObjects[i].Err = err
			default:
				if errors.As(err, &ErrDirtyReadOfDeletedObject{}) || errors.As(err, &ErrDirtyWriteOfDeletedObject{}) {
					batchObjects[i].Err = fmt.Errorf("object %s/%s not found", obj.Class, obj.ID)
				} else if errors.As(err, &authzerrs.Forbidden{}) {
					batchObjects[i].Err = errors.New("forbidden")
				} else {
					batchObjects[i].Err = fmt.Errorf("failed to fetch existing object: %w", err)
				}
			}
			continue
		}
		if existingObj == nil {
			batchObjects[i].Err = fmt.Errorf("object %s/%s not found", obj.Class, obj.ID)
			continue
		}

		// Validate and normalize names
		prevObj := existingObj.Object()
		if err := validateObjectAndNormalizeNames(ctx, b.config, b.vectorRepo.Exists, repl, obj, prevObj, fetchedClasses); err != nil {
			batchObjects[i].Err = err
			continue
		}

		// Ensure properties is a map
		if obj.Properties == nil {
			obj.Properties = map[string]interface{}{}
		}

		// Collect objects per class for vectorization
		if objectsPerClass[obj.Class] == nil {
			objectsPerClass[obj.Class] = make([]*models.Object, 0)
			originalIndexPerClass[obj.Class] = make([]int, 0)
			existingObjectsPerClass[obj.Class] = make([]*models.Object, 0)
		}
		objectsPerClass[obj.Class] = append(objectsPerClass[obj.Class], obj)
		originalIndexPerClass[obj.Class] = append(originalIndexPerClass[obj.Class], i)
		existingObjectsPerClass[obj.Class] = append(existingObjectsPerClass[obj.Class], prevObj)
	}

	// Process vectorization and create merge documents
	for className, objectsForClass := range objectsPerClass {
		class := fetchedClasses[className].Class
		existingObjects := existingObjectsPerClass[className]

		for idx, obj := range objectsForClass {
			origIndex := originalIndexPerClass[className][idx]
			if batchObjects[origIndex].Err != nil {
				continue
			}

			// Reuse the existing object that was already fetched and validated above
			prevObj := existingObjects[idx]

			// Identify properties to delete (null values in merge patch)
			var propertiesToDelete []string
			if obj.Properties != nil {
				for key, val := range obj.Properties.(map[string]interface{}) {
					if val == nil {
						propertiesToDelete = append(propertiesToDelete, schema.LowercaseFirstLetter(key))
					}
				}
			}

			// Split primitive and references
			primitive, refs := b.splitPrimitiveAndRefs(obj.Properties.(map[string]interface{}), obj.Class, obj.ID)

			// Merge and vectorize
			objWithVec, err := mergeObjectSchemaAndVectorizeShared(
				ctx,
				prevObj.Properties,
				primitive,
				prevObj.Vector,
				obj.Vector,
				prevObj.Vectors,
				obj.Vectors,
				obj.ID,
				class,
				b.modulesProvider,
				b.findObject,
				b.logger,
			)
			if err != nil {
				batchObjects[origIndex].Err = fmt.Errorf("merge and vectorize: %w", err)
				continue
			}

			// Create merge document
			mergeDoc := &MergeDocument{
				Class:              obj.Class,
				ID:                 obj.ID,
				PrimitiveSchema:    primitive,
				References:         refs,
				Vector:             objWithVec.Vector,
				Vectors:            objWithVec.Vectors,
				UpdateTime:         now,
				PropertiesToDelete: propertiesToDelete,
			}

			if objWithVec.Additional != nil {
				mergeDoc.AdditionalProperties = objWithVec.Additional
			}

			batchObjects[origIndex].MergeDoc = mergeDoc
		}
	}

	return batchObjects, maxSchemaVersion
}

// splitPrimitiveAndRefs is a helper to split properties into primitives and references
func (b *BatchManager) splitPrimitiveAndRefs(in map[string]interface{}, sourceClass string,
	sourceID strfmt.UUID,
) (map[string]interface{}, BatchReferences) {
	primitive := map[string]interface{}{}
	var outRefs BatchReferences

	for prop, value := range in {
		refs, ok := value.(models.MultipleRef)

		if !ok {
			// this must be a primitive field
			primitive[prop] = value
			continue
		}

		for _, ref := range refs {
			target, _ := crossref.Parse(ref.Beacon.String())
			// safe to ignore error as validation has already been passed

			source := &crossref.RefSource{
				Local:    true,
				PeerName: "localhost",
				Property: schema.PropertyName(prop),
				Class:    schema.ClassName(sourceClass),
				TargetID: sourceID,
			}

			outRefs = append(outRefs, BatchReference{From: source, To: target})
		}
	}

	return primitive, outRefs
}
