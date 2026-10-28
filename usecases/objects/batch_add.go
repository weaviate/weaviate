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

package objects

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/objects/validation"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

var errEmptyObjects = NewErrInvalidUserInput("invalid param 'objects': cannot be empty, need at least one object for batching")

// AddObjects Class Instances in batch to the connected DB
func (b *BatchManager) AddObjects(ctx context.Context, principal *models.Principal,
	objects []*models.Object, fields []*string, repl *additional.ReplicationProperties,
) (BatchObjects, error) {
	ctx = classcache.ContextWithClassCache(ctx)

	classesShards := make(map[string][]string)
	for _, obj := range objects {
		cls, _, err := b.resolveNS(principal, obj.Class)
		if err != nil {
			return nil, userInputOrForbidden(err)
		}
		obj.Class = cls
		classesShards[obj.Class] = append(classesShards[obj.Class], obj.Tenant)
	}
	knownClasses := map[string]versioned.Class{}

	// The whole request fails if any collection lacks a permission. Sorted order
	// returns the same error on every run.
	for _, className := range slices.Sorted(maps.Keys(classesShards)) {
		shards := classesShards[className]

		// we don't leak any info that someone who inserts data does not have anyway
		vClass, err := b.schemaManager.GetCachedClassNoAuth(ctx, className)
		if err != nil {
			return nil, err
		}
		knownClasses[className] = vClass[className]

		if err := b.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.ShardsData(className, shards...)...); err != nil {
			return nil, err
		}
		if err := b.authorizer.AuthorizeAndRequireActiveNamespace(ctx, principal, authorization.CREATE, className, authorization.ShardsData(className, shards...)...); err != nil {
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
	for _, vc := range fetchedClasses {
		maxSchemaVersion = max(maxSchemaVersion, vc.Version)
	}

	batchObjects, schemaVersion := b.validateAndGetVector(ctx, principal, objects, repl, fetchedClasses)
	maxSchemaVersion = max(maxSchemaVersion, schemaVersion)

	schemaVersion, tenantCount, err := b.autoSchemaManager.autoTenants(ctx, principal, objects, fetchedClasses)
	if err != nil {
		return nil, fmt.Errorf("auto create tenants: %w", err)
	}

	maxSchemaVersion = max(maxSchemaVersion, schemaVersion)

	// ensure tenants are active when AutoTenantActivation is enabled
	classTenants := make(map[string][]string)
	for _, obj := range objects {
		if obj.Tenant == "" {
			continue
		}
		classTenants[obj.Class] = append(classTenants[obj.Class], obj.Tenant)
	}
	for className, tenants := range classTenants {
		slices.Sort(tenants)
		tenants = slices.Compact(tenants)
		activationVersion, err := b.schemaManager.EnsureTenantActiveForWrite(ctx, className, tenants...)
		if err != nil {
			return nil, fmt.Errorf("ensure tenant active: %w", err)
		}
		maxSchemaVersion = max(maxSchemaVersion, activationVersion)
	}

	b.metrics.BatchTenants(tenantCount)
	b.metrics.BatchObjects(len(objects))
	b.metrics.BatchOp("total_preprocessing", beforePreProcessing.UnixNano())

	var res BatchObjects

	beforePersistence := time.Now()
	defer b.metrics.BatchOp("total_persistence_level", beforePersistence.UnixNano())

	if res, err = b.vectorRepo.BatchPutObjects(ctx, batchObjects, repl, maxSchemaVersion); err != nil {
		return nil, NewErrInternal("batch objects: %w", err)
	}

	// Reaggregate a unanimous limit-exceeded into a top-level error so
	// the REST/gRPC handler can map it to 429 / RESOURCE_EXHAUSTED.
	if le := unanimousLimitExceeded(res); le != nil {
		return nil, le
	}

	return res, nil
}

// unanimousLimitExceeded returns a *usagelimits.LimitExceededError if and
// only if every BatchObject in res carries the same limit-exceeded error
// (matching Limit + Value). Otherwise nil.
func unanimousLimitExceeded(res BatchObjects) *usagelimits.LimitExceededError {
	if len(res) == 0 {
		return nil
	}
	first, ok := usagelimits.AsLimitExceeded(res[0].Err)
	if !ok {
		return nil
	}
	for i := 1; i < len(res); i++ {
		got, ok := usagelimits.AsLimitExceeded(res[i].Err)
		if !ok || got.Limit != first.Limit || got.Value != first.Value {
			return nil
		}
	}
	return first
}

func (b *BatchManager) validateAndGetVector(ctx context.Context, principal *models.Principal,
	objects []*models.Object, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (BatchObjects, uint64) {
	var (
		now          = time.Now().UnixNano() / int64(time.Millisecond)
		batchObjects = make(BatchObjects, len(objects))

		objectsPerClass       = make(map[string][]*models.Object)
		originalIndexPerClass = make(map[string][]int)
		validator             = validation.New(b.vectorRepo.Exists, b.config, repl,
			principal, b.qualifier)
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
			// Do not overwrite the error autoSchema returned for this entry.
			if err != nil {
				batchObjects[i].Err = err
			}
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
		// Convert BlobHash properties from raw base64 to hashes after vectorization.
		for _, obj := range objectsForClass {
			schema.HashBlobHashProperties(class.Class, obj)
		}
	}

	b.validateBatchVectorDimensions(ctx, batchObjects, fetchedClasses)

	return batchObjects, maxSchemaVersion
}

type IndexDimensionReader interface {
	GetTargetVectorDimension(ctx context.Context, className, targetVector, tenant string) (int, error)
}

func isTargetVectorSkipped(class *models.Class, targetVector string) bool {
	if class == nil {
		return false
	}
	var vic interface{}
	if targetVector == "" {
		vic = class.VectorIndexConfig
	} else if len(class.VectorConfig) > 0 {
		if vc, ok := class.VectorConfig[targetVector]; ok {
			vic = vc.VectorIndexConfig
		}
	}
	if vic == nil {
		return false
	}
	switch c := vic.(type) {
	case hnsw.UserConfig:
		return c.Skip
	case *hnsw.UserConfig:
		return c != nil && c.Skip
	case map[string]interface{}:
		if skip, ok := c["skip"].(bool); ok {
			return skip
		}
	}
	return false
}

func (b *BatchManager) validateBatchVectorDimensions(ctx context.Context, batchObjects BatchObjects, fetchedClasses map[string]versioned.Class) {
	if len(batchObjects) == 0 {
		return
	}

	type classTarget struct {
		class        string
		targetVector string
		tenant       string
	}

	type objectKey struct {
		class  string
		tenant string
		id     strfmt.UUID
	}

	lastIndex := make(map[objectKey]int)
	for i, bObj := range batchObjects {
		if bObj.Object != nil && bObj.Object.ID != "" {
			k := objectKey{
				class:  bObj.Object.Class,
				tenant: bObj.Object.Tenant,
				id:     bObj.Object.ID,
			}
			lastIndex[k] = i
		}
	}

	// classTarget -> dim -> []int (batch indices)
	dimIndices := make(map[classTarget]map[int][]int)

	for i := range batchObjects {
		if batchObjects[i].Err != nil || batchObjects[i].Object == nil {
			continue
		}
		obj := batchObjects[i].Object

		// Exclude superseded duplicate objects (last-write-wins)
		if obj.ID != "" {
			k := objectKey{class: obj.Class, tenant: obj.Tenant, id: obj.ID}
			if lastIndex[k] != i {
				continue
			}
		}

		var classModel *models.Class
		if fetchedClasses != nil {
			if cls, ok := fetchedClasses[obj.Class]; ok {
				classModel = cls.Class
			}
		}

		// Verify model vector extractions upfront. If any vector fails, mark object error
		// and skip tracking so it does not falsely reject valid peer objects.
		var namedDims map[string]int
		if len(obj.Vectors) > 0 {
			namedDims = make(map[string]int, len(obj.Vectors))
			hasExtractionErr := false
			for targetVector, vec := range obj.Vectors {
				dim, err := extractModelVectorDim(vec)
				if err != nil {
					batchObjects[i].Err = fmt.Errorf("target vector %q: %w", targetVector, err)
					hasExtractionErr = true
					break
				}
				namedDims[targetVector] = dim
			}
			if hasExtractionErr {
				continue
			}
		}

		// Legacy default vector
		if len(obj.Vector) > 0 {
			if !isTargetVectorSkipped(classModel, "") {
				ct := classTarget{class: obj.Class, targetVector: "", tenant: obj.Tenant}
				if dimIndices[ct] == nil {
					dimIndices[ct] = make(map[int][]int)
				}
				dim := len(obj.Vector)
				dimIndices[ct][dim] = append(dimIndices[ct][dim], i)
			}
		}

		// Named vectors
		for targetVector, dim := range namedDims {
			if dim > 0 && !isTargetVectorSkipped(classModel, targetVector) {
				ct := classTarget{class: obj.Class, targetVector: targetVector, tenant: obj.Tenant}
				if dimIndices[ct] == nil {
					dimIndices[ct] = make(map[int][]int)
				}
				dimIndices[ct][dim] = append(dimIndices[ct][dim], i)
			}
		}
	}

	for ct, dimsMap := range dimIndices {
		if len(dimsMap) <= 1 {
			continue
		}

		// Established collection check: if the index already has an established dimension,
		// defer validation to shard so valid objects are not falsely rejected.
		if reader, ok := b.vectorRepo.(IndexDimensionReader); ok {
			dim, err := reader.GetTargetVectorDimension(ctx, ct.class, ct.targetVector, ct.tenant)
			if err == nil && dim > 0 {
				continue
			}
		}

		dims := make([]int, 0, len(dimsMap))
		for d := range dimsMap {
			dims = append(dims, d)
		}
		slices.Sort(dims)

		parts := make([]string, len(dims))
		for j, d := range dims {
			parts[j] = strconv.Itoa(d)
		}
		dimList := fmt.Sprintf("[%s]", strings.Join(parts, ", "))

		conflictErr := fmt.Errorf("target vector %q: conflicting dimensions in batch for uninitialized index (found %s); cannot establish dimension", ct.targetVector, dimList)

		for _, indices := range dimsMap {
			for _, idx := range indices {
				if batchObjects[idx].Err == nil {
					batchObjects[idx].Err = conflictErr
				}
			}
		}
	}
}

func extractModelVectorDim(vec models.Vector) (int, error) {
	switch v := vec.(type) {
	case []float32:
		return len(v), nil
	case models.C11yVector:
		return len(v), nil
	case [][]float32:
		if len(v) == 0 {
			return 0, nil
		}
		tokenDim := len(v[0])
		for _, token := range v[1:] {
			if len(token) != tokenDim {
				return -1, errors.New("multi vector array consists of vectors with varying dimensions")
			}
		}
		return tokenDim, nil
	case []interface{}:
		if len(v) == 0 {
			return 0, nil
		}
		if sub, ok := v[0].([]interface{}); ok {
			tokenDim := len(sub)
			for _, token := range v[1:] {
				if tokenSlice, ok := token.([]interface{}); ok {
					if len(tokenSlice) != tokenDim {
						return -1, errors.New("multi vector array consists of vectors with varying dimensions")
					}
				} else {
					return -1, errors.New("multi vector array consists of vectors with varying dimensions")
				}
			}
			return tokenDim, nil
		}
		return len(v), nil
	case [][]interface{}:
		if len(v) == 0 {
			return 0, nil
		}
		tokenDim := len(v[0])
		for _, token := range v[1:] {
			if len(token) != tokenDim {
				return -1, errors.New("multi vector array consists of vectors with varying dimensions")
			}
		}
		return tokenDim, nil
	default:
		return -1, nil
	}
}
