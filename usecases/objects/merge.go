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

	"github.com/weaviate/weaviate/entities/versioned"

	"github.com/weaviate/weaviate/entities/classcache"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type MergeDocument struct {
	Class                string                      `json:"class"`
	ID                   strfmt.UUID                 `json:"id"`
	PrimitiveSchema      map[string]interface{}      `json:"primitiveSchema"`
	References           BatchReferences             `json:"references"`
	Vector               []float32                   `json:"vector"`
	Vectors              models.Vectors              `json:"vectors"`
	UpdateTime           int64                       `json:"updateTime"`
	AdditionalProperties models.AdditionalProperties `json:"additionalProperties"`
	PropertiesToDelete   []string                    `json:"propertiesToDelete"`
}

func (m *Manager) MergeObject(ctx context.Context, principal *models.Principal,
	updates *models.Object, repl *additional.ReplicationProperties,
) *Error {
	if err := m.validateInputs(updates); err != nil {
		return &Error{"bad request", StatusBadRequest, err}
	}
	cls, id := updates.Class, updates.ID
	if err := m.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Objects(cls, updates.Tenant, id)); err != nil {
		return &Error{err.Error(), StatusForbidden, err}
	}

	className := schema.UppercaseClassName(updates.Class)
	updates.Class = className

	ctx = classcache.ContextWithClassCache(ctx)

	// we don't reveal any info that the end users cannot get through the structure of the data anyway
	fetchedClass, err := m.schemaManager.GetCachedClassNoAuth(ctx, className)
	if err != nil {
		if errors.As(err, &authzerrs.Forbidden{}) {
			return &Error{err.Error(), StatusForbidden, err}
		}

		return &Error{err.Error(), StatusBadRequest, NewErrInvalidUserInput("invalid object: %v", err)}
	}

	m.metrics.MergeObjectInc()
	defer m.metrics.MergeObjectDec()

	if err := m.allocChecker.CheckAlloc(memwatch.EstimateObjectMemory(updates)); err != nil {
		m.logger.WithError(err).Errorf("memory pressure: cannot process patch object")
		return &Error{err.Error(), StatusInternalServerError, err}
	}

	obj, err := m.vectorRepo.Object(ctx, cls, id, nil, additional.Properties{}, repl, updates.Tenant)
	if err != nil {
		switch {
		case errors.As(err, &ErrMultiTenancy{}):
			return &Error{"repo.object", StatusUnprocessableEntity, err}
		default:
			if errors.As(err, &ErrDirtyReadOfDeletedObject{}) || errors.As(err, &ErrDirtyWriteOfDeletedObject{}) {
				m.logger.WithError(err).Debugf("object %s/%s not found, possibly due to replication consistency races", cls, id)
				return &Error{"not found", StatusNotFound, err}
			}
			if errors.As(err, &authzerrs.Forbidden{}) {
				return &Error{"forbidden", StatusForbidden, err}
			}
			return &Error{"repo.object", StatusInternalServerError, err}
		}
	}
	if obj == nil {
		return &Error{"not found", StatusNotFound, err}
	}

	maxSchemaVersion, err := m.autoSchemaManager.autoSchema(ctx, principal, false, fetchedClass, updates)
	if err != nil {
		return &Error{"bad request", StatusBadRequest, NewErrInvalidUserInput("invalid object: %v", err)}
	}

	var propertiesToDelete []string
	if updates.Properties != nil {
		for key, val := range updates.Properties.(map[string]interface{}) {
			if val == nil {
				propertiesToDelete = append(propertiesToDelete, schema.LowercaseFirstLetter(key))
			}
		}
	}

	prevObj := obj.Object()
	if err := m.validateObjectAndNormalizeNames(ctx, repl, updates, prevObj, fetchedClass); err != nil {
		return &Error{"bad request", StatusBadRequest, err}
	}

	if updates.Properties == nil {
		updates.Properties = map[string]interface{}{}
	}

	return m.patchObject(ctx, prevObj, updates, repl, propertiesToDelete, updates.Tenant, fetchedClass, maxSchemaVersion)
}

// patchObject patches an existing object obj with updates
func (m *Manager) patchObject(ctx context.Context, prevObj, updates *models.Object, repl *additional.ReplicationProperties,
	propertiesToDelete []string, tenant string, fetchedClass map[string]versioned.Class, maxSchemaVersion uint64,
) *Error {
	cls, id := updates.Class, updates.ID
	class := fetchedClass[cls].Class
	primitive, refs := m.splitPrimitiveAndRefs(updates.Properties.(map[string]interface{}), cls, id)
	objWithVec, err := m.mergeObjectSchemaAndVectorize(ctx, prevObj.Properties,
		primitive, prevObj.Vector, updates.Vector, prevObj.Vectors, updates.Vectors, updates.ID, class)
	if err != nil {
		return &Error{"merge and vectorize", StatusInternalServerError, err}
	}
	mergeDoc := MergeDocument{
		Class:              cls,
		ID:                 id,
		PrimitiveSchema:    primitive,
		References:         refs,
		Vector:             objWithVec.Vector,
		Vectors:            objWithVec.Vectors,
		UpdateTime:         m.timeSource.Now(),
		PropertiesToDelete: propertiesToDelete,
	}

	if objWithVec.Additional != nil {
		mergeDoc.AdditionalProperties = objWithVec.Additional
	}

	// Ensure that the local schema has caught up to the version we used to validate
	if err := m.schemaManager.WaitForUpdate(ctx, maxSchemaVersion); err != nil {
		return &Error{
			Msg:  fmt.Sprintf("error waiting for local schema to catch up to version %d", maxSchemaVersion),
			Code: StatusInternalServerError,
			Err:  err,
		}
	}

	if err := m.vectorRepo.Merge(ctx, mergeDoc, repl, tenant, maxSchemaVersion); err != nil {
		if errors.As(err, &ErrDirtyReadOfDeletedObject{}) || errors.As(err, &ErrDirtyWriteOfDeletedObject{}) {
			m.logger.WithError(err).Debugf("object %s/%s not found, possibly due to replication consistency races", cls, id)
			return &Error{"not found", StatusNotFound, err}
		}
		return &Error{"repo.merge", StatusInternalServerError, err}
	}

	return nil
}

func (m *Manager) validateInputs(updates *models.Object) error {
	if updates == nil {
		return fmt.Errorf("empty updates")
	}
	if updates.Class == "" {
		return fmt.Errorf("empty class")
	}
	if updates.ID == "" {
		return fmt.Errorf("empty uuid")
	}
	return nil
}

func (m *Manager) mergeObjectSchemaAndVectorize(ctx context.Context, prevPropsSch models.PropertySchema,
	nextProps map[string]interface{}, prevVec, nextVec []float32, prevVecs models.Vectors, nextVecs models.Vectors,
	id strfmt.UUID, class *models.Class,
) (*models.Object, error) {
	var mergedProps map[string]interface{}

	vector := nextVec
	vectors := nextVecs
	if prevPropsSch == nil {
		mergedProps = nextProps
	} else {
		prevProps, ok := prevPropsSch.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected previous schema to be map, but got %#v", prevPropsSch)
		}

		mergedProps = map[string]interface{}{}
		for propName, propValue := range prevProps {
			mergedProps[propName] = propValue
		}
		for propName, propValue := range nextProps {
			mergedProps[propName] = propValue
		}
	}

	// Note: vector could be a nil vector in case a vectorizer is configured,
	// then the vectorizer will set it
	obj := &models.Object{Class: class.Class, Properties: mergedProps, Vector: vector, Vectors: vectors, ID: id}
	if err := m.modulesProvider.UpdateVector(ctx, obj, class, m.findObject, m.logger); err != nil {
		return nil, err
	}

	// If there is no vectorization module and no updated vector, use the previous vector(s)
	if obj.Vector == nil && class.Vectorizer == config.VectorizerModuleNone {
		obj.Vector = prevVec
	}

	if obj.Vectors == nil {
		obj.Vectors = models.Vectors{}
	}

	// check for each named vector if the previous vector should be used. This should only happen if
	// - the vectorizer is none
	// - the vector is not set in the update
	// - the vector was set in the previous object
	for name, vectorConfig := range class.VectorConfig {
		if _, ok := vectorConfig.Vectorizer.(map[string]interface{})[config.VectorizerModuleNone]; !ok {
			continue
		}

		prevTargetVector, ok := prevVecs[name]
		if !ok {
			continue
		}

		if _, ok := obj.Vectors[name]; !ok {
			obj.Vectors[name] = prevTargetVector
		}
	}

	return obj, nil
}

func (m *Manager) splitPrimitiveAndRefs(in map[string]interface{}, sourceClass string,
	sourceID strfmt.UUID,
) (map[string]interface{}, BatchReferences) {
	primitive := map[string]interface{}{}
	var outRefs BatchReferences

	for prop, value := range in {
		refs, ok := value.(models.MultipleRef)

		if !ok {
			// this must be a primitive filed
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
