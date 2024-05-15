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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects/validation"
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
	Tenant               string                      `json:"tenant"`
}

type objectMergeValidator struct {
	config            *config.WeaviateConfig
	authorizer        authorizer
	vectorRepo        VectorRepo
	autoSchemaManager *autoSchemaManager
	schemaManager     schemaManager
}

func (m *objectMergeValidator) validateObject(ctx context.Context,
	principal *models.Principal, repl *additional.ReplicationProperties,
	incoming *models.Object, existing *models.Object,
) error {
	class, err := m.validateSchema(ctx, principal, incoming)
	if err != nil {
		return err
	}

	return validation.New(m.vectorRepo.Exists, m.config, repl).
		Object(ctx, class, incoming, existing)
}

func (m *objectMergeValidator) validateSchema(ctx context.Context,
	principal *models.Principal, obj *models.Object,
) (*models.Class, error) {
	// Validate schema given in body with the weaviate schema
	if _, err := uuid.Parse(obj.ID.String()); err != nil {
		return nil, err
	}

	class, err := m.schemaManager.GetClass(ctx, principal, obj.Class)
	if err != nil {
		return nil, err
	}

	if class == nil {
		return nil, fmt.Errorf("class %q not found in schema", obj.Class)
	}

	return class, nil
}

func (m *objectMergeValidator) validateObjectMerge(ctx context.Context, principal *models.Principal,
	updates *models.Object, repl *additional.ReplicationProperties,
) (*models.Object, []string, *Error) {
	if err := m.validateInputs(updates); err != nil {
		return nil, nil, &Error{"bad request", StatusBadRequest, err}
	}
	cls, id := updates.Class, updates.ID
	path := fmt.Sprintf("objects/%s/%s", cls, id)
	if err := m.authorizer.Authorize(principal, "update", path); err != nil {
		return nil, nil, &Error{path, StatusForbidden, err}
	}

	// TODO: support metrics and allocChecker
	// m.metrics.MergeObjectInc()
	// defer m.metrics.MergeObjectDec()
	// if err := m.allocChecker.CheckAlloc(memwatch.EstimateObjectMemory(updates)); err != nil {
	// 	m.logger.WithError(err).Errorf("memory pressure: cannot process patch object")
	// 	return nil, nil, &Error{path, StatusInternalServerError, err}
	// }

	ctx = classcache.ContextWithClassCache(ctx)
	obj, err := m.vectorRepo.Object(ctx, cls, id, nil, additional.Properties{}, repl, updates.Tenant)
	if err != nil {
		switch err.(type) {
		case ErrMultiTenancy:
			return nil, nil, &Error{"repo.object", StatusUnprocessableEntity, err}
		default:
			return nil, nil, &Error{"repo.object", StatusInternalServerError, err}
		}
	}
	if obj == nil {
		return nil, nil, &Error{"not found", StatusNotFound, err}
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
	if err := m.validateObject(
		ctx, principal, repl, updates, prevObj); err != nil {
		return nil, nil, &Error{"bad request", StatusBadRequest, err}
	}

	if updates.Properties == nil {
		updates.Properties = map[string]interface{}{}
	}
	return prevObj, propertiesToDelete, nil
}

func (m *objectMergeValidator) validateInputs(updates *models.Object) error {
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

func (m *Manager) MergeObject(ctx context.Context, principal *models.Principal,
	updates *models.Object, repl *additional.ReplicationProperties, schemaVersion uint64,
) *Error {
	mergeValidator := &objectMergeValidator{
		config:            m.config,
		authorizer:        m.authorizer,
		vectorRepo:        m.vectorRepo,
		autoSchemaManager: m.autoSchemaManager,
		schemaManager:     m.schemaManager,
	}

	prevObj, propertiesToDelete, err := mergeValidator.
		validateObjectMerge(ctx, principal, updates, repl)
	if err != nil {
		return err
	}

	return m.patchObject(ctx, principal, prevObj, updates, repl, propertiesToDelete, updates.Tenant, schemaVersion)
}

// patchObject patches an existing object obj with updates
func (m *Manager) patchObject(ctx context.Context, principal *models.Principal,
	prevObj, updates *models.Object, repl *additional.ReplicationProperties,
	propertiesToDelete []string, tenant string, schemaVersion uint64,
) *Error {
	cls, id := updates.Class, updates.ID
	primitive, refs := splitPrimitiveAndRefs(updates.Properties.(map[string]interface{}), cls, id)
	objWithVec, err := mergeObjectSchemaAndVectorize(ctx, cls, prevObj.Properties,
		primitive, principal, prevObj.Vector, updates.Vector, prevObj.Vectors,
		updates.Vectors, updates.ID, merger{
			schemaManager:   m.schemaManager,
			modulesProvider: m.modulesProvider,
			findObject:      m.findObject,
			logger:          m.logger,
		})
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
	if err := m.schemaManager.WaitForUpdate(ctx, schemaVersion); err != nil {
		return &Error{
			Msg:  fmt.Sprintf("error waiting for local schema to catch up to version %d", schemaVersion),
			Code: StatusInternalServerError,
			Err:  err,
		}
	}
	if err := m.vectorRepo.Merge(ctx, mergeDoc, repl, tenant, schemaVersion); err != nil {
		return &Error{"repo.merge", StatusInternalServerError, err}
	}

	return nil
}

type merger struct {
	schemaManager   schemaManager
	modulesProvider ModulesProvider
	findObject      objectFinder
	logger          logrus.FieldLogger
}

func mergeObjectSchemaAndVectorize(ctx context.Context, className string,
	prevPropsSch models.PropertySchema, nextProps map[string]interface{},
	principal *models.Principal, prevVec, nextVec []float32,
	prevVecs models.Vectors, nextVecs models.Vectors, id strfmt.UUID,
	merger merger,
) (*models.Object, error) {
	vclasses, err := merger.schemaManager.GetCachedClass(ctx, principal, className)
	if err != nil {
		return nil, err
	}
	vclass := vclasses[className]
	class := vclass.Class

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
	obj := &models.Object{Class: className, Properties: mergedProps, Vector: vector, Vectors: vectors, ID: id}
	if err := merger.modulesProvider.UpdateVector(ctx, obj, class, merger.findObject, merger.logger); err != nil {
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

func splitPrimitiveAndRefs(in map[string]interface{}, sourceClass string,
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
