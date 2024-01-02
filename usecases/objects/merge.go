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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
)

type MergeDocument struct {
	Class                string                      `json:"class"`
	ID                   strfmt.UUID                 `json:"id"`
	PrimitiveSchema      map[string]interface{}      `json:"primitiveSchema"`
	References           BatchReferences             `json:"references"`
	Vector               []float32                   `json:"vector"`
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
	path := fmt.Sprintf("objects/%s/%s", cls, id)
	if err := m.authorizer.Authorize(principal, "update", path); err != nil {
		return &Error{path, StatusForbidden, err}
	}

	m.metrics.MergeObjectInc()
	defer m.metrics.MergeObjectDec()

	obj, err := m.vectorRepo.Object(ctx, cls, id, nil, additional.Properties{}, repl, updates.Tenant)
	if err != nil {
		switch err.(type) {
		case ErrMultiTenancy:
			return &Error{"repo.object", StatusUnprocessableEntity, err}
		default:
			return &Error{"repo.object", StatusInternalServerError, err}
		}
	}
	if obj == nil {
		return &Error{"not found", StatusNotFound, err}
	}

	err = m.autoSchemaManager.autoSchema(ctx, principal, updates, false)
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

	if err := m.validateObjectAndNormalizeNames(
		ctx, principal, repl, updates, obj.Object()); err != nil {
		return &Error{"bad request", StatusBadRequest, err}
	}

	if updates.Properties == nil {
		updates.Properties = map[string]interface{}{}
	}

	return m.patchObject(ctx, principal, obj, updates, repl, propertiesToDelete, updates.Tenant)
}

// patchObject patches an existing object obj with updates
func (m *Manager) patchObject(ctx context.Context, principal *models.Principal,
	obj *search.Result, updates *models.Object, repl *additional.ReplicationProperties,
	propertiesToDelete []string, tenant string,
) *Error {
	cls, id := updates.Class, updates.ID
	primitive, refs := m.splitPrimitiveAndRefs(updates.Properties.(map[string]interface{}), cls, id)
	objWithVec, err := m.mergeObjectSchemaAndVectorize(ctx, cls, obj.Schema,
		primitive, principal, obj.Vector, updates.Vector)
	if err != nil {
		return &Error{"merge and vectorize", StatusInternalServerError, err}
	}
	mergeDoc := MergeDocument{
		Class:              cls,
		ID:                 id,
		PrimitiveSchema:    primitive,
		References:         refs,
		Vector:             objWithVec.Vector,
		UpdateTime:         m.timeSource.Now(),
		PropertiesToDelete: propertiesToDelete,
	}

	if objWithVec.Additional != nil {
		mergeDoc.AdditionalProperties = objWithVec.Additional
	}

	if err := m.vectorRepo.Merge(ctx, mergeDoc, repl, tenant); err != nil {
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

func (m *Manager) mergeObjectSchemaAndVectorize(ctx context.Context, className string,
	old interface{}, new map[string]interface{},
	principal *models.Principal, oldVec, newVec []float32,
) (*models.Object, error) {
	var merged map[string]interface{}
	var vector []float32
	var objDiff *moduletools.ObjectDiff

	if old == nil {
		merged = new
		vector = newVec
	} else {
		oldMap, ok := old.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected previous schema to be map, but got %#v", old)
		}

		objDiff = moduletools.NewObjectDiff(oldVec)
		for key, value := range new {
			objDiff.WithProp(key, oldMap[key], value)
			oldMap[key] = value
		}

		merged = oldMap
		if newVec != nil {
			vector = newVec
		} else {
			vectorizerName, err := m.modulesProvider.VectorizerName(className)
			if err != nil {
				return nil, fmt.Errorf("find vectorizer name: %w", err)
			}
			if vectorizerName == config.VectorizerModuleNone {
				vector = oldVec
			}
		}
	}

	// Note: vector could be a nil vector in case a vectorizer is configured,
	// then the vectorizer will set it
	obj := &models.Object{Class: className, Properties: merged, Vector: vector}
	class, err := m.schemaManager.GetClass(ctx, principal, className)
	if err != nil {
		return nil, err
	}
	if err := m.modulesProvider.UpdateVector(ctx, obj, class, objDiff, m.findObject, m.logger); err != nil {
		return nil, err
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
