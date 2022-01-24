//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
)

type MergeDocument struct {
	Class                string                      `json:"class"`
	ID                   strfmt.UUID                 `json:"id"`
	PrimitiveSchema      map[string]interface{}      `json:"primitiveSchema"`
	References           BatchReferences             `json:"references"`
	Vector               []float32                   `json:"vector"`
	UpdateTime           int64                       `json:"updateTime"`
	AdditionalProperties models.AdditionalProperties `json:"additionalProperties"`
}

func (m *Manager) MergeObject(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Object) error {
	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("objects/%s", id.String()))
	if err != nil {
		return err
	}

	previous, err := m.retrievePreviousAndValidateMergeObject(ctx, principal, id, updated)
	if err != nil {
		return NewErrInvalidUserInput("invalid merge: %v", err)
	}

	if updated.Properties == nil {
		updated.Properties = map[string]interface{}{}
	}

	primitive, refs := m.splitPrimitiveAndRefs(updated.Properties.(map[string]interface{}),
		updated.Class, id)

	objWithVec, err := m.mergeObjectSchemaAndVectorize(ctx, previous.ClassName, previous.Schema,
		primitive, principal, previous.Vector, updated.Vector)
	if err != nil {
		return NewErrInternal("vectorize merged: %v", err)
	}
	mergeDoc := MergeDocument{
		Class:           updated.Class,
		ID:              id,
		PrimitiveSchema: primitive,
		References:      refs,
		Vector:          objWithVec.Vector,
		UpdateTime:      m.timeSource.Now(),
	}

	if objWithVec.Additional != nil {
		mergeDoc.AdditionalProperties = objWithVec.Additional
	}

	err = m.vectorRepo.Merge(ctx, mergeDoc)
	if err != nil {
		return NewErrInternal("repo: %v", err)
	}

	return nil
}

func (m *Manager) retrievePreviousAndValidateMergeObject(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Object) (*search.Result, error) {
	if updated.Class == "" {
		return nil, fmt.Errorf("class is a required (and immutable) field")
	}

	object, err := m.vectorRepo.ObjectByID(ctx, id, nil, additional.Properties{})
	if err != nil {
		return nil, err
	}

	if object == nil {
		return nil, fmt.Errorf("object with id '%s' does not exist", id)
	}

	if object.ClassName != updated.Class {
		return nil, fmt.Errorf("class is immutable, but got '%s' for previous class '%s'",
			updated.Class, object.ClassName)
	}

	updated.ID = id
	err = m.validateObject(ctx, principal, updated)
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (m *Manager) mergeObjectSchemaAndVectorize(ctx context.Context, className string,
	old interface{}, new map[string]interface{},
	principal *models.Principal, oldVec, newVec []float32) (*models.Object, error) {
	var merged map[string]interface{}
	var vector []float32
	if old == nil {
		merged = new
		vector = newVec
	} else {
		oldMap, ok := old.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected previous schema to be map, but got %#v", old)
		}

		for key, value := range new {
			oldMap[key] = value
		}

		merged = oldMap
		if newVec != nil {
			vector = newVec
		} else {
			vector = oldVec
		}
	}

	// Note: vector could be a nil vector in case a vectorizer is configered,
	// then the obtainer will set it
	obj := &models.Object{Class: className, Properties: merged, Vector: vector}
	if err := newVectorObtainer(m.vectorizerProvider, m.schemaManager,
		m.logger).Do(ctx, obj, principal); err != nil {
		return nil, err
	}

	return obj, nil
}

func (m *Manager) splitPrimitiveAndRefs(in map[string]interface{}, sourceClass string,
	sourceID strfmt.UUID) (map[string]interface{}, BatchReferences) {
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
