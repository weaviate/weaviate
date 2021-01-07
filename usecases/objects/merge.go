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

package objects

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type MergeDocument struct {
	Kind                 kind.Kind
	Class                string
	ID                   strfmt.UUID
	PrimitiveSchema      map[string]interface{}
	References           BatchReferences
	Vector               []float32
	UpdateTime           int64
	AdditionalProperties models.AdditionalProperties
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
	primitive, refs := m.splitPrimitiveAndRefs(updated.Properties.(map[string]interface{}),
		updated.Class, id, kind.Object)

	vector, source, err := m.mergeObjectSchemasAndVectorize(ctx, previous.ClassName, previous.Schema, primitive)
	if err != nil {
		return NewErrInternal("vectorize merged: %v", err)
	}

	err = m.vectorRepo.Merge(ctx, MergeDocument{
		Kind:            kind.Object,
		Class:           updated.Class,
		ID:              id,
		PrimitiveSchema: primitive,
		References:      refs,
		Vector:          vector,
		UpdateTime:      m.timeSource.Now(),
		AdditionalProperties: models.AdditionalProperties{
			Interpretation: &models.Interpretation{
				Source: source,
			},
		},
	})
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

	object, err := m.vectorRepo.ObjectByID(ctx, id, nil, traverser.AdditionalProperties{})
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

func (m *Manager) mergeObjectSchemasAndVectorize(ctx context.Context, className string,
	old interface{}, new map[string]interface{}) ([]float32, []*models.InterpretationSource, error) {
	var merged map[string]interface{}
	if old == nil {
		merged = new
	} else {
		oldMap, ok := old.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("expected previous schema to be map, but got %#v", old)
		}

		for key, value := range new {
			oldMap[key] = value
		}

		merged = oldMap
	}

	v, source, err := m.vectorizer.Object(ctx, &models.Object{Class: className, Properties: merged})
	if err != nil {
		return nil, nil, err
	}

	return v, sourceFromInputElements(source), nil
}

func (m *Manager) splitPrimitiveAndRefs(in map[string]interface{}, sourceClass string,
	sourceID strfmt.UUID, sourceKind kind.Kind) (map[string]interface{}, BatchReferences) {
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
				Kind:     sourceKind,
			}

			outRefs = append(outRefs, BatchReference{From: source, To: target})
		}
	}

	return primitive, outRefs
}
