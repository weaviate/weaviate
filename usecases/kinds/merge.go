//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

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
	Kind            kind.Kind
	Class           string
	ID              strfmt.UUID
	PrimitiveSchema map[string]interface{}
	References      BatchReferences
	Vector          []float32
	UpdateTime      int64
}

func (m *Manager) MergeAction(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Action) error {

	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("actions/%s", id.String()))
	if err != nil {
		return err
	}

	previous, err := m.retrievePreviousAndValidateMergeAction(ctx, principal, id, updated)
	if err != nil {
		return NewErrInvalidUserInput("invalid merge: %v", err)
	}
	primitive, refs := m.splitPrimitiveAndRefs(updated.Schema.(map[string]interface{}),
		updated.Class, id, kind.Action)

	vector, err := m.mergeActionSchemasAndVectorize(ctx, previous.ClassName, previous.Schema, primitive)
	if err != nil {
		return NewErrInternal("vectorize merged: %v", err)
	}

	err = m.vectorRepo.Merge(ctx, MergeDocument{
		Kind:            kind.Action,
		Class:           updated.Class,
		ID:              id,
		PrimitiveSchema: primitive,
		References:      refs,
		Vector:          vector,
		UpdateTime:      m.timeSource.Now(),
	})
	if err != nil {
		return NewErrInternal("repo: %v", err)
	}

	return nil
}

func (m *Manager) retrievePreviousAndValidateMergeAction(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Action) (*search.Result, error) {

	if updated.Class == "" {
		return nil, fmt.Errorf("class is a required (and immutable) field")
	}

	action, err := m.vectorRepo.ActionByID(ctx, id, nil, traverser.UnderscoreProperties{})
	if err != nil {
		return nil, err
	}

	if action == nil {
		return nil, fmt.Errorf("action object with id '%s' does not exist", id)
	}

	if action.ClassName != updated.Class {
		return nil, fmt.Errorf("class is immutable, but got '%s' for previous class '%s'",
			updated.Class, action.ClassName)
	}

	updated.ID = id
	err = m.validateAction(ctx, principal, updated)
	if err != nil {
		return nil, err
	}

	return action, nil
}

func (m *Manager) mergeActionSchemasAndVectorize(ctx context.Context, className string,
	old interface{}, new map[string]interface{}) ([]float32, error) {
	var merged map[string]interface{}
	if old == nil {
		merged = new
	} else {
		oldMap, ok := old.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected previous schema to be map, but got %#v", old)
		}

		for key, value := range new {
			oldMap[key] = value
		}

		merged = oldMap
	}

	return m.vectorizer.Action(ctx, &models.Action{Class: className, Schema: merged})
}

func (m *Manager) MergeThing(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Thing) error {

	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("things/%s", id.String()))
	if err != nil {
		return err
	}

	previous, err := m.retrievePreviousAndValidateMergeThing(ctx, principal, id, updated)
	if err != nil {
		return NewErrInvalidUserInput("invalid merge: %v", err)
	}
	primitive, refs := m.splitPrimitiveAndRefs(updated.Schema.(map[string]interface{}),
		updated.Class, id, kind.Thing)

	vector, err := m.mergeThingSchemasAndVectorize(ctx, previous.ClassName, previous.Schema, primitive)
	if err != nil {
		return NewErrInternal("vectorize merged: %v", err)
	}

	err = m.vectorRepo.Merge(ctx, MergeDocument{
		Kind:            kind.Thing,
		Class:           updated.Class,
		ID:              id,
		PrimitiveSchema: primitive,
		References:      refs,
		Vector:          vector,
		UpdateTime:      m.timeSource.Now(),
	})
	if err != nil {
		return NewErrInternal("repo: %v", err)
	}

	return nil
}

func (m *Manager) retrievePreviousAndValidateMergeThing(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Thing) (*search.Result, error) {

	if updated.Class == "" {
		return nil, fmt.Errorf("class is a required (and immutable) field")
	}

	thing, err := m.vectorRepo.ThingByID(ctx, id, nil, traverser.UnderscoreProperties{})
	if err != nil {
		return nil, err
	}

	if thing == nil {
		return nil, fmt.Errorf("thing object with id '%s' does not exist", id)
	}

	if thing.ClassName != updated.Class {
		return nil, fmt.Errorf("class is immutable, but got '%s' for previous class '%s'",
			updated.Class, thing.ClassName)
	}

	updated.ID = id
	err = m.validateThing(ctx, principal, updated)
	if err != nil {
		return nil, err
	}

	return thing, nil
}

func (m *Manager) mergeThingSchemasAndVectorize(ctx context.Context, className string,
	old interface{}, new map[string]interface{}) ([]float32, error) {
	var merged map[string]interface{}
	if old == nil {
		merged = new
	} else {
		oldMap, ok := old.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected previous schema to be map, but got %#v", old)
		}

		for key, value := range new {
			oldMap[key] = value
		}

		merged = oldMap
	}

	return m.vectorizer.Thing(ctx, &models.Thing{Class: className, Schema: merged})
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
