//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
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
)

type MergeDocument struct {
	Kind            kind.Kind
	Class           string
	ID              strfmt.UUID
	PrimitiveSchema map[string]interface{}
	References      BatchReferences
}

func (m *Manager) MergeThing(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Thing) error {

	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("things/%s", id.String()))
	if err != nil {
		return err
	}

	if err := m.validateMergeThing(ctx, principal, id, updated); err != nil {
		return fmt.Errorf("invalid merge: %v", err)
	}
	primitive, refs := m.splitPrimitiveAndRefs(updated.Schema.(map[string]interface{}),
		updated.Class, id, kind.Thing)
	m.vectorRepo.Merge(ctx, MergeDocument{
		Kind:            kind.Thing,
		Class:           updated.Class,
		ID:              id,
		PrimitiveSchema: primitive,
		References:      refs,
	})

	return nil
}

func (m *Manager) validateMergeThing(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Thing) error {

	if updated.Class == "" {
		return fmt.Errorf("class is a required (and immutable) field")
	}

	ok, err := m.vectorRepo.Exists(ctx, id)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("thing object with id '%s' does not exist", id)
	}

	updated.ID = id
	err = m.validateThing(ctx, principal, updated)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manager) MergeAction(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Action) error {

	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("actions/%s", id.String()))
	if err != nil {
		return err
	}

	if err := m.validateMergeAction(ctx, principal, id, updated); err != nil {
		return fmt.Errorf("invalid merge: %v", err)
	}
	primitive, refs := m.splitPrimitiveAndRefs(updated.Schema.(map[string]interface{}),
		updated.Class, id, kind.Action)
	m.vectorRepo.Merge(ctx, MergeDocument{
		Kind:            kind.Action,
		Class:           updated.Class,
		ID:              id,
		PrimitiveSchema: primitive,
		References:      refs,
	})

	return nil
}

func (m *Manager) validateMergeAction(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Action) error {

	if updated.Class == "" {
		return fmt.Errorf("class is a required (and immutable) field")
	}

	ok, err := m.vectorRepo.Exists(ctx, id)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("action object with id '%s' does not exist", id)
	}

	updated.ID = id
	err = m.validateAction(ctx, principal, updated)
	if err != nil {
		return err
	}

	return nil
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
