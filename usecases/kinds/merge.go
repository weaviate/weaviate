package kinds

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

type MergeDocument struct {
	Kind            kind.Kind
	Class           string
	ID              strfmt.UUID
	PrimitiveSchema map[string]interface{}
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

	m.vectorRepo.Merge(ctx, MergeDocument{
		Kind:            kind.Thing,
		Class:           updated.Class,
		ID:              id,
		PrimitiveSchema: updated.Schema.(map[string]interface{}),
	})

	return nil
}

func (m *Manager) validateMergeThing(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, updated *models.Thing) error {

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
