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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/objects/validation"
)

type schemaManager interface {
	GetSchema(principal *models.Principal) (schema.Schema, error)
	AddClass(ctx context.Context, principal *models.Principal,
		class *models.Class) error
	AddClassProperty(ctx context.Context, principal *models.Principal,
		class string, property *models.Property) error
}

// AddObject Class Instance to the connected DB.
func (m *Manager) AddObject(ctx context.Context, principal *models.Principal,
	object *models.Object) (*models.Object, error) {
	err := m.authorizer.Authorize(principal, "create", "objects")
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return m.addObjectToConnectorAndSchema(ctx, principal, object)
}

func (m *Manager) checkIDOrAssignNew(ctx context.Context, class string,
	id strfmt.UUID) (strfmt.UUID, error) {
	if id == "" {
		newID, err := generateUUID()
		if err != nil {
			return "", NewErrInternal("could not generate id: %v", err)
		}
		return newID, nil
	}

	// only validate ID uniqueness if explicitly set
	if ok, err := m.vectorRepo.Exists(ctx, class, id); ok {
		return "", NewErrInvalidUserInput("id '%s' already exists", id)
	} else if err != nil {
		return "", NewErrInternal(err.Error())
	}
	return id, nil
}

func (m *Manager) addObjectToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	object *models.Object) (*models.Object, error) {
	id, err := m.checkIDOrAssignNew(ctx, object.Class, object.ID)
	if err != nil {
		return nil, err
	}
	object.ID = id

	err = m.autoSchemaManager.autoSchema(ctx, principal, object)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	err = m.validateObject(ctx, principal, object)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	now := m.timeSource.Now()
	object.CreationTimeUnix = now
	object.LastUpdateTimeUnix = now

	err = m.vectorizeAndPutObject(ctx, object, principal)
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (m *Manager) vectorizeAndPutObject(ctx context.Context, object *models.Object,
	principal *models.Principal) error {
	err := newVectorObtainer(m.vectorizerProvider, m.schemaManager,
		m.logger).Do(ctx, object, principal)
	if err != nil {
		return err
	}

	err = m.vectorRepo.PutObject(ctx, object, object.Vector)
	if err != nil {
		return NewErrInternal("store: %v", err)
	}

	return nil
}

func (m *Manager) validateObject(ctx context.Context, principal *models.Principal, object *models.Object) error {
	// Validate schema given in body with the weaviate schema
	if _, err := uuid.Parse(object.ID.String()); err != nil {
		return err
	}

	s, err := m.schemaManager.GetSchema(principal)
	if err != nil {
		return err
	}

	return validation.New(s, m.vectorRepo.Exists, m.config).Object(ctx, object)
}
