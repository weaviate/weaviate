//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
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
	UpdatePropertyAddDataType(context.Context, *models.Principal, string, string, string) error
	GetSchema(principal *models.Principal) (schema.Schema, error)
}

// AddObject Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddObject(ctx context.Context, principal *models.Principal,
	class *models.Object) (*models.Object, error) {
	err := m.authorizer.Authorize(principal, "create", "objects")
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return m.addObjectToConnectorAndSchema(ctx, principal, class)
}

func (m *Manager) checkIDOrAssignNew(ctx context.Context,
	id strfmt.UUID) (strfmt.UUID, error) {
	if id == "" {
		newID, err := generateUUID()
		if err != nil {
			return "", NewErrInternal("could not generate id: %v", err)
		}
		return newID, nil
	}

	// only validate ID uniqueness if explicitly set
	if ok, err := m.exists(ctx, id); ok {
		return "", NewErrInvalidUserInput("id '%s' already exists", id)
	} else if err != nil {
		return "", NewErrInternal(err.Error())
	}
	return id, nil
}

func (m *Manager) addObjectToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	class *models.Object) (*models.Object, error) {
	id, err := m.checkIDOrAssignNew(ctx, class.ID)
	if err != nil {
		return nil, err
	}
	class.ID = id

	err = m.validateObject(ctx, principal, class)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	now := m.timeSource.Now()
	class.CreationTimeUnix = now
	class.LastUpdateTimeUnix = now

	err = m.vectorizeAndPutObject(ctx, class, principal)
	if err != nil {
		return nil, err
	}

	return class, nil
}

func (m *Manager) vectorizeAndPutObject(ctx context.Context, class *models.Object,
	principal *models.Principal) error {
	err := newVectorObtainer(m.vectorizerProvider, m.schemaManager,
		m.logger).Do(ctx, class, principal)
	if err != nil {
		return err
	}

	err = m.vectorRepo.PutObject(ctx, class, class.Vector)
	if err != nil {
		return NewErrInternal("store: %v", err)
	}

	return nil
}

func (m *Manager) validateObject(ctx context.Context, principal *models.Principal, class *models.Object) error {
	// Validate schema given in body with the weaviate schema
	if _, err := uuid.Parse(class.ID.String()); err != nil {
		return err
	}

	s, err := m.schemaManager.GetSchema(principal)
	if err != nil {
		return err
	}

	return validation.New(s, m.exists, m.config).Object(ctx, class)
}

func (m *Manager) exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	return m.vectorRepo.Exists(ctx, id)
}
