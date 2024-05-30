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
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

// AddObject Class Instance to the connected DB.
func (m *Manager) AddObject(ctx context.Context, principal *models.Principal, object *models.Object,
	repl *additional.ReplicationProperties,
) (*models.Object, error) {
	err := m.authorizer.Authorize(principal, "create", "objects")
	if err != nil {
		return nil, err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	m.metrics.AddObjectInc()
	defer m.metrics.AddObjectDec()

	if err := m.allocChecker.CheckAlloc(memwatch.EstimateObjectMemory(object)); err != nil {
		m.logger.WithError(err).Errorf("memory pressure: cannot process add object")
		return nil, fmt.Errorf("cannot process add object: %w", err)
	}

	ctx = classcache.ContextWithClassCache(ctx)
	return m.addObjectToConnectorAndSchema(ctx, principal, object, repl)
}

func (m *Manager) checkIDOrAssignNew(ctx context.Context, principal *models.Principal,
	class *models.Class, id strfmt.UUID, repl *additional.ReplicationProperties, tenant string,
) (validatedID strfmt.UUID, err error) {
	if id == "" {
		validatedID, err = generateUUID()
		if err != nil {
			err = NewErrInternal("could not generate id: %v", err)
			return
		}
	} else {
		// IDs are always returned lowercase, but they are written
		// to disk as uppercase, when provided that way. Here we
		// ensure they are lowercase on disk as well, so things
		// like filtering are not affected.
		// See: https://github.com/weaviate/weaviate/issues/2647
		validatedID = strfmt.UUID(strings.ToLower(id.String()))

		if ok, existErr := m.vectorRepo.Exists(ctx, class.Class, validatedID, repl, tenant); ok {
			err = NewErrInvalidUserInput("id '%s' already exists", id)
			return
		} else if existErr != nil {
			err = existErr
			switch err.(type) {
			case ErrInvalidUserInput:
				return
			case ErrMultiTenancy:
				if enterrors.IsTenantNotFound(err) && schema.AutoTenantCreationEnabled(class) {
					// This is fine, the class is configured to create non-existing tenants
					break
				}
				return
			default:
				err = NewErrInternal(err.Error())
				return
			}
		}
	}

	if schema.AutoTenantCreationEnabled(class) {
		_, err = m.schemaManager.AddTenants(ctx, principal, class.Class, []*models.Tenant{{Name: tenant}})
		if err != nil {
			err = NewErrInternal(err.Error())
			return
		}
	}

	return
}

func (m *Manager) addObjectToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	object *models.Object, repl *additional.ReplicationProperties,
) (*models.Object, error) {
	vclasses, err := m.schemaManager.GetCachedClass(ctx, principal, object.Class)
	if err != nil {
		return nil, err
	}
	class := vclasses[object.Class].Class

	id, err := m.checkIDOrAssignNew(ctx, principal, class, object.ID, repl, object.Tenant)
	if err != nil {
		return nil, err
	}
	object.ID = id

	schemaVersion, err := m.autoSchemaManager.autoSchema(ctx, principal, true, object)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	err = m.validateObjectAndNormalizeNames(ctx, principal, repl, object, nil)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	now := m.timeSource.Now()
	object.CreationTimeUnix = now
	object.LastUpdateTimeUnix = now
	if object.Properties == nil {
		object.Properties = map[string]interface{}{}
	}

	err = m.modulesProvider.UpdateVector(ctx, object, class, m.findObject, m.logger)
	if err != nil {
		return nil, err
	}

	// Ensure that the local schema has caught up to the version we used to validate
	if err := m.schemaManager.WaitForUpdate(ctx, schemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", schemaVersion, err)
	}
	err = m.vectorRepo.PutObject(ctx, object, object.Vector, object.Vectors, repl, schemaVersion)
	if err != nil {
		return nil, fmt.Errorf("put object: %w", err)
	}

	return object, nil
}

func (m *Manager) validateObjectAndNormalizeNames(ctx context.Context,
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

func (m *Manager) validateSchema(ctx context.Context,
	principal *models.Principal, obj *models.Object,
) (*models.Class, error) {
	// Validate schema given in body with the weaviate schema
	if _, err := uuid.Parse(obj.ID.String()); err != nil {
		return nil, err
	}

	vclasses, err := m.schemaManager.GetCachedClass(ctx, principal, obj.Class)
	if err != nil {
		return nil, err
	}

	if len(vclasses) == 0 || vclasses[obj.Class].Class == nil {
		return nil, fmt.Errorf("class %q not found in schema", obj.Class)
	}

	return vclasses[obj.Class].Class, nil
}
