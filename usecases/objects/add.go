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
	"errors"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/versioned"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/dto"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

// AddObject Class Instance to the connected DB.
func (m *Manager) AddObject(ctx context.Context, principal *models.Principal, object *models.Object,
	repl *additional.ReplicationProperties,
) (*models.Object, error) {
	className := schema.UppercaseClassName(object.Class)
	object.Class = className

	if err := m.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.ShardsData(className, object.Tenant)...); err != nil {
		return nil, err
	}

	m.metrics.AddObjectInc()
	defer m.metrics.AddObjectDec()

	ctx = classcache.ContextWithClassCache(ctx)
	// we don't reveal any info that the end users cannot get through the structure of the data anyway
	fetchedClasses, err := m.schemaManager.GetCachedClassNoAuth(ctx, className)
	if err != nil {
		return nil, err
	}

	if err := m.allocChecker.CheckAlloc(memwatch.EstimateObjectMemory(object)); err != nil {
		m.logger.WithError(err).Errorf("memory pressure: cannot process add object")
		return nil, fmt.Errorf("cannot process add object: %w", err)
	}

	return m.addObjectToConnectorAndSchema(ctx, principal, object, repl, fetchedClasses)
}

func (m *Manager) addObjectToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	object *models.Object, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (*models.Object, error) {
	id, err := m.checkIDOrAssignNew(ctx, principal, object.Class, object.ID, repl, object.Tenant)
	if err != nil {
		return nil, err
	}
	object.ID = id

	schemaVersion, err := m.autoSchemaManager.autoSchema(ctx, principal, true, fetchedClasses, object)
	if err != nil {
		return nil, fmt.Errorf("invalid object: %w", err)
	}

	if _, _, err = m.autoSchemaManager.autoTenants(ctx, principal, []*models.Object{object}, fetchedClasses); err != nil {
		return nil, err
	}

	class := fetchedClasses[object.Class].Class

	err = m.validateObjectAndNormalizeNames(ctx, repl, object, nil, fetchedClasses)
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
	vectors, multiVectors, err := dto.GetVectors(object.Vectors)
	if err != nil {
		return nil, fmt.Errorf("put object: cannot get vectors: %w", err)
	}
	err = m.vectorRepo.PutObject(ctx, object, object.Vector, vectors, multiVectors, repl, schemaVersion)
	if err != nil {
		return nil, fmt.Errorf("put object: %w", err)
	}

	return object, nil
}

func (m *Manager) checkIDOrAssignNew(ctx context.Context, principal *models.Principal,
	className string, id strfmt.UUID, repl *additional.ReplicationProperties, tenant string,
) (strfmt.UUID, error) {
	if id == "" {
		validatedID, err := generateUUID()
		if err != nil {
			return "", NewErrInternal("could not generate id: %v", err)
		}
		return validatedID, err
	}

	// IDs are always returned lowercase, but they are written
	// to disk as uppercase, when provided that way. Here we
	// ensure they are lowercase on disk as well, so things
	// like filtering are not affected.
	// See: https://github.com/weaviate/weaviate/issues/2647
	validatedID := strfmt.UUID(strings.ToLower(id.String()))

	exists, err := m.vectorRepo.Exists(ctx, className, validatedID, repl, tenant)
	if exists {
		return "", NewErrInvalidUserInput("id '%s' already exists", id)
	} else if err != nil {
		switch {
		case errors.As(err, &ErrInvalidUserInput{}):
			return "", err
		case errors.As(err, &ErrMultiTenancy{}):
			// This may be fine, the class is configured to create non-existing tenants.
			// A non-existing tenant will still be detected later on
			if enterrors.IsTenantNotFound(err) {
				break
			}
			return "", err
		default:
			if errors.As(err, &authzerrs.Forbidden{}) {
				return "", err
			}
			return "", NewErrInternal("%v", err)
		}
	}

	return validatedID, nil
}

func (m *Manager) validateObjectAndNormalizeNames(ctx context.Context,
	repl *additional.ReplicationProperties,
	incoming *models.Object, existing *models.Object, fetchedClasses map[string]versioned.Class,
) error {
	err := m.validateUUID(incoming)
	if err != nil {
		return err
	}

	if _, ok := fetchedClasses[incoming.Class]; !ok || fetchedClasses[incoming.Class].Class == nil {
		return fmt.Errorf("class %q not found in schema", incoming.Class)
	}
	class := fetchedClasses[incoming.Class].Class

	return validation.New(m.vectorRepo.Exists, m.config, repl).
		Object(ctx, class, incoming, existing)
}

func (m *Manager) validateUUID(obj *models.Object) error {
	// Validate schema given in body with the weaviate schema
	_, err := uuid.Parse(obj.ID.String())
	return err
}
