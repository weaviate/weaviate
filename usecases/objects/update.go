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

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/versioned"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// UpdateObject updates object of class.
// If the class contains a network ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateObject(ctx context.Context, principal *models.Principal,
	class string, id strfmt.UUID, updates *models.Object,
	repl *additional.ReplicationProperties,
) (*models.Object, error) {
	className := schema.UppercaseClassName(updates.Class)
	updates.Class = className

	if err := m.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Objects(updates.Class, updates.Tenant, updates.ID)); err != nil {
		return nil, err
	}

	ctx = classcache.ContextWithClassCache(ctx)
	// we don't reveal any info that the end users cannot get through the structure of the data anyway
	fetchedClasses, err := m.schemaManager.GetCachedClassNoAuth(ctx, className)
	if err != nil {
		return nil, err
	}

	m.metrics.UpdateObjectInc()
	defer m.metrics.UpdateObjectDec()

	if err := m.allocChecker.CheckAlloc(memwatch.EstimateObjectMemory(updates)); err != nil {
		m.logger.WithError(err).Errorf("memory pressure: cannot process update object")
		return nil, fmt.Errorf("cannot process update object: %w", err)
	}

	return m.updateObjectToConnectorAndSchema(ctx, principal, class, id, updates, repl, fetchedClasses)
}

func (m *Manager) updateObjectToConnectorAndSchema(ctx context.Context,
	principal *models.Principal, className string, id strfmt.UUID, updates *models.Object,
	repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (*models.Object, error) {
	if id != updates.ID {
		return nil, NewErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	obj, err := m.getObjectFromRepo(ctx, className, id, additional.Properties{}, repl, updates.Tenant)
	if err != nil {
		return nil, err
	}

	maxSchemaVersion := fetchedClasses[className].Version
	schemaVersion, err := m.autoSchemaManager.autoSchema(ctx, principal, false, fetchedClasses, updates)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}
	if schemaVersion > maxSchemaVersion {
		maxSchemaVersion = schemaVersion
	}

	m.logger.
		WithField("object", "kinds_update_requested").
		WithField("original", obj).
		WithField("updated", updates).
		WithField("id", id).
		Debug("received update kind request")

	class := fetchedClasses[className].Class

	prevObj := obj.Object()
	err = m.validateObjectAndNormalizeNames(ctx, repl, updates, prevObj, fetchedClasses)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	// Set the original creation timestamp before call to put,
	// otherwise it is lost. This is because `class` is unmarshalled
	// directly from the request body, therefore `CreationTimeUnix`
	// inherits the zero value.
	updates.CreationTimeUnix = obj.Created
	updates.LastUpdateTimeUnix = m.timeSource.Now()

	err = m.modulesProvider.UpdateVector(ctx, updates, class, m.findObject, m.logger)
	if err != nil {
		return nil, NewErrInternal("update object: %v", err)
	}

	if err := m.schemaManager.WaitForUpdate(ctx, maxSchemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", maxSchemaVersion, err)
	}

	vectors, multiVectors, err := dto.GetVectors(updates.Vectors)
	if err != nil {
		return nil, fmt.Errorf("put object: cannot get vectors: %w", err)
	}
	err = m.vectorRepo.PutObject(ctx, updates, updates.Vector, vectors, multiVectors, repl, maxSchemaVersion)
	if err != nil {
		return nil, fmt.Errorf("put object: %w", err)
	}

	return updates, nil
}
