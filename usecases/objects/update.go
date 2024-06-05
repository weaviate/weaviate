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

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// UpdateObject updates object of class.
// If the class contains a network ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateObject(ctx context.Context, principal *models.Principal,
	class string, id strfmt.UUID, updates *models.Object,
	repl *additional.ReplicationProperties,
) (*models.Object, error) {
	path := fmt.Sprintf("objects/%s/%s", class, id)
	if class == "" {
		path = fmt.Sprintf("objects/%s", id)
	}
	err := m.authorizer.Authorize(principal, "update", path)
	if err != nil {
		return nil, err
	}

	m.metrics.UpdateObjectInc()
	defer m.metrics.UpdateObjectDec()

	ctx = classcache.ContextWithClassCache(ctx)

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	if err := m.allocChecker.CheckAlloc(memwatch.EstimateObjectMemory(updates)); err != nil {
		m.logger.WithError(err).Errorf("memory pressure: cannot process update object")
		return nil, fmt.Errorf("cannot process update object: %w", err)
	}

	return m.updateObjectToConnectorAndSchema(ctx, principal, class, id, updates, repl)
}

func (m *Manager) updateObjectToConnectorAndSchema(ctx context.Context,
	principal *models.Principal, className string, id strfmt.UUID, updates *models.Object,
	repl *additional.ReplicationProperties,
) (*models.Object, error) {
	if id != updates.ID {
		return nil, NewErrInvalidUserInput("invalid update: field 'id' is immutable")
	}

	obj, err := m.getObjectFromRepo(ctx, className, id, additional.Properties{}, repl, updates.Tenant)
	if err != nil {
		return nil, err
	}

	var schemaVersion uint64
	if schemaVersion, err = m.autoSchemaManager.autoSchema(ctx, principal, false, updates); err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	m.logger.
		WithField("object", "kinds_update_requested").
		WithField("original", obj).
		WithField("updated", updates).
		WithField("id", id).
		Debug("received update kind request")

	prevObj := obj.Object()
	err = m.validateObjectAndNormalizeNames(
		ctx, principal, repl, updates, prevObj)
	if err != nil {
		return nil, NewErrInvalidUserInput("invalid object: %v", err)
	}

	// Set the original creation timestamp before call to put,
	// otherwise it is lost. This is because `class` is unmarshalled
	// directly from the request body, therefore `CreationTimeUnix`
	// inherits the zero value.
	updates.CreationTimeUnix = obj.Created
	updates.LastUpdateTimeUnix = m.timeSource.Now()

	vclasses, err := m.schemaManager.GetCachedClass(ctx, principal, className)
	if err != nil {
		return nil, err
	}

	vclass := vclasses[className]
	err = m.modulesProvider.UpdateVector(ctx, updates, vclass.Class, m.findObject, m.logger)
	if err != nil {
		return nil, NewErrInternal("update object: %v", err)
	}

	if err := m.schemaManager.WaitForUpdate(ctx, schemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", schemaVersion, err)
	}

	err = m.vectorRepo.PutObject(ctx, updates, updates.Vector, updates.Vectors, repl, schemaVersion)
	if err != nil {
		return nil, fmt.Errorf("put object: %w", err)
	}

	return updates, nil
}
