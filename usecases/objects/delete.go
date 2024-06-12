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

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// DeleteObject Class Instance from the connected DB
//
// if class == "" it will delete all object with same id regardless of the class name.
// This is due to backward compatibility reasons and should be removed in the future
func (m *Manager) DeleteObject(ctx context.Context,
	principal *models.Principal, class string, id strfmt.UUID,
	repl *additional.ReplicationProperties, tenant string,
) error {
	path := fmt.Sprintf("objects/%s/%s", class, id)
	if class == "" {
		path = fmt.Sprintf("objects/%s", id)
	}
	err := m.authorizer.Authorize(principal, "delete", path)
	if err != nil {
		return err
	}

	ctx = classcache.ContextWithClassCache(ctx)

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	if err := m.allocChecker.CheckAlloc(memwatch.EstimateObjectDeleteMemory()); err != nil {
		m.logger.WithError(err).Errorf("memory pressure: cannot process delete object")
		return fmt.Errorf("cannot process delete object: %w", err)
	}

	m.metrics.DeleteObjectInc()
	defer m.metrics.DeleteObjectDec()

	if class == "" { // deprecated
		return m.deleteObjectFromRepo(ctx, id)
	}

	vclasses, err := m.schemaManager.GetCachedClass(ctx, principal, class)
	if err != nil {
		return fmt.Errorf("could not get class %s: %w", class, err)
	}

	// Ensure that the local schema has caught up to the version we used to validate
	if err := m.schemaManager.WaitForUpdate(ctx, vclasses[class].Version); err != nil {
		return fmt.Errorf("error waiting for local schema to catch up to version %d: %w", vclasses[class].Version, err)
	}
	if err = m.vectorRepo.DeleteObject(ctx, class, id, repl, tenant, vclasses[class].Version); err != nil {
		var e1 ErrMultiTenancy
		if errors.As(err, &e1) {
			return NewErrMultiTenancy(fmt.Errorf("delete object from vector repo: %w", err))
		}
		var e2 ErrInvalidUserInput
		if errors.As(err, &e2) {
			return NewErrMultiTenancy(fmt.Errorf("delete object from vector repo: %w", err))
		}
		return NewErrInternal("could not delete object from vector repo: %v", err)
	}

	return nil
}

// deleteObjectFromRepo deletes objects with same id and different classes.
//
// Deprecated
func (m *Manager) deleteObjectFromRepo(ctx context.Context, id strfmt.UUID) error {
	// There might be a situation to have UUIDs which are not unique across classes.
	// Added loop in order to delete all of the objects with given UUID across all classes.
	// This change is added in response to this issue:
	// https://github.com/weaviate/weaviate/issues/1836
	deleteCounter := 0
	for {
		objectRes, err := m.getObjectFromRepo(ctx, "", id, additional.Properties{}, nil, "")
		if err != nil {
			_, ok := err.(ErrNotFound)
			if ok {
				if deleteCounter == 0 {
					return err
				}
				return nil
			}
			return err
		}

		object := objectRes.Object()
		err = m.vectorRepo.DeleteObject(ctx, object.Class, id, nil, "", 0)
		if err != nil {
			return NewErrInternal("could not delete object from vector repo: %v", err)
		}
		deleteCounter++
	}
}
