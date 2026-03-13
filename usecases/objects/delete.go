//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// DeleteObject Class Instance from the connected DB
//
// if class == "" it will delete all object with same id regardless of the class name.
// This is due to backward compatibility reasons and should be removed in the future
func (m *Manager) DeleteObject(ctx context.Context,
	principal *models.Principal, className string, id strfmt.UUID,
	repl *additional.ReplicationProperties, tenant string,
) error {
	className = schema.UppercaseClassName(className)
	className, _ = m.resolveAlias(className)

	err := m.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Objects(className, tenant, id))
	if err != nil {
		return err
	}
	ctx = classcache.ContextWithClassCache(ctx)

	if err := m.allocChecker.CheckAlloc(memwatch.EstimateObjectDeleteMemory()); err != nil {
		m.logger.WithError(err).Errorf("memory pressure: cannot process delete object")
		return fmt.Errorf("cannot process delete object: %w", err)
	}

	m.metrics.DeleteObjectInc()
	defer m.metrics.DeleteObjectDec()

	// we only use the schemaVersion in this endpoint
	fetchedClasses, err := m.schemaManager.GetCachedClassNoAuth(ctx, className)
	if err != nil {
		return fmt.Errorf("could not get class %s: %w", className, err)
	}

	maxSchemaVersion := fetchedClasses[className].Version
	if tenant != "" {
		tenantSchemaVersion, err := m.schemaManager.EnsureTenantActiveForWrite(ctx, className, tenant)
		if err != nil {
			return fmt.Errorf("error ensuring tenant active for write: %w", err)
		}
		maxSchemaVersion = max(maxSchemaVersion, tenantSchemaVersion)
	}

	if className == "" { // deprecated
		return m.deleteObjectFromRepo(ctx, id, time.UnixMilli(m.timeSource.Now()), maxSchemaVersion)
	}

	if err = m.vectorRepo.DeleteObject(ctx, className, id, time.UnixMilli(m.timeSource.Now()), repl, tenant, maxSchemaVersion); err != nil {
		var e1 ErrMultiTenancy
		if errors.As(err, &e1) {
			return NewErrMultiTenancy(fmt.Errorf("delete object from vector repo: %w", err))
		}
		var e2 ErrInvalidUserInput
		if errors.As(err, &e2) {
			return NewErrMultiTenancy(fmt.Errorf("delete object from vector repo: %w", err))
		}
		var e3 authzerrs.Forbidden
		if errors.As(err, &e3) {
			return fmt.Errorf("delete object from vector repo: %w", err)
		}
		var e4 ErrNotFound
		if errors.As(err, &e4) {
			return err
		}
		return NewErrInternal("could not delete object from vector repo: %v", err)
	}

	return nil
}

// deleteObjectFromRepo deletes objects with same id and different classes.
//
// Deprecated
func (m *Manager) deleteObjectFromRepo(ctx context.Context, id strfmt.UUID, deletionTime time.Time, maxSchemaVersion uint64) error {
	// There might be a situation to have UUIDs which are not unique across classes.
	// Added loop in order to delete all of the objects with given UUID across all classes.
	// This change is added in response to this issue:
	// https://github.com/weaviate/weaviate/issues/1836
	if err := m.schemaManager.WaitForUpdate(ctx, maxSchemaVersion); err != nil {
		return fmt.Errorf("error waiting for local schema to catch up to version %d: %w", maxSchemaVersion, err)
	}

	deleteCounter := 0
	for {
		objectRes, err := m.getObjectFromRepo(ctx, "", id, additional.Properties{}, nil, "")
		if err != nil {
			if errors.As(err, &ErrNotFound{}) {
				if deleteCounter == 0 {
					return err
				}
				return nil
			}
			return err
		}

		object := objectRes.Object()
		err = m.vectorRepo.DeleteObject(ctx, object.Class, id, deletionTime, nil, "", maxSchemaVersion)
		if err != nil {
			return NewErrInternal("could not delete object from vector repo: %v", err)
		}
		deleteCounter++
	}
}
