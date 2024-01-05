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
	"github.com/weaviate/weaviate/entities/models"
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

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	m.metrics.DeleteObjectInc()
	defer m.metrics.DeleteObjectDec()

	if class == "" { // deprecated
		return m.deleteObjectFromRepo(ctx, id)
	}

	ok, err := m.vectorRepo.Exists(ctx, class, id, repl, tenant)
	if err != nil {
		switch err.(type) {
		case ErrMultiTenancy:
			return NewErrMultiTenancy(fmt.Errorf("check object existence: %w", err))
		default:
			return NewErrInternal("check object existence: %v", err)
		}
	}
	if !ok {
		return NewErrNotFound("object %v could not be found", path)
	}

	err = m.vectorRepo.DeleteObject(ctx, class, id, repl, tenant)
	if err != nil {
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
		err = m.vectorRepo.DeleteObject(ctx, object.Class, id, nil, "")
		if err != nil {
			return NewErrInternal("could not delete object from vector repo: %v", err)
		}
		deleteCounter++
	}
}
