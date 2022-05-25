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
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
)

// DeleteObject Class Instance from the conncected DB
//
// if class == "" it will delete all object with same id regardless of the class name.
// This is due to backward compatibility reasons and should be removed in the future
func (m *Manager) DeleteObject(ctx context.Context, principal *models.Principal, class string, id strfmt.UUID) error {
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

	if class == "" { // deprecated
		return m.deleteObjectFromRepo(ctx, id)
	}
	err = m.vectorRepo.DeleteObject(ctx, class, id)
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
	// https://github.com/semi-technologies/weaviate/issues/1836
	deleteCounter := 0
	for {
		objectRes, err := m.getObjectFromRepo(ctx, "", id, additional.Properties{})
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
		err = m.vectorRepo.DeleteObject(ctx, object.Class, id)
		if err != nil {
			return NewErrInternal("could not delete object from vector repo: %v", err)
		}
		deleteCounter++
	}
}
