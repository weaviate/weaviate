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
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
)

// DeleteObject Class Instance from the conncected DB
func (m *Manager) DeleteObject(ctx context.Context, principal *models.Principal, id strfmt.UUID) error {
	err := m.authorizer.Authorize(principal, "delete", fmt.Sprintf("objects/%s", id.String()))
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return m.deleteObjectFromRepo(ctx, id)
}

func (m *Manager) deleteObjectFromRepo(ctx context.Context, id strfmt.UUID) error {
	objectRes, err := m.getObjectFromRepo(ctx, id, additional.Properties{})
	if err != nil {
		return err
	}

	object := objectRes.Object()
	err = m.vectorRepo.DeleteObject(ctx, object.Class, id)
	if err != nil {
		return NewErrInternal("could not delete object from vector repo: %v", err)
	}

	return nil
}
