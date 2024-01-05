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

// HeadObject check object's existence in the connected DB
func (m *Manager) HeadObject(ctx context.Context, principal *models.Principal, class string,
	id strfmt.UUID, repl *additional.ReplicationProperties, tenant string,
) (bool, *Error) {
	path := fmt.Sprintf("objects/%s", id)
	if class != "" {
		path = fmt.Sprintf("objects/%s/%s", class, id)
	}
	if err := m.authorizer.Authorize(principal, "head", path); err != nil {
		return false, &Error{path, StatusForbidden, err}
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return false, &Error{"cannot lock", StatusInternalServerError, err}
	}
	defer unlock()

	m.metrics.HeadObjectInc()
	defer m.metrics.HeadObjectDec()

	ok, err := m.vectorRepo.Exists(ctx, class, id, repl, tenant)
	if err != nil {
		switch err.(type) {
		case ErrMultiTenancy:
			return false, &Error{"repo.exists", StatusUnprocessableEntity, err}
		default:
			return false, &Error{"repo.exists", StatusInternalServerError, err}
		}
	}
	return ok, nil
}
