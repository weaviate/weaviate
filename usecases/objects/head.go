//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"errors"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// HeadObject check object's existence in the connected DB
func (m *Manager) HeadObject(ctx context.Context, principal *models.Principal, className string,
	id strfmt.UUID, repl *additional.ReplicationProperties, tenant string,
) (bool, *Error) {
	className, _ = m.resolveAlias(className)
	if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Objects(className, tenant, id)); err != nil {
		return false, &Error{err.Error(), StatusForbidden, err}
	}

	m.metrics.HeadObjectInc()
	defer m.metrics.HeadObjectDec()

	if cls := m.schemaManager.ResolveAlias(className); cls != "" {
		className = cls
	}

	ok, err := m.vectorRepo.Exists(ctx, className, id, repl, tenant)
	if err != nil {
		switch {
		case errors.As(err, &ErrMultiTenancy{}):
			return false, &Error{"repo.exists", StatusUnprocessableEntity, err}
		default:
			if (errors.As(err, &ErrDirtyReadOfDeletedObject{})) {
				return false, nil
			}
			return false, &Error{"repo.exists", StatusInternalServerError, err}
		}
	}
	return ok, nil
}
