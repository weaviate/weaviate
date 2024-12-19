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

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// ValidateObject without adding it to the database. Can be used in UIs for
// async validation before submitting
func (m *Manager) ValidateObject(ctx context.Context, principal *models.Principal,
	obj *models.Object, repl *additional.ReplicationProperties,
) error {
	err := m.authorizer.Authorize(principal, authorization.READ, authorization.Objects(obj.Class, obj.Tenant, obj.ID))
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	ctx = classcache.ContextWithClassCache(ctx)
	err = m.validateObjectAndNormalizeNames(ctx, principal, repl, obj, nil)
	if err != nil {
		if _, ok := err.(autherrs.Forbidden); ok {
			return err
		}
		return NewErrInvalidUserInput("invalid object: %v", err)
	}

	return nil
}
