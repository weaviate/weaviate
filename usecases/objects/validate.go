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

	"github.com/weaviate/weaviate/entities/classcache"

	"github.com/weaviate/weaviate/entities/schema"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
)

// ValidateObject without adding it to the database. Can be used in UIs for
// async validation before submitting
func (m *Manager) ValidateObject(ctx context.Context, principal *models.Principal,
	obj *models.Object, repl *additional.ReplicationProperties,
) error {
	className := schema.UppercaseClassName(obj.Class)
	obj.Class = className

	err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Objects(className, obj.Tenant, obj.ID))
	if err != nil {
		return err
	}

	ctx = classcache.ContextWithClassCache(ctx)

	// we don't reveal any info that the end users cannot get through the structure of the data anyway
	fetchedClasses, err := m.schemaManager.GetCachedClassNoAuth(ctx, className)
	if err != nil {
		return err
	}

	err = m.validateObjectAndNormalizeNames(ctx, repl, obj, nil, fetchedClasses)
	if err != nil {
		var forbidden autherrs.Forbidden
		if errors.As(err, &forbidden) {
			return err
		}
		return NewErrInvalidUserInput("invalid object: %v", err)
	}

	return nil
}
