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

package authz

import (
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

var (
	ErrCollectionRequired = fmt.Errorf("collection is required")
)

func validatePermissions(permissions ...*models.Permission) error {
	if len(permissions) == 0 {
		return fmt.Errorf("role has to have at least 1 permission")
	}

	for _, perm := range permissions {
		var multiErr error
		if perm.Collections != nil {
			if perm.Collections.Collection == nil {
				return ErrCollectionRequired
			}
			_, err := schema.ValidateClassNameIncludesRegex(*perm.Collections.Collection)
			multiErr = errors.Join(err)
		}

		if perm.Tenants != nil {
			if perm.Tenants.Collection == nil {
				return ErrCollectionRequired
			}

			_, classErr := schema.ValidateClassNameIncludesRegex(*perm.Tenants.Collection)
			multiErr = errors.Join(classErr)

			if perm.Tenants.Tenant != nil {
				multiErr = errors.Join(schema.ValidateTenantNameIncludesRegex(*perm.Tenants.Tenant))
			}
		}

		if perm.Data != nil {
			if perm.Data.Collection == nil {
				return ErrCollectionRequired
			}
			_, err := schema.ValidateClassNameIncludesRegex(*perm.Data.Collection)
			multiErr = errors.Join(err)

			if perm.Data.Tenant != nil {
				multiErr = errors.Join(schema.ValidateTenantNameIncludesRegex(*perm.Data.Tenant))
			}
		}

		if perm.Backups != nil {
			if perm.Data.Collection == nil {
				return ErrCollectionRequired
			}
			_, err := schema.ValidateClassNameIncludesRegex(*perm.Backups.Collection)
			multiErr = errors.Join(err)
		}

		if perm.Nodes != nil {
			if perm.Data.Collection == nil {
				return ErrCollectionRequired
			}
			_, err := schema.ValidateClassNameIncludesRegex(*perm.Nodes.Collection)
			multiErr = errors.Join(err)
		}

		if multiErr != nil {
			return multiErr
		}
	}

	return nil
}
