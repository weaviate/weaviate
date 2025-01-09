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

func validatePermissions(permissions ...*models.Permission) error {
	// TODO: allow regex pattern in collection and tenant name
	if len(permissions) == 0 {
		return fmt.Errorf("role has to have at least 1 permission")
	}

	for _, perm := range permissions {
		if perm.Collections != nil {
			if _, err := schema.ValidateClassNameIncludesRegex(*perm.Collections.Collection); err != nil {
				return err
			}
		}

		if perm.Tenants != nil {
			if _, err := schema.ValidateClassNameIncludesRegex(*perm.Tenants.Collection); err != nil {
				return err
			}
			if err := schema.ValidateTenantNameIncludesRegex(*perm.Tenants.Tenant); err != nil {
				return err
			}
		}

		if perm.Data != nil {
			var multiErr error
			if _, err := schema.ValidateClassNameIncludesRegex(*perm.Data.Collection); err != nil {
				multiErr = errors.Join(err)
			}
			if err := schema.ValidateTenantNameIncludesRegex(*perm.Data.Tenant); err != nil {
				multiErr = errors.Join(err)
			}
			if multiErr != nil {
				return multiErr
			}
		}
	}

	return nil
}
