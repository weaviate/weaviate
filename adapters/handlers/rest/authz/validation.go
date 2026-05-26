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

package authz

import (
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func validatePermissions(allowEmpty bool, permissions ...*models.Permission) error {
	if !allowEmpty && len(permissions) == 0 {
		return fmt.Errorf("role has to have at least 1 permission")
	}

	for _, perm := range permissions {

		var (
			multiErr         error
			collectionsInput = perm.Collections
			tenantsInput     = perm.Tenants
			dataInput        = perm.Data
			backupsInput     = perm.Backups
			nodesInput       = perm.Nodes
			replicateInput   = perm.Replicate
		)
		if collectionsInput != nil {
			if collectionsInput.Collection != nil {
				_, err := schema.ValidateClassNameIncludesRegex(*collectionsInput.Collection)
				multiErr = errors.Join(multiErr, err)
			}
		}

		if tenantsInput != nil {
			if tenantsInput.Collection != nil {
				_, classErr := schema.ValidateClassNameIncludesRegex(*tenantsInput.Collection)
				multiErr = errors.Join(multiErr, classErr)
			}
			if tenantsInput.Tenant != nil {
				multiErr = errors.Join(multiErr, schema.ValidateTenantNameIncludesRegex(*tenantsInput.Tenant))
			}
		}

		if dataInput != nil {
			if dataInput.Collection != nil {
				_, err := schema.ValidateClassNameIncludesRegex(*dataInput.Collection)
				multiErr = errors.Join(multiErr, err)
			}

			if dataInput.Tenant != nil {
				multiErr = errors.Join(multiErr, schema.ValidateTenantNameIncludesRegex(*dataInput.Tenant))
			}
		}

		if backupsInput != nil {
			// Collection and User are a discriminated union (see conv.policy).
			if backupsInput.Collection != nil && backupsInput.User != nil {
				multiErr = errors.Join(multiErr,
					fmt.Errorf("backups permission cannot set both 'collection' and 'user'"))
			}
			if backupsInput.Collection != nil {
				_, err := schema.ValidateClassNameIncludesRegex(*backupsInput.Collection)
				multiErr = errors.Join(multiErr, err)
			}
		}

		if nodesInput != nil && nodesInput.Collection != nil {
			_, err := schema.ValidateClassNameIncludesRegex(*nodesInput.Collection)
			multiErr = errors.Join(multiErr, err)
		}

		if replicateInput != nil && replicateInput.Collection != nil {
			_, err := schema.ValidateClassNameIncludesRegex(*replicateInput.Collection)
			multiErr = errors.Join(multiErr, err)
		}

		if multiErr != nil {
			return multiErr
		}
	}

	return nil
}
