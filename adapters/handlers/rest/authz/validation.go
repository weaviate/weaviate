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
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func validatePermissions(namespacesEnabled, allowEmpty bool, permissions ...*models.Permission) error {
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
		if collectionsInput != nil && collectionsInput.Collection != nil {
			multiErr = errors.Join(multiErr, validatePermissionClassName(namespacesEnabled, *collectionsInput.Collection))
		}

		if tenantsInput != nil {
			if tenantsInput.Collection != nil {
				multiErr = errors.Join(multiErr, validatePermissionClassName(namespacesEnabled, *tenantsInput.Collection))
			}
			if tenantsInput.Tenant != nil {
				multiErr = errors.Join(multiErr, schema.ValidateTenantNameIncludesRegex(*tenantsInput.Tenant))
			}
		}

		if dataInput != nil {
			if dataInput.Collection != nil {
				multiErr = errors.Join(multiErr, validatePermissionClassName(namespacesEnabled, *dataInput.Collection))
			}

			if dataInput.Tenant != nil {
				multiErr = errors.Join(multiErr, schema.ValidateTenantNameIncludesRegex(*dataInput.Tenant))
			}
		}

		if backupsInput != nil && backupsInput.Collection != nil {
			multiErr = errors.Join(multiErr, validatePermissionClassName(namespacesEnabled, *backupsInput.Collection))
		}

		if nodesInput != nil && nodesInput.Collection != nil {
			multiErr = errors.Join(multiErr, validatePermissionClassName(namespacesEnabled, *nodesInput.Collection))
		}

		if replicateInput != nil && replicateInput.Collection != nil {
			multiErr = errors.Join(multiErr, validatePermissionClassName(namespacesEnabled, *replicateInput.Collection))
		}

		if multiErr != nil {
			return multiErr
		}
	}

	return nil
}

// validatePermissionClassName validates a class-name field in a permission. On
// namespace-enabled clusters it tolerates an optional "<namespace>:" qualifier so
// a global operator can check a namespace-local role's qualified rows; whether a
// given caller may submit a qualified name is enforced separately by
// validateNoQualifiedNamespaceInPolicies. The class part always follows the
// regular permission class-name rules.
func validatePermissionClassName(namespacesEnabled bool, name string) error {
	if namespacesEnabled {
		if ns, cls, ok := strings.Cut(name, schema.NamespaceSeparator); ok {
			if err := schema.ValidateNamespaceNameSyntax(ns); err != nil {
				return fmt.Errorf("'%s' is not a valid class name", name)
			}
			name = cls
		}
	}
	_, err := schema.ValidateClassNameIncludesRegex(name)
	return err
}
