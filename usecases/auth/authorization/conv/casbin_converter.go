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

package conv

import (
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func RolesToPolicies(roles ...*models.Role) (map[string][]authorization.Policy, error) {
	rolesmap := make(map[string][]authorization.Policy)
	for idx := range roles {
		rolesmap[*roles[idx].Name] = []authorization.Policy{}
		for _, permission := range roles[idx].Permissions {
			policy, err := policy(permission)
			if err != nil {
				return rolesmap, fmt.Errorf("policy: %w", err)
			}
			rolesmap[*roles[idx].Name] = append(rolesmap[*roles[idx].Name], *policy)
		}
	}

	return rolesmap, nil
}

func PermissionToPolicies(permissions ...*models.Permission) ([]*authorization.Policy, error) {
	policies := []*authorization.Policy{}
	for idx := range permissions {
		policy, err := policy(permissions[idx])
		if err != nil {
			return nil, fmt.Errorf("policy: %w", err)
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

func PathToPermission(verb, path string) (*models.Permission, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 1 {
		return nil, fmt.Errorf("invalid path")
	}

	return permission([]string{"", path, verb, parts[0]}, false)
}

func PoliciesToPermission(policies ...authorization.Policy) ([]*models.Permission, error) {
	permissions := []*models.Permission{}
	for idx := range policies {
		// 1st empty string to replace casbin pattern of having policy name as 1st place
		// e.g.  tester, roles/.*, (C)|(R)|(U)|(D), roles
		// see newPolicy()
		perm, err := permission([]string{"", policies[idx].Resource, policies[idx].Verb, policies[idx].Domain}, true)
		if err != nil {
			return nil, err
		}
		if perm.Action == nil {
			continue
		}
		permissions = append(permissions, perm)
	}
	return permissions, nil
}

func CasbinPolicies(casbinPolicies ...[][]string) (map[string][]authorization.Policy, error) {
	rolesPermissions := make(map[string][]authorization.Policy)
	for _, p := range casbinPolicies {
		for _, policyParts := range p {
			name := TrimRoleNamePrefix(policyParts[0])
			if slices.Contains(authorization.BuiltInRoles, name) {
				perms := authorization.BuiltInPermissions[name]
				for _, p := range perms {
					perm, err := policy(p)
					if err != nil {
						return nil, fmt.Errorf("policy: %w", err)
					}
					rolesPermissions[name] = append(rolesPermissions[name], *perm)
				}
			} else {
				perm, err := permission(policyParts, true)
				if err != nil {
					return nil, fmt.Errorf("permission: %w", err)
				}
				weaviatePerm, err := policy(perm)
				if err != nil {
					return nil, fmt.Errorf("policy: %w", err)
				}
				rolesPermissions[name] = append(rolesPermissions[name], *weaviatePerm)
			}
		}
	}
	return rolesPermissions, nil
}
