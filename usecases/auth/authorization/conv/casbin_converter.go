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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	RolesD            = "roles"
	Cluster           = "cluster"
	Collections       = "collections"
	Tenants           = "tenants"
	ObjectsCollection = "objects_collection"
	ObjectsTenant     = "objects_tenant"

	// rolePrefix = "r_"
	// userPrefix = "u_"
)

func RolesToPolicies(roles ...*models.Role) (map[string][]authorization.Policy, error) {
	rolesmap := make(map[string][]authorization.Policy)
	for idx := range roles {
		for _, permission := range roles[idx].Permissions {
			// TODO prefix roles names
			// roleName := fmt.Sprintf("%s%s", rolePrefix, *roles[idx].Name)
			policy, err := policy(permission)
			if err != nil {
				return rolesmap, err
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
			return nil, err
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

func PoliciesToPermission(policies ...authorization.Policy) ([]*models.Permission, error) {
	permissions := []*models.Permission{}
	for idx := range policies {

		permissions = append(permissions, permission([]string{policies[idx].Resource, policies[idx].Verb, policies[idx].Domain}))
	}
	return permissions, nil
}

func CasbinPolicies(casbinPolicies ...[][]string) (map[string][]authorization.Policy, error) {
	rolesPermissions := make(map[string][]authorization.Policy)
	for _, p := range casbinPolicies {
		for _, policyParts := range p {
			name := policyParts[0]
			if name == "admin" || name == "editor" || name == "viewer" {
				perms := BuiltInPermissions[name]
				for _, p := range perms {
					perm, err := policy(p)
					if err != nil {
						return nil, err
					}
					rolesPermissions[name] = append(rolesPermissions[name], *perm)
				}
			} else {
				perm, err := policy(permission(policyParts))
				if err != nil {
					return nil, err
				}
				rolesPermissions[name] = append(rolesPermissions[name], *perm)
			}
		}
	}
	return rolesPermissions, nil

}
