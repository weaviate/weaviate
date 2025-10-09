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

package authorization

import (
	"github.com/weaviate/weaviate/usecases/auth/authentication"
)

type Controller interface {
	UpdateRolesPermissions(roles map[string][]Policy) error
	CreateRolesPermissions(roles map[string][]Policy) error
	GetRoles(names ...string) (map[string][]Policy, error)
	DeleteRoles(roles ...string) error
	AddRolesForUser(user string, roles []string) error
	GetRolesForUserOrGroup(user string, authMethod authentication.AuthType, isGroup bool) (map[string][]Policy, error)
	GetUsersOrGroupForRole(role string, authMethod authentication.AuthType, IsGroup bool) ([]string, error)
	RevokeRolesForUser(user string, roles ...string) error
	RemovePermissions(role string, permissions []*Policy) error
	HasPermission(role string, permission *Policy) (bool, error)
	GetUsersOrGroupsWithRoles(isGroup bool, authMethod authentication.AuthType) ([]string, error)
}
