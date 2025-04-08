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

package authorization

import "github.com/weaviate/weaviate/entities/models"

type Controller interface {
	UpdateRolesPermissions(roles map[string][]Policy) error
	CreateRolesPermissions(roles map[string][]Policy) error
	GetRoles(names ...string) (map[string][]Policy, error)
	DeleteRoles(roles ...string) error
	AddRolesForUser(user string, roles []string) error
	GetRolesForUser(user string, userType models.UserTypeInput) (map[string][]Policy, error)
	GetUsersForRole(role string, userType models.UserTypeInput) ([]string, error)
	RevokeRolesForUser(user string, roles ...string) error
	RemovePermissions(role string, permissions []*Policy) error
	HasPermission(role string, permission *Policy) (bool, error)
}
