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

package api

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	// NOTE: in case changes happens to the RBAC message, add new version before the RBACLatestCommandPolicyVersion
	// RBACCommandPolicyVersionV0 represents the first version of RBAC commands where it wasn't set and equal 0
	// this version was needed because we did migrate paths of SchemaDomain to limit the collection
	// old "schema/collections/{collection_name}/shards/*" all shards in collection
	// new "schema/collections/{collection_name}/shards/#" limited to collection only
	RBACCommandPolicyVersionV0 = iota

	// this version was needed because we did migrate verbs of RolesDomain to control the scope
	// of Role permissions and default to MATCH scope instead of ALL
	// old verb was (C)|(R)|(U)|(D)
	// new verb was MATCH
	RBACCommandPolicyVersionV1

	// this version was needed because we did flatten manage_roles to C+U+D_roles
	RBACCommandPolicyVersionV2
	// this version was needed because assign_and_revoke_users was saved with verb UPDATE. However with dynamic user
	// management we need a special permission to update users
	RBACCommandPolicyVersionV3

	// RBACLatestCommandPolicyVersion represents the latest version of RBAC commands policies
	// It's used to migrate policy changes. if we end up with a cluster having different version
	// that won't be a problem because the version here is not about the message change but more about
	// the content of the body which will dumbed anyway in RBAC storage.
	RBACLatestCommandPolicyVersion
)

const (
	RBACAssignRevokeCommandPolicyVersionV0 = iota
	RBACAssignRevokeLatestCommandPolicyVersion
)

type CreateRolesRequest struct {
	Roles        map[string][]authorization.Policy
	RoleCreation bool
	Version      int
}

type DeleteRolesRequest struct {
	Roles []string
}

type RemovePermissionsRequest struct {
	Role        string
	Permissions []*authorization.Policy
	Version     int
}

type AddRolesForUsersRequest struct {
	User    string
	Roles   []string
	Version int
}

type RevokeRolesForUserRequest struct {
	User    string
	Roles   []string
	Version int
}

type QueryHasPermissionRequest struct {
	Role       string
	Permission *authorization.Policy
}

type QueryHasPermissionResponse struct {
	HasPermission bool
}

type QueryGetRolesRequest struct {
	Roles []string
}

type QueryGetRolesResponse struct {
	Roles map[string][]authorization.Policy
}

type QueryGetRolesForUserRequest struct {
	User     string
	UserType models.UserTypeInput
}

type QueryGetRolesForUserResponse struct {
	Roles map[string][]authorization.Policy
}

type QueryGetUsersForRoleRequest struct {
	Role     string
	UserType models.UserTypeInput
}

type QueryGetUsersForRoleResponse struct {
	Users []string
}
