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

package rbac

import (
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/usecases/config"

	"github.com/weaviate/weaviate/entities/models"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func migrateUpsertRolesPermissions(req *cmd.CreateRolesRequest) (*cmd.CreateRolesRequest, error) {
	// loop through updates until current version is reached
UPDATE_LOOP:
	for {
		switch req.Version {
		case cmd.RBACCommandPolicyVersionV0:
			for roleName, policies := range req.Roles {
				// create new permissions
				for idx := range policies {
					if req.Roles[roleName][idx].Domain == authorization.SchemaDomain {
						parts := strings.Split(req.Roles[roleName][idx].Resource, "/")
						if len(parts) < 3 {
							// shall never happens
							return nil, fmt.Errorf("invalid schema path")
						}
						req.Roles[roleName][idx].Resource = authorization.CollectionsMetadata(parts[2])[0]
					}
				}
			}
		case cmd.RBACCommandPolicyVersionV1:
			for roleName, policies := range req.Roles {
				// create new permissions
				for idx := range policies {
					if req.Roles[roleName][idx].Domain == authorization.RolesDomain &&
						req.Roles[roleName][idx].Verb == conv.CRUD {
						// this will override any role was created before 1.28
						// to reset default to
						req.Roles[roleName][idx].Verb = authorization.ROLE_SCOPE_MATCH
					}
				}
			}
		case cmd.RBACCommandPolicyVersionV2:
			req.Roles = migrateUpsertRolesPermissionsV2(req.Roles)
		case cmd.RBACCommandPolicyVersionV3:
			req.Roles = migrateUpsertRolesPermissionsV3(req.Roles)
		case cmd.RBACLatestCommandPolicyVersion:
			break UPDATE_LOOP
		default:
			continue
		}
		req.Version += 1
	}

	return req, nil
}

func migrateUpsertRolesPermissionsV2(roles map[string][]authorization.Policy) map[string][]authorization.Policy {
	for roleName, policies := range roles {
		// create new permissions
		for idx := range policies {
			if roles[roleName][idx].Domain != authorization.RolesDomain {
				continue
			}

			switch roles[roleName][idx].Verb {
			default:
				continue

			case conv.CRUD:
				// replace manage ALL (verb CRUD) with individual CUD permissions
				roles[roleName][idx].Verb = authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_ALL)
				// new permissions for U+D needed
				for _, verb := range []string{authorization.UPDATE, authorization.DELETE} {
					newPolicy := authorization.Policy{
						Resource: roles[roleName][idx].Resource,
						Verb:     authorization.VerbWithScope(verb, authorization.ROLE_SCOPE_ALL),
						Domain:   roles[roleName][idx].Domain,
					}
					roles[roleName] = append(roles[roleName], newPolicy)
				}
			case authorization.ROLE_SCOPE_MATCH:
				// replace manage MATCH (verb MATCH) with individual CUD permissions
				roles[roleName][idx].Verb = authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_MATCH)
				// new permissions for U+D needed
				for _, verb := range []string{authorization.UPDATE, authorization.DELETE} {
					newPolicy := authorization.Policy{
						Resource: roles[roleName][idx].Resource,
						Verb:     authorization.VerbWithScope(verb, authorization.ROLE_SCOPE_MATCH),
						Domain:   roles[roleName][idx].Domain,
					}
					roles[roleName] = append(roles[roleName], newPolicy)
				}
			case authorization.READ:
				// add scope to read
				roles[roleName][idx].Verb = authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH)

			}
		}
	}
	return roles
}

func migrateUpsertRolesPermissionsV3(roles map[string][]authorization.Policy) map[string][]authorization.Policy {
	for roleName, policies := range roles {
		for idx := range policies {
			if roles[roleName][idx].Domain != authorization.UsersDomain {
				continue
			}

			if roles[roleName][idx].Verb != authorization.UPDATE {
				continue
			}

			roles[roleName][idx].Verb = authorization.USER_ASSIGN_AND_REVOKE

		}
	}
	return roles
}

func migrateRemovePermissions(req *cmd.RemovePermissionsRequest) (*cmd.RemovePermissionsRequest, error) {
	// loop through updates until current version is reached
UPDATE_LOOP:
	for {
		switch req.Version {
		case cmd.RBACCommandPolicyVersionV0:
			for idx := range req.Permissions {
				if req.Permissions[idx].Domain != authorization.SchemaDomain {
					continue
				}
				parts := strings.Split(req.Permissions[idx].Resource, "/")
				if len(parts) < 3 {
					// shall never happens
					return nil, fmt.Errorf("invalid schema path")
				}
				req.Permissions[idx].Resource = authorization.CollectionsMetadata(parts[2])[0]
			}
		case cmd.RBACCommandPolicyVersionV1:
			req.Permissions = migrateRemoveRolesPermissionsV1(req.Permissions)
		case cmd.RBACCommandPolicyVersionV2:
			req.Permissions = migrateRemoveRolesPermissionsV2(req.Permissions)
		case cmd.RBACCommandPolicyVersionV3:
			req.Permissions = migrateRemoveRolesPermissionsV3(req.Permissions)
		case cmd.RBACLatestCommandPolicyVersion:
			break UPDATE_LOOP
		default:
			continue
		}
		req.Version += 1
	}

	return req, nil
}

func migrateRemoveRolesPermissionsV1(permissions []*authorization.Policy) []*authorization.Policy {
	initialPerms := len(permissions)
	for idx := 0; idx < initialPerms; idx++ {
		if permissions[idx].Domain == authorization.RolesDomain && permissions[idx].Verb == conv.CRUD {
			permissions[idx].Verb = authorization.ROLE_SCOPE_MATCH
		}
	}
	return permissions
}

func migrateRemoveRolesPermissionsV2(permissions []*authorization.Policy) []*authorization.Policy {
	initialPerms := len(permissions)
	for idx := 0; idx < initialPerms; idx++ {
		if permissions[idx].Domain != authorization.RolesDomain {
			continue
		}

		switch permissions[idx].Verb {
		default:
			continue
		case conv.CRUD:
			// also remove individual CUD permissions for manage_roles with ALL
			permissions[idx].Verb = authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_ALL)
			// new permissions for U+D needed
			for _, verb := range []string{authorization.UPDATE, authorization.DELETE} {
				newPolicy := &authorization.Policy{
					Resource: permissions[idx].Resource,
					Verb:     authorization.VerbWithScope(verb, authorization.ROLE_SCOPE_ALL),
					Domain:   permissions[idx].Domain,
				}
				permissions = append(permissions, newPolicy)
			}

		case authorization.ROLE_SCOPE_MATCH:
			// also remove individual CUD permissions for manage_roles with MATCH
			permissions[idx].Verb = authorization.VerbWithScope(authorization.CREATE, authorization.ROLE_SCOPE_MATCH)
			// new permissions for U+D needed
			for _, verb := range []string{authorization.UPDATE, authorization.DELETE} {
				newPolicy := &authorization.Policy{
					Resource: permissions[idx].Resource,
					Verb:     authorization.VerbWithScope(verb, authorization.ROLE_SCOPE_MATCH),
					Domain:   permissions[idx].Domain,
				}
				permissions = append(permissions, newPolicy)
			}
		case authorization.READ:
			permissions[idx].Verb = authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH)
		}
	}
	return permissions
}

func migrateRemoveRolesPermissionsV3(permissions []*authorization.Policy) []*authorization.Policy {
	initialPerms := len(permissions)
	for idx := 0; idx < initialPerms; idx++ {
		if permissions[idx].Domain != authorization.UsersDomain {
			continue
		}

		if permissions[idx].Verb != authorization.UPDATE {
			continue
		}

		permissions[idx].Verb = authorization.USER_ASSIGN_AND_REVOKE
	}
	return permissions
}

func migrateRevokeRoles(req *cmd.RevokeRolesForUserRequest) []*cmd.RevokeRolesForUserRequest {
	if req.Version == cmd.RBACAssignRevokeCommandPolicyVersionV0 {
		return migrateRevokeRolesV0(req)
	}
	return []*cmd.RevokeRolesForUserRequest{req}
}

func migrateRevokeRolesV0(req *cmd.RevokeRolesForUserRequest) []*cmd.RevokeRolesForUserRequest {
	user, _ := conv.GetUserAndPrefix(req.User)

	req1 := &cmd.RevokeRolesForUserRequest{
		Version: req.Version + 1,
		Roles:   req.Roles,
		User:    conv.UserNameWithTypeFromId(user, models.UserTypeInputDb),
	}
	req2 := &cmd.RevokeRolesForUserRequest{
		Version: req.Version + 1,
		Roles:   req.Roles,
		User:    conv.UserNameWithTypeFromId(user, models.UserTypeInputOidc),
	}

	return []*cmd.RevokeRolesForUserRequest{req1, req2}
}

func migrateAssignRoles(req *cmd.AddRolesForUsersRequest, authNconfig config.Authentication) []*cmd.AddRolesForUsersRequest {
	if req.Version == cmd.RBACAssignRevokeCommandPolicyVersionV0 {
		return migrateAssignRolesV0(req, authNconfig)
	}
	return []*cmd.AddRolesForUsersRequest{req}
}

func migrateAssignRolesV0(req *cmd.AddRolesForUsersRequest, authNconfig config.Authentication) []*cmd.AddRolesForUsersRequest {
	user, _ := conv.GetUserAndPrefix(req.User)

	var reqs []*cmd.AddRolesForUsersRequest
	if authNconfig.APIKey.Enabled && slices.Contains(authNconfig.APIKey.Users, user) {
		reqs = append(reqs, &cmd.AddRolesForUsersRequest{
			Version: req.Version + 1,
			Roles:   req.Roles,
			User:    conv.UserNameWithTypeFromId(user, models.UserTypeInputDb),
		})
	}

	if authNconfig.OIDC.Enabled {
		reqs = append(reqs, &cmd.AddRolesForUsersRequest{
			Version: req.Version + 1,
			Roles:   req.Roles,
			User:    conv.UserNameWithTypeFromId(user, models.UserTypeInputOidc),
		})
	}

	return reqs
}
