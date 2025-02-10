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
	"strings"

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

		default:
			break UPDATE_LOOP
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
