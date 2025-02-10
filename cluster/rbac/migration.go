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
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func migrateV2(roles map[string][]authorization.Policy) map[string][]authorization.Policy {
	for roleName, policies := range roles {
		// create new permissions
		for idx := range policies {
			// replace manage ALL (verb CRUD) with individual CUD permissions
			if roles[roleName][idx].Domain == authorization.RolesDomain && roles[roleName][idx].Verb == conv.CRUD {
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
			}

			// replace manage MATCH (verb MATCH) with individual CUD permissions
			if roles[roleName][idx].Domain == authorization.RolesDomain && roles[roleName][idx].Verb == authorization.ROLE_SCOPE_MATCH {
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
			}

			// replace manage ALL (verb CRUD) with individual CUD permissions
			if roles[roleName][idx].Domain == authorization.RolesDomain && roles[roleName][idx].Verb == conv.CRUD {
				if roles[roleName][idx].Verb == authorization.ROLE_SCOPE_MATCH {
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
				}
			}

			// add scope to read
			if roles[roleName][idx].Domain == authorization.RolesDomain && roles[roleName][idx].Verb == authorization.READ {
				roles[roleName][idx].Verb = authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH)
			}
		}
	}
	return roles
}
