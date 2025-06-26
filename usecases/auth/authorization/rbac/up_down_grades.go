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

	"github.com/casbin/casbin/v2"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/config"
)

func upgradeGroupingsFrom129(enforcer *casbin.SyncedCachedEnforcer, authNconf config.Authentication) error {
	// clear out assignments without namespaces and re-add them with namespaces
	roles, _ := enforcer.GetAllSubjects()
	for _, role := range roles {
		users, err := enforcer.GetUsersForRole(role)
		if err != nil {
			return err
		}

		for _, user := range users {
			// internal user assignments (for empty roles) need to be converted from namespaced assignment to db-user only
			// other assignments need to be converted to both namespaces
			if strings.Contains(user, conv.InternalPlaceHolder) {
				if _, err := enforcer.DeleteRoleForUser(user, role); err != nil {
					return err
				}

				if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conv.InternalPlaceHolder, models.UserTypeInputDb), role); err != nil {
					return err
				}
			} else if strings.HasPrefix(user, "user:") {
				userNoPrefix := strings.TrimPrefix(user, "user:")
				if _, err := enforcer.DeleteRoleForUser(user, role); err != nil {
					return err
				}
				if authNconf.APIKey.Enabled && slices.Contains(authNconf.APIKey.Users, userNoPrefix) {
					if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(userNoPrefix, models.UserTypeInputDb), role); err != nil {
						return err
					}
				}
				if authNconf.OIDC.Enabled {
					if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(userNoPrefix, models.UserTypeInputOidc), role); err != nil {
						return err
					}
				}
			}
		}

	}
	return nil
}

func upgradePoliciesFrom129(enforcer *casbin.SyncedCachedEnforcer, keepBuildInRoles bool) error {
	policies, err := enforcer.GetPolicy()
	if err != nil {
		return err
	}

	// a role can have multiple policies, so first all old role need to be removed and then re-added
	policiesToAdd := make([][]string, 0, len(policies))
	for _, policy := range policies {
		if _, err := enforcer.RemoveFilteredNamedPolicy("p", 0, policy[0]); err != nil {
			return err
		}

		if policy[3] == authorization.UsersDomain && policy[2] == authorization.UPDATE {
			policy[2] = authorization.USER_ASSIGN_AND_REVOKE
		}

		policiesToAdd = append(policiesToAdd, policy)
	}

	for _, policy := range policiesToAdd {
		roleName := conv.TrimRoleNamePrefix(policy[0])
		if _, ok := conv.BuiltInPolicies[roleName]; ok {
			if !keepBuildInRoles {
				continue
			} else if policy[2] == conv.CRUD {
				policy[2] = conv.VALID_VERBS
			}
		}

		if _, err := enforcer.AddNamedPolicy("p", policy[0], policy[1], policy[2], policy[3]); err != nil {
			return fmt.Errorf("readd policy: %w", err)
		}
	}

	return nil
}
