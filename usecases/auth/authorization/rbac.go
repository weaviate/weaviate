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

import (
	"fmt"
	"log"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	casbinutil "github.com/casbin/casbin/v2/util"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/config"
)

func InitRBAC(authConfig config.APIKey, policyPath string) (*casbin.SyncedCachedEnforcer, error) {
	numRoles := len(authConfig.Roles)
	if numRoles == 0 {
		log.Printf("no roles found")
		return nil, nil
	}

	if len(authConfig.Users) != numRoles || len(authConfig.AllowedKeys) != numRoles {
		return nil, fmt.Errorf(
			"AUTHENTICATION_APIKEY_ROLES must contain the same number of entries as " +
				"AUTHENTICATION_APIKEY_USERS and AUTHENTICATION_APIKEY_ALLOWED_KEYS")
	}

	m, err := model.NewModelFromString(rbac.MODEL)
	if err != nil {
		return nil, fmt.Errorf("load rbac model: %w", err)
	}

	enforcer, err := casbin.NewSyncedCachedEnforcer(m)
	if err != nil {
		return nil, fmt.Errorf("failed to create enforcer: %w", err)
	}
	enforcer.EnableCache(true)

	rbacStoragePath := fmt.Sprintf("./%s/rbac/policy.csv", policyPath)
	if err := rbac.CreateStorage(rbacStoragePath); err != nil {
		return nil, err
	}

	enforcer.SetAdapter(fileadapter.NewAdapter(rbacStoragePath))

	if err := enforcer.LoadPolicy(); err != nil {
		return nil, err
	}

	enforcer.AddNamedMatchingFunc("g", "keyMatch5", casbinutil.KeyMatch5)
	enforcer.AddNamedMatchingFunc("g", "regexMatch", casbinutil.RegexMatch)

	for i := range authConfig.Roles {
		entry := BuiltInRoles[authConfig.Roles[i]]
		for _, verb := range entry.Verbs {
			for _, domain := range entry.Domains {
				if _, err := enforcer.AddPolicy(authConfig.Roles[i], "*", verb, domain.String()); err != nil {
					return nil, fmt.Errorf("add policy: %w", err)
				}
			}
		}

		if _, err := enforcer.AddRoleForUser(authConfig.AllowedKeys[i], authConfig.Roles[i]); err != nil {
			return nil, fmt.Errorf("add role for key: %w", err)
		}
		if _, err := enforcer.AddRoleForUser(authConfig.Users[i], authConfig.Roles[i]); err != nil {
			return nil, fmt.Errorf("add role for user: %w", err)
		}
	}

	if err := enforcer.SavePolicy(); err != nil {
		return nil, err
	}

	return enforcer, nil
}
