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
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	casbinutil "github.com/casbin/casbin/v2/util"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

const (
	// MODEL is the used model for casbin to store roles, permissions, users and comparisons patterns
	// docs: https://casbin.org/docs/syntax-for-models
	MODEL = `
	[request_definition]
	r = sub, obj, act

	[policy_definition]
	p = sub, obj, act, dom

	[role_definition]
	g = _, _

	[policy_effect]
	e = some(where (p.eft == allow))

	[matchers]
	m = g(r.sub, p.sub) && weaviateMatcher(r.obj, p.obj) && regexMatch(r.act, p.act)
`
)

func createStorage(filePath string) error {
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	_, err := os.Stat(filePath)
	if err == nil { // file exists
		return nil
	}

	if os.IsNotExist(err) {
		file, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
		defer file.Close()
		return nil
	}

	return err
}

func Init(conf rbacconf.Config, policyPath string) (*casbin.SyncedCachedEnforcer, error) {
	if !conf.Enabled {
		return nil, nil
	}

	m, err := model.NewModelFromString(MODEL)
	if err != nil {
		return nil, fmt.Errorf("load rbac model: %w", err)
	}

	enforcer, err := casbin.NewSyncedCachedEnforcer(m)
	if err != nil {
		return nil, fmt.Errorf("failed to create enforcer: %w", err)
	}
	enforcer.EnableCache(true)

	rbacStoragePath := fmt.Sprintf("%s/rbac/policy.csv", policyPath)

	if err := createStorage(rbacStoragePath); err != nil {
		return nil, errors.Wrapf(err, "create storage path: %v", rbacStoragePath)
	}

	enforcer.SetAdapter(fileadapter.NewAdapter(rbacStoragePath))

	if err := enforcer.LoadPolicy(); err != nil {
		return nil, err
	}

	// docs: https://casbin.org/docs/function/
	enforcer.AddFunction("weaviateMatcher", WeaviateMatcherFunc)

	// remove preexisting root role including assignments
	_, err = enforcer.RemoveFilteredNamedPolicy("p", 0, conv.PrefixRoleName(authorization.Root))
	if err != nil {
		return nil, err
	}
	_, err = enforcer.RemoveFilteredGroupingPolicy(1, conv.PrefixRoleName(authorization.Root))
	if err != nil {
		return nil, err
	}

	// add pre existing roles
	for name, verb := range conv.BuiltInPolicies {
		if verb == "" {
			continue
		}
		if _, err := enforcer.AddNamedPolicy("p", conv.PrefixRoleName(name), "*", verb, "*"); err != nil {
			return nil, fmt.Errorf("add policy: %w", err)
		}
	}

	for i := range conf.RootUsers {
		if strings.TrimSpace(conf.RootUsers[i]) == "" {
			continue
		}

		// add root role to db as well as OIDC users - we block dynamic users from being created with the same name
		if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.RootUsers[i], models.UserTypesDb), conv.PrefixRoleName(authorization.Root)); err != nil {
			return nil, fmt.Errorf("add role for user: %w", err)
		}

		if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.RootUsers[i], models.UserTypesOidc), conv.PrefixRoleName(authorization.Root)); err != nil {
			return nil, fmt.Errorf("add role for user: %w", err)
		}
	}

	for _, group := range conf.RootGroups {
		if strings.TrimSpace(group) == "" {
			continue
		}
		if _, err := enforcer.AddRoleForUser(conv.PrefixGroupName(group), conv.PrefixRoleName(authorization.Root)); err != nil {
			return nil, fmt.Errorf("add role for group %s: %w", group, err)
		}
	}

	for _, viewerGroup := range conf.ViewerRootGroups {
		if strings.TrimSpace(viewerGroup) == "" {
			continue
		}
		if _, err := enforcer.AddRoleForUser(conv.PrefixGroupName(viewerGroup), conv.PrefixRoleName(authorization.Viewer)); err != nil {
			return nil, fmt.Errorf("add viewer role for group %s: %w", viewerGroup, err)
		}
	}

	if err := enforcer.SavePolicy(); err != nil {
		return nil, errors.Wrapf(err, "save policy")
	}

	return enforcer, nil
}

func WeaviateMatcher(key1 string, key2 string) bool {
	// If we're dealing with a tenant-specific path (matches /shards/#$)
	if strings.HasSuffix(key1, "/shards/#") {
		// Don't allow matching with wildcard patterns
		if strings.HasSuffix(key2, "/shards/.*") {
			return false
		}
	}
	// For all other cases, use standard KeyMatch5
	return casbinutil.KeyMatch5(key1, key2)
}

func WeaviateMatcherFunc(args ...interface{}) (interface{}, error) {
	name1 := args[0].(string)
	name2 := args[1].(string)

	return (bool)(WeaviateMatcher(name1, name2)), nil
}
