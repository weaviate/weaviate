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
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	casbinutil "github.com/casbin/casbin/v2/util"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/build"
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

	rbacStoragePath := fmt.Sprintf("%s/rbac", policyPath)
	rbacStorageFilePath := fmt.Sprintf("%s/rbac/policy.csv", policyPath)

	if err := createStorage(rbacStorageFilePath); err != nil {
		return nil, errors.Wrapf(err, "create storage path: %v", rbacStorageFilePath)
	}

	err = writeVersion(rbacStoragePath, build.Version)
	if err != nil {
		return nil, err
	}

	enforcer.SetAdapter(fileadapter.NewAdapter(rbacStorageFilePath))

	if err := enforcer.LoadPolicy(); err != nil {
		return nil, err
	}

	// docs: https://casbin.org/docs/function/
	enforcer.AddFunction("weaviateMatcher", WeaviateMatcherFunc)

	if err := downgradesAssignmentsFrom130(enforcer); err != nil {
		return nil, err
	}
	if err := downgradeRolesFrom130(enforcer); err != nil {
		return nil, err
	}

	// remove preexisting root role including assignments
	_, err = enforcer.RemoveFilteredNamedPolicy("p", 0, conv.PrefixRoleName(authorization.Root))
	if err != nil {
		return nil, err
	}
	_, err = enforcer.RemoveFilteredGroupingPolicy(1, conv.PrefixRoleName(authorization.Root))
	if err != nil {
		return nil, err
	}

	// remove assignments to namespaces to allow for downgrades (added in 1.30+)

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
		if _, err := enforcer.AddRoleForUser(conv.PrefixUserName(conf.RootUsers[i]), conv.PrefixRoleName(authorization.Root)); err != nil {
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

func downgradesAssignmentsFrom130(enforcer *casbin.SyncedCachedEnforcer) error {
	// remove build-in roles with potentially changed verbs. These will be re-added in the next step
	policies, err := enforcer.GetPolicy()
	if err != nil {
		return err
	}

	for _, policy := range policies {
		roleName := conv.TrimRoleNamePrefix(policy[0])
		if _, ok := conv.BuiltInPolicies[roleName]; ok {
			if _, err := enforcer.RemoveFilteredNamedPolicy("p", 0, policy[0]); err != nil {
				return err
			}
		}
	}

	// clear out assignments to namespaces that have been introduced in 1.30. This code should not be part of 1.30+
	roles, _ := enforcer.GetAllSubjects()
	for _, role := range roles {
		users, err := enforcer.GetUsersForRole(role)
		if err != nil {
			return err
		}

		for _, user := range users {
			// internal user assignments (for empty roles) need to be converted back from namespaced assignment
			// these assignments are only added for the db namespace
			if strings.Contains(user, conv.InternalPlaceHolder) {
				if _, err := enforcer.DeleteRoleForUser(user, role); err != nil {
					return err
				}

				if _, err := enforcer.AddRoleForUser(conv.PrefixUserName(conv.InternalPlaceHolder), role); err != nil {
					return err
				}
			}

			// other internal assignments
			if strings.HasPrefix(user, "db:") || strings.HasPrefix(user, "oidc:") {
				if _, err := enforcer.DeleteRoleForUser(user, role); err != nil {
					return err
				}
			}
			if strings.HasPrefix(user, "db:") {
				userWithoutPrefix := strings.TrimPrefix(user, "db:")
				if _, err := enforcer.AddRoleForUser(conv.PrefixUserName(userWithoutPrefix), role); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func downgradeRolesFrom130(enforcer *casbin.SyncedCachedEnforcer) error {
	policies, err := enforcer.GetPolicy()
	if err != nil {
		return err
	}

	policiesToAdd := make([][]string, 0, len(policies))
	for _, policy := range policies {
		if _, err := enforcer.RemoveFilteredNamedPolicy("p", 0, policy[0]); err != nil {
			return err
		}

		// undo the migration from UPDATE to ASSIGN
		if policy[3] == authorization.UsersDomain && policy[2] == "A" {
			policy[2] = authorization.UPDATE
		}
		policiesToAdd = append(policiesToAdd, policy)
	}

	// re-add policy with changed server version, leave out build-in roles
	for _, policy := range policiesToAdd {
		roleName := conv.TrimRoleNamePrefix(policy[0])
		if _, ok := conv.BuiltInPolicies[roleName]; ok {
			continue
		}

		if _, err := enforcer.AddNamedPolicy("p", policy[0], policy[1], policy[2], policy[3]); err != nil {
			return fmt.Errorf("readd policy: %w", err)
		}
	}

	return nil
}

func writeVersion(path, version string) error {
	tmpFile, err := os.CreateTemp(path, "policy-temp-*.tmp")
	if err != nil {
		return err
	}
	tempFilename := tmpFile.Name()

	defer func() {
		tmpFile.Close()
		os.Remove(tempFilename) // Remove temp file if it still exists
	}()

	writer := bufio.NewWriter(tmpFile)
	if _, err := fmt.Fprint(writer, version); err != nil {
		return err
	}

	// Flush the writer to ensure all data is written, then sync and flush tmpfile and atomically rename afterwards
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	return os.Rename(tempFilename, path+"/version")
}
