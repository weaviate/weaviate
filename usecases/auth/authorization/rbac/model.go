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
	"slices"
	"strconv"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	casbinutil "github.com/casbin/casbin/v2/util"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/config"
)

const DEFAULT_POLICY_VERSION = "1.29.0"

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

func Init(conf rbacconf.Config, policyPath string, authNconf config.Authentication) (*casbin.SyncedCachedEnforcer, error) {
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

	enforcer.SetAdapter(fileadapter.NewAdapter(rbacStorageFilePath))

	if err := enforcer.LoadPolicy(); err != nil {
		return nil, err
	}
	// parse version string to check if upgrade is needed
	policyVersion, err := getVersion(rbacStoragePath)
	if err != nil {
		return nil, err
	}
	versionParts := strings.Split(policyVersion, ".")
	minorVersion, err := strconv.Atoi(versionParts[1])
	if err != nil {
		return nil, err
	}

	if versionParts[0] == "1" && minorVersion < 30 {
		if err := upgradePoliciesFrom129(enforcer, false); err != nil {
			return nil, err
		}

		if err := upgradeGroupingsFrom129(enforcer, authNconf); err != nil {
			return nil, err
		}
	}
	// docs: https://casbin.org/docs/function/
	enforcer.AddFunction("weaviateMatcher", WeaviateMatcherFunc)

	if err := applyPredefinedRoles(enforcer, conf, authNconf); err != nil {
		return nil, errors.Wrapf(err, "apply env config")
	}

	// update version after casbin policy has been written
	if err := writeVersion(rbacStoragePath, build.Version); err != nil {
		return nil, err
	}

	return enforcer, nil
}

// applyPredefinedRoles adds pre-defined roles (admin/viewer/root) and assigns them to the users provided in the
// local config
func applyPredefinedRoles(enforcer *casbin.SyncedCachedEnforcer, conf rbacconf.Config, authNconf config.Authentication) error {
	// remove preexisting root role including assignments
	_, err := enforcer.RemoveFilteredNamedPolicy("p", 0, conv.PrefixRoleName(authorization.Root))
	if err != nil {
		return err
	}
	_, err = enforcer.RemoveFilteredGroupingPolicy(1, conv.PrefixRoleName(authorization.Root))
	if err != nil {
		return err
	}

	// add pre existing roles
	for name, verb := range conv.BuiltInPolicies {
		if verb == "" {
			continue
		}
		if _, err := enforcer.AddNamedPolicy("p", conv.PrefixRoleName(name), "*", verb, "*"); err != nil {
			return fmt.Errorf("add policy: %w", err)
		}
	}

	for i := range conf.RootUsers {
		if strings.TrimSpace(conf.RootUsers[i]) == "" {
			continue
		}

		if authNconf.APIKey.Enabled && slices.Contains(authNconf.APIKey.Users, conf.RootUsers[i]) {
			if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.RootUsers[i], models.UserTypeInputDb), conv.PrefixRoleName(authorization.Root)); err != nil {
				return fmt.Errorf("add role for user: %w", err)
			}
		}

		if authNconf.OIDC.Enabled {
			if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.RootUsers[i], models.UserTypeInputOidc), conv.PrefixRoleName(authorization.Root)); err != nil {
				return fmt.Errorf("add role for user: %w", err)
			}
		}
	}

	// temporary to enable import of existing keys to WCD (Admin + readonly)
	for i := range conf.AdminUsers {
		if strings.TrimSpace(conf.AdminUsers[i]) == "" {
			continue
		}

		if authNconf.APIKey.Enabled && slices.Contains(authNconf.APIKey.Users, conf.AdminUsers[i]) {
			if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.AdminUsers[i], models.UserTypeInputDb), conv.PrefixRoleName(authorization.Admin)); err != nil {
				return fmt.Errorf("add role for user: %w", err)
			}
		}

		if authNconf.OIDC.Enabled {
			if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.AdminUsers[i], models.UserTypeInputOidc), conv.PrefixRoleName(authorization.Admin)); err != nil {
				return fmt.Errorf("add role for user: %w", err)
			}
		}
	}

	for i := range conf.ViewerUsers {
		if strings.TrimSpace(conf.ViewerUsers[i]) == "" {
			continue
		}

		if authNconf.APIKey.Enabled && slices.Contains(authNconf.APIKey.Users, conf.ViewerUsers[i]) {
			if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.ViewerUsers[i], models.UserTypeInputDb), conv.PrefixRoleName(authorization.Viewer)); err != nil {
				return fmt.Errorf("add role for user: %w", err)
			}
		}

		if authNconf.OIDC.Enabled {
			if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.ViewerUsers[i], models.UserTypeInputOidc), conv.PrefixRoleName(authorization.Viewer)); err != nil {
				return fmt.Errorf("add role for user: %w", err)
			}
		}
	}

	for _, group := range conf.RootGroups {
		if strings.TrimSpace(group) == "" {
			continue
		}
		if _, err := enforcer.AddRoleForUser(conv.PrefixGroupName(group), conv.PrefixRoleName(authorization.Root)); err != nil {
			return fmt.Errorf("add role for group %s: %w", group, err)
		}
	}

	for _, viewerGroup := range conf.ViewerGroups {
		if strings.TrimSpace(viewerGroup) == "" {
			continue
		}
		if _, err := enforcer.AddRoleForUser(conv.PrefixGroupName(viewerGroup), conv.PrefixRoleName(authorization.Viewer)); err != nil {
			return fmt.Errorf("add viewer role for group %s: %w", viewerGroup, err)
		}
	}

	if err := enforcer.SavePolicy(); err != nil {
		return errors.Wrapf(err, "save policy")
	}

	return nil
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

func getVersion(path string) (string, error) {
	filePath := path + "/version"
	_, err := os.Stat(filePath)
	if err != nil { // file exists
		return DEFAULT_POLICY_VERSION, nil
	}
	b, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return string(b), nil
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
