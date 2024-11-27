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

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	casbinutil "github.com/casbin/casbin/v2/util"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
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
	m = g(r.sub, p.sub) && keyMatch5(r.obj, p.obj) && regexMatch(r.act, p.act)
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

func Init(authConfig Config, policyPath string) (*casbin.SyncedCachedEnforcer, error) {
	m, err := model.NewModelFromString(MODEL)
	if err != nil {
		return nil, fmt.Errorf("load rbac model: %w", err)
	}

	enforcer, err := casbin.NewSyncedCachedEnforcer(m)
	if err != nil {
		return nil, fmt.Errorf("failed to create enforcer: %w", err)
	}
	enforcer.EnableCache(false)

	rbacStoragePath := fmt.Sprintf("./%s/rbac/policy.csv", policyPath)
	if err := createStorage(rbacStoragePath); err != nil {
		return nil, err
	}

	enforcer.SetAdapter(fileadapter.NewAdapter(rbacStoragePath))

	if err := enforcer.LoadPolicy(); err != nil {
		return nil, err
	}

	// docs: https://casbin.org/docs/function/
	enforcer.AddNamedMatchingFunc("g", "keyMatch5", casbinutil.KeyMatch5)
	enforcer.AddNamedMatchingFunc("g", "regexMatch", casbinutil.RegexMatch)

	// add pre existing roles
	for name, verb := range authorization.BuiltInPolicies {
		if verb == "" {
			continue
		}
		if _, err := enforcer.AddNamedPolicy("p", name, "*", verb, "*"); err != nil {
			return nil, fmt.Errorf("add policy: %w", err)
		}
	}

	for i := range authConfig.Admins {
		if _, err := enforcer.AddRoleForUser(authConfig.Admins[i], authorization.BuiltInPolicies[authorization.Admin]); err != nil {
			return nil, fmt.Errorf("add role for user: %w", err)
		}
	}

	for i := range authConfig.Viewers {
		if _, err := enforcer.AddRoleForUser(authConfig.Admins[i], authorization.BuiltInPolicies[authorization.Viewer]); err != nil {
			return nil, fmt.Errorf("add role for user: %w", err)
		}
	}

	if err := enforcer.SavePolicy(); err != nil {
		return nil, err
	}

	return enforcer, nil
}
