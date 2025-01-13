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
	"sync"

	"github.com/casbin/casbin/v2"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

type manager struct {
	casbin *casbin.SyncedCachedEnforcer
	logger logrus.FieldLogger
}

func New(rbacStoragePath string, rbac rbacconf.Config, logger logrus.FieldLogger) (*manager, error) {
	casbin, err := Init(rbac, rbacStoragePath)
	if err != nil {
		return nil, err
	}

	return &manager{casbin, logger}, nil
}

func (m *manager) UpsertRolesPermissions(roles map[string][]authorization.Policy) error {
	for roleName, policies := range roles {
		// assign role to internal user to make sure to catch empty roles
		// e.g. : g, user:wv_internal_empty, role:roleName
		if _, err := m.casbin.AddRoleForUser(conv.PrefixUserName(conv.InternalPlaceHolder), conv.PrefixRoleName(roleName)); err != nil {
			return err
		}
		for _, policy := range policies {
			if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName(roleName), policy.Resource, policy.Verb, policy.Domain); err != nil {
				return err
			}
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return err
	}

	return m.casbin.InvalidateCache()
}

func (m *manager) GetRoles(names ...string) (map[string][]authorization.Policy, error) {
	var (
		casbinStoragePolicies    [][][]string
		casbinStoragePoliciesMap = make(map[string]struct{})
	)

	if len(names) == 0 {
		// get all roles
		polices, err := m.casbin.GetNamedPolicy("p")
		if err != nil {
			return nil, err
		}
		casbinStoragePolicies = append(casbinStoragePolicies, polices)

		for _, p := range polices {
			// e.g. policy line in casbin -> role:roleName resource verb domain, that's why p[0]
			casbinStoragePoliciesMap[p[0]] = struct{}{}
		}

		polices, err = m.casbin.GetNamedGroupingPolicy("g")
		if err != nil {
			return nil, err
		}
		casbinStoragePolicies = collectStaleRoles(polices, casbinStoragePoliciesMap, casbinStoragePolicies)
	} else {
		for _, name := range names {
			polices, err := m.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName(name))
			if err != nil {
				return nil, err
			}
			casbinStoragePolicies = append(casbinStoragePolicies, polices)

			for _, p := range polices {
				// e.g. policy line in casbin -> role:roleName resource verb domain, that's why p[0]
				casbinStoragePoliciesMap[p[0]] = struct{}{}
			}

			polices, err = m.casbin.GetFilteredNamedGroupingPolicy("g", 1, conv.PrefixRoleName(name))
			if err != nil {
				return nil, err
			}
			casbinStoragePolicies = collectStaleRoles(polices, casbinStoragePoliciesMap, casbinStoragePolicies)
		}
	}

	return conv.CasbinPolicies(casbinStoragePolicies...)
}

func (m *manager) RemovePermissions(roleName string, permissions []*authorization.Policy) error {
	for _, permission := range permissions {
		ok, err := m.casbin.RemoveNamedPolicy("p", conv.PrefixRoleName(roleName), permission.Resource, permission.Verb, permission.Domain)
		if err != nil {
			return err
		}
		if !ok {
			return nil // deletes are idempotent
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return err
	}
	return m.casbin.InvalidateCache()
}

func (m *manager) HasPermission(roleName string, permission *authorization.Policy) (bool, error) {
	return m.casbin.HasNamedPolicy("p", conv.PrefixRoleName(roleName), permission.Resource, permission.Verb, permission.Domain)
}

func (m *manager) DeleteRoles(roles ...string) error {
	for _, roleName := range roles {
		// remove role
		roleRemoved, err := m.casbin.RemoveFilteredNamedPolicy("p", 0, conv.PrefixRoleName(roleName))
		if err != nil {
			return err
		}
		// remove role assignment
		roleAssignmentsRemoved, err := m.casbin.RemoveFilteredGroupingPolicy(1, conv.PrefixRoleName(roleName))
		if err != nil {
			return err
		}

		if !roleRemoved && !roleAssignmentsRemoved {
			return nil // deletes are idempotent
		}
	}

	if err := m.casbin.SavePolicy(); err != nil {
		return err
	}

	return m.casbin.InvalidateCache()
}

func (m *manager) AddRolesForUser(user string, roles []string) error {
	for _, role := range roles {
		if _, err := m.casbin.AddRoleForUser(conv.PrefixUserName(user), conv.PrefixRoleName(role)); err != nil {
			return err
		}
	}

	if err := m.casbin.SavePolicy(); err != nil {
		return err
	}

	return m.casbin.InvalidateCache()
}

func (m *manager) GetRolesForUser(userName string) (map[string][]authorization.Policy, error) {
	rolesNames, err := m.casbin.GetRolesForUser(conv.PrefixUserName(userName))
	if err != nil {
		return nil, err
	}
	if len(rolesNames) == 0 {
		return map[string][]authorization.Policy{}, err
	}

	return m.GetRoles(rolesNames...)
}

func (m *manager) GetUsersForRole(roleName string) ([]string, error) {
	pusers, err := m.casbin.GetUsersForRole(conv.PrefixRoleName(roleName))
	if err != nil {
		return nil, err
	}

	users := make([]string, 0, len(pusers))
	for idx := range pusers {
		user := conv.TrimUserNamePrefix(pusers[idx])
		if user == conv.InternalPlaceHolder {
			continue
		}
		users = append(users, user)
	}
	return users, err
}

func (m *manager) RevokeRolesForUser(userName string, roles ...string) error {
	for _, roleName := range roles {
		if _, err := m.casbin.DeleteRoleForUser(conv.PrefixUserName(userName), conv.PrefixRoleName(roleName)); err != nil {
			return err
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return err
	}
	return m.casbin.InvalidateCache()
}

// Authorize verify if the user has access to a resource to do specific action
func (m *manager) Authorize(principal *models.Principal, verb string, resources ...string) error {
	if m == nil {
		return fmt.Errorf("rbac enforcer expected but not set up")
	}
	if principal == nil {
		return errors.NewUnauthenticated()
	}

	// BatchEnforcers is not needed after some digging they just loop over requests,
	// w.r.t.
	// source code https://github.com/casbin/casbin/blob/master/enforcer.go#L872
	// issue https://github.com/casbin/casbin/issues/710

	username := conv.PrefixUserName(principal.Username)

	type result struct {
		resource string
		allow    bool
		perm     *models.Permission
		err      error
	}

	// Create channels
	jobs := make(chan string, len(resources))
	results := make(chan result, len(resources))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for resource := range jobs {
				r := result{resource: resource}

				allow, err := m.casbin.Enforce(username, resource, verb)
				if err != nil {
					r.err = err
					results <- r
					continue
				}
				r.allow = allow

				perm, err := conv.PathToPermission(verb, resource)
				if err != nil {
					r.err = err
					results <- r
					continue
				}
				r.perm = perm

				results <- r
			}
		}, m.logger)
	}

	// Send jobs
	enterrors.GoWrapper(func() {
		for _, resource := range resources {
			jobs <- resource
		}
		close(jobs)
	}, m.logger)

	// Close results channel after all workers are done
	enterrors.GoWrapper(func() {
		wg.Wait()
		close(results)
	}, m.logger)

	// Collect results
	for r := range results {
		if r.err != nil {
			m.logger.WithError(r.err).Error("failed to enforce policies")
			return r.err
		}

		m.logger.WithFields(logrus.Fields{
			"action":         "authorize",
			"component":      authorization.ComponentName,
			"user":           principal.Username,
			"resources":      prettyPermissionsResources(r.perm),
			"request_action": prettyPermissionsActions(r.perm),
			"results":        prettyStatus(r.allow),
		}).Info()

		if !r.allow {
			return errors.NewForbidden(principal, prettyPermissionsActions(r.perm), prettyPermissionsResources(r.perm))
		}
	}

	return nil
}

func prettyPermissionsActions(perm *models.Permission) string {
	if perm == nil || perm.Action == nil {
		return ""
	}
	return *perm.Action
}

func prettyPermissionsResources(perm *models.Permission) string {
	res := ""
	if perm == nil {
		return ""
	}

	if perm.Backups != nil && perm.Backups.Collection != nil && *perm.Backups.Collection != "" {
		res += fmt.Sprintf("Backups.Collection: %s,", *perm.Backups.Collection)
	}

	if perm.Data != nil {
		if perm.Data.Collection != nil && *perm.Data.Collection != "" {
			res += fmt.Sprintf(" Data.Collection: %s,", *perm.Data.Collection)
		}
		if perm.Data.Tenant != nil && *perm.Data.Tenant != "" {
			res += fmt.Sprintf(" Data.Tenant: %s,", *perm.Data.Tenant)
		}
		if perm.Data.Object != nil && *perm.Data.Object != "" {
			res += fmt.Sprintf(" Data.Object: %s,", *perm.Data.Object)
		}
	}

	if perm.Nodes != nil {
		if perm.Nodes.Verbosity != nil && *perm.Nodes.Verbosity != "" {
			res += fmt.Sprintf(" Nodes.Verbosity: %s,", *perm.Nodes.Verbosity)
		}
		if perm.Nodes.Collection != nil && *perm.Nodes.Collection != "" {
			res += fmt.Sprintf(" Nodes.Collection: %s,", *perm.Nodes.Collection)
		}
	}

	if perm.Roles != nil && perm.Roles.Role != nil && *perm.Roles.Role != "" {
		res += fmt.Sprintf(" Roles.Role: %s,", *perm.Roles.Role)
	}

	if perm.Collections != nil {
		if perm.Collections.Collection != nil && *perm.Collections.Collection != "" {
			res += fmt.Sprintf(" Schema.Collection: %s,", *perm.Collections.Collection)
		}
	}

	if perm.Tenants != nil {
		if perm.Tenants.Tenant != nil && *perm.Tenants.Tenant != "" {
			res += fmt.Sprintf(" Schema.Collection: %s,", *perm.Tenants.Collection)
			res += fmt.Sprintf(" Schema.Tenant: %s,", *perm.Tenants.Tenant)
		}
	}

	if many := strings.Count(res, ","); many == 1 {
		res = strings.ReplaceAll(res, ",", "")
		res = strings.TrimSpace(res)
	}
	return res
}

func prettyStatus(value bool) string {
	if value {
		return "success"
	}
	return "failed"
}

func collectStaleRoles(polices [][]string, casbinStoragePoliciesMap map[string]struct{}, casbinStoragePolicies [][][]string) [][][]string {
	for _, p := range polices {
		// ignore builtin roles
		if slices.Contains(authorization.BuiltInRoles, conv.TrimRoleNamePrefix(p[1])) {
			continue
		}
		// collect stale or empty roles
		if _, ok := casbinStoragePoliciesMap[p[1]]; !ok {
			// e.g. policy line in casbin -> g, user:wv_internal_empty, role:roleName, that's why p[1]
			casbinStoragePolicies = append(casbinStoragePolicies, [][]string{{
				p[1], conv.InternalPlaceHolder, conv.InternalPlaceHolder, "*",
			}})
		}
	}
	return casbinStoragePolicies
}
