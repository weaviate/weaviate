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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/usecases/config"

	"github.com/casbin/casbin/v2"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

type manager struct {
	casbin *casbin.SyncedCachedEnforcer
	logger logrus.FieldLogger
}

func New(rbacStoragePath string, rbac rbacconf.Config, authNconf config.Authentication, logger logrus.FieldLogger) (*manager, error) {
	csbin, err := Init(rbac, rbacStoragePath, authNconf)
	if err != nil {
		return nil, err
	}

	return &manager{csbin, logger}, nil
}

// there is no different between UpdateRolesPermissions and CreateRolesPermissions, purely to satisfy an interface
func (m *manager) UpdateRolesPermissions(roles map[string][]authorization.Policy) error {
	return m.upsertRolesPermissions(roles)
}

func (m *manager) CreateRolesPermissions(roles map[string][]authorization.Policy) error {
	return m.upsertRolesPermissions(roles)
}

func (m *manager) upsertRolesPermissions(roles map[string][]authorization.Policy) error {
	for roleName, policies := range roles {
		// assign role to internal user to make sure to catch empty roles
		// e.g. : g, user:wv_internal_empty, role:roleName
		if _, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId(conv.InternalPlaceHolder, models.UserTypeInputDb), conv.PrefixRoleName(roleName)); err != nil {
			return fmt.Errorf("AddRoleForUser: %w", err)
		}
		for _, policy := range policies {
			if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName(roleName), policy.Resource, policy.Verb, policy.Domain); err != nil {
				return fmt.Errorf("AddNamedPolicy: %w", err)
			}
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return fmt.Errorf("SavePolicy: %w", err)
	}
	if err := m.casbin.InvalidateCache(); err != nil {
		return fmt.Errorf("InvalidateCache: %w", err)
	}
	return nil
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
			return nil, fmt.Errorf("GetNamedPolicy: %w", err)
		}
		casbinStoragePolicies = append(casbinStoragePolicies, polices)

		for _, p := range polices {
			// e.g. policy line in casbin -> role:roleName resource verb domain, that's why p[0]
			casbinStoragePoliciesMap[p[0]] = struct{}{}
		}

		polices, err = m.casbin.GetNamedGroupingPolicy("g")
		if err != nil {
			return nil, fmt.Errorf("GetNamedGroupingPolicy: %w", err)
		}
		casbinStoragePolicies = collectStaleRoles(polices, casbinStoragePoliciesMap, casbinStoragePolicies)
	} else {
		for _, name := range names {
			polices, err := m.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName(name))
			if err != nil {
				return nil, fmt.Errorf("GetFilteredNamedPolicy: %w", err)
			}
			casbinStoragePolicies = append(casbinStoragePolicies, polices)

			for _, p := range polices {
				// e.g. policy line in casbin -> role:roleName resource verb domain, that's why p[0]
				casbinStoragePoliciesMap[p[0]] = struct{}{}
			}

			polices, err = m.casbin.GetFilteredNamedGroupingPolicy("g", 1, conv.PrefixRoleName(name))
			if err != nil {
				return nil, fmt.Errorf("GetFilteredNamedGroupingPolicy: %w", err)
			}
			casbinStoragePolicies = collectStaleRoles(polices, casbinStoragePoliciesMap, casbinStoragePolicies)
		}
	}
	policies, err := conv.CasbinPolicies(casbinStoragePolicies...)
	if err != nil {
		return nil, fmt.Errorf("CasbinPolicies: %w", err)
	}
	return policies, nil
}

func (m *manager) RemovePermissions(roleName string, permissions []*authorization.Policy) error {
	for _, permission := range permissions {
		ok, err := m.casbin.RemoveNamedPolicy("p", conv.PrefixRoleName(roleName), permission.Resource, permission.Verb, permission.Domain)
		if err != nil {
			return fmt.Errorf("RemoveNamedPolicy: %w", err)
		}
		if !ok {
			return nil // deletes are idempotent
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return fmt.Errorf("SavePolicy: %w", err)
	}
	if err := m.casbin.InvalidateCache(); err != nil {
		return fmt.Errorf("InvalidateCache: %w", err)
	}
	return nil
}

func (m *manager) HasPermission(roleName string, permission *authorization.Policy) (bool, error) {
	policy, err := m.casbin.HasNamedPolicy("p", conv.PrefixRoleName(roleName), permission.Resource, permission.Verb, permission.Domain)
	if err != nil {
		return false, fmt.Errorf("HasNamedPolicy: %w", err)
	}
	return policy, nil
}

func (m *manager) DeleteRoles(roles ...string) error {
	for _, roleName := range roles {
		// remove role
		roleRemoved, err := m.casbin.RemoveFilteredNamedPolicy("p", 0, conv.PrefixRoleName(roleName))
		if err != nil {
			return fmt.Errorf("RemoveFilteredNamedPolicy: %w", err)
		}
		// remove role assignment
		roleAssignmentsRemoved, err := m.casbin.RemoveFilteredGroupingPolicy(1, conv.PrefixRoleName(roleName))
		if err != nil {
			return fmt.Errorf("RemoveFilteredGroupingPolicy: %w", err)
		}

		if !roleRemoved && !roleAssignmentsRemoved {
			return nil // deletes are idempotent
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return fmt.Errorf("SavePolicy: %w", err)
	}
	if err := m.casbin.InvalidateCache(); err != nil {
		return fmt.Errorf("InvalidateCache: %w", err)
	}
	return nil
}

// AddRolesFroUser NOTE: user has to be prefixed by user:, group:, key: etc.
// see func PrefixUserName(user) it will prefix username and nop-op if already prefixed
func (m *manager) AddRolesForUser(user string, roles []string) error {
	if !conv.NameHasPrefix(user) {
		return errors.New("user does not contain a prefix")
	}

	for _, role := range roles {
		if _, err := m.casbin.AddRoleForUser(user, conv.PrefixRoleName(role)); err != nil {
			return fmt.Errorf("AddRoleForUser: %w", err)
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return fmt.Errorf("SavePolicy: %w", err)
	}
	if err := m.casbin.InvalidateCache(); err != nil {
		return fmt.Errorf("InvalidateCache: %w", err)
	}
	return nil
}

func (m *manager) GetRolesForUser(userName string, userType models.UserTypeInput) (map[string][]authorization.Policy, error) {
	rolesNames, err := m.casbin.GetRolesForUser(conv.UserNameWithTypeFromId(userName, userType))
	if err != nil {
		return nil, fmt.Errorf("GetRolesForUser: %w", err)
	}
	if len(rolesNames) == 0 {
		return map[string][]authorization.Policy{}, err
	}
	roles, err := m.GetRoles(rolesNames...)
	if err != nil {
		return nil, fmt.Errorf("GetRoles: %w", err)
	}
	return roles, err
}

func (m *manager) GetUsersForRole(roleName string, userType models.UserTypeInput) ([]string, error) {
	pusers, err := m.casbin.GetUsersForRole(conv.PrefixRoleName(roleName))
	if err != nil {
		return nil, fmt.Errorf("GetUsersForRole: %w", err)
	}
	users := make([]string, 0, len(pusers))
	for idx := range pusers {
		user, prefix := conv.GetUserAndPrefix(pusers[idx])
		if user == conv.InternalPlaceHolder {
			continue
		}
		if prefix != string(userType) {
			continue
		}
		users = append(users, user)
	}
	return users, nil
}

func (m *manager) RevokeRolesForUser(userName string, roles ...string) error {
	if !conv.NameHasPrefix(userName) {
		return errors.New("user does not contain a prefix")
	}

	for _, roleName := range roles {
		if _, err := m.casbin.DeleteRoleForUser(userName, conv.PrefixRoleName(roleName)); err != nil {
			return fmt.Errorf("DeleteRoleForUser: %w", err)
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return fmt.Errorf("SavePolicy: %w", err)
	}
	if err := m.casbin.InvalidateCache(); err != nil {
		return fmt.Errorf("InvalidateCache: %w", err)
	}
	return nil
}

// BatchEnforcers is not needed after some digging they just loop over requests,
// w.r.t.
// source code https://github.com/casbin/casbin/blob/master/enforcer.go#L872
// issue https://github.com/casbin/casbin/issues/710
func (m *manager) checkPermissions(principal *models.Principal, resource, verb string) (bool, error) {
	// first check group permissions
	for _, group := range principal.Groups {
		allowed, err := m.casbin.Enforce(conv.PrefixGroupName(group), resource, verb)
		if err != nil {
			return false, err
		}
		if allowed {
			return true, nil
		}
	}

	// If no group permissions, check user permissions
	return m.casbin.Enforce(conv.UserNameWithTypeFromPrincipal(principal), resource, verb)
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
		res += fmt.Sprintf(" Collection: %s,", *perm.Backups.Collection)
	}

	if perm.Data != nil {
		if perm.Data.Collection != nil && *perm.Data.Collection != "" {
			res += fmt.Sprintf(" Collection: %s,", *perm.Data.Collection)
		}
		if perm.Data.Tenant != nil && *perm.Data.Tenant != "" {
			res += fmt.Sprintf(" Tenant: %s,", *perm.Data.Tenant)
		}
		if perm.Data.Object != nil && *perm.Data.Object != "" {
			res += fmt.Sprintf(" Object: %s,", *perm.Data.Object)
		}
	}

	if perm.Nodes != nil {
		if perm.Nodes.Verbosity != nil && *perm.Nodes.Verbosity != "" {
			res += fmt.Sprintf(" Verbosity: %s,", *perm.Nodes.Verbosity)
		}
		if perm.Nodes.Collection != nil && *perm.Nodes.Collection != "" {
			res += fmt.Sprintf(" Collection: %s,", *perm.Nodes.Collection)
		}
	}

	if perm.Roles != nil && perm.Roles.Role != nil && *perm.Roles.Role != "" {
		res += fmt.Sprintf(" Role: %s,", *perm.Roles.Role)
	}

	if perm.Collections != nil {
		if perm.Collections.Collection != nil && *perm.Collections.Collection != "" {
			res += fmt.Sprintf(" Collection: %s,", *perm.Collections.Collection)
		}
	}

	if perm.Tenants != nil {
		if perm.Tenants.Tenant != nil && *perm.Tenants.Tenant != "" {
			res += fmt.Sprintf(" Collection: %s,", *perm.Tenants.Collection)
			res += fmt.Sprintf(" Tenant: %s,", *perm.Tenants.Tenant)
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
