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

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"

	"github.com/casbin/casbin/v2"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
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
		for _, policy := range policies {
			if _, err := m.casbin.AddNamedPolicy("p", roleName, policy.Resource, policy.Verb, policy.Domain); err != nil {
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
	// TODO sort by name
	var casbinStoragePolicies [][][]string
	if len(names) == 0 {
		// get all roles
		polices, err := m.casbin.GetNamedPolicy("p")
		if err != nil {
			return nil, err
		}
		casbinStoragePolicies = append(casbinStoragePolicies, polices)
	} else {
		for _, name := range names {
			polices, err := m.casbin.GetFilteredNamedPolicy("p", 0, name)
			if err != nil {
				return nil, err
			}
			if len(polices) == 0 {
				continue
			}
			casbinStoragePolicies = append(casbinStoragePolicies, polices)
		}
	}

	return conv.CasbinPolicies(casbinStoragePolicies...)
}

func (m *manager) RemovePermissions(role string, permissions []*authorization.Policy) error {
	for _, permission := range permissions {
		ok, err := m.casbin.RemoveNamedPolicy("p", role, permission.Resource, permission.Verb, permission.Domain)
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

func (m *manager) DeleteRoles(roles ...string) error {
	for _, role := range roles {
		ok, err := m.casbin.RemoveFilteredNamedPolicy("p", 0, role)
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

func (m *manager) AddRolesForUser(user string, roles []string) error {
	// userName := fmt.Sprintf("%s%s", userPrefix, user)
	for _, role := range roles {
		// roleName := fmt.Sprintf("%s%s", rolePrefix, role)
		if _, err := m.casbin.AddRoleForUser(user, role); err != nil {
			return err
		}
	}

	return m.casbin.SavePolicy()
}

func (m *manager) GetRolesForUser(user string) (map[string][]authorization.Policy, error) {
	rolesNames, err := m.casbin.GetRolesForUser(user)
	if err != nil {
		return nil, err
	}
	if len(rolesNames) == 0 {
		return map[string][]authorization.Policy{}, err
	}

	return m.GetRoles(rolesNames...)
}

func (m *manager) GetUsersForRole(role string) ([]string, error) {
	return m.casbin.GetUsersForRole(role)
}

func (m *manager) RevokeRolesForUser(user string, roles ...string) error {
	for _, role := range roles {
		if _, err := m.casbin.DeleteRoleForUser(user, role); err != nil {
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
		return fmt.Errorf("user is unauthenticated")
	}

	// TODO batch enforce
	for _, resource := range resources {
		allow, err := m.casbin.Enforce(principal.Username, resource, verb)
		if err != nil {
			m.logger.WithFields(logrus.Fields{
				"action":         "authorize",
				"user":           principal.Username,
				"component":      authorization.ComponentName,
				"resource":       resource,
				"request_action": verb,
			}).WithError(err).Error("failed to enforce policy")
			return err
		}

		perm, err := conv.PathToPermission(verb, resource)
		if err != nil {
			return err
		}

		m.logger.WithFields(logrus.Fields{
			"action":         "authorize",
			"component":      authorization.ComponentName,
			"user":           principal.Username,
			"resources":      prettyPermissionsResources(perm),
			"request_action": prettyPermissionsActions(perm),
			"results":        prettyStatus(allow),
		}).Info()

		if !allow {
			return errors.NewForbidden(principal, prettyPermissionsActions(perm), prettyPermissionsResources(perm))
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

	if perm.Collection != nil && *perm.Collection != "" {
		res += fmt.Sprintf("Collection: %s,", *perm.Collection)
	}

	if perm.Tenant != nil && *perm.Tenant != "" {
		res += fmt.Sprintf(" Tenant: %s,", *perm.Tenant)
	}

	if perm.Object != nil && *perm.Object != "" {
		res += fmt.Sprintf(" Object: %s,", *perm.Object)
	}

	if perm.Role != nil && *perm.Role != "" {
		res += fmt.Sprintf(" Role: %s,", *perm.Role)
	}

	if perm.User != nil && *perm.User != "" {
		res += fmt.Sprintf(" User: %s,", *perm.User)
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
