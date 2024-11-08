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
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
)

type AuthzController struct {
	rbac rbacManager
}

type rbacManager interface {
	AddPolicies(policies []*rbac.Policy) error
	GetPolicies(name *string) ([]*rbac.Policy, error)
	RemovePolicies(policies []*rbac.Policy) error
	AddRolesForUser(user string, roles []string) error
	GetRolesForUser(user string) ([]string, error)
	GetUsersForRole(role string) ([]string, error)
	DeleteRolesForUser(user string, roles []string) error
	DeleteRoleFromUsers(role string) error
}

var ErrRoleNotFound = errors.New("role not found")

func NewAuthzController(rbac rbacManager) *AuthzController {
	return &AuthzController{rbac: rbac}
}

func (c *AuthzController) CreateRole(name string, permissions []*models.Permission) error {
	return c.rbac.AddPolicies(roleToPolicies(name, permissions))
}

func (c *AuthzController) GetRoles() ([]*models.Role, error) {
	policies, err := c.rbac.GetPolicies(nil)
	if err != nil {
		return nil, err
	}
	return rolesFromPolicies(policies)
}

func (c *AuthzController) GetRole(name string) (*models.Role, error) {
	policies, err := c.rbac.GetPolicies(&name)
	if err != nil {
		return nil, err
	}
	if len(policies) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrRoleNotFound, name)
	}
	roles, err := rolesFromPolicies(policies)
	if err != nil {
		return nil, err
	}
	return roles[0], nil
}

func (m *AuthzController) GetRolesByName(names ...string) ([]*models.Role, error) {
	var roles []*models.Role
	for _, name := range names {
		policies, err := m.rbac.GetPolicies(&name)
		if err != nil {
			return nil, err
		}
		role, err := rolesFromPolicies(policies)
		if err != nil {
			return nil, err
		}
		if len(role) == 0 {
			return nil, fmt.Errorf("%w: %s", ErrRoleNotFound, name)
		}
		roles = append(roles, role[0])
	}
	return roles, nil
}

func (m *AuthzController) GetRolesForUser(user string) ([]*models.Role, error) {
	roleNames, err := m.rbac.GetRolesForUser(user)
	if err != nil {
		return nil, err
	}
	return m.GetRolesByName(roleNames...)
}

func (c *AuthzController) DeleteRole(name string) error {
	err := c.rbac.DeleteRoleFromUsers(name)
	if err != nil {
		return err
	}
	policies, err := c.rbac.GetPolicies(&name)
	if err != nil {
		return err
	}
	if err := c.rbac.RemovePolicies(policies); err != nil {
		return err
	}
	return nil
}

func (c *AuthzController) AddRolesForUser(user string, roles []string) error {
	return c.rbac.AddRolesForUser(user, roles)
}

func (c *AuthzController) GetUsersForRole(role string) ([]string, error) {
	return c.rbac.GetUsersForRole(role)
}

func (c *AuthzController) DeleteRolesForUser(user string, roles []string) error {
	return c.rbac.DeleteRolesForUser(user, roles)
}

func roleToPolicies(name string, permissions []*models.Permission) []*rbac.Policy {
	policies := []*rbac.Policy{}
	for _, permission := range permissions {
		domain := DomainByAction[*permission.Action]
		var resource string
		if permission.Collection == nil || *permission.Collection == "" { // no filters
			resource = "*"
		} else {
			resource = *permission.Collection
		}
		for _, verb := range Verbs(ActionsByDomain[domain][*permission.Action]) {
			policies = append(policies, &rbac.Policy{Name: name, Resource: resource, Verb: verb, Domain: string(domain)})
		}
	}
	return policies
}

func rolesFromPolicies(policies []*rbac.Policy) ([]*models.Role, error) {
	verbsByDomainByRole := make(map[string]map[Domain]map[string]struct{})
	resourcesByDomainByRole := make(map[string]map[Domain]map[string]struct{})

	for _, policy := range policies {
		domain, err := ToDomain(policy.Domain)
		if err != nil {
			return nil, err
		}
		if _, ok := verbsByDomainByRole[policy.Name]; !ok {
			verbsByDomainByRole[policy.Name] = map[Domain]map[string]struct{}{}
		}
		if _, ok := verbsByDomainByRole[policy.Name][domain]; !ok {
			verbsByDomainByRole[policy.Name][domain] = map[string]struct{}{}
		}
		if _, ok := resourcesByDomainByRole[policy.Name]; !ok {
			resourcesByDomainByRole[policy.Name] = map[Domain]map[string]struct{}{}
		}
		if _, ok := resourcesByDomainByRole[policy.Name][domain]; !ok {
			resourcesByDomainByRole[policy.Name][domain] = map[string]struct{}{}
		}
		verbsByDomainByRole[policy.Name][domain][policy.Verb] = struct{}{}
		resourcesByDomainByRole[policy.Name][domain][policy.Resource] = struct{}{}
	}

	out := make([]*models.Role, 0, len(verbsByDomainByRole))
	for role, verbsByDomain := range verbsByDomainByRole {
		permissions := make([]*models.Permission, 0)
		for domain, verbs := range verbsByDomain {
			actions, names := AllActionsForDomain(domain)

			vs := make([]string, 0, len(verbs))
			for v := range verbs {
				vs = append(vs, v)
			}
			for idx, action := range actions {
				if containsAllElements(vs, action.Verbs()) {
					for r := range resourcesByDomainByRole[role][domain] {
						permissions = append(permissions, &models.Permission{
							Action:     &names[idx],
							Collection: &r,
						})
					}
				}
			}
		}
		out = append(out, &models.Role{
			Name:        &role,
			Permissions: permissions,
		})
	}
	return out, nil
}

func containsAllElements(mainSlice, subSlice []string) bool {
	elementMap := make(map[string]bool)

	// Add all elements of mainSlice to the map
	for _, val := range mainSlice {
		elementMap[val] = true
	}

	// Check if each element of subSlice is in the map
	for _, val := range subSlice {
		if !elementMap[val] {
			return false // An element is missing
		}
	}

	return true
}
