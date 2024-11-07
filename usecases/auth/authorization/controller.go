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

	"github.com/casbin/casbin/v2"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
)

type AuthzController struct {
	enforcer *rbac.Enforcer
}

var ErrRoleNotFound = errors.New("role not found")

func NewAuthzManager(casbin *casbin.SyncedCachedEnforcer) *AuthzController {
	return &AuthzController{enforcer: rbac.NewEnforcer(casbin)}
}

func (m *AuthzController) Enforcer() *rbac.Enforcer {
	return m.enforcer
}

func (m *AuthzController) CreateRole(role *models.Role) error {
	policies := []*rbac.Policy{}
	for _, permission := range role.Permissions {
		if permission.Resource == nil || *permission.Resource == "" { // no filters
			action := *permission.Action
			domain := DomainByAction[action]
			for _, verb := range Verbs(ActionsByDomain[domain][action]) {
				policies = append(policies, &rbac.Policy{Name: *role.Name, Resource: "*", Verb: verb, Domain: string(domain)})
			}
		} else {
			resource := *permission.Resource // with filtering
			action := *permission.Action
			domain := DomainByAction[action]
			for _, verb := range Verbs(ActionsByDomain[domain][action]) {
				policies = append(policies, &rbac.Policy{Name: *role.Name, Resource: resource, Verb: verb, Domain: string(domain)}) // TODO: add filter to specific resource
			}
		}
	}
	return m.enforcer.AddPolicies(policies)
}

func (m *AuthzController) GetRoles() ([]*models.Role, error) {
	policies, err := m.enforcer.GetPolicies(nil)
	if err != nil {
		return nil, err
	}
	return m.rolesFromPolicies(policies)
}

func (m *AuthzController) GetRole(name string) (*models.Role, error) {
	policies, err := m.enforcer.GetPolicies(&name)
	if err != nil {
		return nil, err
	}
	roles, err := m.rolesFromPolicies(policies)
	if err != nil {
		return nil, err
	}
	if len(roles) == 0 {
		return nil, ErrRoleNotFound
	}
	return roles[0], nil
}

func (m *AuthzController) DeleteRole(name string) error {
	policies, err := m.enforcer.GetPolicies(&name)
	if err != nil {
		return err
	}
	if err := m.enforcer.RemovePolicies(policies); err != nil {
		return err
	}
	return nil
}

func (m *AuthzController) rolesFromPolicies(policies []*rbac.Policy) ([]*models.Role, error) {
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
							Action:   &names[idx],
							Resource: &r,
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
