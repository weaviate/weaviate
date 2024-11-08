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

	"github.com/casbin/casbin/v2"
)

type Manager struct {
	casbin *casbin.SyncedCachedEnforcer
}

func NewManager(casbin *casbin.SyncedCachedEnforcer) *Manager {
	return &Manager{casbin: casbin}
}

func (m *Manager) AddPolicies(policies []*Policy) error {
	for _, policy := range policies {
		if _, err := m.casbin.AddPolicy(policy.Name, policy.Resource, policy.Verb, policy.Domain); err != nil {
			return err
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return err
	}
	if err := m.casbin.InvalidateCache(); err != nil {
		return err
	}
	return nil
}

func (m *Manager) SavePolicy() error {
	return m.casbin.SavePolicy()
}

func (m *Manager) InvalidateCache() error {
	return m.casbin.InvalidateCache()
}

func (m *Manager) GetPolicies(name *string) ([]*Policy, error) {
	ps, err := m.casbin.GetPolicy()
	if err != nil {
		return nil, err
	}
	policies := []*Policy{}
	for _, p := range ps {
		if name != nil && p[0] != *name {
			continue
		}
		policies = append(policies, &Policy{Name: p[0], Resource: p[1], Verb: p[2], Domain: p[3]})
	}
	return policies, nil
}

func (m *Manager) RemovePolicies(policies []*Policy) error {
	for _, policy := range policies {
		ok, err := m.casbin.RemovePolicy(policy.Name, policy.Resource, policy.Verb, policy.Domain)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("failed to remove policy %v", policy)
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return err
	}

	if err := m.casbin.InvalidateCache(); err != nil {
		return err
	}
	return nil
}

func (m *Manager) AddRolesForUser(user string, roles []string) error {
	for _, role := range roles {
		if _, err := m.casbin.AddRoleForUser(user, role); err != nil {
			return err
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return err
	}
	return nil
}

func (m *Manager) GetRolesForUser(user string) ([]string, error) {
	return m.casbin.GetRolesForUser(user)
}

func (m *Manager) GetUsersForRole(role string) ([]string, error) {
	return m.casbin.GetUsersForRole(role)
}

func (m *Manager) DeleteRolesForUser(user string, role []string) error {
	for _, role := range role {
		if _, err := m.casbin.DeleteRoleForUser(user, role); err != nil {
			return err
		}
	}
	if err := m.casbin.SavePolicy(); err != nil {
		return err
	}
	return nil
}
