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
	"github.com/casbin/casbin/v2"
)

type Enforcer struct {
	casbin *casbin.SyncedCachedEnforcer
}

func NewEnforcer(casbin *casbin.SyncedCachedEnforcer) *Enforcer {
	return &Enforcer{casbin: casbin}
}

func (e *Enforcer) Enforce(username, resource, verb string) (bool, error) {
	return e.casbin.Enforce(username, resource, verb)
}

func (e *Enforcer) AddPolicies(policies []*Policy) error {
	for _, policy := range policies {
		if _, err := e.casbin.AddPolicy(policy.Name, policy.Resource, policy.Verb, policy.Domain); err != nil {
			return err
		}
	}
	if err := e.casbin.SavePolicy(); err != nil {
		return err
	}
	if err := e.casbin.InvalidateCache(); err != nil {
		return err
	}
	return nil
}

func (e *Enforcer) SavePolicy() error {
	return e.casbin.SavePolicy()
}

func (e *Enforcer) InvalidateCache() error {
	return e.casbin.InvalidateCache()
}

func (e *Enforcer) GetPolicies(name *string) ([]*Policy, error) {
	ps, err := e.casbin.GetPolicy()
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

func (e *Enforcer) RemovePolicies(roleNmae string) error {
	if _, err := e.casbin.RemovePolicy(roleNmae); err != nil {
		return err
	}
	if err := e.casbin.SavePolicy(); err != nil {
		return err
	}

	if err := e.casbin.InvalidateCache(); err != nil {
		return err
	}
	return nil
}

func (e *Enforcer) AddRolesForUser(user string, roles []string) error {
	for _, role := range roles {
		if _, err := e.casbin.AddRoleForUser(user, role); err != nil {
			return err
		}
	}
	if err := e.casbin.SavePolicy(); err != nil {
		return err
	}
	return nil
}

func (e *Enforcer) GetRolesForUser(user string) ([]string, error) {
	return e.casbin.GetRolesForUser(user)
}

func (e *Enforcer) GetUsersForRole(role string) ([]string, error) {
	return e.casbin.GetUsersForRole(role)
}

func (e *Enforcer) DeleteRolesForUser(user string, role []string) error {
	for _, role := range role {
		if _, err := e.casbin.DeleteRoleForUser(user, role); err != nil {
			return err
		}
	}
	if err := e.casbin.SavePolicy(); err != nil {
		return err
	}
	return nil
}
