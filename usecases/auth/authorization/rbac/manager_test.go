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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

type fakeRepo struct {
	mock.Mock
	enforcer

	policies [][]string
}

func (f *fakeRepo) GetNamedPolicy(ptype string) ([][]string, error) {
	return f.policies, nil
}

func (f *fakeRepo) GetFilteredNamedPolicy(ptype string, fieldIndex int, fieldValues ...string) ([][]string, error) {
	if len(fieldValues) == 0 {
		return f.policies, nil
	}
	if len(fieldValues) == 1 {
		for _, policy := range f.policies {
			if policy[fieldIndex] == fieldValues[0] {
				return [][]string{policy}, nil
			}
		}
	}
	return nil, nil
}

func (f *fakeRepo) AddNamedPolicy(ptype string, params ...any) (bool, error) {
	return true, nil
}

func (f *fakeRepo) SavePolicy() error {
	return nil
}

func (f *fakeRepo) InvalidateCache() error {
	return nil
}

func NewFakeRepo() *fakeRepo {
	return &fakeRepo{policies: [][]string{{"admin", "*", "(C)|(R)|(U)|(D)", "roles"}}}
}

func TestRbacManager(t *testing.T) {
	manager := New(NewFakeRepo(), logrus.New())

	t.Run("get existing role", func(t *testing.T) {
		roles, err := manager.GetRoles("admin")
		require.Nil(t, err)
		require.Len(t, roles, 1)
		require.Equal(t, "admin", *roles[0].Name)
		require.Equal(t, "manage_roles", *roles[0].Permissions[0].Action)
		require.Equal(t, "*", *roles[0].Permissions[0].Role)
	})

	t.Run("get non-existing role", func(t *testing.T) {
		roles, err := manager.GetRoles("non-existing-role")
		require.Nil(t, err)
		require.Equal(t, 0, len(roles))
	})

	all := "*"
	tests := []struct {
		name string
		role *models.Role
		err  error
	}{
		{name: "manage roles", role: &models.Role{Name: String("roles"), Permissions: []*models.Permission{{Action: String("manage_roles"), Role: &all}}}},
		{name: "create collections", role: &models.Role{Name: String("collections"), Permissions: []*models.Permission{{Action: String("create_collections"), Collection: &all}}}},
		{name: "create tenants", role: &models.Role{Name: String("tenants"), Permissions: []*models.Permission{{Action: String("create_tenants"), Collection: &all, Tenant: &all}}}},
		{name: "create objects", role: &models.Role{Name: String("objects"), Permissions: []*models.Permission{{Action: String("create_objects"), Collection: &all, Object: &all, Tenant: &all}}}},
	}
	t.Run("create role", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := manager.CreateRoles(tt.role)
				require.Nil(t, err)
			})
		}
	})
}

func String(s string) *string {
	return &s
}
