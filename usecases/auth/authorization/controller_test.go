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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
)

type fakeRbacManager struct{}

func (f *fakeRbacManager) AddPolicies(policies []*rbac.Policy) error {
	return nil
}

func (f *fakeRbacManager) GetPolicies(name *string) ([]*rbac.Policy, error) {
	if name != nil && *name != "new-role" {
		return nil, nil
	}
	return []*rbac.Policy{{
		Name: "new-role",
		// Resource: "*",
		Verb:   "(C)|(R)|(U)|(D)",
		Domain: "roles",
	}}, nil
}

func (f *fakeRbacManager) RemovePolicies(policies []*rbac.Policy) error {
	return nil
}

func (f *fakeRbacManager) AddRolesForUser(user string, roles []string) error {
	return nil
}

func (f *fakeRbacManager) GetRolesForUser(user string) ([]string, error) {
	return nil, nil
}

func (f *fakeRbacManager) GetUsersForRole(role string) ([]string, error) {
	return nil, nil
}

func (f *fakeRbacManager) DeleteRolesForUser(user string, roles []string) error {
	return nil
}

func Test_roleToPolicies(t *testing.T) {
	tests := []struct {
		name        string
		permissions []*models.Permission
		expected    []*rbac.Policy
	}{
		{
			name: "new-role",
			permissions: []*models.Permission{{
				Action: String("manage_roles"),
			}},
			expected: []*rbac.Policy{{
				Name:     "new-role",
				Resource: "*",
				Verb:     "(C)|(R)|(U)|(D)",
				Domain:   "roles",
			}},
		},
		{
			name: "new-role",
			permissions: []*models.Permission{{
				Action: String("manage_roles"),
			}},
			expected: []*rbac.Policy{{
				Name:     "new-role",
				Resource: "*",
				Verb:     "(C)|(R)|(U)|(D)",
				Domain:   "roles",
			}},
		},
		{
			name: "new-role",
			permissions: []*models.Permission{{
				Action: String("manage_roles"),
			}},
			expected: []*rbac.Policy{{
				Name:     "new-role",
				Resource: "*",
				Verb:     "(C)|(R)|(U)|(D)",
				Domain:   "roles",
			}},
		},
	}
	for _, tt := range tests {
		policies := roleToPolicies(tt.name, tt.permissions)
		require.Equal(t, tt.expected, policies)
	}
}

func Test_rolesFromPolicies(t *testing.T) {
	tests := []struct {
		policies []*rbac.Policy
		expected []*models.Role
	}{
		{
			policies: []*rbac.Policy{{
				Name: "new-role",
				// Resource: "*",
				Verb:   "(C)|(R)|(U)|(D)",
				Domain: "roles",
			}},
			expected: []*models.Role{{
				Name: String("new-role"),
				Permissions: []*models.Permission{{
					Action:     String("manage_roles"),
					Collection: String(""),
				}},
			}},
		},
	}
	for _, tt := range tests {
		roles, err := rolesFromPolicies(tt.policies)
		require.Nil(t, err)
		require.Equal(t, tt.expected, roles)
	}
}

func TestAuthzController(t *testing.T) {
	controller := NewAuthzController(&fakeRbacManager{})

	t.Run("get roles", func(t *testing.T) {
		roles, err := controller.GetRoles()
		require.Nil(t, err)
		require.Equal(t, 1, len(roles))
		require.Equal(t, "new-role", *roles[0].Name)
		require.Equal(t, "manage_roles", *roles[0].Permissions[0].Action)
		// require.Equal(t, "*", *roles[0].Permissions[0].Resource)
	})

	t.Run("get existing role", func(t *testing.T) {
		role, err := controller.GetRole("new-role")
		require.Nil(t, err)
		require.Equal(t, "new-role", *role.Name)
		require.Equal(t, "manage_roles", *role.Permissions[0].Action)
		// require.Equal(t, "*", *role.Permissions[0].Resource)
	})

	t.Run("get non-existing role", func(t *testing.T) {
		role, err := controller.GetRole("non-existing-role")
		require.Nil(t, role)
		require.Equal(t, ErrRoleNotFound, err)
	})
}

func String(s string) *string {
	return &s
}
