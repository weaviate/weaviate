package authorization

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
)

type fakeRbacController struct{}

func (f *fakeRbacController) AddPolicies(policies []*rbac.Policy) error {
	return nil
}

func (f *fakeRbacController) GetPolicies(name *string) ([]*rbac.Policy, error) {
	return nil, nil
}

func (f *fakeRbacController) RemovePolicies(policies []*rbac.Policy) error {
	return nil
}

func (f *fakeRbacController) AddRolesForUser(user string, roles []string) error {
	return nil
}

func (f *fakeRbacController) GetRolesForUser(user string) ([]string, error) {
	return nil, nil
}

func (f *fakeRbacController) GetUsersForRole(role string) ([]string, error) {
	return nil, nil
}

func (f *fakeRbacController) DeleteRolesForUser(user string, roles []string) error {
	return nil
}

func Test_roleToPolicies(t *testing.T) {
	controller := NewAuthzController(&fakeRbacController{})
	tests := []struct {
		name        string
		permissions []*Permission
		expected    []*rbac.Policy
	}{
		{
			name: "new-role",
			permissions: []*Permission{{
				Action:   "manage_roles",
				Resource: String("*"),
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
			permissions: []*Permission{{
				Action:   "manage_roles",
				Resource: String(""),
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
			permissions: []*Permission{{
				Action: "manage_roles",
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
		policies := controller.roleToPolicies(tt.name, tt.permissions)
		require.Equal(t, tt.expected, policies)
	}
}

func Test_rolesFromPolicies(t *testing.T) {
	controller := NewAuthzController(&fakeRbacController{})
	tests := []struct {
		policies []*rbac.Policy
		expected []*models.Role
	}{
		{
			policies: []*rbac.Policy{{
				Name:     "new-role",
				Resource: "*",
				Verb:     "(C)|(R)|(U)|(D)",
				Domain:   "roles",
			}},
			expected: []*models.Role{{
				Name: String("new-role"),
				Permissions: []*models.Permission{{
					Action:   String("manage_roles"),
					Resource: String("*"),
				}},
			}},
		},
	}
	for _, tt := range tests {
		roles, err := controller.rolesFromPolicies(tt.policies)
		require.Nil(t, err)
		require.Equal(t, tt.expected, roles)
	}
}

func String(s string) *string {
	return &s
}
