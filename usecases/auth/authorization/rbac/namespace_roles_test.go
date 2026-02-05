//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rbac

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestNamespaceRoleNameFormats(t *testing.T) {
	tests := []struct {
		namespace string
		roleType  NamespaceRoleType
		expected  string
	}{
		{"tenanta", NamespaceRoleAdmin, "namespace-admin-tenanta"},
		{"tenantb", NamespaceRoleAdmin, "namespace-admin-tenantb"},
		{"myapp", NamespaceRoleEditor, "namespace-editor-myapp"},
		{"myapp", NamespaceRoleViewer, "namespace-viewer-myapp"},
		{"longnamespace123", NamespaceRoleAdmin, "namespace-admin-longnamespace123"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := NamespaceRoleName(tt.namespace, tt.roleType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCreateNamespaceAdminPoliciesStructure(t *testing.T) {
	policies := CreateNamespaceAdminPolicies("tenanta")

	// Should have 8 policies: 2 for schema (collection + tenant level), data, backups, aliases, replicate, users, roles
	assert.Len(t, policies, 8)

	// Verify collection-scoped policies have the correct namespace prefix pattern (capitalized)
	collectionScopedDomains := map[string]bool{
		authorization.SchemaDomain:    true,
		authorization.DataDomain:      true,
		authorization.BackupsDomain:   true,
		authorization.AliasesDomain:   true,
		authorization.ReplicateDomain: true,
	}
	for _, p := range policies {
		if collectionScopedDomains[p.Domain] {
			assert.Contains(t, p.Resource, "Tenanta__", "resource should contain capitalized namespace prefix")
			assert.Equal(t, conv.CRUD, p.Verb, "verb should be CRUD for collection-scoped policies")
		}
	}

	// Verify users domain has READ verb and roles domain has READ_ALL verb
	for _, p := range policies {
		if p.Domain == authorization.UsersDomain {
			assert.Equal(t, authorization.READ, p.Verb, "verb should be READ for users domain")
		}
		if p.Domain == authorization.RolesDomain {
			assert.Equal(t, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), p.Verb, "verb should be READ_ALL for roles domain")
		}
	}

	// Verify all expected domains are covered
	domains := make(map[string]bool)
	for _, p := range policies {
		domains[p.Domain] = true
	}

	expectedDomains := []string{
		authorization.SchemaDomain,
		authorization.DataDomain,
		authorization.BackupsDomain,
		authorization.AliasesDomain,
		authorization.ReplicateDomain,
		authorization.UsersDomain,
		authorization.RolesDomain,
	}

	for _, domain := range expectedDomains {
		assert.True(t, domains[domain], "should have %s domain", domain)
	}

	// Verify schema domain has both collection-level (#) and tenant-level (.*) policies
	schemaCollectionLevel := false
	schemaTenantLevel := false
	for _, p := range policies {
		if p.Domain == authorization.SchemaDomain {
			if strings.HasSuffix(p.Resource, "/shards/#") {
				schemaCollectionLevel = true
			}
			if strings.HasSuffix(p.Resource, "/shards/.*") {
				schemaTenantLevel = true
			}
		}
	}
	assert.True(t, schemaCollectionLevel, "should have collection-level schema policy (ending with #)")
	assert.True(t, schemaTenantLevel, "should have tenant-level schema policy (ending with .*)")
}

func TestCreateNamespaceAdminPoliciesDifferentNamespaces(t *testing.T) {
	tests := []struct {
		namespace      string
		expectedPrefix string
	}{
		{"tenanta", "Tenanta__"},
		{"myapp", "Myapp__"},
		{"testing123", "Testing123__"},
	}

	// Collection-scoped domains that should have namespace prefix
	collectionScopedDomains := map[string]bool{
		authorization.SchemaDomain:    true,
		authorization.DataDomain:      true,
		authorization.BackupsDomain:   true,
		authorization.AliasesDomain:   true,
		authorization.ReplicateDomain: true,
	}

	for _, tt := range tests {
		t.Run(tt.namespace, func(t *testing.T) {
			policies := CreateNamespaceAdminPolicies(tt.namespace)

			// Collection-scoped policies should reference the specific namespace (capitalized)
			for _, p := range policies {
				if collectionScopedDomains[p.Domain] {
					assert.Contains(t, p.Resource, tt.expectedPrefix,
						"policy resource should contain capitalized namespace prefix")
				}
			}
		})
	}
}

func TestIsNamespaceRole(t *testing.T) {
	tests := []struct {
		roleName string
		expected bool
	}{
		{"namespace-admin-tenanta", true},
		{"namespace-editor-myapp", true},
		{"namespace-viewer-testing", true},
		{"admin", false},
		{"viewer", false},
		{"custom-role", false},
		{"namespace", false},
		{"namespace-", true}, // Has the prefix, but ParseNamespaceRole would return false
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.roleName, func(t *testing.T) {
			result := IsNamespaceRole(tt.roleName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseNamespaceRole(t *testing.T) {
	tests := []struct {
		roleName     string
		expectedType string
		expectedNs   string
		expectedOk   bool
	}{
		{"namespace-admin-tenanta", "admin", "tenanta", true},
		{"namespace-editor-myapp", "editor", "myapp", true},
		{"namespace-viewer-testing123", "viewer", "testing123", true},
		{"admin", "", "", false},
		{"namespace-unknown-test", "", "", false},
		{"namespace-admin", "", "", false}, // missing namespace
		{"", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.roleName, func(t *testing.T) {
			roleType, namespace, ok := ParseNamespaceRole(tt.roleName)
			assert.Equal(t, tt.expectedOk, ok)
			if ok {
				assert.Equal(t, tt.expectedType, roleType)
				assert.Equal(t, tt.expectedNs, namespace)
			}
		})
	}
}

func TestCleanNamespaceRoleName(t *testing.T) {
	tests := []struct {
		roleName      string
		userNamespace string
		expected      string
	}{
		// Namespace-scoped role for same namespace -> cleaned
		{"namespace-admin-tenanta", "tenanta", "admin"},
		{"namespace-editor-myapp", "myapp", "editor"},
		{"namespace-viewer-testing", "testing", "viewer"},
		// Namespace-scoped role for different namespace -> unchanged
		{"namespace-admin-tenanta", "tenantb", "namespace-admin-tenanta"},
		{"namespace-editor-myapp", "other", "namespace-editor-myapp"},
		// Non-namespace roles -> unchanged
		{"admin", "tenanta", "admin"},
		{"viewer", "myapp", "viewer"},
		{"custom-role", "testing", "custom-role"},
		// Empty user namespace -> unchanged
		{"namespace-admin-tenanta", "", "namespace-admin-tenanta"},
	}

	for _, tt := range tests {
		t.Run(tt.roleName+"_"+tt.userNamespace, func(t *testing.T) {
			result := CleanNamespaceRoleName(tt.roleName, tt.userNamespace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRoleBelongsToNamespace(t *testing.T) {
	tests := []struct {
		roleName  string
		namespace string
		expected  bool
	}{
		{"namespace-admin-tenanta", "tenanta", true},
		{"namespace-editor-tenanta", "tenanta", true},
		{"namespace-viewer-tenanta", "tenanta", true},
		{"namespace-admin-tenanta", "tenantb", false},
		{"namespace-admin-tenantb", "tenanta", false},
		{"admin", "tenanta", false},
		{"custom-role", "tenanta", false},
		{"namespace-admin-tenanta", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.roleName+"_"+tt.namespace, func(t *testing.T) {
			result := RoleBelongsToNamespace(tt.roleName, tt.namespace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCleanPermissionCollectionName(t *testing.T) {
	tests := []struct {
		collectionName string
		namespace      string
		expected       string
	}{
		// With namespace prefix -> cleaned
		{"Tenanta__Articles", "tenanta", "Articles"},
		{"Tenanta__Products", "tenanta", "Products"},
		{"Myapp__Users", "myapp", "Users"},
		// Wildcard pattern -> cleaned
		{"Tenanta__*", "tenanta", "*"},
		// Without matching prefix -> unchanged
		{"Articles", "tenanta", "Articles"},
		{"Tenantb__Articles", "tenanta", "Tenantb__Articles"},
		{"Products", "myapp", "Products"},
		// Empty namespace -> unchanged
		{"Tenanta__Articles", "", "Tenanta__Articles"},
		// Case sensitivity - namespace is capitalized in prefix
		{"tenanta__Articles", "tenanta", "tenanta__Articles"}, // lowercase doesn't match
	}

	for _, tt := range tests {
		t.Run(tt.collectionName+"_"+tt.namespace, func(t *testing.T) {
			result := CleanPermissionCollectionName(tt.collectionName, tt.namespace)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEnsureNamespaceRoleForUser(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "rbac-namespace-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	rbacDir := filepath.Join(tmpDir, "rbac")
	require.NoError(t, os.MkdirAll(rbacDir, 0o755))

	policyPath := filepath.Join(rbacDir, "policy.csv")
	logger, _ := test.NewNullLogger()

	conf := rbacconf.Config{Enabled: true}
	authNConf := config.Authentication{
		OIDC:   config.OIDC{Enabled: true},
		APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"test-user"}},
	}

	m, err := New(policyPath, conf, authNConf, logger, nil)
	require.NoError(t, err)

	t.Run("skips default namespace", func(t *testing.T) {
		err := m.EnsureNamespaceRoleForUser("testuser", "default", authentication.AuthTypeDb)
		require.NoError(t, err)

		// No role should be created for default namespace
		roles, err := m.GetRoles("namespace-admin-default")
		require.NoError(t, err)
		assert.Empty(t, roles)
	})

	t.Run("skips empty namespace", func(t *testing.T) {
		err := m.EnsureNamespaceRoleForUser("testuser", "", authentication.AuthTypeDb)
		require.NoError(t, err)

		// No role should be created for empty namespace
		roles, err := m.GetRoles("namespace-admin-")
		require.NoError(t, err)
		assert.Empty(t, roles)
	})

	t.Run("creates role and assigns to user", func(t *testing.T) {
		namespace := "tenanta"
		username := "testuser-tenanta"
		expectedRole := "namespace-admin-tenanta"

		err := m.EnsureNamespaceRoleForUser(username, namespace, authentication.AuthTypeDb)
		require.NoError(t, err)

		// Verify role exists
		roles, err := m.GetRoles(expectedRole)
		require.NoError(t, err)
		assert.NotEmpty(t, roles)
		assert.NotEmpty(t, roles[expectedRole], "role should have policies")

		// Verify user has the role
		userRoles, err := m.GetRolesForUserOrGroup(username, authentication.AuthTypeDb, false)
		require.NoError(t, err)
		_, hasRole := userRoles[expectedRole]
		assert.True(t, hasRole, "user should have namespace role assigned")
	})

	t.Run("reuses existing role", func(t *testing.T) {
		namespace := "tenantb"
		username1 := "user1-tenantb"
		username2 := "user2-tenantb"
		expectedRole := "namespace-admin-tenantb"

		// First user
		err := m.EnsureNamespaceRoleForUser(username1, namespace, authentication.AuthTypeDb)
		require.NoError(t, err)

		// Get role policies count
		roles1, err := m.GetRoles(expectedRole)
		require.NoError(t, err)
		policiesCount1 := len(roles1[expectedRole])

		// Second user - should reuse existing role
		err = m.EnsureNamespaceRoleForUser(username2, namespace, authentication.AuthTypeDb)
		require.NoError(t, err)

		// Role should still have same number of policies (wasn't recreated)
		roles2, err := m.GetRoles(expectedRole)
		require.NoError(t, err)
		assert.Equal(t, policiesCount1, len(roles2[expectedRole]),
			"role should be reused, not recreated")

		// Both users should have the role
		user1Roles, err := m.GetRolesForUserOrGroup(username1, authentication.AuthTypeDb, false)
		require.NoError(t, err)
		_, hasRole1 := user1Roles[expectedRole]
		assert.True(t, hasRole1, "user1 should have namespace role")

		user2Roles, err := m.GetRolesForUserOrGroup(username2, authentication.AuthTypeDb, false)
		require.NoError(t, err)
		_, hasRole2 := user2Roles[expectedRole]
		assert.True(t, hasRole2, "user2 should have namespace role")
	})
}
