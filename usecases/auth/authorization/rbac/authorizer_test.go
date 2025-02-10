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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzErrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

func TestAuthorize(t *testing.T) {
	tests := []struct {
		name          string
		principal     *models.Principal
		verb          string
		resources     []string
		skipAudit     bool
		setupPolicies func(*manager) error
		wantErr       bool
		errContains   string
	}{
		{
			name:      "nil principal returns unauthenticated error",
			principal: nil,
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test"),
			wantErr:   true,
		},
		{
			name: "empty resources returns error",
			principal: &models.Principal{
				Username: "test-user",
				Groups:   []string{},
			},
			verb:        authorization.READ,
			resources:   []string{},
			wantErr:     true,
			errContains: "at least 1 resource is required",
		},
		{
			name: "authorized user with correct permissions",
			principal: &models.Principal{
				Username: "admin-user",
				Groups:   []string{"admin-group"},
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", authorization.SchemaDomain, authorization.READ)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.PrefixUserName("admin-user"),
					conv.PrefixRoleName("admin"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
		},
		{
			name: "unauthorized user returns forbidden error",
			principal: &models.Principal{
				Username: "regular-user",
				Groups:   []string{},
			},
			verb:        authorization.UPDATE,
			resources:   authorization.CollectionsMetadata("Test1"),
			wantErr:     true,
			errContains: "forbidden",
		},
		{
			name: "partial authorization fails completely",
			principal: &models.Principal{
				Username: "partial-user",
				Groups:   []string{},
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("partial"), authorization.CollectionsMetadata("Test1")[0], authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.PrefixUserName("partial-user"),
					conv.PrefixRoleName("partial"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
			wantErr:     true,
			errContains: "Test2",
		},
		{
			name: "group-based authorization",
			principal: &models.Principal{
				Username: "group-user",
				Groups:   []string{"authorized-group"},
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1"),
			setupPolicies: func(m *manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("group-role"), authorization.CollectionsMetadata("Test1")[0], authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.PrefixGroupName("authorized-group"),
					conv.PrefixRoleName("group-role"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for group")
				}
				return nil
			},
		},
		{
			name: "audit logging can be skipped",
			principal: &models.Principal{
				Username: "audit-test-user",
				Groups:   []string{},
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1"),
			skipAudit: true,
			setupPolicies: func(m *manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("audit-role"), authorization.CollectionsMetadata("Test1")[0], authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.PrefixUserName("audit-test-user"),
					conv.PrefixRoleName("audit-role"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup logger with hook for testing
			logger, hook := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			// Setup policies if needed
			if tt.setupPolicies != nil {
				err := tt.setupPolicies(m)
				require.NoError(t, err)
			}

			// Execute
			err = m.authorize(tt.principal, tt.verb, tt.skipAudit, tt.resources...)

			// Assert error conditions
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)

			// Verify logging behavior
			if !tt.skipAudit {
				require.NotEmpty(t, hook.AllEntries())
				lastEntry := hook.LastEntry()
				require.NotNil(t, lastEntry)

				// Verify log fields
				assert.Equal(t, "authorize", lastEntry.Data["action"])
				assert.Equal(t, tt.principal.Username, lastEntry.Data["user"])
				assert.Equal(t, authorization.ComponentName, lastEntry.Data["component"])
				assert.Equal(t, tt.verb, lastEntry.Data["request_action"])

				if len(tt.principal.Groups) > 0 {
					assert.Contains(t, lastEntry.Data, "groups")
					assert.ElementsMatch(t, tt.principal.Groups, lastEntry.Data["groups"])
				}
			} else {
				// Verify no info logs when audit is skipped
				for _, entry := range hook.AllEntries() {
					assert.NotEqual(t, logrus.InfoLevel, entry.Level)
				}
			}
		})
	}
}

func TestFilterAuthorizedResources(t *testing.T) {
	tests := []struct {
		name          string
		principal     *models.Principal
		verb          string
		resources     []string
		setupPolicies func(*manager) error
		wantResources []string
		wantErr       bool
		errType       error
	}{
		{
			name:      "nil principal returns unauthenticated error",
			principal: nil,
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test"),
			wantErr:   true,
			errType:   authzErrors.Unauthenticated{},
		},
		{
			name: "wildcard permission allows all resources",
			principal: &models.Principal{
				Username: "admin-user",
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"),
					"*", authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.PrefixUserName("admin-user"),
					conv.PrefixRoleName("admin"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
			wantResources: authorization.CollectionsMetadata("Test1", "Test2"),
		},
		{
			name: "specific permission allows only matching resource",
			principal: &models.Principal{
				Username: "limited-user",
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("limited"),
					authorization.CollectionsMetadata("Test1")[0], authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.PrefixUserName("limited-user"),
					conv.PrefixRoleName("limited"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
			wantResources: authorization.CollectionsMetadata("Test1"),
		},
		{
			name: "no permissions returns empty list",
			principal: &models.Principal{
				Username: "no-perm-user",
			},
			verb:          authorization.READ,
			resources:     authorization.CollectionsMetadata("Test1", "Test2"),
			wantResources: []string{},
		},
		{
			name: "wildcard collection permission allows all collections",
			principal: &models.Principal{
				Username: "collections-admin",
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("collections-admin"),
					authorization.CollectionsMetadata()[0], authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.PrefixUserName("collections-admin"),
					conv.PrefixRoleName("collections-admin"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
			wantResources: authorization.CollectionsMetadata("Test1", "Test2"),
		},
		{
			name: "empty resources list returns empty result",
			principal: &models.Principal{
				Username: "test-user",
			},
			verb:          authorization.READ,
			resources:     []string{},
			wantResources: []string{},
			wantErr:       true,
			errType:       fmt.Errorf("at least 1 resource is required"),
		},
		{
			name: "user with multiple roles",
			principal: &models.Principal{
				Username: "multi-role-user",
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2", "Test3"),
			setupPolicies: func(m *manager) error {
				if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("role1"), authorization.CollectionsMetadata("Test1")[0], authorization.READ, authorization.SchemaDomain); err != nil {
					return err
				}
				if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("role2"), authorization.CollectionsMetadata("Test2")[0], authorization.READ, authorization.SchemaDomain); err != nil {
					return err
				}
				if ok, err := m.casbin.AddRoleForUser(conv.PrefixUserName("multi-role-user"), conv.PrefixRoleName("role1")); err != nil {
					return err
				} else if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				if ok, err := m.casbin.AddRoleForUser(conv.PrefixUserName("multi-role-user"), conv.PrefixRoleName("role2")); err != nil {
					return err
				} else if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
			wantResources: authorization.CollectionsMetadata("Test1", "Test2"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			// Setup policies if needed
			if tt.setupPolicies != nil {
				err := tt.setupPolicies(m)
				require.NoError(t, err)
			}

			// Execute
			got, err := m.FilterAuthorizedResources(tt.principal, tt.verb, tt.resources...)

			// Assert
			if tt.wantErr {
				require.Error(t, err)
				if tt.errType != nil {
					assert.Contains(t, err.Error(), tt.errType.Error())
				}
				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantResources, got)
		})
	}
}

// Additional test for logging behavior
func TestFilterAuthorizedResourcesLogging(t *testing.T) {
	logger, hook := test.NewNullLogger()
	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	principal := &models.Principal{
		Username: "test-user",
		Groups:   []string{"group1"},
	}

	// Setup a policy
	_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", "*", authorization.RolesDomain)
	require.NoError(t, err)
	_, err = m.casbin.AddRoleForUser(conv.PrefixUserName("test-user"), conv.PrefixRoleName("admin"))
	require.NoError(t, err)

	_, err = m.FilterAuthorizedResources(principal, authorization.READ, authorization.CollectionsMetadata("Test")...)
	require.NoError(t, err)

	// Verify logging
	require.NotEmpty(t, hook.AllEntries())
	lastEntry := hook.LastEntry()
	require.NotNil(t, lastEntry)

	// Verify log fields
	assert.Equal(t, "authorize", lastEntry.Data["action"])
	assert.Equal(t, principal.Username, lastEntry.Data["user"])
	assert.Equal(t, principal.Groups, lastEntry.Data["groups"])
	assert.Equal(t, authorization.ComponentName, lastEntry.Data["component"])
	assert.Equal(t, authorization.READ, lastEntry.Data["request_action"])

}

func setupTestManager(t *testing.T, logger *logrus.Logger) (*manager, error) {
	tmpDir, err := os.MkdirTemp("", "rbac-test-*")
	if err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	rbacDir := filepath.Join(tmpDir, "rbac")
	if err := os.MkdirAll(rbacDir, 0o755); err != nil {
		return nil, err
	}

	policyPath := filepath.Join(rbacDir, "policy.csv")

	config := rbacconf.Config{
		Enabled: true,
	}

	return New(policyPath, config, logger)
}
