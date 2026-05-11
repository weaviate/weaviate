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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzErrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestAuthorize(t *testing.T) {
	tests := []struct {
		name          string
		principal     *models.Principal
		verb          string
		resources     []string
		skipAudit     bool
		setupPolicies func(*Manager) error
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
				UserType: models.UserTypeInputDb,
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *Manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", authorization.SchemaDomain, authorization.READ)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("admin-user", authentication.AuthTypeDb),
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
				UserType: models.UserTypeInputDb,
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *Manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("partial"), authorization.CollectionsMetadata("Test1")[0], authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("partial-user", authentication.AuthTypeDb),
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
			setupPolicies: func(m *Manager) error {
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
				UserType: models.UserTypeInputDb,
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1"),
			skipAudit: true,
			setupPolicies: func(m *Manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("audit-role"), authorization.CollectionsMetadata("Test1")[0], authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("audit-test-user", authentication.AuthTypeDb),
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
		{
			name: "namespaced principal allowed in own namespace",
			principal: &models.Principal{
				Username:  "ns-user",
				UserType:  models.UserTypeInputDb,
				Namespace: "customer1",
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("customer1:Movies"),
			setupPolicies: func(m *Manager) error {
				if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("ns-role"),
					authorization.CollectionsMetadata("Movies")[0], authorization.READ, authorization.SchemaDomain); err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("ns-user", authentication.AuthTypeDb),
					conv.PrefixRoleName("ns-role"))
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
			name: "namespaced principal denied for matching name in another namespace",
			principal: &models.Principal{
				Username:  "ns-user-2",
				UserType:  models.UserTypeInputDb,
				Namespace: "customer1",
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("customer2:Movies"),
			setupPolicies: func(m *Manager) error {
				if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("ns-role-2"),
					authorization.CollectionsMetadata("Movies")[0], authorization.READ, authorization.SchemaDomain); err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("ns-user-2", authentication.AuthTypeDb),
					conv.PrefixRoleName("ns-role-2"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
			wantErr:     true,
			errContains: "forbidden",
		},
		{
			name: "namespaced group inherits role and is scoped to its namespace",
			principal: &models.Principal{
				Username:  "ns-group-user",
				Groups:    []string{"customer1-group"},
				UserType:  models.UserTypeInputDb,
				Namespace: "customer1",
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("customer1:Movies"),
			setupPolicies: func(m *Manager) error {
				if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("ns-group-role"),
					authorization.CollectionsMetadata("Movies")[0], authorization.READ, authorization.SchemaDomain); err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.PrefixGroupName("customer1-group"),
					conv.PrefixRoleName("ns-group-role"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for group")
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
			err = m.authorize(context.Background(), tt.principal, tt.verb, tt.skipAudit, tt.resources...)

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
		setupPolicies func(*Manager) error
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
				UserType: models.UserTypeInputDb,
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *Manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"),
					"*", authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("admin-user", authentication.AuthTypeDb),
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
				UserType: models.UserTypeInputDb,
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *Manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("limited"),
					authorization.CollectionsMetadata("Test1")[0], authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("limited-user", authentication.AuthTypeDb),
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
				UserType: models.UserTypeInputDb,
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2"),
			setupPolicies: func(m *Manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("collections-admin"),
					authorization.CollectionsMetadata()[0], authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("collections-admin", authentication.AuthTypeDb),
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
				UserType: models.UserTypeInputDb,
			},
			verb:      authorization.READ,
			resources: authorization.CollectionsMetadata("Test1", "Test2", "Test3"),
			setupPolicies: func(m *Manager) error {
				if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("role1"), authorization.CollectionsMetadata("Test1")[0], authorization.READ, authorization.SchemaDomain); err != nil {
					return err
				}
				if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("role2"), authorization.CollectionsMetadata("Test2")[0], authorization.READ, authorization.SchemaDomain); err != nil {
					return err
				}
				if ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("multi-role-user", authentication.AuthTypeDb), conv.PrefixRoleName("role1")); err != nil {
					return err
				} else if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				if ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("multi-role-user", authentication.AuthTypeDb), conv.PrefixRoleName("role2")); err != nil {
					return err
				} else if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
			wantResources: authorization.CollectionsMetadata("Test1", "Test2"),
		},
		{
			name: "namespaced principal filters by both namespace and collection scope",
			principal: &models.Principal{
				Username:  "ns-filter-user",
				UserType:  models.UserTypeInputDb,
				Namespace: "customer1",
			},
			verb: authorization.READ,
			resources: append(append(append(
				authorization.CollectionsMetadata("customer1:Movies1"),
				authorization.CollectionsMetadata("customer1:Movies2")...),
				authorization.CollectionsMetadata("customer1:Films")...),
				authorization.CollectionsMetadata("customer2:Movies1")...),
			setupPolicies: func(m *Manager) error {
				if _, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("ns-filter-role"),
					authorization.CollectionsMetadata("Movies.*")[0], authorization.READ, authorization.SchemaDomain); err != nil {
					return err
				}
				ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("ns-filter-user", authentication.AuthTypeDb),
					conv.PrefixRoleName("ns-filter-role"))
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to add role for user")
				}
				return nil
			},
			wantResources: authorization.CollectionsMetadata("customer1:Movies1", "customer1:Movies2"),
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
			got, err := m.FilterAuthorizedResources(context.Background(), tt.principal, tt.verb, tt.resources...)

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

func TestFilterAuthorizedResourcesLogging(t *testing.T) {
	logger, hook := test.NewNullLogger()
	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	principal := &models.Principal{
		Username: "test-user",
		Groups:   []string{"group1"},
		UserType: models.UserTypeInputDb,
	}

	testResources := authorization.CollectionsMetadata("Test1", "Test2")

	// Setup a policy
	_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", "*", authorization.RolesDomain)
	require.NoError(t, err)
	_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("admin"))
	require.NoError(t, err)

	// Call the function
	allowedResources, err := m.FilterAuthorizedResources(context.Background(), principal, authorization.READ, testResources...)
	require.NoError(t, err)

	// Verify logging
	require.NotEmpty(t, hook.AllEntries())
	entry := hook.LastEntry()
	require.NotNil(t, entry)

	// Check the permissions array exists and has the correct structure
	permissions, ok := entry.Data["permissions"].([]logrus.Fields)
	require.True(t, ok, "permissions should be []logrus.Fields")
	require.NotEmpty(t, permissions, "permissions should not be empty")

	// Check that we have entries for both resources
	require.Len(t, permissions, 2, "Should have permissions entries for both resources")

	// Check the first permission entry
	firstPerm := permissions[0]
	assert.Contains(t, firstPerm, "resource", "First permission entry should contain resource field")
	assert.Contains(t, firstPerm, "results", "First permission entry should contain results field")
	assert.Equal(t, "[Domain: collections, Collection: Test1]", firstPerm["resource"])
	assert.Equal(t, "success", firstPerm["results"])

	// Check the second permission entry
	secondPerm := permissions[1]
	assert.Contains(t, secondPerm, "resource", "Second permission entry should contain resource field")
	assert.Contains(t, secondPerm, "results", "Second permission entry should contain results field")
	assert.Equal(t, "[Domain: collections, Collection: Test2]", secondPerm["resource"])
	assert.Equal(t, "success", secondPerm["results"])

	// Check other required fields
	assert.Equal(t, "authorize", entry.Data["action"])
	assert.Equal(t, principal.Username, entry.Data["user"])
	assert.Equal(t, principal.Groups, entry.Data["groups"])
	assert.Equal(t, authorization.ComponentName, entry.Data["component"])
	assert.Equal(t, authorization.READ, entry.Data["request_action"])
	assert.Equal(t, AuditLogVersion, entry.Data["rbac_log_version"])
	assert.Contains(t, entry.Data, "source_ip", "source_ip must be present when IpInAuditDisabled=false")

	// Verify the final result matches the logged permissions
	assert.ElementsMatch(t, testResources, allowedResources,
		"Allowed resources should match input resources")
}

func TestAuthorizeResourceAggregation(t *testing.T) {
	// Setup proper logger with hook for testing
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	// Create a hook to capture log entries
	hook := &test.Hook{}
	logger.AddHook(hook)

	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	// Setup admin policy
	_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", authorization.READ, authorization.DataDomain)
	require.NoError(t, err)
	ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("admin-user", authentication.AuthTypeDb),
		conv.PrefixRoleName("admin"))
	require.NoError(t, err)
	require.True(t, ok)

	principal := &models.Principal{
		Username: "admin-user",
		Groups:   []string{"admin-group"},
		UserType: models.UserTypeInputDb,
	}

	// Test with 1000 duplicate resources (simulating the original issue)
	resources := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		resources[i] = "data/collections/ContactRecommendations/shards/*/objects/*"
	}

	// Execute authorization
	err = m.authorize(context.Background(), principal, authorization.READ, false, resources...)
	require.NoError(t, err)

	// Verify logging behavior
	require.NotEmpty(t, hook.AllEntries())
	lastEntry := hook.LastEntry()
	require.NotNil(t, lastEntry)

	// Verify log fields
	assert.Equal(t, "authorize", lastEntry.Data["action"])
	assert.Equal(t, "admin-user", lastEntry.Data["user"])
	assert.Equal(t, authorization.ComponentName, lastEntry.Data["component"])
	assert.Equal(t, authorization.READ, lastEntry.Data["request_action"])
	assert.Equal(t, AuditLogVersion, lastEntry.Data["rbac_log_version"])
	assert.Contains(t, lastEntry.Data, "source_ip", "source_ip must be present when IpInAuditDisabled=false")

	// Verify permissions field exists
	permissions, ok := lastEntry.Data["permissions"].([]logrus.Fields)
	require.True(t, ok, "permissions field should be present")

	// Verify aggregation - should only have 1 entry instead of 1000
	assert.Len(t, permissions, 1, "should aggregate 1000 duplicate resources into 1 entry")

	// Verify the single entry has the correct resource and count
	require.Len(t, permissions, 1)
	perm := permissions[0]

	resource, ok := perm["resource"].(string)
	require.True(t, ok, "resource should be a string")
	assert.Equal(t, "[Domain: data, Collection: ContactRecommendations, Tenant: *, Object: *]", resource)

	// Verify aggregation by checking that we have fewer log entries than resources
	// This proves that 1000 identical resources were aggregated into 1 log entry
	assert.Len(t, permissions, 1, "should aggregate 1000 duplicate resources into 1 log entry")

	results, ok := perm["results"].(string)
	require.True(t, ok, "results should be a string")
	assert.Equal(t, "success", results)
}

func TestFilterAuthorizedResourcesAggregation(t *testing.T) {
	// Setup proper logger with hook for testing
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	// Create a hook to capture log entries
	hook := &test.Hook{}
	logger.AddHook(hook)

	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	// Setup admin policy
	_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", authorization.READ, authorization.DataDomain)
	require.NoError(t, err)
	ok, err := m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("admin-user", authentication.AuthTypeDb),
		conv.PrefixRoleName("admin"))
	require.NoError(t, err)
	require.True(t, ok)

	principal := &models.Principal{
		Username: "admin-user",
		Groups:   []string{"admin-group"},
		UserType: models.UserTypeInputDb,
	}

	// Test with 1000 duplicate resources (simulating the original issue)
	resources := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		resources[i] = "data/collections/ContactRecommendations/shards/*/objects/*"
	}

	// Execute FilterAuthorizedResources
	allowedResources, err := m.FilterAuthorizedResources(context.Background(), principal, authorization.READ, resources...)
	require.NoError(t, err)

	// Verify logging behavior
	require.NotEmpty(t, hook.AllEntries())
	lastEntry := hook.LastEntry()
	require.NotNil(t, lastEntry)

	// Verify log fields
	assert.Equal(t, "authorize", lastEntry.Data["action"])
	assert.Equal(t, "admin-user", lastEntry.Data["user"])
	assert.Equal(t, authorization.ComponentName, lastEntry.Data["component"])
	assert.Equal(t, authorization.READ, lastEntry.Data["request_action"])
	assert.Equal(t, AuditLogVersion, lastEntry.Data["rbac_log_version"])
	assert.Contains(t, lastEntry.Data, "source_ip", "source_ip must be present when IpInAuditDisabled=false")

	// Verify permissions field exists
	permissions, ok := lastEntry.Data["permissions"].([]logrus.Fields)
	require.True(t, ok, "permissions field should be present")

	// Verify aggregation - should only have 1 entry instead of 1000
	assert.Len(t, permissions, 1, "should aggregate 1000 duplicate resources into 1 entry")

	// Verify the single entry has the correct resource and count
	require.Len(t, permissions, 1)
	perm := permissions[0]

	resource, ok := perm["resource"].(string)
	require.True(t, ok, "resource should be a string")
	assert.Equal(t, "[Domain: data, Collection: ContactRecommendations, Tenant: *, Object: *]", resource)

	results, ok := perm["results"].(string)
	require.True(t, ok, "results should be a string")
	assert.Equal(t, "success", results)

	// Verify that all 1000 resources are returned in allowedResources
	assert.Len(t, allowedResources, 1, "should return 1 unique resource (duplicates are aggregated)")

	// Verify the returned resource is correct
	assert.Equal(t, "data/collections/ContactRecommendations/shards/*/objects/*", allowedResources[0], "returned resource should be the same as input")
}

// TestAudit_NamespaceFields verifies the top-level audit fields emitted by
// the success paths of Authorize and FilterAuthorizedResources: NS-disabled
// clusters never emit namespace/global_operator; NS-enabled clusters emit
// exactly one of them depending on the principal shape. It also confirms
// the permissions[].resource string strips own-namespace prefixes for
// namespace-bound principals and keeps them raw for global operators.
func TestAudit_NamespaceFields(t *testing.T) {
	type principalCase struct {
		name            string
		nsEnabled       bool
		principal       *models.Principal
		resources       []string
		wantField       map[string]any
		wantAbsent      []string
		wantPrettyMatch string
	}

	nsDisabledPrincipal := &models.Principal{Username: "alice", UserType: models.UserTypeInputDb}
	nsNamespacedPrincipal := &models.Principal{Username: "customer1:alice", Namespace: "customer1", UserType: models.UserTypeInputDb}
	nsGlobalPrincipal := &models.Principal{Username: "admin-key", IsGlobalOperator: true, UserType: models.UserTypeInputDb}

	principalCases := []principalCase{
		{
			name:      "ns_disabled_no_new_fields",
			nsEnabled: false,
			principal: nsDisabledPrincipal,
			resources: authorization.ShardsData("Movies", "#"),
			wantField: map[string]any{
				"rbac_log_version": AuditLogVersion,
				"user":             "alice",
				"action":           "authorize",
			},
			wantAbsent:      []string{"namespace", "global_operator"},
			wantPrettyMatch: "Collection: Movies",
		},
		{
			name:      "ns_enabled_namespaced_caller",
			nsEnabled: true,
			principal: nsNamespacedPrincipal,
			resources: authorization.ShardsData("customer1:Movies", "#"),
			wantField: map[string]any{
				"namespace":        "customer1",
				"rbac_log_version": AuditLogVersion,
				"user":             "customer1:alice",
				"action":           "authorize",
			},
			wantAbsent:      []string{"global_operator"},
			wantPrettyMatch: "Collection: Movies",
		},
		{
			name:      "ns_enabled_global_operator",
			nsEnabled: true,
			principal: nsGlobalPrincipal,
			resources: authorization.ShardsData("customer1:Movies", "#"),
			wantField: map[string]any{
				"global_operator":  true,
				"rbac_log_version": AuditLogVersion,
				"user":             "admin-key",
				"action":           "authorize",
			},
			wantAbsent:      []string{"namespace"},
			wantPrettyMatch: "Collection: customer1:Movies",
		},
	}

	callSites := []struct {
		name          string
		wantAuditMode string
		run           func(m *Manager, p *models.Principal, rs []string) error
	}{
		{"Authorize", "authorize", func(m *Manager, p *models.Principal, rs []string) error {
			return m.Authorize(context.Background(), p, authorization.READ, rs...)
		}},
		{"FilterAuthorizedResources", "filter", func(m *Manager, p *models.Principal, rs []string) error {
			_, err := m.FilterAuthorizedResources(context.Background(), p, authorization.READ, rs...)
			return err
		}},
	}

	for _, pc := range principalCases {
		for _, cs := range callSites {
			t.Run(pc.name+"/"+cs.name, func(t *testing.T) {
				logger, hook := test.NewNullLogger()
				var (
					m   *Manager
					err error
				)
				if pc.nsEnabled {
					m, err = setupNSEnabledTestManager(t, logger)
				} else {
					m, err = setupTestManager(t, logger)
				}
				require.NoError(t, err)

				// Grant a permissive READ in the data domain to the principal so
				// the success path runs and emits one info-level entry.
				_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("audit-role"),
					"*", authorization.READ, authorization.DataDomain)
				require.NoError(t, err)
				_, err = m.casbin.AddRoleForUser(
					conv.UserNameWithTypeFromId(pc.principal.Username, authentication.AuthTypeDb),
					conv.PrefixRoleName("audit-role"))
				require.NoError(t, err)

				require.NoError(t, cs.run(m, pc.principal, pc.resources))

				entry := hook.LastEntry()
				require.NotNil(t, entry)
				for k, v := range pc.wantField {
					require.Equal(t, v, entry.Data[k], "field %q", k)
				}
				for _, k := range pc.wantAbsent {
					_, present := entry.Data[k]
					require.False(t, present, "field %q must be absent", k)
				}
				require.Equal(t, cs.wantAuditMode, entry.Data["audit_mode"])
				perms, ok := entry.Data["permissions"].([]logrus.Fields)
				require.True(t, ok, "permissions should be []logrus.Fields")
				require.NotEmpty(t, perms)
				require.Contains(t, perms[0]["resource"].(string), pc.wantPrettyMatch)
			})
		}
	}
}

// TestAudit_NamespaceFields_DenyPath ensures the deny-side log entry on an
// NS-enabled cluster carries the top-level namespace field, matching the
// success-path shape so SIEM consumers see consistent fields across allow
// and deny.
func TestAudit_NamespaceFields_DenyPath(t *testing.T) {
	logger, hook := test.NewNullLogger()
	m, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)

	p := &models.Principal{Username: "customer1:alice", Namespace: "customer1", UserType: models.UserTypeInputDb}
	err = m.Authorize(context.Background(), p, authorization.READ,
		authorization.ShardsData("customer1:Movies", "#")...)
	require.Error(t, err)

	entry := hook.LastEntry()
	require.NotNil(t, entry)
	require.Equal(t, logrus.ErrorLevel, entry.Level)
	require.Equal(t, "customer1", entry.Data["namespace"])
	require.Equal(t, AuditLogVersion, entry.Data["rbac_log_version"])
	require.Equal(t, "authorize", entry.Data["audit_mode"])
	_, hasGlobal := entry.Data["global_operator"]
	require.False(t, hasGlobal)
}

func setupTestManager(t *testing.T, logger *logrus.Logger) (*Manager, error) {
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

	conf := rbacconf.Config{
		Enabled: true,
	}

	return New(policyPath, conf, config.Authentication{OIDC: config.OIDC{Enabled: true}, APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"test-user"}}}, false, logger)
}

// setupNSEnabledTestManager is setupTestManager with namespacesEnabled=true:
// admin/viewer get the narrowed shape, root/read-only stay wildcard.
func setupNSEnabledTestManager(t *testing.T, logger *logrus.Logger) (*Manager, error) {
	tmpDir, err := os.MkdirTemp("", "rbac-test-ns-*")
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	rbacDir := filepath.Join(tmpDir, "rbac")
	if err := os.MkdirAll(rbacDir, 0o755); err != nil {
		return nil, err
	}
	policyPath := filepath.Join(rbacDir, "policy.csv")

	return New(policyPath, rbacconf.Config{Enabled: true},
		config.Authentication{OIDC: config.OIDC{Enabled: true}, APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"test-user"}}},
		true, logger)
}

// TestNarrowedViewerVsReadOnly_ClusterReadDenied asserts enforcement of
// the narrowed viewer on NS-enabled clusters: a viewer-assigned principal
// is denied on cluster-only read endpoints, while a read-only principal
// is allowed.
func TestNarrowedViewerVsReadOnly_ClusterReadDenied(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)

	// viewerUser inherits the narrowed viewer; readOnlyUser inherits
	// read-only (grouping added directly to bypass the env-var bootstrap).
	const (
		viewerSubject   = "viewer-user"
		readOnlySubject = "read-only-user"
	)
	viewerKey := conv.UserNameWithTypeFromId(viewerSubject, authentication.AuthTypeDb)
	readOnlyKey := conv.UserNameWithTypeFromId(readOnlySubject, authentication.AuthTypeDb)

	_, err = m.casbin.AddRoleForUser(viewerKey, conv.PrefixRoleName(authorization.Viewer))
	require.NoError(t, err)
	_, err = m.casbin.AddRoleForUser(readOnlyKey, conv.PrefixRoleName(authorization.ReadOnly))
	require.NoError(t, err)

	viewer := &models.Principal{Username: viewerSubject, UserType: models.UserTypeInputDb}
	readOnly := &models.Principal{Username: readOnlySubject, UserType: models.UserTypeInputDb}

	// Cluster-only read surfaces — none of these are in the narrowed viewer
	// allowlist (collections/data/tenants/aliases). read-only keeps wildcard
	// READ via its operator policy.
	cases := []struct {
		name     string
		resource string
	}{
		{"GET /nodes (minimal)", authorization.Nodes("minimal")[0]},
		{"GET /nodes/<class> (verbose)", authorization.Nodes("verbose", "Movies")[0]},
		{"backups list (per-class)", authorization.Backups("Movies")[0]},
		{"cluster status", authorization.Cluster()},
	}

	for _, tc := range cases {
		t.Run(tc.name+" — viewer denied", func(t *testing.T) {
			err := m.Authorize(context.Background(), viewer, authorization.READ, tc.resource)
			require.Error(t, err, "narrowed viewer must not have READ on %s", tc.resource)
			assert.ErrorAs(t, err, new(authzErrors.Forbidden))
		})
		t.Run(tc.name+" — read-only allowed", func(t *testing.T) {
			err := m.Authorize(context.Background(), readOnly, authorization.READ, tc.resource)
			assert.NoError(t, err, "env-var-only read-only must keep cluster-wide READ on %s", tc.resource)
		})
	}
}
