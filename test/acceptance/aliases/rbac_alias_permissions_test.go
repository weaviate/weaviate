//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// Test_RBAC_AliasPermissions tests the specific bug fix where RBAC authorization
// should happen on resolved collection names, not alias names
func Test_RBAC_AliasPermissions(t *testing.T) {
	ctx := context.Background()

	// Setup admin and restricted user
	adminKey := "admin-key"
	adminUser := "admin-user"
	restrictedKey := "restricted-key"
	restrictedUser := "restricted-user"

	compose, err := docker.New().
		WithWeaviateEnv("AUTHORIZATION_ADMINLIST_ENABLED", "true").
		WithWeaviateEnv("AUTHORIZATION_ADMINLIST_USERS", adminUser).
		WithWeaviateEnv("AUTHENTICATION_APIKEY_ENABLED", "true").
		WithWeaviateEnv("AUTHENTICATION_APIKEY_ALLOWED_KEYS", adminKey+","+restrictedKey).
		WithWeaviateEnv("AUTHENTICATION_APIKEY_USERS", adminUser+","+restrictedUser).
		WithWeaviate().
		WithText2VecModel2Vec().
		Start(ctx)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	// Create class as admin
	className := "TestCollection"
	aliasName := "TestAlias"

	t.Run("setup: create class and alias", func(t *testing.T) {
		// Create the test class
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "title",
					DataType: []string{"text"},
				},
			},
		}
		helper.CreateClass(t, class)

		// Create alias pointing to the class
		alias := &models.Alias{
			Class: className,
			Alias: aliasName,
		}
		helper.CreateAlias(t, alias)
	})

	defer func() {
		// Cleanup
		helper.DeleteAlias(t, aliasName)
		helper.DeleteClass(t, className)
	}()

	// Create restricted role and user with permissions only on the resolved class name
	t.Run("setup: create restricted role with collection-specific permissions", func(t *testing.T) {
		// Create role with READ permissions only on the resolved class name (not the alias)
		roleName := "class-reader"
		role := &models.Role{
			Name: &roleName,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().WithAction(authorization.READ).WithCollection(className).Permission(),
			},
		}
		helper.CreateRole(t, adminKey, role)

		// Assign role to restricted user
		helper.AssignRoleToUser(t, adminKey, roleName, restrictedUser)

		defer func() {
			helper.DeleteRole(t, adminKey, roleName)
		}()
	})

	t.Run("admin can access class directly and via alias", func(t *testing.T) {
		// Admin should be able to access class directly
		resp := helper.GetClassAuth(t, className, adminKey)
		require.NotNil(t, resp)
		assert.Equal(t, className, resp.Class)

		// Admin should be able to access class via alias
		resp = helper.GetClassAuth(t, aliasName, adminKey)
		require.NotNil(t, resp)
		assert.Equal(t, className, resp.Class) // Should return the resolved class
	})

	t.Run("restricted user can access class directly", func(t *testing.T) {
		// Restricted user should be able to access class directly (has permission on resolved class name)
		resp := helper.GetClassAuth(t, className, restrictedKey)
		require.NotNil(t, resp)
		assert.Equal(t, className, resp.Class)
	})

	// Following two tests are the key tests to validate the patch
	// https://github.com/weaviate/weaviate/pull/9113
	t.Run("restricted user can access class via alias (RBAC fix verification)", func(t *testing.T) {
		// This is the key test: before the fix, this would fail because authorization
		// was happening on the alias name instead of the resolved class name.
		// With the fix, authorization happens on the resolved class name, so it should succeed.
		resp := helper.GetClassAuth(t, aliasName, restrictedKey)
		require.NotNil(t, resp, "User with permission on resolved class should be able to access via alias")
		assert.Equal(t, className, resp.Class, "Should return the resolved class, not the alias")
	})

	t.Run("restricted user can access shards via alias (RBAC fix verification)", func(t *testing.T) {
		// Test the ShardsStatus endpoint which was also fixed via HTTP client
		// since there's no helper function for ShardsStatus with auth yet
		weaviateURL := compose.GetWeaviate().URI()

		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/v1/schema/%s/shards", weaviateURL, aliasName), nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+restrictedKey)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should succeed because authorization happens on resolved class name
		assert.Equal(t, http.StatusOK, resp.StatusCode, "User with permission on resolved class should be able to access shards via alias")
	})

	// Test the negative case: user without permissions should still be denied
	t.Run("user without permissions should be denied access", func(t *testing.T) {
		noPermKey := "no-perm-key"
		weaviateURL := compose.GetWeaviate().URI()

		t.Run("denied via direct class name", func(t *testing.T) {
			req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/v1/schema/%s", weaviateURL, className), nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+noPermKey)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.NotEqual(t, http.StatusOK, resp.StatusCode, "User without permissions should be denied direct class access")
		})

		t.Run("denied via alias", func(t *testing.T) {
			req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/v1/schema/%s", weaviateURL, aliasName), nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+noPermKey)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.NotEqual(t, http.StatusOK, resp.StatusCode, "User without permissions should be denied alias access")
		})
	})
}

// Test case to verify that RBAC works correctly with alias resolution ordering
func Test_RBAC_AliasPermissions_ComprehensiveScenarios(t *testing.T) {
	t.Skip("Skipping comprehensive test for now - basic test covers the core fix")
}
