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

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gql "github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// composeUp sets up proper RBAC test environment
func composeUp(t *testing.T, admins map[string]string, users map[string]string, viewers map[string]string) (*docker.DockerCompose, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	builder := docker.New().WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").WithWeaviateWithGRPC().WithRBAC().WithApiKey()
	adminUserNames := make([]string, 0, len(admins))
	viewerUserNames := make([]string, 0, len(viewers))
	for userName, key := range admins {
		builder = builder.WithUserApiKey(userName, key)
		adminUserNames = append(adminUserNames, userName)
	}
	for userName, key := range viewers {
		builder = builder.WithUserApiKey(userName, key)
		viewerUserNames = append(viewerUserNames, userName)
	}
	if len(admins) > 0 {
		builder = builder.WithRbacRoots(adminUserNames...)
	}
	if len(viewers) > 0 {
		builder = builder.WithRbacViewers(viewerUserNames...)
	}
	for userName, key := range users {
		builder = builder.WithUserApiKey(userName, key)
	}
	compose, err := builder.Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())

	return compose, func() {
		helper.ResetClient()
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
		cancel()
	}
}

func queryGraphQL(t *testing.T, key, query string) *models.GraphQLResponse {
	params := gql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{Query: query})
	resp, err := helper.Client(t).Graphql.GraphqlPost(params, helper.CreateAuth(key))
	require.NoError(t, err)
	return resp.Payload
}

func Test_RBAC_AliasPermissions(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"
	restrictedKey := "restricted-key"
	restrictedUser := "restricted-user"
	collectionReaderKey := "collection-reader-key"
	collectionReaderUser := "collection-reader-user"

	compose, down := composeUp(t, map[string]string{adminUser: adminKey},
		map[string]string{collectionReaderUser: collectionReaderKey},
		map[string]string{restrictedUser: restrictedKey})
	defer down()

	weaviateURL := compose.GetWeaviate().URI()

	className := "TestCollection"
	aliasName := "TestAlias"

	t.Run("setup: create class and alias as admin", func(t *testing.T) {
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
		helper.CreateClassAuth(t, class, adminKey)

		// Create alias pointing to the class
		alias := &models.Alias{
			Class: className,
			Alias: aliasName,
		}
		helper.CreateAliasAuth(t, alias, adminKey)
	})

	defer func() {
		// Cleanup as admin - ignore errors since resources might not exist
		helper.DeleteAliasWithAuthz(t, aliasName, helper.CreateAuth(adminKey))
		helper.DeleteClassAuth(t, className, adminKey)
	}()

	t.Run("setup: assign built-in viewer role to restricted user", func(t *testing.T) {
		// Use the built-in "viewer" role which has read permissions for schema endpoints
		// This is the same role used in the existing rbac_viewer_test.go
		helper.AssignRoleToUser(t, adminKey, "viewer", restrictedUser)
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

	t.Run("restricted user can access class via alias", func(t *testing.T) {
		resp := helper.GetClassAuth(t, aliasName, restrictedKey)
		require.NotNil(t, resp, "User with permission on resolved class should be able to access via alias")
		assert.Equal(t, className, resp.Class, "Should return the resolved class, not the alias")
	})

	t.Run("restricted user can access shards via alias", func(t *testing.T) {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/v1/schema/%s/shards", weaviateURL, aliasName), nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+restrictedKey)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Should succeed because authorization happens on resolved class name
		assert.Equal(t, http.StatusOK, resp.StatusCode, "User with permission on resolved class should be able to access shards via alias")
	})

	t.Run("user without permissions should be denied access", func(t *testing.T) {
		noPermKey := "no-perm-key"

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

	t.Run("read_data on the collection allows GraphQL where through the alias", func(t *testing.T) {
		require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
			Class:      className,
			Properties: map[string]interface{}{"title": "Dune"},
		}, adminKey))

		// /graphql itself requires read_collections on every collection
		helper.CreateRoleAndAssign(t, adminKey, collectionReaderUser, collectionReaderUser+"-role",
			helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("*").Permission(),
			helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection(className).Permission())

		where := `(where:{path:["title"] operator:Equal valueText:"Dune"})`
		resp := queryGraphQL(t, collectionReaderKey, fmt.Sprintf(`{ Get { %s%s { title } } }`, aliasName, where))
		require.Empty(t, resp.Errors)
		assert.Len(t, resp.Data["Get"].(map[string]interface{})[aliasName], 1)

		resp = queryGraphQL(t, collectionReaderKey, fmt.Sprintf(`{ Aggregate { %s%s { meta { count } } } }`, aliasName, where))
		require.Empty(t, resp.Errors)
		groups := resp.Data["Aggregate"].(map[string]interface{})[aliasName].([]interface{})
		require.Len(t, groups, 1)
		assert.Equal(t, json.Number("1"), groups[0].(map[string]interface{})["meta"].(map[string]interface{})["count"])
	})
}
