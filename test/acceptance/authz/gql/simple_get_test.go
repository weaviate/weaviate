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

package gql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	gql "github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZGraphQLGetST(t *testing.T) {
	adminUser := "existing-user"
	adminKey := "existing-key"
	adminRole := "admin"

	customUser := "custom-user"
	customKey := "custom-key"
	customRole := "custom"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithRBAC().
		WithRbacUser(adminUser, adminKey, adminRole).
		WithRbacUser(customUser, customKey, customRole).
		WithText2VecContextionary().
		Start(ctx)

	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	class := books.ClassContextionaryVectorizer()
	readBooksRole := "read-books"

	t.Run("create books class", func(t *testing.T) {
		helper.CreateClassAuth(t, class, adminKey)
	})

	t.Run("import books objects", func(t *testing.T) {
		objects := books.Objects()
		helper.CreateObjectsBatchAuth(t, objects, adminKey)
	})

	t.Run("create and assign a role that can only read objects in books class", func(t *testing.T) {
		role := &models.Role{
			Name: String(readBooksRole),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadObjectsCollection),
				Collection: String(class.Class),
			}},
		}
		helper.CreateRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, *role.Name, customUser)
	})

	t.Run("fail with 403 to query with GQL due to lack of read all collections permission", func(t *testing.T) {
		query := "{ Get { Books { title } } }"
		_, err := queryGQL(t, query, customKey)
		require.NotNil(t, err)
		_, forbidden := err.(*gql.GraphqlPostForbidden)
		require.True(t, forbidden)
	})

	t.Run("add the read all collections permission to the role", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name: String(readBooksRole),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadCollections),
				Collection: String("*"),
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("successfully query with GQL with the sufficient permissions", func(t *testing.T) {
		assertGQL(t, "{ Get { Books { title } } }", customKey)
	})

	t.Run("remove the read objects in book class permission", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RemovePermissions(authz.NewRemovePermissionsParams().WithBody(authz.RemovePermissionsBody{
			Name: String(readBooksRole),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadObjectsCollection),
				Collection: String(class.Class),
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("fail with 200 to query with GQL due to lack of read objects permission", func(t *testing.T) {
		query := "{ Get { Books { title } } }"
		resp, err := queryGQL(t, query, customKey)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload.Errors)
		require.Len(t, resp.Payload.Errors, 1)
		require.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
	})
}

func TestAuthZGraphQLGetMT(t *testing.T) {
	adminUser := "existing-user"
	adminKey := "existing-key"
	adminRole := "admin"

	customUser := "custom-user"
	customKey := "custom-key"
	customRole := "custom"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithRBAC().
		WithRbacUser(adminUser, adminKey, adminRole).
		WithRbacUser(customUser, customKey, customRole).
		WithText2VecContextionary().
		Start(ctx)

	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	class := books.ClassContextionaryVectorizer()
	readBooksRole := "read-books"

	t.Run("create books class", func(t *testing.T) {
		class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true, AutoTenantCreation: true}
		helper.CreateClassAuth(t, class, adminKey)
	})

	t.Run("import books objects", func(t *testing.T) {
		objects := books.Objects()
		for i := range objects {
			objects[i].Tenant = customUser
		}
		helper.CreateObjectsBatchAuth(t, objects, adminKey)
	})

	t.Run("create and assign a role that can only read objects in books class and customUser tenant", func(t *testing.T) {
		role := &models.Role{
			Name: String(readBooksRole),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadObjectsTenant),
				Collection: String(class.Class),
				Tenant:     String(customUser),
			}},
		}
		helper.CreateRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, *role.Name, customUser)
	})

	t.Run("fail with 403 to query with GQL due to lack of read all collections permission", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { %s(tenant:"%s") { title } } }`, class.Class, customUser)
		_, err := queryGQL(t, query, customKey)
		require.NotNil(t, err)
		_, forbidden := err.(*gql.GraphqlPostForbidden)
		require.True(t, forbidden)
	})

	t.Run("add the read all collections permission to the role", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name: String(readBooksRole),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadCollections),
				Collection: String("*"),
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("successfully query with GQL with the sufficient permissions", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { %s(tenant:"%s") { title } } }`, class.Class, customUser)
		assertGQL(t, query, customKey)
	})

	t.Run("remove the read objects in books class and customUser tenant permission", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RemovePermissions(authz.NewRemovePermissionsParams().WithBody(authz.RemovePermissionsBody{
			Name: String(readBooksRole),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadObjectsTenant),
				Collection: String(class.Class),
				Tenant:     String(customUser),
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("add the read objects in books class and non-existent tenant permission", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name: String(readBooksRole),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadObjectsTenant),
				Collection: String(class.Class),
				Tenant:     String("non-existent-tenant"),
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("fail with 200 to query with GQL due to lack of read objects and customUser tenant permission", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { %s(tenant:"%s") { title } } }`, class.Class, customUser)
		resp, err := queryGQL(t, query, customKey)
		require.Nil(t, err)
		require.NotNil(t, resp.Payload.Errors)
		require.Len(t, resp.Payload.Errors, 1)
		require.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
	})

	t.Run("remove the read objects in books class and non-existent tenant permission", func(t *testing.T) {
		_, err := helper.Client(t).Authz.RemovePermissions(authz.NewRemovePermissionsParams().WithBody(authz.RemovePermissionsBody{
			Name: String(readBooksRole),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadObjectsTenant),
				Collection: String(class.Class),
				Tenant:     String("non-existent-tenant"),
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("add the read objects in books class permission", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name: String(readBooksRole),
			Permissions: []*models.Permission{{
				Action:     String(authorization.ReadObjectsTenant),
				Collection: String(class.Class),
			}},
		}), helper.CreateAuth(adminKey))
		require.Nil(t, err)
	})

	t.Run("successfully query with GQL with the sufficient permissions", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { %s(tenant:"%s") { title } } }`, class.Class, customUser)
		assertGQL(t, query, customKey)
	})
}
