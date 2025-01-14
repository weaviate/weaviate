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

package authz

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestRbacWithOIDC(t *testing.T) {
	tests := []struct {
		name  string
		image *docker.Compose
	}{
		{
			name: "RBAC with OIDC",
			image: docker.New().
				WithWeaviate().WithMockOIDC().WithRBAC().WithRbacAdmins("admin-user"),
		},
		{
			name: "RBAC with OIDC and API key",
			image: docker.New().
				WithWeaviate().WithMockOIDC().WithRBAC().WithRbacAdmins("admin-user").
				WithApiKey().WithUserApiKey("other", "random-key"),
		},
		{
			name: "RBAC with OIDC and API key overlapping user names",
			image: docker.New().
				WithWeaviate().WithMockOIDC().
				WithRBAC().WithRbacAdmins("admin-user").
				WithApiKey().WithUserApiKey("other", "random-key").
				WithApiKey().WithUserApiKey("custom-user", "custom-key"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			compose, err := test.image.Start(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, compose.Terminate(ctx))
			}()

			helper.SetupClient(compose.GetWeaviate().URI())
			defer helper.ResetClient()

			authEndpoint, tokenEndpoint := docker.GetEndpointsFromMockOIDC(compose.GetMockOIDC().URI())

			// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
			// description for details
			tokenAdmin, _ := docker.GetTokensFromMockOIDC(t, authEndpoint, tokenEndpoint)
			tokenCustom, _ := docker.GetTokensFromMockOIDC(t, authEndpoint, tokenEndpoint)

			// prepare roles to assign later
			className := strings.Replace(t.Name(), "/", "", 1) + "Class"
			readSchemaAction := authorization.ReadCollections
			createSchemaAction := authorization.CreateCollections
			createSchemaRoleName := "createSchema"
			createSchemaRole := &models.Role{
				Name: &createSchemaRoleName,
				Permissions: []*models.Permission{
					{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &className}},
					{Action: &createSchemaAction, Collections: &models.PermissionCollections{Collection: &className}},
				},
			}
			helper.DeleteRole(t, tokenAdmin, createSchemaRoleName)
			helper.CreateRole(t, tokenAdmin, createSchemaRole)
			defer helper.DeleteRole(t, tokenAdmin, createSchemaRoleName)

			// custom-user does not have any roles/permissions
			err = createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenCustom))
			require.Error(t, err)
			var forbidden *clschema.SchemaObjectsCreateForbidden
			require.True(t, errors.As(err, &forbidden))

			// assigning to user just works, no matter if user is using OIDC or API keys
			helper.AssignRoleToUser(t, tokenAdmin, createSchemaRoleName, "custom-user")
			err = createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenCustom))
			require.NoError(t, err)

			// assign role to non-existing user => no error (if OIDC is enabled)
			helper.AssignRoleToUser(t, tokenAdmin, createSchemaRoleName, "i-dont-exist")
		})
	}
}
