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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAutoTenantActivation(t *testing.T) {
	existingUser := "existing-user"
	existingKey := "existing-key"

	customUser := "custom-user"
	customKey := "custom-key"

	testRoleName := "test-role"

	adminAuth := helper.CreateAuth(existingKey)
	customAuth := helper.CreateAuth(customKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithRBAC().WithApiKey().WithUserApiKey(existingUser, existingKey).WithUserApiKey(customUser, customKey).WithRbacAdmins(existingUser).Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	cls := articles.ParagraphsClass()
	tenant := "tenant"
	obj := articles.NewParagraph().WithID("00000000-0000-0000-0000-000000000001").WithTenant(tenant).Object()

	defer func() {
		helper.DeleteClassWithAuthz(t, cls.Class, adminAuth)
		helper.DeleteRole(t, existingKey, testRoleName)
		helper.ResetClient()
	}()

	t.Run("setup", func(*testing.T) {
		cls.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
			AutoTenantCreation:   false,
		}
		helper.CreateClassAuth(t, cls, existingKey)
		helper.CreateTenantsAuth(t, cls.Class, []*models.Tenant{{Name: tenant, ActivityStatus: models.TenantActivityStatusCOLD}}, existingKey)
	})

	t.Run("create and assign role that can create objects in and read schema of tenant of collection", func(t *testing.T) {
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(testRoleName),
			Permissions: []*models.Permission{
				{
					Action:     String(authorization.CreateSchema),
					Collection: String(cls.Class),
				},
				{
					Action:     String(authorization.ReadSchema),
					Collection: String(cls.Class),
				},
			},
		})
		_, err := helper.Client(t).Authz.AssignRole(authz.NewAssignRoleParams().WithID(customKey).WithBody(authz.AssignRoleBody{Roles: []string{testRoleName}}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("fail with 403 when trying to create an object in an inactive tenant due to lacking permissions for autoTenantActivation", func(t *testing.T) {
		err := helper.CreateObjectWithAuthz(t, obj, customAuth)
		require.NotNil(t, err)
		parsed, forbidden := err.(*objects.ObjectsCreateForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add permission allowing to update schema of collection and tenant", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name: String(testRoleName),
			Permissions: []*models.Permission{{
				Action:     String(authorization.UpdateSchema),
				Collection: String(cls.Class),
				Tenant:     String(tenant),
			}},
		}), adminAuth)
		require.Nil(t, err)
	})

	t.Run("successfully create object in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		err := helper.CreateObjectWithAuthz(t, obj, customAuth)
		require.Nil(t, err)
	})
}
