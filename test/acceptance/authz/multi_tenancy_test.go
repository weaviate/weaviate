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
	existingRole := "admin"

	testRoleName := "test-role"
	testAction1 := authorization.CreateObjectsTenant
	testAction2 := authorization.UpdateSchema

	clientAuth := helper.CreateAuth(existingKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithRBAC().WithRbacUser(existingUser, existingKey, existingRole).Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	cls := articles.ParagraphsClass()
	tenant := "tenant"

	t.Run("setup", func(*testing.T) {
		cls.MultiTenancyConfig = &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
			AutoTenantCreation:   false,
		}
		helper.CreateClassWithAuthz(t, cls, clientAuth)
		helper.CreateTenantsWithAuthz(t, cls.Class, []*models.Tenant{{Name: tenant, ActivityStatus: "INACTIVE"}}, clientAuth)
	})

	t.Run("create role that can create objects in tenant of collection", func(t *testing.T) {
		helper.CreateRole(t, existingKey, &models.Role{
			Name: String(testRoleName),
			Permissions: []*models.Permission{{
				Action:     String(testAction1),
				Collection: String(cls.Class),
				Tenant:     String(tenant),
			}},
		})
	})

	t.Run("fail with 403 when trying to create an object in an inactive tenant due to lacking permissions for autoTenantActivation", func(t *testing.T) {
		obj := articles.NewParagraph().WithID("00000000-0000-0000-0000-000000000001").WithTenant(tenant).Object()
		err := helper.CreateObjectWithAuthz(t, obj, clientAuth)
		require.NotNil(t, err)
		parsed, forbidden := err.(*objects.ObjectsCreateForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error, "forbidden")
	})

	t.Run("add permission allowing to update schema of collection and tenant", func(t *testing.T) {
		_, err := helper.Client(t).Authz.AddPermissions(authz.NewAddPermissionsParams().WithBody(authz.AddPermissionsBody{
			Name: String(testRoleName),
			Permissions: []*models.Permission{{
				Action:     String(testAction2),
				Collection: String(cls.Class),
				Tenant:     String(tenant),
			}},
		}), clientAuth)
		require.Nil(t, err)
	})

	t.Run("successfully create object in tenant after adding permission for autoTenantActivation", func(t *testing.T) {
		obj := articles.NewParagraph().WithID("00000000-0000-0000-0000-000000000001").WithTenant(tenant).Object()
		err := helper.CreateObjectWithAuthz(t, obj, clientAuth)
		require.Nil(t, err)

		// check if object was created
		res, err := helper.Client(t).Objects.ObjectsGet(objects.NewObjectsGetParams().WithID(obj.ID), clientAuth)
		require.Nil(t, err)
		require.NotNil(t, res.Payload)
		require.Equal(t, obj.ID, res.Payload.ID)
	})
}
