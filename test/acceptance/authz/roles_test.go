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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestAuthzRoles(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithRBAC().WithRbacUser("testing-user", "testing-key", "admin").Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("get all roles before create", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRoles(authz.NewGetRolesParams(), helper.CreateAuth("testing-key"))
		require.Nil(t, err)
		require.Equal(t, 1, len(res.Payload))
	})

	t.Run("create role", func(t *testing.T) {
		_, err = helper.Client(t).Authz.CreateRole(
			authz.NewCreateRoleParams().WithBody(&models.Role{
				Name: makeStrPtr("test-role"),
				Permissions: []*models.Permission{{
					Action: makeStrPtr("create_collections"),
				}},
			}),
			helper.CreateAuth("testing-key"),
		)
		require.Nil(t, err)
	})

	t.Run("get all roles after create", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRoles(authz.NewGetRolesParams(), helper.CreateAuth("testing-key"))
		require.Nil(t, err)
		require.Equal(t, 2, len(res.Payload))
	})

	t.Run("get role by name", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID("test-role"), helper.CreateAuth("testing-key"))
		require.Nil(t, err)
		require.Equal(t, "test-role", *res.Payload.Name)
		require.Equal(t, 1, len(res.Payload.Permissions))
		require.Equal(t, "create_collections", *res.Payload.Permissions[0].Action)
	})

	t.Run("delete role by name", func(t *testing.T) {
		_, err = helper.Client(t).Authz.DeleteRole(authz.NewDeleteRoleParams().WithID("test-role"), helper.CreateAuth("testing-key"))
		require.Nil(t, err)
	})

	t.Run("get all roles after delete", func(t *testing.T) {
		res, err := helper.Client(t).Authz.GetRoles(authz.NewGetRolesParams(), helper.CreateAuth("testing-key"))
		require.Nil(t, err)
		require.Equal(t, 1, len(res.Payload))
	})

	t.Run("get non-existent role by name", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetRole(authz.NewGetRoleParams().WithID("test-role"), helper.CreateAuth("testing-key"))
		require.NotNil(t, err)
	})
}

func makeStrPtr(s string) *string {
	return &s
}
