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

package authn

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/weaviate/weaviate/test/docker"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/test/helper"
)

func TestCreateUser(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	compose, err := docker.New().WithWeaviate().WithApiKey().WithUserApiKey(adminUser, adminKey).WithDbUsers().
		Start(ctx)
	require.Nil(t, err)
	helper.SetupClient(compose.GetWeaviate().URI())

	defer func() {
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()

	userName := "CreateUserTestUser"

	t.Run("create and delete user", func(t *testing.T) {
		helper.DeleteUser(t, userName, adminKey)
		resp, err := helper.Client(t).Users.CreateUser(users.NewCreateUserParams().WithUserID(userName), helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotEmpty(t, resp)
		require.Greater(t, len(*resp.Payload.Apikey), 10)

		info := helper.GetInfoForOwnUser(t, *resp.Payload.Apikey)
		require.Equal(t, userName, *info.Username)
		require.Len(t, info.Roles, 0)
		require.Len(t, info.Groups, 0)

		respDelete, err := helper.Client(t).Users.DeleteUser(users.NewDeleteUserParams().WithUserID(userName), helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, respDelete)
		var parsedDelete *users.DeleteUserNoContent
		require.True(t, errors.As(respDelete, &parsedDelete))
		require.Equal(t, 204, respDelete.Code())

		_, err = helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth("non-existent"))
		require.NotNil(t, err)
		var parsed *users.GetOwnInfoUnauthorized
		require.True(t, errors.As(err, &parsed))
		require.Equal(t, 401, parsed.Code())
	})

	t.Run("create and rotate key", func(t *testing.T) {
		helper.DeleteUser(t, userName, adminKey)
		oldKey := helper.CreateUser(t, userName, adminKey)

		// login works after user creation
		info := helper.GetInfoForOwnUser(t, oldKey)
		require.Equal(t, userName, *info.Username)

		// rotate key and test that old key is not working anymore
		newKey := helper.RotateKey(t, userName, adminKey)
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(oldKey))
		require.Error(t, err)

		infoNew := helper.GetInfoForOwnUser(t, newKey)
		require.Equal(t, userName, *infoNew.Username)

		helper.DeleteUser(t, userName, adminKey)
	})
}

func TestWithStaticUser(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"

	otherKey := "custom-key"
	otherUser := "custom-user"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	compose, err := docker.New().WithWeaviate().WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(otherUser, otherKey).WithDbUsers().Start(ctx)
	require.Nil(t, err)
	helper.SetupClient(compose.GetWeaviate().URI())

	defer func() {
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()

	t.Run("create with existing static user name", func(t *testing.T) {
		resp, err := helper.Client(t).Users.CreateUser(users.NewCreateUserParams().WithUserID(otherUser), helper.CreateAuth(adminKey))
		require.Error(t, err)
		require.Nil(t, resp)
		var parsed *users.CreateUserConflict
		require.True(t, errors.As(err, &parsed))
	})

	t.Run("delete existing static user name", func(t *testing.T) {
		resp, err := helper.Client(t).Users.DeleteUser(users.NewDeleteUserParams().WithUserID(otherUser), helper.CreateAuth(adminKey))
		require.Error(t, err)
		require.Nil(t, resp)
		var parsed *users.DeleteUserUnprocessableEntity
		require.True(t, errors.As(err, &parsed))
	})

	t.Run("rotate existing static user name", func(t *testing.T) {
		resp, err := helper.Client(t).Users.RotateUserAPIKey(users.NewRotateUserAPIKeyParams().WithUserID(otherUser), helper.CreateAuth(adminKey))
		require.Error(t, err)
		require.Nil(t, resp)
		var parsed *users.RotateUserAPIKeyUnprocessableEntity
		require.True(t, errors.As(err, &parsed))
	})
}

func TestSuspendAndActivate(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	compose, err := docker.New().WithWeaviate().WithApiKey().WithUserApiKey(adminUser, adminKey).WithDbUsers().Start(ctx)
	require.Nil(t, err)
	helper.SetupClient(compose.GetWeaviate().URI())

	defer func() {
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()
	helper.SetupClient(compose.GetWeaviate().URI())

	dynamicUser := "dynamic-user"

	t.Run("suspend and activate without revocation", func(t *testing.T) {
		helper.DeleteUser(t, dynamicUser, adminKey)
		apiKey := helper.CreateUser(t, dynamicUser, adminKey)

		info := helper.GetInfoForOwnUser(t, apiKey)
		require.NotNil(t, info)

		helper.DeactivateUser(t, adminKey, dynamicUser, false)
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(apiKey))
		require.Error(t, err)

		helper.ActivateUser(t, adminKey, dynamicUser)
		infoActive := helper.GetInfoForOwnUser(t, apiKey)
		require.NotNil(t, infoActive)
	})

	t.Run("suspend and activate with revocation", func(t *testing.T) {
		helper.DeleteUser(t, dynamicUser, adminKey)
		apiKey := helper.CreateUser(t, dynamicUser, adminKey)

		info := helper.GetInfoForOwnUser(t, apiKey)
		require.NotNil(t, info)

		helper.DeactivateUser(t, adminKey, dynamicUser, true)
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(apiKey))
		require.Error(t, err)

		helper.ActivateUser(t, adminKey, dynamicUser)
		_, err = helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(apiKey))
		require.Error(t, err)

		// need to rotate key for user to work again
		apiKey = helper.RotateKey(t, dynamicUser, adminKey)
		require.NotNil(t, helper.GetInfoForOwnUser(t, apiKey))
	})

	t.Run("suspend and activate with revocation - first rotate then activate", func(t *testing.T) {
		helper.DeleteUser(t, dynamicUser, adminKey)
		apiKey := helper.CreateUser(t, dynamicUser, adminKey)

		info := helper.GetInfoForOwnUser(t, apiKey)
		require.NotNil(t, info)

		helper.DeactivateUser(t, adminKey, dynamicUser, true)
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(apiKey))
		require.Error(t, err)

		apiKey = helper.RotateKey(t, dynamicUser, adminKey)

		helper.ActivateUser(t, adminKey, dynamicUser)
		require.NotNil(t, helper.GetInfoForOwnUser(t, apiKey))
	})

	t.Run("suspend and delete with deactivate key", func(t *testing.T) {
		for _, deactivateKey := range []bool{true, false} {
			helper.DeleteUser(t, dynamicUser, adminKey)
			apiKey := helper.CreateUser(t, dynamicUser, adminKey)

			info := helper.GetInfoForOwnUser(t, apiKey)
			require.NotNil(t, info)

			helper.DeactivateUser(t, adminKey, dynamicUser, deactivateKey)
			helper.DeleteUser(t, dynamicUser, adminKey)

			// create new user with same name, should not be suspended anymore
			apiKey = helper.CreateUser(t, dynamicUser, adminKey)
			require.NotNil(t, helper.GetInfoForOwnUser(t, apiKey))
		}
	})

	t.Run("double suspend", func(t *testing.T) {
		helper.DeleteUser(t, dynamicUser, adminKey)
		helper.CreateUser(t, dynamicUser, adminKey)
		helper.DeactivateUser(t, adminKey, dynamicUser, false)
		// suspend again
		_, err := helper.Client(t).Users.DeactivateUser(users.NewDeactivateUserParams().WithUserID(dynamicUser), helper.CreateAuth(adminKey))
		require.Error(t, err)
		var conflict *users.DeactivateUserConflict
		require.True(t, errors.As(err, &conflict))
	})

	t.Run("activate active user", func(t *testing.T) {
		helper.DeleteUser(t, dynamicUser, adminKey)
		helper.CreateUser(t, dynamicUser, adminKey)
		_, err := helper.Client(t).Users.ActivateUser(users.NewActivateUserParams().WithUserID(dynamicUser), helper.CreateAuth(adminKey))
		require.Error(t, err)
		var conflict *users.ActivateUserConflict
		require.True(t, errors.As(err, &conflict))
	})
}
