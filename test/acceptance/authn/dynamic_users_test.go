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

package authn

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/test/docker"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/test/helper"
)

func TestCreateUser(t *testing.T) {
	adminKey := "admin-key"
	adminUser := "admin-user"

	otherUser := "custom-user"
	otherKey := "custom-key"

	otherUser2 := "custom-user2"
	otherKey2 := "custom-key2"

	otherUser3 := "custom-user3"
	otherKey3 := "custom-key3"

	otherUser4 := "custom-user4"
	otherKey4 := "custom-key4"

	otherUser5 := "custom-user5"
	otherKey5 := "custom-key5"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	compose, err := docker.New().WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(otherUser, otherKey).WithUserApiKey(otherUser2, otherKey2).WithUserApiKey(otherUser3, otherKey3).WithUserApiKey(otherUser4, otherKey4).WithUserApiKey(otherUser5, otherKey5).
		WithDbUsers().
		WithRBAC().WithRbacRoots(adminUser).
		Start(ctx)
	require.Nil(t, err)
	helper.SetupClient(compose.GetWeaviate().URI())
	defer func() {
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()
	userName := "CreateUserTestUser"

	t.Run("qualified name on NS-disabled cluster rejects", func(t *testing.T) {
		_, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID("ns1:"+userName).WithBody(users.CreateUserBody{}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var unproc *users.CreateUserUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected CreateUserUnprocessableEntity, got %T: %v", err, err)
	})

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
		user := helper.GetUser(t, userName, adminKey)
		require.Equal(t, user.APIKeyFirstLetters, oldKey[:3])

		// rotate key and test that old key is not working anymore
		newKey := helper.RotateKey(t, userName, adminKey)
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(oldKey))
		require.Error(t, err)

		infoNew := helper.GetInfoForOwnUser(t, newKey)
		require.Equal(t, userName, *infoNew.Username)

		user = helper.GetUser(t, userName, adminKey)
		require.Equal(t, user.APIKeyFirstLetters, newKey[:3])
		require.NotEqual(t, newKey, oldKey)
		require.NotEqual(t, newKey[:10], oldKey[:10])

		helper.DeleteUser(t, userName, adminKey)
	})

	t.Run("import static user and rotate key", func(t *testing.T) {
		allUsers := helper.ListAllUsers(t, adminKey)
		found := false
		for _, user := range allUsers {
			if *user.UserID == otherUser {
				require.Equal(t, *user.DbUserType, string(models.UserTypeOutputDbEnvUser))
				found = true
				break
			}
		}
		require.True(t, found)

		timeBeforeImport := time.Now()
		time.Sleep(time.Millisecond * 2) // make sure that times are actually less, as we lose ns precision during serialization
		oldKey := helper.CreateUserWithApiKey(t, otherUser, adminKey, nil)
		require.Equal(t, oldKey, otherKey)
		time.Sleep(time.Millisecond * 2)
		timeAfterImport := time.Now()

		info := helper.GetInfoForOwnUser(t, oldKey)
		require.Equal(t, otherUser, *info.Username)
		user := helper.GetUser(t, otherUser, adminKey)
		require.Equal(t, user.APIKeyFirstLetters, oldKey[:3])
		require.Equal(t, *user.DbUserType, string(models.UserTypeOutputDbUser))

		// rotate key and test that old key is not working anymore
		newKey := helper.RotateKey(t, otherUser, adminKey)
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(oldKey))
		require.Error(t, err)

		infoNew := helper.GetInfoForOwnUser(t, newKey)
		require.Equal(t, otherUser, *infoNew.Username)

		user = helper.GetUser(t, otherUser, adminKey)
		require.Equal(t, user.APIKeyFirstLetters, newKey[:3])
		require.NotEqual(t, newKey, oldKey)
		require.NotEqual(t, newKey[:10], oldKey[:10])
		require.Less(t, timeBeforeImport.UTC(), time.Time(user.CreatedAt).UTC())
		require.Less(t, time.Time(user.CreatedAt).UTC(), timeAfterImport.UTC())

		helper.DeleteUser(t, otherUser, adminKey)
	})

	t.Run("import static user with time", func(t *testing.T) {
		createTime := time.Now().Add(-time.Hour)
		helper.CreateUserWithApiKey(t, otherUser2, adminKey, &createTime)

		user := helper.GetUser(t, otherUser2, adminKey)
		require.Equal(t, time.Time(user.CreatedAt).UTC().Truncate(time.Millisecond), createTime.UTC().Truncate(time.Millisecond))
	})

	t.Run("import static user and delete", func(t *testing.T) {
		key := helper.CreateUserWithApiKey(t, otherUser3, adminKey, nil)

		info := helper.GetInfoForOwnUser(t, key)
		require.Equal(t, otherUser3, *info.Username)

		helper.DeleteUser(t, otherUser3, adminKey)

		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(otherKey3))
		require.Error(t, err)
		var parsed *users.GetOwnInfoUnauthorized
		require.True(t, errors.As(err, &parsed))
		require.Equal(t, 401, parsed.Code())
	})

	t.Run("import static user and suspend with rotate", func(t *testing.T) {
		key := helper.CreateUserWithApiKey(t, otherUser4, adminKey, nil)

		info := helper.GetInfoForOwnUser(t, key)
		require.Equal(t, otherUser4, *info.Username)

		helper.DeactivateUser(t, adminKey, otherUser4, true)

		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(otherKey4))
		require.Error(t, err)
		var parsed *users.GetOwnInfoUnauthorized
		require.True(t, errors.As(err, &parsed))
		require.Equal(t, 401, parsed.Code())

		helper.ActivateUser(t, adminKey, otherUser4)
		_, err = helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(otherKey4))
		require.Error(t, err)
		require.True(t, errors.As(err, &parsed))
		require.Equal(t, 401, parsed.Code())

		newKey := helper.RotateKey(t, otherUser4, adminKey)
		helper.GetInfoForOwnUser(t, newKey)
	})

	t.Run("import static user and suspend without rotate", func(t *testing.T) {
		key := helper.CreateUserWithApiKey(t, otherUser5, adminKey, nil)

		info := helper.GetInfoForOwnUser(t, key)
		require.Equal(t, otherUser5, *info.Username)

		helper.DeactivateUser(t, adminKey, otherUser5, false)

		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(otherKey5))
		require.Error(t, err)
		var parsed *users.GetOwnInfoUnauthorized
		require.True(t, errors.As(err, &parsed))
		require.Equal(t, 401, parsed.Code())

		helper.ActivateUser(t, adminKey, otherUser5)
		helper.GetInfoForOwnUser(t, key)
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

func TestCreateUser_Namespaces(t *testing.T) {
	const (
		adminKey  = "admin-key"
		adminUser = "admin-user"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	// NS clusters require RBAC; adminUser is the root that creates namespaces and
	// namespaced DB users.
	compose, err := docker.New().WithWeaviate().
		WithApiKey().WithRBAC().WithUserApiKey(adminUser, adminKey).WithRbacRoots(adminUser).
		WithDbUsers().
		WithNamespaces().
		Start(ctx)
	require.NoError(t, err)
	helper.SetupClient(compose.GetWeaviate().URI())
	defer func() {
		helper.ResetClient()
		require.NoError(t, compose.Terminate(ctx))
		cancel()
	}()

	helper.CreateNamespace(t, "ns1", adminKey)
	helper.CreateNamespace(t, "ns2", adminKey)
	defer helper.DeleteNamespace(t, "ns1", adminKey)
	defer helper.DeleteNamespace(t, "ns2", adminKey)

	t.Run("happy path: create user bound to ns1", func(t *testing.T) {
		const (
			userID  = "u1"
			fullKey = "ns1:u1" // operator-facing form for namespaced users
		)
		helper.DeleteUser(t, fullKey, adminKey)

		resp, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID(fullKey).WithBody(users.CreateUserBody{}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
		require.NotNil(t, resp.Payload.Apikey)

		// admin is a static API key (global operator) — namespace must be returned.
		// The storage/operator-addressable key is the namespace-prefixed form.
		got := helper.GetUser(t, fullKey, adminKey)
		require.Equal(t, "ns1", got.Namespace)

		// The bare short id must not address a namespaced user. This locks
		// the prefix-only addressing contract: an operator who omits the
		// namespace gets 404, not the user from a different namespace (or
		// any user at all).
		_, err = helper.Client(t).Users.GetUserInfo(
			users.NewGetUserInfoParams().WithUserID(userID),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var notFound *users.GetUserInfoNotFound
		require.True(t, errors.As(err, &notFound), "expected GetUserInfoNotFound, got %T: %v", err, err)

		helper.DeleteUser(t, fullKey, adminKey)
	})

	t.Run("missing namespace rejects", func(t *testing.T) {
		_, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID("u-missing"),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var unproc *users.CreateUserUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected CreateUserUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("empty namespace prefix rejected", func(t *testing.T) {
		_, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID(":u-empty-ns").WithBody(users.CreateUserBody{}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var unproc *users.CreateUserUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected CreateUserUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("empty user part rejected", func(t *testing.T) {
		_, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID("ns1:").WithBody(users.CreateUserBody{}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var unproc *users.CreateUserUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected CreateUserUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("multi-colon name rejected", func(t *testing.T) {
		// First ':' is the ns separator; "user:extra" then fails the user-name regex.
		_, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID("ns1:user:extra").WithBody(users.CreateUserBody{}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var unproc *users.CreateUserUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected CreateUserUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("unknown namespace rejects", func(t *testing.T) {
		_, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID("ns404:u-unknown").WithBody(users.CreateUserBody{}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var unproc *users.CreateUserUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected CreateUserUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("import on NS-enabled cluster rejects", func(t *testing.T) {
		imp := true
		_, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID("ns1:"+adminUser).WithBody(users.CreateUserBody{Import: &imp}),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var unproc *users.CreateUserUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected CreateUserUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("namespaced caller is confined to its own namespace", func(t *testing.T) {
		// A namespaced caller's short-name create lands in its own namespace —
		// u1 (ns1) cannot create into ns2.
		const callerKey = "ns1:u1" // operator-facing form
		helper.DeleteUser(t, callerKey, adminKey)
		createResp, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID(callerKey).WithBody(users.CreateUserBody{}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
		require.NotNil(t, createResp.Payload.Apikey)
		callerApiKey := *createResp.Payload.Apikey
		t.Cleanup(func() { helper.DeleteUser(t, callerKey, adminKey) })

		_, err = helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID("u2").WithBody(users.CreateUserBody{}),
			helper.CreateAuth(callerApiKey),
		)
		require.NoError(t, err)
		t.Cleanup(func() { helper.DeleteUser(t, "ns1:u2", adminKey) })

		got := helper.GetUser(t, "ns1:u2", adminKey)
		require.Equal(t, "ns1", got.Namespace)
		_, err = helper.Client(t).Users.GetUserInfo(
			users.NewGetUserInfoParams().WithUserID("ns2:u2"),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var notFound *users.GetUserInfoNotFound
		require.True(t, errors.As(err, &notFound), "expected ns2:u2 to not exist, got %T: %v", err, err)
	})
}
