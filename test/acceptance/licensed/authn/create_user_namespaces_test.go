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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

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

		// Grant the namespaced admin role so the caller has CreateUsers
		// within ns1. Without RBAC the handler-level operator gate is gone,
		// so creates need an actual permission.
		helper.AssignRoleToUser(t, adminKey, authorization.Admin, callerKey)
		helper.WaitForOwnRole(t, callerApiKey, authorization.Admin)

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
