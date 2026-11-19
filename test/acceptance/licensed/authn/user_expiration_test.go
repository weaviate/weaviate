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
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestUserExpiration covers DB user expiry, which needs a license key. Its last
// subtest restarts the node without one.
func TestUserExpiration(t *testing.T) {
	adminUser, adminKey := "admin-user", "admin-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	compose, err := docker.New().WithWeaviate().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithDbUsers().
		WithRBAC().WithRbacRoots(adminUser).
		WithLicenseKeyFile().
		Start(ctx)
	require.NoError(t, err)
	helper.SetupClient(compose.GetWeaviate().URI())
	defer func() {
		helper.ResetClient()
		// The test waits for a key to expire and restarts the node, which can
		// outlast the ctx Start ran under.
		terminateCtx, terminateCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer terminateCancel()
		require.NoError(t, compose.Terminate(terminateCtx))
	}()

	in24h := helper.ExpiryIn(24 * time.Hour)

	ownInfo := func(t *testing.T, key string) error {
		_, err := helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(key))
		return err
	}

	t.Run("create with expiresAt shows it on GET, list and own-info", func(t *testing.T) {
		const userID = "exp-user"
		helper.DeleteUser(t, userID, adminKey)
		apiKey := helper.CreateUserWithExpiry(t, userID, adminKey, &in24h)

		want := (*strfmt.DateTime)(&in24h)
		require.Equal(t, want, helper.GetUser(t, userID, adminKey).ExpiresAt)
		listed := helper.ListAllUsers(t, adminKey)
		i := slices.IndexFunc(listed, func(u *models.DBUserInfo) bool { return *u.UserID == userID })
		require.NotEqual(t, -1, i, "%s not listed", userID)
		require.Equal(t, want, listed[i].ExpiresAt)
		require.Equal(t, want, helper.GetInfoForOwnUser(t, apiKey).ExpiresAt)
	})

	t.Run("expiry holds after the node restarts without a license key", func(t *testing.T) {
		const userID = "exp-restart"
		helper.DeleteUser(t, userID, adminKey)
		in10s := helper.ExpiryIn(10 * time.Second)
		apiKey := helper.CreateUserWithExpiry(t, userID, adminKey, &in10s)

		var unauthorized *users.GetOwnInfoUnauthorized
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.ErrorAs(c, ownInfo(t, apiKey), &unauthorized)
		}, 30*time.Second, 500*time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		require.NoError(t, compose.SetLicenseKeyFileAt(ctx, 0, ""))
		grace := 10 * time.Second
		require.NoError(t, compose.RestartAt(ctx, 0, &grace))
		helper.SetupClient(compose.GetWeaviate().URI())

		require.ErrorAs(t, ownInfo(t, apiKey), &unauthorized)
		require.Equal(t, (*strfmt.DateTime)(&in10s), helper.GetUser(t, userID, adminKey).ExpiresAt)
		require.Equal(t, (*strfmt.DateTime)(&in24h), helper.GetUser(t, "exp-user", adminKey).ExpiresAt)

		in48h := helper.ExpiryIn(48 * time.Hour)
		_, err := helper.Client(t).Users.SetUserExpiration(
			users.NewSetUserExpirationParams().WithUserID("exp-user").WithBody(users.SetUserExpirationBody{ExpiresAt: (*strfmt.DateTime)(&in48h)}),
			helper.CreateAuth(adminKey),
		)
		var putForbidden *users.SetUserExpirationForbidden
		require.ErrorAs(t, err, &putForbidden)
		require.Contains(t, putForbidden.Payload.Error[0].Message, "license key")

		_, err = helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID("exp-unlicensed").WithBody(users.CreateUserBody{ExpiresAt: (*strfmt.DateTime)(&in48h)}),
			helper.CreateAuth(adminKey),
		)
		var createForbidden *users.CreateUserForbidden
		require.ErrorAs(t, err, &createForbidden)
	})
}
