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
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestAuthzRolesForUsers(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	usernameWithoutAssignedRole := "existing-username-without-role"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithRBAC().
		WithRbacUser(adminUser, adminKey, "admin").
		WithRbacUser(usernameWithoutAssignedRole, "some-key", "some-role").
		Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("get empty roles for existing user without role", func(t *testing.T) {
		roles := helper.GetRolesForUser(t, usernameWithoutAssignedRole, adminKey)
		require.Equal(t, 0, len(roles))
	})

	t.Run("get roles for non existing user", func(t *testing.T) {
		_, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID("notExists"), helper.CreateAuth(adminKey))
		require.NotNil(t, err)
		targetErr, ok := err.(*authz.GetRolesForUserInternalServerError)
		require.True(t, ok)

		require.Equal(t, 500, targetErr.Code())
		require.Equal(t, "user notExists doesn't exist", targetErr.Payload.Error[0].Message)
	})
}
