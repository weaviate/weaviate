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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/test/helper"
)

func TestCreateUser(t *testing.T) {
	adminKey := "admin-key"

	helper.SetupClient("127.0.0.1:8081")
	// defer helper.ResetClient()

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
}
