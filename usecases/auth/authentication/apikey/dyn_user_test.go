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

package apikey

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"

	"github.com/stretchr/testify/require"
)

func TestDynUserConcurrency(t *testing.T) {
	dynUsers := NewDynamicApiKey()

	numUsers := 10

	wg := sync.WaitGroup{}
	wg.Add(numUsers)

	userNames := make([]string, 0, numUsers)
	for i := 0; i < numUsers; i++ {
		userName := fmt.Sprintf("user%v", i)
		go func() {
			err := dynUsers.CreateUser(userName, "something", userName)
			require.NoError(t, err)
			wg.Done()
		}()
		userNames = append(userNames, userName)
	}
	wg.Wait()

	users, err := dynUsers.GetUsers(userNames...)
	require.NoError(t, err)
	require.Equal(t, len(userNames), len(users))
}

func TestDynUserTestSlowAfterWeakHash(t *testing.T) {
	dynUsers := NewDynamicApiKey()
	userId := "id"

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash("")
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId, hash, identifier))

	randomKey, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)

	_, ok := dynUsers.weakKeyStorageById[userId]
	require.False(t, ok)

	startSlow := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKey, identifier)
	require.NoError(t, err)
	tookSlow := time.Since(startSlow)

	_, ok = dynUsers.weakKeyStorageById[userId]
	require.True(t, ok)

	startFast := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKey, identifier)
	require.NoError(t, err)
	tookFast := time.Since(startFast)
	require.Less(t, tookFast, tookSlow)
}

func TestUpdateUser(t *testing.T) {
	dynUsers := NewDynamicApiKey()
	userId := "id"

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash("")
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId, hash, identifier))

	// login works
	randomKeyOld, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)

	principal, err := dynUsers.ValidateAndExtract(randomKeyOld, identifier)
	require.NoError(t, err)
	require.NotNil(t, principal)

	// update key and check that original key does not work, but new one does
	apiKeyNew, hashNew, identifier, err := keys.CreateApiKeyAndHash(identifier)
	require.NoError(t, err)
	require.NoError(t, dynUsers.RotateKey(userId, hashNew))

	randomKeyNew, _, err := keys.DecodeApiKey(apiKeyNew)
	require.NoError(t, err)

	principal, err = dynUsers.ValidateAndExtract(randomKeyOld, identifier)
	require.Error(t, err)
	require.Nil(t, principal)

	// first login with new key is slow again, second is fast
	startSlow := time.Now()
	principal, err = dynUsers.ValidateAndExtract(randomKeyNew, identifier)
	require.NoError(t, err)
	require.NotNil(t, principal)
	tookSlow := time.Since(startSlow)

	startFast := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKeyNew, identifier)
	require.NoError(t, err)
	tookFast := time.Since(startFast)
	require.Less(t, tookFast, tookSlow)
}
