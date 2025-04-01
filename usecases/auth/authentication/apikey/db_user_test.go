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
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"

	"github.com/stretchr/testify/require"
)

func TestDynUserConcurrency(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir())
	require.NoError(t, err)

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
	dynUsers, err := NewDBUser(t.TempDir())
	require.NoError(t, err)
	userId := "id"

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash("")
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId, hash, identifier))

	randomKey, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)

	_, ok := dynUsers.memoryOnyData.WeakKeyStorageById[userId]
	require.False(t, ok)

	startSlow := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKey, identifier)
	require.NoError(t, err)
	tookSlow := time.Since(startSlow)

	_, ok = dynUsers.memoryOnyData.WeakKeyStorageById[userId]
	require.True(t, ok)

	startFast := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKey, identifier)
	require.NoError(t, err)
	tookFast := time.Since(startFast)
	require.Less(t, tookFast, tookSlow)
}

func TestUpdateUser(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir())
	require.NoError(t, err)
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

func TestSnapShotAndRestore(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir())
	require.NoError(t, err)

	userId1 := "id-1"
	userId2 := "id-2"

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash("")
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId1, hash, identifier))
	login1, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)

	apiKey2, hash2, identifier2, err := keys.CreateApiKeyAndHash("")
	require.NoError(t, err)
	require.NoError(t, dynUsers.CreateUser(userId2, hash2, identifier2))
	login2, _, err := keys.DecodeApiKey(apiKey2)
	require.NoError(t, err)

	// first login is slow, second is fast
	startSlow := time.Now()
	principal, err := dynUsers.ValidateAndExtract(login1, identifier)
	require.NoError(t, err)
	require.NotNil(t, principal)
	tookSlow := time.Since(startSlow)

	startFast := time.Now()
	_, err = dynUsers.ValidateAndExtract(login1, identifier)
	require.NoError(t, err)
	tookFast := time.Since(startFast)
	require.Less(t, tookFast, tookSlow)

	principal2, err := dynUsers.ValidateAndExtract(login2, identifier2)
	require.NoError(t, err)
	require.NotNil(t, principal2)

	require.NoError(t, dynUsers.DeactivateUser(userId2, true))

	snapShot := dynUsers.Snapshot()

	// create snapshot and restore to an empty new DBUser struct
	marshal, err := json.Marshal(snapShot)
	require.NoError(t, err)

	snapshotRestore := DBUserSnapshot{}
	require.NoError(t, json.Unmarshal(marshal, &snapshotRestore))

	dynUsers2, err := NewDBUser(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, dynUsers2.Restore(snapshotRestore))

	// content should be identical:
	// - all users and their status present
	// - taking a new snapshot should be identical
	// - only weak hash is missing => first login should be slow again
	require.Equal(t, snapshotRestore, dynUsers2.Snapshot())

	startAfterRestoreSlow := time.Now()
	_, err = dynUsers2.ValidateAndExtract(login1, identifier)
	require.NoError(t, err)
	tookAfterRestore := time.Since(startAfterRestoreSlow)
	require.Less(t, tookFast, tookAfterRestore)

	_, err = dynUsers2.ValidateAndExtract(login2, identifier2)
	require.Error(t, err)

	apiKey3, hash3, identifier3, err := keys.CreateApiKeyAndHash("")
	require.NoError(t, err)
	require.NoError(t, dynUsers2.RotateKey(userId2, hash3))

	login3, _, err := keys.DecodeApiKey(apiKey3)
	require.NoError(t, err)
	_, err = dynUsers2.ValidateAndExtract(login3, identifier3)
	require.Error(t, err)
}
