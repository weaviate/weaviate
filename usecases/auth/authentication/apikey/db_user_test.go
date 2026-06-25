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

package apikey

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
)

var log, _ = test.NewNullLogger()

func TestDynUserConcurrency(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)

	numUsers := 10

	wg := sync.WaitGroup{}
	wg.Add(numUsers)

	userNames := make([]string, 0, numUsers)
	for i := 0; i < numUsers; i++ {
		userName := fmt.Sprintf("user%v", i)
		go func() {
			err := dynUsers.CreateUser(userName, "something", userName, "", "", time.Now())
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

// Pins UserView's data fields to User's. Adding a field to User without
// mirroring it in UserView (and view()) would silently drop it from the
// GetUsers snapshot.
func TestUserView_MirrorsUserFields(t *testing.T) {
	collect := func(typ reflect.Type) map[string]string {
		out := make(map[string]string, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.Anonymous || !f.IsExported() {
				continue
			}
			out[f.Name] = f.Type.String()
		}
		return out
	}

	require.Equal(t, collect(reflect.TypeOf(User{})), collect(reflect.TypeOf(UserView{})),
		"User and UserView must expose the same exported data fields; update UserView and (*User).view() when adding fields to User")
}

// Concurrent Activate/Deactivate vs GetUsers field reads must stay race-free under -race.
func TestGetUsers_NoRaceWithMutators(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)

	const numUsers = 5
	userIds := make([]string, 0, numUsers)
	for i := 0; i < numUsers; i++ {
		id := fmt.Sprintf("u%d", i)
		require.NoError(t, dynUsers.CreateUser(id, "h", id, "", "", time.Now()))
		userIds = append(userIds, id)
	}

	const iterations = 200
	done := make(chan struct{})
	wg := sync.WaitGroup{}

	// Mutator: flips Active under c.lock.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			id := userIds[i%numUsers]
			if i%2 == 0 {
				_ = dynUsers.DeactivateUser(id, false)
			} else {
				_ = dynUsers.ActivateUser(id)
			}
		}
		close(done)
	}()

	// Reader: GetUsers + read .Active.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			users, err := dynUsers.GetUsers(userIds...)
			assert.NoError(t, err)
			for _, u := range users {
				_ = u.Active
				_ = u.LastUsedAt
				_ = u.InternalIdentifier
			}
		}
	}()

	wg.Wait()
}

func TestConcurrentValidate(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	userId1 := "id"
	userId2 := "id2"

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId1, hash, identifier, "", "", time.Now()))

	apiKey2, hash2, identifier2, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId2, hash2, identifier2, "", "", time.Now()))

	randomKey, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)
	randomKey2, _, err := keys.DecodeApiKey(apiKey2)
	require.NoError(t, err)
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			_, err := dynUsers.ValidateAndExtract(randomKey, identifier)
			require.NoError(t, err)
			wg.Done()
		}()

		go func() {
			_, err := dynUsers.ValidateAndExtract(randomKey2, identifier2)
			require.NoError(t, err)
			wg.Done()
		}()
	}
	wg.Wait()

	users, err := dynUsers.GetUsers(userId1)
	require.NoError(t, err)
	user := users[userId1]
	require.Less(t, start, user.LastUsedAt)
}

func TestDynUserTestSlowAfterWeakHash(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	userId := "id"

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId, hash, identifier, "", "", time.Now()))

	randomKey, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)

	_, ok := dynUsers.memoryOnlyData.weakKeyStorageById.Load(userId)
	require.False(t, ok)

	startSlow := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKey, identifier)
	require.NoError(t, err)
	tookSlow := time.Since(startSlow)

	_, ok = dynUsers.memoryOnlyData.weakKeyStorageById.Load(userId)
	require.True(t, ok)

	startFast := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKey, identifier)
	require.NoError(t, err)
	tookFast := time.Since(startFast)
	require.Less(t, tookFast, tookSlow)
}

func TestUpdateUser(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	userId := "id"

	apiKey, hash, oldIdentifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId, hash, oldIdentifier, "", "", time.Now()))

	// login works
	randomKeyOld, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)

	principal, err := dynUsers.ValidateAndExtract(randomKeyOld, oldIdentifier)
	require.NoError(t, err)
	require.NotNil(t, principal)

	// update key and check that original key does not work, but new one does
	apiKeyNew, hashNew, newIdentifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, dynUsers.RotateKey(userId, apiKeyNew[:3], hashNew, oldIdentifier, newIdentifier))

	randomKeyNew, _, err := keys.DecodeApiKey(apiKeyNew)
	require.NoError(t, err)

	principal, err = dynUsers.ValidateAndExtract(randomKeyOld, oldIdentifier)
	require.Error(t, err)
	require.Nil(t, principal)

	// first login with new key is slow again, second is fast
	startSlow := time.Now()
	principal, err = dynUsers.ValidateAndExtract(randomKeyNew, newIdentifier)
	require.NoError(t, err)
	require.NotNil(t, principal)
	tookSlow := time.Since(startSlow)

	startFast := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKeyNew, newIdentifier)
	require.NoError(t, err)
	tookFast := time.Since(startFast)
	require.Less(t, tookFast, tookSlow)
}

func TestCheckUserIdentifierExists(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)

	userId := "id"
	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	exists, err := dynUsers.CheckUserIdentifierExists(identifier)
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, dynUsers.CreateUser(userId, hash, identifier, "", "", time.Now()))

	exists, err = dynUsers.CheckUserIdentifierExists(identifier)
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = dynUsers.CheckUserIdentifierExists(userId)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestSnapShotAndRestore(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)

	userId1 := "id-1"
	userId2 := "id-2"

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId1, hash, identifier, "", "", time.Now()))
	login1, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)

	apiKey2, hash2, identifier2, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, dynUsers.CreateUser(userId2, hash2, identifier2, "", "", time.Now()))
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

	// create snapshot and restore to an empty new DBUser struct
	snapShot, err := dynUsers.Snapshot()
	require.NoError(t, err)

	dynUsers2, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	require.NoError(t, dynUsers2.Restore(snapShot, false))

	// content should be identical:
	// - all users and their status present
	// - taking a new snapshot should be identical
	// - only weak hash is missing => first login should be slow again
	snapshot2, err := dynUsers2.Snapshot()
	require.NoError(t, err)
	require.Equal(t, snapShot, snapshot2)

	startAfterRestoreSlow := time.Now()
	_, err = dynUsers2.ValidateAndExtract(login1, identifier)
	require.NoError(t, err)
	tookAfterRestore := time.Since(startAfterRestoreSlow)
	require.Less(t, tookFast, tookAfterRestore)

	_, err = dynUsers2.ValidateAndExtract(login2, identifier2)
	require.Error(t, err)

	apiKey3, hash3, identifier3, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, dynUsers2.RotateKey(userId2, apiKey3[:3], hash3, identifier2, identifier3))

	login3, _, err := keys.DecodeApiKey(apiKey3)
	require.NoError(t, err)
	_, err = dynUsers2.ValidateAndExtract(login3, identifier3)
	require.Error(t, err)
}

func TestSuspendAfterDelete(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	userId := "id"

	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId, hash, identifier, "", "", time.Now()))

	users, err := dynUsers.GetUsers(userId)
	require.NoError(t, err)
	require.Contains(t, users, userId)
	require.Len(t, users, 1)

	require.NoError(t, dynUsers.DeleteUser(userId))

	require.Error(t, dynUsers.DeactivateUser(userId, false))
	require.Error(t, dynUsers.ActivateUser(userId))
	require.Error(t, dynUsers.RotateKey(userId, "", "", "", ""))
	require.Error(t, dynUsers.ActivateUser(userId))
}

func TestLastUsedTime(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	userId := "user"

	start := time.Now()

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId, hash, identifier, "", "", time.Now()))

	user, err := dynUsers.GetUsers(userId)
	require.NoError(t, err)
	require.Less(t, user[userId].LastUsedAt, start) // no usage yet

	login, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)
	_, err = dynUsers.ValidateAndExtract(login, identifier)
	require.NoError(t, err)

	user, err = dynUsers.GetUsers(userId)
	require.NoError(t, err)
	require.Less(t, start, user[userId].LastUsedAt) // was just used
	require.Less(t, user[userId].LastUsedAt, time.Now())
	lastUsedTime := user[userId].LastUsedAt

	// try to update with older timestamp => no effect
	dynUsers.UpdateLastUsedTimestamp(map[string]time.Time{userId: start})
	user, err = dynUsers.GetUsers(userId)
	require.NoError(t, err)

	require.Equal(t, user[userId].LastUsedAt, lastUsedTime)

	// update with newer timestamp (that another node has seen)
	updateTime := time.Now()
	dynUsers.UpdateLastUsedTimestamp(map[string]time.Time{userId: updateTime})
	user, err = dynUsers.GetUsers(userId)
	require.NoError(t, err)

	require.Equal(t, user[userId].LastUsedAt, updateTime)
}

func TestUpdateLastUsedTimestamp_NonExistentUser(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)

	// Should not panic when updating timestamp for a user that doesn't exist
	require.NotPanics(t, func() {
		dynUsers.UpdateLastUsedTimestamp(map[string]time.Time{
			"non-existent-user": time.Now(),
		})
	})
}

func TestImportingAndSuspendingStaticKeys(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)

	createdAt := time.Now()
	userId := "user"
	importedApiKey := "importedApiKey"
	require.NoError(t, dynUsers.CreateUserWithKey(userId, importedApiKey[:3], sha256.Sum256([]byte(importedApiKey)), createdAt))

	principal, err := dynUsers.ValidateImportedKey(importedApiKey)
	require.NoError(t, err)
	require.NotNil(t, principal)
	require.Equal(t, userId, principal.Username)

	require.NoError(t, dynUsers.DeactivateUser(userId, true))

	principal, err = dynUsers.ValidateImportedKey(importedApiKey)
	require.Error(t, err)
	require.Nil(t, principal)

	require.NoError(t, dynUsers.ActivateUser(userId))
	principal, err = dynUsers.ValidateImportedKey(importedApiKey)
	require.Error(t, err)
	require.Nil(t, principal)

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, dynUsers.RotateKey(userId, apiKey[:3], hash, "imported_"+userId, identifier))

	login, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)
	_, err = dynUsers.ValidateAndExtract(login, identifier)
	require.NoError(t, err)

	principal, err = dynUsers.ValidateImportedKey(importedApiKey)
	require.NoError(t, err) // error is only returned if key is deactivated
	require.Nil(t, principal)
}

func TestImportingStaticKeys(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	createdAt := time.Now()
	for i := 0; i < 10; i++ {
		userId := "user" + strconv.Itoa(i)
		importedApiKey := "importedApiKey" + strconv.Itoa(i)
		require.NoError(t, dynUsers.CreateUserWithKey(userId, importedApiKey[:3], sha256.Sum256([]byte(importedApiKey)), createdAt))

		principal, err := dynUsers.ValidateImportedKey(importedApiKey)
		require.NoError(t, err)
		require.NotNil(t, principal)
		require.Equal(t, userId, principal.Username)
		require.Equal(t, principal.UserType, models.UserTypeInputDb)

		require.True(t, dynUsers.IsBlockedKey(importedApiKey))
	}

	for i := 0; i < 10; i++ {
		userId := "user" + strconv.Itoa(i)
		importedApiKey := "importedApiKey" + strconv.Itoa(i)

		users, err := dynUsers.GetUsers(userId)
		require.NoError(t, err)
		require.Len(t, users, 1)
		user, ok := users[userId]
		require.True(t, ok)
		require.Equal(t, user.Id, userId)
		require.Equal(t, user.InternalIdentifier, "imported_"+userId)
		require.Equal(t, user.CreatedAt, createdAt)
		require.True(t, user.ImportedWithKey)

		apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
		require.NoError(t, err)
		require.NoError(t, dynUsers.RotateKey(userId, apiKey[:3], hash, "imported_"+userId, identifier))

		login, _, err := keys.DecodeApiKey(apiKey)
		require.NoError(t, err)
		_, err = dynUsers.ValidateAndExtract(login, identifier)
		require.NoError(t, err)

		users, err = dynUsers.GetUsers(userId)
		require.NoError(t, err)
		require.Len(t, users, 1)
		user, ok = users[userId]
		require.True(t, ok)
		require.Equal(t, user.Id, userId)
		require.Equal(t, user.InternalIdentifier, identifier)
		require.Equal(t, user.CreatedAt, createdAt)
		require.False(t, user.ImportedWithKey)

		require.True(t, dynUsers.IsBlockedKey(importedApiKey))

	}
}

func TestImportingStaticKeysWithTime(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	createdAt := time.Now().Add(-time.Hour)

	importedApiKey := "importedApiKey"
	userId := "user"
	require.NoError(t, dynUsers.CreateUserWithKey(userId, importedApiKey[:3], sha256.Sum256([]byte(importedApiKey)), createdAt))

	users, err := dynUsers.GetUsers(userId)
	require.NoError(t, err)
	require.Len(t, users, 1)
	user, ok := users[userId]
	require.True(t, ok)
	require.Equal(t, user.CreatedAt, createdAt)
}

func TestSnapshotRestoreEmpty(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	userId := "user"

	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId, hash, identifier, "", "", time.Now()))
	user, err := dynUsers.GetUsers(userId)
	require.NoError(t, err)
	require.Equal(t, user[userId].Id, userId)

	err = dynUsers.Restore([]byte{}, false)
	require.NoError(t, err)

	// nothing overwritten
	user, err = dynUsers.GetUsers(userId)
	require.NoError(t, err)
	require.Equal(t, user[userId].Id, userId)
}

func TestRestoreInvalidData(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)

	require.Error(t, dynUsers.Restore([]byte("invalid json"), false))
}

func TestCreateUserStoresNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
	}{
		{name: "with namespace", namespace: "customer1"},
		{name: "empty namespace", namespace: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dynUsers, err := NewDBUser(t.TempDir(), true, log)
			require.NoError(t, err)

			_, hash, identifier, err := keys.CreateApiKeyAndHash()
			require.NoError(t, err)

			require.NoError(t, dynUsers.CreateUser("u1", hash, identifier, "", tc.namespace, time.Now()))

			users, err := dynUsers.GetUsers("u1")
			require.NoError(t, err)
			require.Equal(t, tc.namespace, users["u1"].Namespace)
		})
	}
}

func TestSnapshotRestoreMultipleNamespaces(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)

	type seed struct {
		userId    string
		namespace string
	}
	seeds := []seed{
		{userId: "u1", namespace: "ns1"},
		{userId: "u2", namespace: "ns1"},
		{userId: "u3", namespace: "ns2"},
		{userId: "u4", namespace: ""}, // pre-namespace record
	}
	for _, s := range seeds {
		_, hash, identifier, err := keys.CreateApiKeyAndHash()
		require.NoError(t, err)
		require.NoError(t, dynUsers.CreateUser(s.userId, hash, identifier, "", s.namespace, time.Now()))
	}

	snap, err := dynUsers.Snapshot()
	require.NoError(t, err)

	restored, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	require.NoError(t, restored.Restore(snap, false))

	users, err := restored.GetUsers()
	require.NoError(t, err)
	require.Len(t, users, len(seeds))
	for _, s := range seeds {
		require.Equal(t, s.namespace, users[s.userId].Namespace, "userId=%s", s.userId)
	}
}

func TestValidateAndExtractReturnsNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
	}{
		{name: "with namespace", namespace: "customer1"},
		{name: "empty namespace", namespace: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dynUsers, err := NewDBUser(t.TempDir(), true, log)
			require.NoError(t, err)

			apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
			require.NoError(t, err)
			require.NoError(t, dynUsers.CreateUser("u1", hash, identifier, "", tc.namespace, time.Now()))

			randomKey, _, err := keys.DecodeApiKey(apiKey)
			require.NoError(t, err)
			principal, err := dynUsers.ValidateAndExtract(randomKey, identifier)
			require.NoError(t, err)
			require.NotNil(t, principal)
			require.Equal(t, tc.namespace, principal.Namespace)
			require.False(t, principal.IsGlobalOperator, "dynamic users are never global operators")
		})
	}
}

func TestUsersInNamespace(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)

	seeds := []struct {
		id, namespace string
	}{
		{"u-alpha-1", "alpha"},
		{"u-alpha-2", "alpha"},
		{"u-beta", "beta"},
		{"u-unscoped", ""},
	}
	for _, s := range seeds {
		_, hash, identifier, err := keys.CreateApiKeyAndHash()
		require.NoError(t, err)
		require.NoError(t, dynUsers.CreateUser(s.id, hash, identifier, "", s.namespace, time.Now()))
	}

	require.ElementsMatch(t, []string{"u-alpha-1", "u-alpha-2"}, dynUsers.UsersInNamespace("alpha"))
	require.ElementsMatch(t, []string{"u-beta"}, dynUsers.UsersInNamespace("beta"))
	require.Empty(t, dynUsers.UsersInNamespace("never-existed"))
	require.Nil(t, dynUsers.UsersInNamespace(""))
}

func TestDeleteUsersInNamespace(t *testing.T) {
	t.Run("removes only matching users", func(t *testing.T) {
		dynUsers, err := NewDBUser(t.TempDir(), false, log)
		require.NoError(t, err)

		seeds := []struct {
			id, namespace string
		}{
			{"u-alpha-1", "alpha"},
			{"u-alpha-2", "alpha"},
			{"u-beta", "beta"},
			{"u-unscoped", ""},
		}
		for _, s := range seeds {
			_, hash, identifier, err := keys.CreateApiKeyAndHash()
			require.NoError(t, err)
			require.NoError(t, dynUsers.CreateUser(s.id, hash, identifier, "", s.namespace, time.Now()))
		}

		require.NoError(t, dynUsers.DeleteUsersInNamespace("alpha"))

		users, err := dynUsers.GetUsers()
		require.NoError(t, err)
		require.Len(t, users, 2)
		require.NotContains(t, users, "u-alpha-1")
		require.NotContains(t, users, "u-alpha-2")
		require.Contains(t, users, "u-beta")
		require.Contains(t, users, "u-unscoped")
	})

	t.Run("rerun on empty namespace is a no-op", func(t *testing.T) {
		dynUsers, err := NewDBUser(t.TempDir(), false, log)
		require.NoError(t, err)

		_, hash, identifier, err := keys.CreateApiKeyAndHash()
		require.NoError(t, err)
		require.NoError(t, dynUsers.CreateUser("u1", hash, identifier, "", "alpha", time.Now()))

		require.NoError(t, dynUsers.DeleteUsersInNamespace("alpha"))
		require.NoError(t, dynUsers.DeleteUsersInNamespace("alpha"))
		require.NoError(t, dynUsers.DeleteUsersInNamespace("never-existed"))
	})

	t.Run("frees the user id for re-creation", func(t *testing.T) {
		dynUsers, err := NewDBUser(t.TempDir(), false, log)
		require.NoError(t, err)

		_, hash, identifier, err := keys.CreateApiKeyAndHash()
		require.NoError(t, err)
		require.NoError(t, dynUsers.CreateUser("u1", hash, identifier, "", "alpha", time.Now()))
		require.NoError(t, dynUsers.DeleteUsersInNamespace("alpha"))

		_, hash2, identifier2, err := keys.CreateApiKeyAndHash()
		require.NoError(t, err)
		require.NoError(t, dynUsers.CreateUser("u1", hash2, identifier2, "", "alpha", time.Now()))

		users, err := dynUsers.GetUsers("u1")
		require.NoError(t, err)
		require.NotNil(t, users["u1"])
		require.Equal(t, "alpha", users["u1"].Namespace)
	})

	t.Run("empty namespace argument is rejected", func(t *testing.T) {
		dynUsers, err := NewDBUser(t.TempDir(), false, log)
		require.NoError(t, err)
		require.Error(t, dynUsers.DeleteUsersInNamespace(""))
	})
}

// seedUser creates a dynamic user with a real API key and returns the decoded
// login key + identifier, so a test can prove the credential actually survives
// snapshot/restore by authenticating with it (a far stronger check than reading
// the GetUsers map). userId is the storage key: bare on a non-namespaced
// cluster, MakeUserKey("id","ns") on a namespaced one.
func seedUser(t *testing.T, u *DBUser, userId, namespace string) (login, identifier string) {
	t.Helper()
	apiKey, hash, ident, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, u.CreateUser(userId, hash, ident, "", namespace, time.Now()))
	login, _, err = keys.DecodeApiKey(apiKey)
	require.NoError(t, err)
	return login, ident
}

func requireLogin(t *testing.T, u *DBUser, login, identifier, wantUserId, wantNamespace string) {
	t.Helper()
	p, err := u.ValidateAndExtract(login, identifier)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, wantUserId, p.Username)
	require.Equal(t, wantNamespace, p.Namespace)
}

func requireNoLogin(t *testing.T, u *DBUser, login, identifier string) {
	t.Helper()
	p, err := u.ValidateAndExtract(login, identifier)
	require.Error(t, err)
	require.Nil(t, p)
}

// TestSnapshotRestore_IncludeUsers_NonNamespaced: the includeUsers backup path
// on a flat cluster. Snapshot(subset) must ship ONLY the selected users; the
// excluded user's credentials must not travel and must not authenticate after
// restore. This is the assertion that fails if filterDBUserData stops filtering.
func TestSnapshotRestore_IncludeUsers_NonNamespaced(t *testing.T) {
	src, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)

	login1, ident1 := seedUser(t, src, "u1", "")
	loginExcluded, identExcluded := seedUser(t, src, "u2", "")
	login3, ident3 := seedUser(t, src, "u3", "")

	snap, err := src.Snapshot("u1", "u3")
	require.NoError(t, err)

	dst, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	require.NoError(t, dst.Restore(snap, false))

	users, err := dst.GetUsers()
	require.NoError(t, err)
	require.Len(t, users, 2)
	require.Contains(t, users, "u1")
	require.Contains(t, users, "u3")

	requireLogin(t, dst, login1, ident1, "u1", "")
	requireLogin(t, dst, login3, ident3, "u3", "")

	require.NotContains(t, users, "u2")
	requireNoLogin(t, dst, loginExcluded, identExcluded)
}

// TestSnapshotRestore_IncludeUsers_Namespaced: the includeUsers backup path on a
// namespaced cluster, where users are stored under their qualified id
// (MakeUserKey -> "ns:id") with User.Namespace set. Covers both restore modes:
// graduation (stripNamespaces=true -> bare id, namespace cleared) and an
// ordinary restore (stripNamespaces=false -> qualified id, namespace kept). A
// user from a non-selected namespace must never appear.
func TestSnapshotRestore_IncludeUsers_Namespaced(t *testing.T) {
	tests := []struct {
		name            string
		stripNamespaces bool
		wantNamespace   string
	}{
		{name: "graduation strips the namespace", stripNamespaces: true, wantNamespace: ""},
		{name: "ordinary restore preserves the namespace", stripNamespaces: false, wantNamespace: "ns1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantID := func(short string) string {
				if tc.stripNamespaces {
					return short
				}
				return MakeUserKey(short, "ns1")
			}

			src, err := NewDBUser(t.TempDir(), false, log)
			require.NoError(t, err)

			login1, ident1 := seedUser(t, src, MakeUserKey("u1", "ns1"), "ns1")
			login2, ident2 := seedUser(t, src, MakeUserKey("u2", "ns1"), "ns1")
			loginOther, identOther := seedUser(t, src, MakeUserKey("u3", "ns2"), "ns2")

			snap, err := src.Snapshot(MakeUserKey("u1", "ns1"), MakeUserKey("u2", "ns1"))
			require.NoError(t, err)

			dst, err := NewDBUser(t.TempDir(), false, log)
			require.NoError(t, err)
			require.NoError(t, dst.Restore(snap, tc.stripNamespaces))

			users, err := dst.GetUsers()
			require.NoError(t, err)
			require.Len(t, users, 2)
			require.Contains(t, users, wantID("u1"))
			require.Contains(t, users, wantID("u2"))
			require.Equal(t, tc.wantNamespace, users[wantID("u1")].Namespace)

			requireLogin(t, dst, login1, ident1, wantID("u1"), tc.wantNamespace)
			requireLogin(t, dst, login2, ident2, wantID("u2"), tc.wantNamespace)

			require.NotContains(t, users, MakeUserKey("u3", "ns2"))
			require.NotContains(t, users, "u3")
			requireNoLogin(t, dst, loginOther, identOther)
		})
	}
}

// TestSnapshot_IncludeUsers_RejectsUnknownID: selecting a user that does not
// exist must fail loudly at snapshot time rather than ship an incomplete backup
// that silently drops the missing id.
func TestSnapshot_IncludeUsers_RejectsUnknownID(t *testing.T) {
	src, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	seedUser(t, src, "u1", "")

	_, err = src.Snapshot("u1", "ghost")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ghost")
}

// TestSnapshotRestore_IncludeUsers_GraduationAliasCollision: includeUsers
// spanning two namespaces whose users share a short id. After the graduation
// strip both collapse to the same bare id; restore must fail closed rather than
// silently overwrite one user's credentials with the other's, and the target's
// existing users must survive the rejected restore.
func TestSnapshotRestore_IncludeUsers_GraduationAliasCollision(t *testing.T) {
	src, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	seedUser(t, src, MakeUserKey("alice", "ns1"), "ns1")
	seedUser(t, src, MakeUserKey("alice", "ns2"), "ns2")

	snap, err := src.Snapshot(MakeUserKey("alice", "ns1"), MakeUserKey("alice", "ns2"))
	require.NoError(t, err)

	dst, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	loginIncumbent, identIncumbent := seedUser(t, dst, "incumbent", "")

	err = dst.Restore(snap, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "alias")

	users, err := dst.GetUsers()
	require.NoError(t, err)
	require.Contains(t, users, "incumbent")
	require.NotContains(t, users, "alice")
	requireLogin(t, dst, loginIncumbent, identIncumbent, "incumbent", "")
}

// TestSnapshotRestore_IncludeUsers_RestoreReplacesTarget: restoring an
// includeUsers subset REPLACES the target's user store, it does not merge. An
// incumbent user in the target is wiped, so stale credentials cannot linger
// past a restore.
func TestSnapshotRestore_IncludeUsers_RestoreReplacesTarget(t *testing.T) {
	src, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	login1, ident1 := seedUser(t, src, "u1", "")

	snap, err := src.Snapshot("u1")
	require.NoError(t, err)

	dst, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	loginIncumbent, identIncumbent := seedUser(t, dst, "incumbent", "")

	require.NoError(t, dst.Restore(snap, false))

	users, err := dst.GetUsers()
	require.NoError(t, err)
	require.Len(t, users, 1)
	require.Contains(t, users, "u1")
	require.NotContains(t, users, "incumbent")

	requireLogin(t, dst, login1, ident1, "u1", "")
	requireNoLogin(t, dst, loginIncumbent, identIncumbent)
}

// TestSnapshotRestore_IncludeUsers_PreservesStatus: deactivation and key
// revocation must survive an includeUsers backup, or a restore would silently
// re-enable a disabled credential.
func TestSnapshotRestore_IncludeUsers_PreservesStatus(t *testing.T) {
	src, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	loginActive, identActive := seedUser(t, src, "active", "")
	loginDeact, identDeact := seedUser(t, src, "deactivated", "")
	require.NoError(t, src.DeactivateUser("deactivated", true)) // revoke the key too

	snap, err := src.Snapshot("active", "deactivated")
	require.NoError(t, err)

	dst, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	require.NoError(t, dst.Restore(snap, false))

	users, err := dst.GetUsers()
	require.NoError(t, err)
	require.True(t, users["active"].Active)
	require.False(t, users["deactivated"].Active)

	requireLogin(t, dst, loginActive, identActive, "active", "")
	requireNoLogin(t, dst, loginDeact, identDeact)
}

// TestSnapshotRestore_IncludeUsers_ImportedKey: imported (static) keys are never
// namespaced and are carried through the strip wholesale. An imported user
// selected by includeUsers must still validate after a graduation restore,
// alongside a stripped namespaced dynamic user.
func TestSnapshotRestore_IncludeUsers_ImportedKey(t *testing.T) {
	src, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)

	seedUser(t, src, MakeUserKey("u1", "ns1"), "ns1")
	importedKey := "imported-static-key"
	require.NoError(t, src.CreateUserWithKey("svc", importedKey[:3], sha256.Sum256([]byte(importedKey)), time.Now()))

	snap, err := src.Snapshot(MakeUserKey("u1", "ns1"), "svc")
	require.NoError(t, err)

	dst, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)
	require.NoError(t, dst.Restore(snap, true)) // graduation strip

	p, err := dst.ValidateImportedKey(importedKey)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, "svc", p.Username)

	users, err := dst.GetUsers()
	require.NoError(t, err)
	require.Contains(t, users, "u1")
	require.Contains(t, users, "svc")
}

func TestRestoreIncompleteData(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), false, log)
	require.NoError(t, err)

	snapShot, err := dynUsers.Snapshot()
	require.NoError(t, err)

	var root map[string]interface{}
	require.NoError(t, json.Unmarshal(snapShot, &root))
	if data, ok := root["Data"].(map[string]interface{}); ok {
		delete(data, "ImportedApiKeysWeakHash")
	}
	snapShot, err = json.Marshal(root)
	require.NoError(t, err)

	dynUsers2, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)
	err = dynUsers2.Restore(snapShot, false)
	require.NoError(t, err)

	importedApiKey := "importedApiKey"
	userId := "user"
	createdAt := time.Now().Add(-time.Hour)
	require.NoError(t, dynUsers2.CreateUserWithKey(userId, importedApiKey[:3], sha256.Sum256([]byte(importedApiKey)), createdAt))
	user, err := dynUsers2.GetUsers(userId)
	require.NoError(t, err)
	require.Equal(t, user[userId].Id, userId)
}
