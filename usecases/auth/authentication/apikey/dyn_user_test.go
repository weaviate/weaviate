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

	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)

	require.NoError(t, dynUsers.CreateUser(userId, hash, identifier))

	randomKey, _, err := keys.DecodeApiKey(apiKey)
	require.NoError(t, err)

	_, ok := dynUsers.weakKeyStorage[userId]
	require.False(t, ok)

	startSlow := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKey, identifier)
	require.NoError(t, err)
	tookSlow := time.Since(startSlow)

	_, ok = dynUsers.weakKeyStorage[userId]
	require.True(t, ok)

	startFast := time.Now()
	_, err = dynUsers.ValidateAndExtract(randomKey, identifier)
	require.NoError(t, err)
	tookFast := time.Since(startFast)
	require.Less(t, tookFast, tookSlow)
}
