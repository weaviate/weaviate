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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
)

// TestValidateAndExtractSingleFlightPerKey pins that a key authenticates even
// while another key for the same user is being verified. The Argon2
// verification is deduplicated, so a request that joins an in-flight
// verification must not inherit a verdict reached for a different key.
//
// The rotate journey hits this: the caller polls with the superseded key while
// the new key's first use lands, and both resolve to the same user.
//
// The in-flight verification is simulated by occupying the singleflight slot
// that keying on the user alone would use, rather than racing a real one, so
// the test fails without the fix instead of depending on timing.
func TestValidateAndExtractSingleFlightPerKey(t *testing.T) {
	dynUsers, err := NewDBUser(t.TempDir(), true, log)
	require.NoError(t, err)

	goodKey, goodHash, goodIdentifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, dynUsers.CreateUser("bob", goodHash, goodIdentifier, "", time.Now()))

	goodSecret, _, err := keys.DecodeApiKey(goodKey)
	require.NoError(t, err)

	// A second, unrelated key. It is not bob's stored key, so on its own it must
	// fail — and it must not decide the outcome for the real key.
	otherKey, _, _, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	otherSecret, _, err := keys.DecodeApiKey(otherKey)
	require.NoError(t, err)

	_, err = dynUsers.ValidateAndExtract(otherSecret, goodIdentifier)
	require.Error(t, err, "a key that is not the user's stored key must be rejected")

	occupied := make(chan struct{})
	release := make(chan struct{})
	// Keying on the user alone makes the call below join the occupied slot and
	// block on it, so the slot has to be released for the test to report rather
	// than deadlock.
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _, _ = dynUsers.singleFlight.Do("auth:bob", func() (any, error) {
			close(occupied)
			<-release
			return nil, errors.New("verification failed for a different key")
		})
	})

	// Returns once the slot is registered, so the call below joins it if the
	// singleflight key does not include the key itself.
	<-occupied

	var principal *models.Principal
	done := make(chan error, 1)
	wg.Go(func() {
		var err error
		principal, err = dynUsers.ValidateAndExtract(goodSecret, goodIdentifier)
		done <- err
	})

	select {
	case err := <-done:
		require.NoError(t, err, "the correct key must authenticate regardless of a concurrent verification for the same user")
		require.NotNil(t, principal)
	case <-time.After(30 * time.Second):
		releaseOnce()
		t.Fatal("the correct key blocked on an in-flight verification for a different key")
	}

	releaseOnce()
	wg.Wait()
}
