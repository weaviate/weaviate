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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
)

// TestMockOIDCHarness validates the test/docker mockoidc scaffolding
// without depending on the cluster's OIDC classifier — it asserts that
// the mock server emits the expected claims for each preseed mode and
// honours the helper's ?subject= selector and the /queue admin endpoint.
//
// Without this test, broken harness changes only surface when downstream
// acceptance suites fail.
func TestMockOIDCHarness(t *testing.T) {
	t.Run("namespaces preseed: FIFO and ?subject= selector", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		compose, err := docker.New().
			WithWeaviate().
			WithMockOIDC().
			WithMockOIDCNamespacedUsers().
			Start(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, compose.Terminate(ctx)) }()

		helperURI := compose.GetMockOIDCHelper().URI()

		// FIFO: with no subject the first preseed user is dequeued. In
		// "namespaces" mode that's oidc-namespaced-customer1.
		token, _ := docker.GetTokensFromMockOIDCWithHelper(t, helperURI)
		claims := decodeJWTClaims(t, token)
		assert.Equal(t, "oidc-namespaced-customer1", claims["sub"])
		assert.Equal(t, "customer1", claims["weaviate_namespace"])
		assert.Nil(t, claims["weaviate_global_principal"])

		// ?subject= drains-and-replaces the queue, so order doesn't matter.
		token, _ = docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, "oidc-namespaced-customer2")
		claims = decodeJWTClaims(t, token)
		assert.Equal(t, "oidc-namespaced-customer2", claims["sub"])
		assert.Equal(t, "customer2", claims["weaviate_namespace"])

		// Global-principal user emits the bool claim, no namespace claim.
		token, _ = docker.GetTokensFromMockOIDCWithHelperFor(t, helperURI, "oidc-global")
		claims = decodeJWTClaims(t, token)
		assert.Equal(t, "oidc-global", claims["sub"])
		assert.Nil(t, claims["weaviate_namespace"])
		assert.Equal(t, true, claims["weaviate_global_principal"])

		// Subject not in the active preseed → the helper proxies a 404
		// from the mockoidc admin endpoint and surfaces an HTTP 500.
		// admin-user is in the legacy preseed but not in "namespaces".
		assertHelperRejects(t, helperURI, "admin-user")
		// Likewise an error-class subject is not available in "namespaces".
		assertHelperRejects(t, helperURI, "oidc-both-claims")
	})
}

// decodeJWTClaims extracts the claims segment of a JWT without verifying
// the signature — sufficient for asserting that the mock server emitted
// the right claim shape.
func decodeJWTClaims(t *testing.T, token string) map[string]interface{} {
	t.Helper()
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3, "expected a JWT with three segments")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err, "decode JWT payload segment")
	var claims map[string]interface{}
	require.NoError(t, json.Unmarshal(payload, &claims), "unmarshal JWT claims")
	return claims
}

// assertHelperRejects verifies that requesting an unknown subject from
// the helper surfaces a non-200 status, with the response body mentioning
// the subject — the helper proxies the mockoidc admin endpoint's 404,
// which means the subject is not in the active preseed.
func assertHelperRejects(t *testing.T, helperURI, subject string) {
	t.Helper()
	endpoint := fmt.Sprintf("http://%s/tokens?subject=%s", helperURI, url.QueryEscape(subject))
	resp, err := http.Get(endpoint) //nolint:gosec // test-only fixture
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	assert.NotEqual(t, http.StatusOK, resp.StatusCode,
		"expected non-200 for unknown subject %q in active preseed; body=%s", subject, string(body))
	assert.Contains(t, string(body), subject)
}
