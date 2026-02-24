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

package gcpcommon

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchToken_Success(t *testing.T) {
	expected := AuthBrokerToken{
		AccessToken: "test-access-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour).Truncate(time.Second),
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer identity-token", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

	tok, err := b.fetchToken(t.Context(), "identity-token")
	require.NoError(t, err)
	assert.Equal(t, expected.AccessToken, tok.AccessToken)
	assert.Equal(t, expected.TokenType, tok.TokenType)
	assert.Equal(t, expected.Expiry, tok.Expiry)
}

func TestFetchToken_5xx_ReturnsRetryable(t *testing.T) {
	for _, status := range []int{500, 502, 503, 504} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

			_, err := b.fetchToken(t.Context(), "identity-token")
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrRetryableAuthBroker)
		})
	}
}

func TestFetchToken_4xx_NotRetryable(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

			_, err := b.fetchToken(t.Context(), "identity-token")
			require.Error(t, err)
			assert.NotErrorIs(t, err, ErrRetryableAuthBroker)
		})
	}
}

func TestFetchToken_NetworkError_ReturnsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // close immediately so all connections fail

	b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

	_, err := b.fetchToken(t.Context(), "identity-token")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRetryableAuthBroker)
}

func TestFetchTokenWithRetry_RetriesOnRetryableError(t *testing.T) {
	attempt := 0
	expected := AuthBrokerToken{
		AccessToken: "test-access-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour).Truncate(time.Second),
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

	tok, err := b.fetchTokenWithRetry(t.Context(), "identity-token")
	require.NoError(t, err)
	assert.Equal(t, expected.AccessToken, tok.AccessToken)
	assert.Equal(t, 3, attempt)
}

func TestFetchTokenWithRetry_NoRetryOnNonRetryableError(t *testing.T) {
	attempt := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

	_, err := b.fetchTokenWithRetry(t.Context(), "identity-token")
	require.Error(t, err)
	assert.Equal(t, 1, attempt)
}
