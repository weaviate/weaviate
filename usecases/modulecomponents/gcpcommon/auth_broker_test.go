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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testIdentityToken = "test-identity-token"

func TestFetchTokenSuccess(t *testing.T) {
	expected := AuthBrokerToken{
		AccessToken: "test-access-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().UTC().Add(time.Hour).Truncate(time.Second),
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, fmt.Sprintf("Bearer %s", testIdentityToken), r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

	tok, err := b.fetchToken(t.Context(), testIdentityToken)
	require.NoError(t, err)
	assert.Equal(t, expected.AccessToken, tok.AccessToken)
	assert.Equal(t, expected.TokenType, tok.TokenType)
	assert.Equal(t, expected.Expiry, tok.Expiry)
}

func TestFetchTokenRetryableStatuses(t *testing.T) {
	for _, status := range []int{429, 500, 502, 503, 504} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

			_, err := b.fetchToken(t.Context(), testIdentityToken)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrRetryableAuthBroker)
		})
	}
}

func TestFetchToken4xxNotRetryable(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

			_, err := b.fetchToken(t.Context(), testIdentityToken)
			require.Error(t, err)
			assert.NotErrorIs(t, err, ErrRetryableAuthBroker)
		})
	}
}

func TestFetchTokenNetworkErrorReturnsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(nil))
	srv.Close() // close immediately so all connections fail

	b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

	_, err := b.fetchToken(t.Context(), testIdentityToken)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRetryableAuthBroker)
}

func TestFetchTokenWithRetryRetriesOnRetryableError(t *testing.T) {
	attempt := 0
	expected := AuthBrokerToken{
		AccessToken: "test-access-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().UTC().Add(time.Hour).Truncate(time.Second),
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

	tok, err := b.fetchTokenWithRetry(t.Context(), testIdentityToken)
	require.NoError(t, err)
	assert.Equal(t, expected.AccessToken, tok.AccessToken)
	assert.Equal(t, 3, attempt)
}

func TestFetchTokenWithRetryNoRetryOnNonRetryableError(t *testing.T) {
	attempt := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

	_, err := b.fetchTokenWithRetry(t.Context(), testIdentityToken)
	require.Error(t, err)
	assert.Equal(t, 1, attempt)
}

func TestFetchTokenWithRetryStopsWhenContextExpires(t *testing.T) {
	attempt := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	// Short deadline; Initial=500ms so we expect only a couple of attempts
	// before the sleep is cancelled by the ctx deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

	_, err := b.fetchTokenWithRetry(ctx, testIdentityToken)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "should surface the ctx deadline as the primary cause")
	assert.ErrorIs(t, err, ErrRetryableAuthBroker, "should keep the underlying retryable error in the chain")
	assert.GreaterOrEqual(t, attempt, 1, "should attempt at least once before context expires")
}

func TestResolveTokenTimeout(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		set   bool
		want  time.Duration
	}{
		{"unset_uses_default", "", false, defaultTokenTimeout},
		{"empty_uses_default", "", true, defaultTokenTimeout},
		{"garbage_uses_default", "not-a-duration", true, defaultTokenTimeout},
		{"bare_number_uses_default", "60", true, defaultTokenTimeout},
		{"negative_uses_default", "-5s", true, defaultTokenTimeout},
		{"zero_uses_default", "0s", true, defaultTokenTimeout},
		{"seconds_are_respected", "45s", true, 45 * time.Second},
		{"minutes_are_respected", "2m", true, 2 * time.Minute},
		{"compound_is_respected", "1m30s", true, 90 * time.Second},
		{"milliseconds_are_respected", "500ms", true, 500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(tokenTimeoutEnvVar, tc.value)
			}
			assert.Equal(t, tc.want, resolveTokenTimeout())
		})
	}
}

func TestNewAuthBrokerTokenSourceReadsTimeoutFromEnv(t *testing.T) {
	t.Setenv(tokenTimeoutEnvVar, "45s")
	b := NewAuthBrokerTokenSource("http://example.invalid")
	assert.Equal(t, 45*time.Second, b.tokenTimeout)
}
