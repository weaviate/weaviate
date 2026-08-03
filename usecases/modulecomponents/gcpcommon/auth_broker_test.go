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

func TestFetchTokenRejectsIncompleteResponse(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"empty object", `{}`},
		{"missing access token", `{"expiry":"2099-01-01T00:00:00Z","token_type":"Bearer"}`},
		{"zero expiry", `{"access_token":"tok","token_type":"Bearer"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, tt.body)
			}))
			defer srv.Close()

			b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}
			_, err := b.fetchToken(t.Context(), testIdentityToken)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "missing required fields")
			assert.NotErrorIs(t, err, ErrRetryableAuthBroker)
		})
	}
}

func TestFetchTokenMalformedURLIsNotRetryable(t *testing.T) {
	b := &AuthBrokerTokenSource{endpoint: "http://\x7f/bad", client: http.DefaultClient}

	_, err := b.fetchToken(t.Context(), testIdentityToken)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrRetryableAuthBroker)
}

func TestFetchTokenWithRetryAbortsOnContextDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	b := &AuthBrokerTokenSource{endpoint: srv.URL, client: srv.Client()}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	_, err := b.fetchTokenWithRetry(ctx, testIdentityToken)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestResolveTokenTimeout(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"unset", "", defaultTokenTimeout},
		{"valid duration", "5s", 5 * time.Second},
		{"malformed", "not-a-duration", defaultTokenTimeout},
		{"zero", "0s", defaultTokenTimeout},
		{"negative", "-1s", defaultTokenTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("AUTH_PROXY_TOKEN_TIMEOUT", tt.value)
			assert.Equal(t, tt.want, resolveTokenTimeout())
		})
	}
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
