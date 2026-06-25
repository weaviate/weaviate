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

package awscommon

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testIdentityToken = "test-identity-token"

func writeIdentityTokenFile(t *testing.T, contents string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", path)
}

func TestFetchCredentialsSuccess(t *testing.T) {
	expected := AuthBrokerCredentialValue{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "test-secret",
		SessionToken:    "test-session",
		Expiration:      time.Now().UTC().Add(time.Hour).Truncate(time.Second),
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, fmt.Sprintf("Bearer %s", testIdentityToken), r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	b := &AuthBrokerCredentials{endpoint: srv.URL, client: srv.Client()}

	creds, err := b.fetchCredentials(t.Context(), testIdentityToken)
	require.NoError(t, err)
	assert.Equal(t, expected.AccessKeyID, creds.AccessKeyID)
	assert.Equal(t, expected.SecretAccessKey, creds.SecretAccessKey)
	assert.Equal(t, expected.SessionToken, creds.SessionToken)
	assert.Equal(t, expected.Expiration, creds.Expiration)
}

func TestFetchCredentials5xxReturnsRetryable(t *testing.T) {
	for _, status := range []int{500, 502, 503, 504} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			b := &AuthBrokerCredentials{endpoint: srv.URL, client: srv.Client()}

			_, err := b.fetchCredentials(t.Context(), testIdentityToken)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrRetryableAuthBroker)
		})
	}
}

func TestFetchCredentials4xxNotRetryable(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()

			b := &AuthBrokerCredentials{endpoint: srv.URL, client: srv.Client()}

			_, err := b.fetchCredentials(t.Context(), testIdentityToken)
			require.Error(t, err)
			assert.NotErrorIs(t, err, ErrRetryableAuthBroker)
		})
	}
}

func TestFetchCredentialsNetworkErrorReturnsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(nil))
	srv.Close() // close immediately so all connections fail

	b := &AuthBrokerCredentials{endpoint: srv.URL, client: srv.Client()}

	_, err := b.fetchCredentials(t.Context(), testIdentityToken)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRetryableAuthBroker)
}

func TestFetchCredentialsWithRetryRetriesOnRetryableError(t *testing.T) {
	attempt := 0
	expected := AuthBrokerCredentialValue{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "test-secret",
		SessionToken:    "test-session",
		Expiration:      time.Now().UTC().Add(time.Hour).Truncate(time.Second),
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

	b := &AuthBrokerCredentials{endpoint: srv.URL, client: srv.Client()}

	creds, err := b.fetchCredentialsWithRetry(t.Context(), testIdentityToken)
	require.NoError(t, err)
	assert.Equal(t, expected.AccessKeyID, creds.AccessKeyID)
	assert.Equal(t, 3, attempt)
}

func TestFetchCredentialsWithRetryNoRetryOnNonRetryableError(t *testing.T) {
	attempt := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	b := &AuthBrokerCredentials{endpoint: srv.URL, client: srv.Client()}

	_, err := b.fetchCredentialsWithRetry(t.Context(), testIdentityToken)
	require.Error(t, err)
	assert.Equal(t, 1, attempt)
}

func TestRetrievePopulatesValueAndExpiry(t *testing.T) {
	writeIdentityTokenFile(t, testIdentityToken)

	expected := AuthBrokerCredentialValue{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "test-secret",
		SessionToken:    "test-session",
		Expiration:      time.Now().UTC().Add(time.Hour).Truncate(time.Second),
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, fmt.Sprintf("Bearer %s", testIdentityToken), r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(expected)
	}))
	defer srv.Close()

	b := NewAuthBrokerCredentials(srv.URL)
	b.client = srv.Client()

	val, err := b.RetrieveWithCredContext(nil)
	require.NoError(t, err)
	assert.Equal(t, expected.AccessKeyID, val.AccessKeyID)
	assert.Equal(t, expected.SecretAccessKey, val.SecretAccessKey)
	assert.Equal(t, expected.SessionToken, val.SessionToken)
	assert.Equal(t, expected.Expiration, val.Expiration)
	assert.Equal(t, credentials.SignatureV4, val.SignerType)
	assert.False(t, b.IsExpired(), "expiry should be set in the future")
}

func TestRetrieveRefreshesIdentityTokenFromFile(t *testing.T) {
	// Simulate kubelet rotation by rewriting the token file between calls.
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("token-v1"), 0o600))
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", path)

	seen := make([]string, 0, 2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(AuthBrokerCredentialValue{
			AccessKeyID: "AKIATEST", SecretAccessKey: "s", SessionToken: "t",
			Expiration: time.Now().UTC().Add(time.Hour),
		})
	}))
	defer srv.Close()

	b := NewAuthBrokerCredentials(srv.URL)
	b.client = srv.Client()

	_, err := b.RetrieveWithCredContext(nil)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte("token-v2"), 0o600))
	_, err = b.RetrieveWithCredContext(nil)
	require.NoError(t, err)

	require.Len(t, seen, 2)
	assert.Equal(t, "Bearer token-v1", seen[0])
	assert.Equal(t, "Bearer token-v2", seen[1])
}

func TestRetrieveMissingTokenFile(t *testing.T) {
	t.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")

	b := NewAuthBrokerCredentials("http://unused")
	_, err := b.RetrieveWithCredContext(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AWS_WEB_IDENTITY_TOKEN_FILE")
}
