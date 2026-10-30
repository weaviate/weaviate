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

package digitalocean

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModelLister_ListModels_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/models", r.URL.Path)
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"object": "list",
			"data": [
				{"id": "qwen3-embedding-0.6b", "object": "model", "owned_by": "digitalocean", "created": 1},
				{"id": "openai-gpt-oss-20b", "object": "model", "owned_by": "digitalocean", "created": 2}
			]
		}`)
	}))
	t.Cleanup(server.Close)

	l := NewModelLister(5 * time.Second)
	ids, err := l.ListModels(context.Background(), server.URL, "test-key", "test-uuid")
	require.NoError(t, err)
	assert.Equal(t, []string{"qwen3-embedding-0.6b", "openai-gpt-oss-20b"}, ids)
}

func TestModelLister_Cache(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"m1","object":"model","owned_by":"do","created":1}]}`)
	}))
	t.Cleanup(server.Close)

	l := NewModelLister(5 * time.Second)
	for i := 0; i < 5; i++ {
		ids, err := l.ListModels(context.Background(), server.URL, "test-key", "test-uuid")
		require.NoError(t, err)
		assert.Equal(t, []string{"m1"}, ids)
	}
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "second and later calls should be served from cache")

	// different api key should miss the cache
	_, err := l.ListModels(context.Background(), server.URL, "other-key", "test-uuid")
	require.NoError(t, err)
	assert.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

func TestModelLister_PrunesExpiredEntries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"m1","object":"model","owned_by":"do","created":1}]}`)
	}))
	t.Cleanup(server.Close)

	l := NewModelLister(5 * time.Second)
	expiredKey := l.cacheKey("https://expired.example.com", "old-key")
	freshKey := l.cacheKey("https://fresh.example.com", "other-key")
	l.cache[expiredKey] = modelCacheEntry{models: []string{"old"}, fetchedAt: time.Now().Add(-modelCacheTTL)}
	l.cache[freshKey] = modelCacheEntry{models: []string{"fresh"}, fetchedAt: time.Now()}

	_, err := l.ListModels(context.Background(), server.URL, "test-key", "test-uuid")
	require.NoError(t, err)

	assert.NotContains(t, l.cache, expiredKey)
	assert.Contains(t, l.cache, freshKey)
	assert.Contains(t, l.cache, l.cacheKey(server.URL, "test-key"))
}

func TestModelLister_Error(t *testing.T) {
	tests := []struct {
		name        string
		statusCode  int
		body        string
		expectedErr string
	}{
		{
			name:        "openai-style error",
			statusCode:  http.StatusUnauthorized,
			body:        `{"error":{"message":"bad token","code":"unauthorized"}}`,
			expectedErr: "DigitalOcean /v1/models returned status 401: bad token",
		},
		{
			name:        "native error",
			statusCode:  http.StatusPaymentRequired,
			body:        `{"id":"Payment Required","message":"insufficient balance","request_id":"req-123"}`,
			expectedErr: "DigitalOcean /v1/models returned status 402: insufficient balance request_id: req-123",
		},
		{
			name:        "status only",
			statusCode:  http.StatusInternalServerError,
			body:        `{}`,
			expectedErr: "DigitalOcean /v1/models returned status 500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				_, _ = io.WriteString(w, tt.body)
			}))
			t.Cleanup(server.Close)

			l := NewModelLister(5 * time.Second)
			_, err := l.ListModels(context.Background(), server.URL, "test-key", "test-uuid")
			require.Error(t, err)
			assert.Equal(t, tt.expectedErr, err.Error())
		})
	}
}
