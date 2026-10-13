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

package clusterapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema"
)

func Test_staticRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/indices", okHandler)
	mux.HandleFunc("/replicas/", okHandler)

	cases := []struct {
		name     string
		req      *http.Request
		expected string
	}{
		{
			name:     "unmatched route",
			req:      newRequest(t, "/foo"), // un-matched route
			expected: "/foo",
		},
		{
			name:     "matched route",
			req:      newRequest(t, "/indices"), // matched route
			expected: "/indices",
		},
		{
			name:     "un-matched route with dynamic path",
			req:      newRequest(t, "/indices/objects/Movies"), // un-matched route. Note original handler is `/indices` (without `/` suffix)
			expected: "/indices/objects/Movies",
		},
		{
			name:     "matched route with dynamic path",
			req:      newRequest(t, "/replicas/objects/Movies"), // matched route.
			expected: "/replicas/",                              // yay!
		},
		{
			name:     "matched route with dynamic path 2",
			req:      newRequest(t, "/replicas/objects/Movies2"), // matched route.
			expected: "/replicas/",                               // yay!
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := staticRoute(mux)(tc.req)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func newRequest(t *testing.T, path string) *http.Request {
	t.Helper()

	r, err := http.NewRequest("GET", path, nil)
	require.NoError(t, err)
	return r
}

func okHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok")
	w.WriteHeader(http.StatusOK)
}

// Test_buildHandlerChain_SentryKeepsClusterMiddleware guards the wiring of the
// cluster listener: each wrapper has to wrap the chain built before it. Handing
// the Sentry wrapper the bare mux instead dropped the cluster middleware, and
// with it the /v1/cluster/* raft routes and the auth applied to them, whenever
// Sentry was enabled -- silently, since every other route came from the mux and
// kept working.
func Test_buildHandlerChain_SentryKeepsClusterMiddleware(t *testing.T) {
	for _, sentryEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("sentry_enabled=%v", sentryEnabled), func(t *testing.T) {
			var baseCalled bool
			base := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				baseCalled = true
				w.WriteHeader(http.StatusOK)
			})

			// Minimal appState: buildHandlerChain reads only the two feature
			// flags, and addClusterHandlerMiddleware only SchemaManager.Handler,
			// which an empty-bodied request never reaches.
			appState := &state.State{
				SchemaManager: &schema.Manager{},
				ServerConfig: &config.WeaviateConfig{Config: config.Config{
					Sentry: &entsentry.ConfigOpts{Enabled: sentryEnabled},
				}},
			}

			handler := buildHandlerChain(base, http.NewServeMux(), appState,
				NewBasicAuthHandler(cluster.AuthConfig{}))

			// /v1/cluster/* belongs to the raft router the middleware installs.
			// An empty body makes that router answer 400; falling through to the
			// handler underneath answers 200 and means the middleware is gone.
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, newPostRequest(t, "/v1/cluster/join"))
			assert.False(t, baseCalled, "cluster route must not fall through to the mux")
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			// Everything else still reaches the handler underneath.
			baseCalled = false
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, newPostRequest(t, "/indices"))
			assert.True(t, baseCalled, "non-cluster route must reach the mux")
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// newPostRequest builds a request with an empty (but non-nil) body, which the
// raft handler needs in order to answer rather than dereference a nil body.
func newPostRequest(t *testing.T, path string) *http.Request {
	t.Helper()

	r, err := http.NewRequest(http.MethodPost, path, strings.NewReader(""))
	require.NoError(t, err)
	return r
}
