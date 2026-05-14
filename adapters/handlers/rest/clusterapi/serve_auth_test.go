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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/schema"
)

// Test_addClusterHandlerMiddleware_BasicAuth verifies that the cluster API
// middleware enforces basic auth on /v1/cluster/* endpoints when configured,
// and bypasses auth otherwise.
func Test_addClusterHandlerMiddleware_BasicAuth(t *testing.T) {
	const (
		user = "alice"
		pass = "s3cret"
	)

	// next handler stands in for the rest of the middleware chain. It records
	// whether it was called so we can assert routing decisions.
	type nextProbe struct {
		called bool
	}
	makeNext := func(p *nextProbe) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p.called = true
			w.WriteHeader(http.StatusOK)
		})
	}

	// Minimal appState. addClusterHandlerMiddleware only reads
	// appState.SchemaManager.Handler; the embedded zero-value Handler is
	// never invoked because all requests in this test have empty bodies and
	// short-circuit in the raft handler before touching the schema manager.
	appState := &state.State{SchemaManager: &schema.Manager{}}

	authEnabled := NewBasicAuthHandler(cluster.AuthConfig{
		BasicAuth: cluster.BasicAuth{Username: user, Password: pass},
	})
	authDisabled := NewBasicAuthHandler(cluster.AuthConfig{})

	cases := []struct {
		name           string
		auth           auth
		path           string
		setBasicAuth   bool
		user, pass     string
		wantStatus     int
		wantNextCalled bool
	}{
		{
			name:           "auth disabled, /v1/cluster/join reaches raft handler",
			auth:           authDisabled,
			path:           "/v1/cluster/join",
			wantStatus:     http.StatusBadRequest, // empty body → 400 from raft handler
			wantNextCalled: false,
		},
		{
			name:           "auth disabled, /v1/cluster/remove reaches raft handler",
			auth:           authDisabled,
			path:           "/v1/cluster/remove",
			wantStatus:     http.StatusBadRequest,
			wantNextCalled: false,
		},
		{
			name:           "auth enabled, /v1/cluster/join no creds → 401",
			auth:           authEnabled,
			path:           "/v1/cluster/join",
			wantStatus:     http.StatusUnauthorized,
			wantNextCalled: false,
		},
		{
			name:           "auth enabled, /v1/cluster/remove no creds → 401",
			auth:           authEnabled,
			path:           "/v1/cluster/remove",
			wantStatus:     http.StatusUnauthorized,
			wantNextCalled: false,
		},
		{
			name:           "auth enabled, /v1/cluster/join wrong user → 401",
			auth:           authEnabled,
			path:           "/v1/cluster/join",
			setBasicAuth:   true,
			user:           "bob",
			pass:           pass,
			wantStatus:     http.StatusUnauthorized,
			wantNextCalled: false,
		},
		{
			name:           "auth enabled, /v1/cluster/join wrong password → 401",
			auth:           authEnabled,
			path:           "/v1/cluster/join",
			setBasicAuth:   true,
			user:           user,
			pass:           "wrong",
			wantStatus:     http.StatusUnauthorized,
			wantNextCalled: false,
		},
		{
			name:           "auth enabled, /v1/cluster/join correct creds → reaches raft handler",
			auth:           authEnabled,
			path:           "/v1/cluster/join",
			setBasicAuth:   true,
			user:           user,
			pass:           pass,
			wantStatus:     http.StatusBadRequest, // empty body → 400 from raft handler
			wantNextCalled: false,
		},
		{
			name:           "auth enabled, /v1/cluster/remove correct creds → reaches raft handler",
			auth:           authEnabled,
			path:           "/v1/cluster/remove",
			setBasicAuth:   true,
			user:           user,
			pass:           pass,
			wantStatus:     http.StatusBadRequest,
			wantNextCalled: false,
		},
		{
			name:           "auth enabled, non-cluster path bypasses cluster auth and hits next",
			auth:           authEnabled,
			path:           "/foo",
			wantStatus:     http.StatusOK,
			wantNextCalled: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := &nextProbe{}
			h := addClusterHandlerMiddleware(makeNext(probe), appState, tc.auth)

			req, err := http.NewRequest(http.MethodPost, tc.path, strings.NewReader(""))
			require.NoError(t, err)
			if tc.setBasicAuth {
				req.SetBasicAuth(tc.user, tc.pass)
			}
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)
			assert.Equal(t, tc.wantNextCalled, probe.called)
		})
	}
}
