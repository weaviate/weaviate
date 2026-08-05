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

package clusterapi_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/usecases/cluster"
)

type stubCleanupProber struct {
	cleaningUp bool
	asked      string
}

func (s *stubCleanupProber) AnyCleanupInProgressForCollection(collection string) bool {
	s.asked = collection
	return s.cleaningUp
}

func TestInternalReindexCleanupActivity(t *testing.T) {
	tests := []struct {
		name       string
		prober     *stubCleanupProber
		query      string
		wantStatus int
		wantBody   string
		wantAsked  string
	}{
		{
			name:       "cancel seen or teardown running",
			prober:     &stubCleanupProber{cleaningUp: true},
			query:      "?collection=Movies",
			wantStatus: http.StatusOK,
			wantBody:   `{"cleaningUp":true}`,
			wantAsked:  "Movies",
		},
		{
			name:       "nothing to confirm",
			prober:     &stubCleanupProber{},
			query:      "?collection=Movies",
			wantStatus: http.StatusOK,
			wantBody:   `{"cleaningUp":false}`,
			wantAsked:  "Movies",
		},
		{
			name:       "collection is required",
			prober:     &stubCleanupProber{cleaningUp: true},
			query:      "",
			wantStatus: http.StatusBadRequest,
		},
		{
			// Never answer "not cleaning up" from a node that cannot tell:
			// a cancel's response depends on this.
			name:       "probe not wired",
			prober:     nil,
			query:      "?collection=Movies",
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolve := func() clusterapi.ReindexCleanupProber { return nil }
			if tt.prober != nil {
				resolve = func() clusterapi.ReindexCleanupProber { return tt.prober }
			}
			handler := clusterapi.NewReindexCleanup(resolve, clusterapi.NewNoopAuthHandler(), nil)
			server := httptest.NewServer(handler.Activity())
			defer server.Close()

			res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity" + tt.query)
			require.NoError(t, err)
			defer res.Body.Close()

			require.Equal(t, tt.wantStatus, res.StatusCode)
			if tt.wantBody == "" {
				return
			}
			assert.Equal(t, "application/json", res.Header.Get("Content-Type"))
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.JSONEq(t, tt.wantBody, string(body))
			assert.Equal(t, tt.wantAsked, tt.prober.asked)
		})
	}
}

func TestInternalReindexCleanupActivityRejectsNonGET(t *testing.T) {
	handler := clusterapi.NewReindexCleanup(
		func() clusterapi.ReindexCleanupProber { return &stubCleanupProber{} },
		clusterapi.NewNoopAuthHandler(), nil)
	server := httptest.NewServer(handler.Activity())
	defer server.Close()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequest(method, server.URL+"/reindex/cleanup-activity?collection=Movies", nil)
			require.NoError(t, err)

			res, err := server.Client().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			assert.Equal(t, http.StatusMethodNotAllowed, res.StatusCode,
				"a read-only probe must not answer writes")
		})
	}
}

// The route reports cluster-internal state, so it must sit behind the same
// basic auth as every other internal route: an unauthenticated caller is
// refused before the prober is ever asked.
func TestInternalReindexCleanupActivityRequiresAuth(t *testing.T) {
	const (
		user = "alice"
		pass = "s3cret"
	)
	auth := clusterapi.NewBasicAuthHandler(cluster.AuthConfig{
		BasicAuth: cluster.BasicAuth{Username: user, Password: pass},
	})

	tests := []struct {
		name       string
		setAuth    bool
		user, pass string
		wantStatus int
		wantAsked  string
	}{
		{name: "no credentials", wantStatus: http.StatusUnauthorized},
		{name: "wrong user", setAuth: true, user: "mallory", pass: pass, wantStatus: http.StatusUnauthorized},
		{name: "wrong password", setAuth: true, user: user, pass: "guess", wantStatus: http.StatusUnauthorized},
		{
			name: "correct credentials", setAuth: true, user: user, pass: pass,
			wantStatus: http.StatusOK, wantAsked: "Movies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prober := &stubCleanupProber{}
			handler := clusterapi.NewReindexCleanup(
				func() clusterapi.ReindexCleanupProber { return prober }, auth, nil)
			server := httptest.NewServer(handler.Activity())
			defer server.Close()

			req, err := http.NewRequest(http.MethodGet,
				server.URL+"/reindex/cleanup-activity?collection=Movies", nil)
			require.NoError(t, err)
			if tt.setAuth {
				req.SetBasicAuth(tt.user, tt.pass)
			}

			res, err := server.Client().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			require.Equal(t, tt.wantStatus, res.StatusCode)
			assert.Equal(t, tt.wantAsked, prober.asked,
				"a refused caller must not reach the prober")
		})
	}
}

// Pins isNilProber: a nil pointer boxed into the prober must read as unwired.
func TestInternalReindexCleanupActivityTypedNilProber(t *testing.T) {
	var unset *db.ReindexProvider

	handler := clusterapi.NewReindexCleanup(
		func() clusterapi.ReindexCleanupProber { return unset },
		clusterapi.NewNoopAuthHandler(), nil)
	server := httptest.NewServer(handler.Activity())
	defer server.Close()

	res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity?collection=Movies")
	require.NoError(t, err, "the route must answer, not drop the connection on a panic")
	defer res.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode,
		"an unusable prober must read as unwired, never as 'nothing running'")
}

// Pins the late binding described on the resolve field, through the same
// wiring and construction order production uses.
func TestInternalReindexCleanupActivityResolvesProviderLate(t *testing.T) {
	appState := &state.State{}

	handler := clusterapi.NewReindexCleanupFromState(appState, clusterapi.NewNoopAuthHandler())
	server := httptest.NewServer(handler.Activity())
	defer server.Close()

	get := func() int {
		res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity?collection=Movies")
		require.NoError(t, err, "the route must answer at every stage of bootstrap")
		defer res.Body.Close()
		return res.StatusCode
	}

	require.Equal(t, http.StatusServiceUnavailable, get(),
		"before the provider exists the route must say so")

	appState.ReindexProvider = &db.ReindexProvider{}

	require.Equal(t, http.StatusOK, get(),
		"the route must pick the provider up once bootstrap assigns it")
}
