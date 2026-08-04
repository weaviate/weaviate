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
			name:       "gate is up",
			prober:     &stubCleanupProber{cleaningUp: true},
			query:      "?collection=Movies",
			wantStatus: http.StatusOK,
			wantBody:   `{"cleaningUp":true}`,
			wantAsked:  "Movies",
		},
		{
			name:       "gate is down",
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

// A nil pointer boxed into the prober interface is not == nil, so a guard
// written against the interface waves it through and the call panics on the
// nil receiver. Passing an unset struct field straight into the constructor is
// how that happens, so the route has to survive it.
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

// The internal server is constructed before bootstrap assigns the reindex
// provider, so a route that captures the field at construction captures a nil
// that never becomes real. This drives the same wiring production uses, in the
// same order, rather than a handler built around a ready-made prober — the
// latter stays green through exactly this bug.
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
