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

package rest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	restsearch "github.com/weaviate/weaviate/adapters/handlers/rest/search"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func searchTestAppState(enabled bool, mode string) *state.State {
	return &state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				ExperimentalRESTSearchEnabled: configRuntime.NewDynamicValue(enabled),
				OperationalMode:               configRuntime.NewDynamicValue(mode),
			},
		},
	}
}

// nextRecorder is a sentinel next handler that records having been reached.
type nextRecorder struct {
	called bool
}

func (n *nextRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.called = true
	w.WriteHeader(http.StatusTeapot) // distinguishable from anything real
}

// TestAddOperationalModeSearchRoutes covers the operational-mode
// classification of REST search requests: they carry POST bodies (an HTTP
// "write" method) but are semantically reads, so they stay available in
// READ_ONLY/SCALE_OUT and are blocked in WRITE_ONLY.
func TestAddOperationalModeSearchRoutes(t *testing.T) {
	searchBody := func() *strings.Reader { return strings.NewReader(`{"query":"space"}`) }

	run := func(t *testing.T, appState *state.State, method, path string) (*nextRecorder, *httptest.ResponseRecorder) {
		t.Helper()
		next := &nextRecorder{}
		handler := addOperationalMode(appState, next)
		req := httptest.NewRequest(method, path, searchBody())
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return next, rec
	}

	t.Run("READ_ONLY lets search through", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(true, config.READ_ONLY), http.MethodPost, "/v1/search/Movie/near-text")
		assert.True(t, next.called, "POST search must pass in READ_ONLY")
	})

	t.Run("READ_ONLY lets bm25 search through", func(t *testing.T) {
		// the classification is per-namespace, not per-search-type: every
		// /v1/search/{collection}/{type} route is a read
		next, _ := run(t, searchTestAppState(true, config.READ_ONLY), http.MethodPost, "/v1/search/Movie/bm25")
		assert.True(t, next.called, "POST bm25 search must pass in READ_ONLY")
	})

	t.Run("READ_ONLY still blocks real writes", func(t *testing.T) {
		next, rec := run(t, searchTestAppState(true, config.READ_ONLY), http.MethodPost, "/v1/objects")
		assert.False(t, next.called)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("READ_ONLY with the feature disabled has no carve-out", func(t *testing.T) {
		next, rec := run(t, searchTestAppState(false, config.READ_ONLY), http.MethodPost, "/v1/search/Movie/near-text")
		assert.False(t, next.called)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("SCALE_OUT lets search through", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(true, config.SCALE_OUT), http.MethodPost, "/v1/search/Movie/near-text")
		assert.True(t, next.called)
	})

	t.Run("SCALE_OUT lets bm25 search through", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(true, config.SCALE_OUT), http.MethodPost, "/v1/search/Movie/bm25")
		assert.True(t, next.called, "POST bm25 search must pass in SCALE_OUT")
	})

	t.Run("WRITE_ONLY blocks search", func(t *testing.T) {
		// POST is an HTTP "write" so the method-based check alone would
		// let a search — semantically a read — through write-only mode;
		// the explicit isSearch block closes that
		next, rec := run(t, searchTestAppState(true, config.WRITE_ONLY), http.MethodPost, "/v1/search/Movie/near-text")
		assert.False(t, next.called, "POST search must be blocked in WRITE_ONLY")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("WRITE_ONLY blocks bm25 search", func(t *testing.T) {
		next, rec := run(t, searchTestAppState(true, config.WRITE_ONLY), http.MethodPost, "/v1/search/Movie/bm25")
		assert.False(t, next.called, "POST bm25 search must be blocked in WRITE_ONLY")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("WRITE_ONLY blocks search even when the feature is disabled", func(t *testing.T) {
		// a search is a read → 503 regardless of whether the feature is
		// enabled; the op-mode block takes precedence over the handler's
		// not-enabled 422
		next, rec := run(t, searchTestAppState(false, config.WRITE_ONLY), http.MethodPost, "/v1/search/Movie/near-text")
		assert.False(t, next.called, "disabled search in WRITE_ONLY must still be 503")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("WRITE_ONLY keeps whitelisted reads", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(true, config.WRITE_ONLY), http.MethodGet, "/v1/meta")
		assert.True(t, next.called)
	})

	t.Run("only the static search namespace is a search route", func(t *testing.T) {
		assert.True(t, restsearch.IsSearchRoute("/v1/search/Movie/near-text"))
		assert.False(t, restsearch.IsSearchRoute("/v1/Movie/search/near-text"))
	})
}
