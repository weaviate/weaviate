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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	restsearch "github.com/weaviate/weaviate/adapters/handlers/rest/search"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func searchTestAppState(mode string) *state.State {
	return &state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				OperationalMode: configRuntime.NewDynamicValue(mode),
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
		next, _ := run(t, searchTestAppState(config.READ_ONLY), http.MethodPost, "/v1/search/Movie/near-text")
		assert.True(t, next.called, "POST search must pass in READ_ONLY")
	})

	t.Run("READ_ONLY lets bm25 search through", func(t *testing.T) {
		// the classification is per-namespace, not per-search-type: every
		// /v1/search/{collection}/{type} route is a read
		next, _ := run(t, searchTestAppState(config.READ_ONLY), http.MethodPost, "/v1/search/Movie/bm25")
		assert.True(t, next.called, "POST bm25 search must pass in READ_ONLY")
	})

	t.Run("READ_ONLY lets hybrid search through", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(config.READ_ONLY), http.MethodPost, "/v1/search/Movie/hybrid")
		assert.True(t, next.called, "POST hybrid search must pass in READ_ONLY")
	})

	t.Run("READ_ONLY lets near-object search through", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(config.READ_ONLY), http.MethodPost, "/v1/search/Movie/near-object")
		assert.True(t, next.called, "POST near-object search must pass in READ_ONLY")
	})

	t.Run("READ_ONLY lets aggregate through", func(t *testing.T) {
		// /v1/aggregate/{collection} shares the search family's read
		// carve-out: an aggregation is semantically a read
		next, _ := run(t, searchTestAppState(config.READ_ONLY), http.MethodPost, "/v1/aggregate/Movie")
		assert.True(t, next.called, "POST aggregate must pass in READ_ONLY")
	})

	t.Run("READ_ONLY still blocks real writes", func(t *testing.T) {
		next, rec := run(t, searchTestAppState(config.READ_ONLY), http.MethodPost, "/v1/objects")
		assert.False(t, next.called)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("SCALE_OUT lets search through", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(config.SCALE_OUT), http.MethodPost, "/v1/search/Movie/near-text")
		assert.True(t, next.called)
	})

	t.Run("SCALE_OUT lets bm25 search through", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(config.SCALE_OUT), http.MethodPost, "/v1/search/Movie/bm25")
		assert.True(t, next.called, "POST bm25 search must pass in SCALE_OUT")
	})

	t.Run("SCALE_OUT lets aggregate through", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(config.SCALE_OUT), http.MethodPost, "/v1/aggregate/Movie")
		assert.True(t, next.called, "POST aggregate must pass in SCALE_OUT")
	})

	t.Run("WRITE_ONLY blocks search", func(t *testing.T) {
		// POST is an HTTP "write" so the method-based check alone would
		// let a search — semantically a read — through write-only mode;
		// the explicit isSearch block closes that
		next, rec := run(t, searchTestAppState(config.WRITE_ONLY), http.MethodPost, "/v1/search/Movie/near-text")
		assert.False(t, next.called, "POST search must be blocked in WRITE_ONLY")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("WRITE_ONLY blocks bm25 search", func(t *testing.T) {
		next, rec := run(t, searchTestAppState(config.WRITE_ONLY), http.MethodPost, "/v1/search/Movie/bm25")
		assert.False(t, next.called, "POST bm25 search must be blocked in WRITE_ONLY")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("WRITE_ONLY blocks hybrid search", func(t *testing.T) {
		next, rec := run(t, searchTestAppState(config.WRITE_ONLY), http.MethodPost, "/v1/search/Movie/hybrid")
		assert.False(t, next.called, "POST hybrid search must be blocked in WRITE_ONLY")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("WRITE_ONLY blocks near-object search", func(t *testing.T) {
		next, rec := run(t, searchTestAppState(config.WRITE_ONLY), http.MethodPost, "/v1/search/Movie/near-object")
		assert.False(t, next.called, "POST near-object search must be blocked in WRITE_ONLY")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("WRITE_ONLY blocks aggregate", func(t *testing.T) {
		next, rec := run(t, searchTestAppState(config.WRITE_ONLY), http.MethodPost, "/v1/aggregate/Movie")
		assert.False(t, next.called, "POST aggregate must be blocked in WRITE_ONLY")
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("WRITE_ONLY keeps whitelisted reads", func(t *testing.T) {
		next, _ := run(t, searchTestAppState(config.WRITE_ONLY), http.MethodGet, "/v1/meta")
		assert.True(t, next.called)
	})

	t.Run("only the static search namespace is a search route", func(t *testing.T) {
		assert.True(t, restsearch.IsSearchRoute("/v1/search/Movie/near-text"))
		assert.False(t, restsearch.IsSearchRoute("/v1/Movie/search/near-text"))
	})

	t.Run("only the static aggregate namespace is an aggregate route", func(t *testing.T) {
		assert.True(t, restsearch.IsAggregateRoute("/v1/aggregate/Movie"))
		assert.False(t, restsearch.IsAggregateRoute("/v1/Movie/aggregate"))
		// the two namespaces do not classify each other's routes
		assert.False(t, restsearch.IsAggregateRoute("/v1/search/Movie/near-text"))
		assert.False(t, restsearch.IsSearchRoute("/v1/aggregate/Movie"))
	})
}

// TestAddSearchBodyLimit: a Content-Length over the cap is refused before the
// handler reads the body.
func TestAddSearchBodyLimit(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		contentLength int64
		wantNext      bool
		wantStatus    int
	}{
		{
			name:          "oversize search body is refused",
			path:          "/v1/search/Movie/near-text",
			contentLength: restsearch.MaxBodyBytes + 1,
			wantStatus:    http.StatusRequestEntityTooLarge,
		},
		{
			name:          "oversize aggregate body is refused",
			path:          "/v1/aggregate/Movie",
			contentLength: restsearch.MaxBodyBytes + 1,
			wantStatus:    http.StatusRequestEntityTooLarge,
		},
		{
			name:          "a body at the limit is allowed",
			path:          "/v1/search/Movie/near-text",
			contentLength: restsearch.MaxBodyBytes,
			wantNext:      true,
			wantStatus:    http.StatusTeapot,
		},
		{
			name:          "an ordinary search body is allowed",
			path:          "/v1/search/Movie/bm25",
			contentLength: 17,
			wantNext:      true,
			wantStatus:    http.StatusTeapot,
		},
		{
			name:          "a non-search route keeps its own body rules",
			path:          "/v1/objects",
			contentLength: restsearch.MaxBodyBytes + 1,
			wantNext:      true,
			wantStatus:    http.StatusTeapot,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next := &nextRecorder{}
			req := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(`{"query":"space"}`))
			req.ContentLength = test.contentLength
			rec := httptest.NewRecorder()

			addSearchBodyLimit(next).ServeHTTP(rec, req)

			assert.Equal(t, test.wantNext, next.called)
			assert.Equal(t, test.wantStatus, rec.Code)
			if test.wantNext {
				return
			}
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			var body models.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Len(t, body.Error, 1)
			assert.Equal(t, fmt.Sprintf("request body exceeds the %d byte limit", restsearch.MaxBodyBytes),
				body.Error[0].Message)
		})
	}
}
