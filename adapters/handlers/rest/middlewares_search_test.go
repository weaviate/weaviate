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

	openapierrors "github.com/go-openapi/errors"
	"github.com/go-openapi/loads"
	"github.com/go-openapi/runtime/middleware"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	restsearch "github.com/weaviate/weaviate/adapters/handlers/rest/search"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// searchTestAppState sets the fields makeSetupGlobalMiddleware reads when
// monitoring, Sentry and telemetry are off.
func searchTestAppState(mode string) *state.State {
	logger, _ := logrustest.NewNullLogger()
	return &state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				OperationalMode: configRuntime.NewDynamicValue(mode),
				Sentry:          &entsentry.ConfigOpts{},
			},
		},
		Logger: logger,
	}
}

// routableContext returns a context whose router matches the Weaviate spec.
func routableContext(t *testing.T) *middleware.Context {
	t.Helper()
	spec, err := loads.Embedded(SwaggerJSON, FlatSwaggerJSON)
	require.NoError(t, err)
	api := operations.NewWeaviateAPI(spec)
	api.Init()
	return middleware.NewRoutableContext(spec, api, middleware.DefaultRouter(spec, api))
}

// newGlobalChain builds the production REST handler: the global chain wrapped
// around api.Serve. api.Serve runs its spec, docs and router handlers before
// the operation. A nextRecorder replaces every operation, so a request that
// reaches an operation gets 418.
func newGlobalChain(t *testing.T, appState *state.State) http.Handler {
	t.Helper()
	spec, err := loads.Embedded(SwaggerJSON, FlatSwaggerJSON)
	require.NoError(t, err)
	api := operations.NewWeaviateAPI(spec)
	inner := api.Serve(func(http.Handler) http.Handler { return &nextRecorder{} })
	return makeSetupGlobalMiddleware(appState, api.Context(), nil)(inner)
}

// nextRecorder is a sentinel next handler that records having been reached.
type nextRecorder struct {
	called bool
}

func (n *nextRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.called = true
	w.WriteHeader(http.StatusTeapot) // distinguishable from anything real
}

func TestAddOperationalMode(t *testing.T) {
	searchBody := func() *strings.Reader { return strings.NewReader(`{"query":"space"}`) }
	routes := routableContext(t)

	run := func(t *testing.T, appState *state.State, method, path string) (*nextRecorder, *httptest.ResponseRecorder) {
		t.Helper()
		next := &nextRecorder{}
		handler := addMatchedRoute(routes, addOperationalMode(appState, next))
		req := httptest.NewRequest(method, path, searchBody())
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return next, rec
	}

	// POST /v1 and * match no route, so the gate passes them on and the
	// router answers. GET /v1 matches the root route, which every mode allows.
	shortPathTests := []struct {
		name       string
		mode       string
		method     string
		path       string
		wantNext   bool
		wantStatus int
	}{
		{
			name:       "READ_ONLY leaves a write to the API root to the router",
			mode:       config.READ_ONLY,
			method:     http.MethodPost,
			path:       "/v1",
			wantNext:   true,
			wantStatus: http.StatusTeapot,
		},
		{
			name:       "SCALE_OUT leaves a write to the API root to the router",
			mode:       config.SCALE_OUT,
			method:     http.MethodPost,
			path:       "/v1",
			wantNext:   true,
			wantStatus: http.StatusTeapot,
		},
		{
			name:       "WRITE_ONLY lets the API root be read",
			mode:       config.WRITE_ONLY,
			method:     http.MethodGet,
			path:       "/v1",
			wantNext:   true,
			wantStatus: http.StatusTeapot,
		},
		{
			name:       "READ_ONLY leaves a target without path segments to the router",
			mode:       config.READ_ONLY,
			method:     http.MethodPost,
			path:       "*",
			wantNext:   true,
			wantStatus: http.StatusTeapot,
		},
		{
			name:       "READ_ONLY keeps non-v1 paths available",
			mode:       config.READ_ONLY,
			method:     http.MethodPost,
			path:       "/",
			wantNext:   true,
			wantStatus: http.StatusTeapot,
		},
	}
	for _, test := range shortPathTests {
		t.Run(test.name, func(t *testing.T) {
			next, rec := run(t, searchTestAppState(test.mode), test.method, test.path)

			assert.Equal(t, test.wantNext, next.called)
			assert.Equal(t, test.wantStatus, rec.Code)
		})
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
		{
			// the router binds A%2FB to one collection parameter
			name:          "encoded-slash search is capped",
			path:          "/v1/search/A%2FB/bm25",
			contentLength: restsearch.MaxBodyBytes + 1,
			wantStatus:    http.StatusRequestEntityTooLarge,
		},
		{
			name:          "encoded-slash aggregate is capped",
			path:          "/v1/aggregate/A%2FB",
			contentLength: restsearch.MaxBodyBytes + 1,
			wantStatus:    http.StatusRequestEntityTooLarge,
		},
		{
			name:          "unmatched search-shaped path is not capped",
			path:          "/v1/search/Movie/nosuch",
			contentLength: restsearch.MaxBodyBytes + 1,
			wantNext:      true,
			wantStatus:    http.StatusTeapot,
		},
	}

	routes := routableContext(t)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next := &nextRecorder{}
			req := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(`{"query":"space"}`))
			req.ContentLength = test.contentLength
			rec := httptest.NewRecorder()

			addMatchedRoute(routes, addSearchBodyLimit(next)).ServeHTTP(rec, req)

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

// TestGlobalChainOperationalMode drives the production global chain, so the
// gate and the body cap see only the route that addMatchedRoute adds.
func TestGlobalChainOperationalMode(t *testing.T) {
	const reference = "/v1/objects/Movie/8c29da7a-600a-43dc-85fb-83ab2b08c294/references/p"
	allModes := []string{config.READ_WRITE, config.READ_ONLY, config.SCALE_OUT, config.WRITE_ONLY}
	chains := map[string]http.Handler{}
	for _, mode := range allModes {
		chains[mode] = newGlobalChain(t, searchTestAppState(mode))
	}

	serve := func(h http.Handler, method, target string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, target, strings.NewReader(`{}`)))
		return rec
	}

	t.Run("bypass", func(t *testing.T) {
		type bypass struct{ method, target, canonical string }
		slashAndDot := []string{"//v1/objects", "/./v1/objects", "/x/../v1/objects", "/a%2Fb/../v1/objects"}
		var writes, reads []bypass
		for _, target := range slashAndDot {
			writes = append(writes, bypass{http.MethodPost, target, "/v1/objects"})
			reads = append(reads, bypass{http.MethodGet, target, "/v1/objects"})
		}
		writes = append(writes, bypass{http.MethodPost, "/v1/objects/%2E%2E/search/references/p", reference})
		reads = append(reads,
			bypass{http.MethodPost, "/v1/search/A%2FB/bm25", "/v1/search/Movie/bm25"},
			bypass{http.MethodPost, "/v1/aggregate/A%2FB", "/v1/aggregate/Movie"},
		)
		byMode := map[string][]bypass{config.READ_ONLY: writes, config.SCALE_OUT: writes, config.WRITE_ONLY: reads}

		for _, mode := range []string{config.READ_ONLY, config.SCALE_OUT, config.WRITE_ONLY} {
			h := chains[mode]
			for _, b := range byMode[mode] {
				t.Run(mode+"/"+b.method+" "+b.target, func(t *testing.T) {
					want := serve(h, b.method, b.canonical)
					require.Equal(t, http.StatusServiceUnavailable, want.Code)

					got := serve(h, b.method, b.target)
					assert.Equal(t, want.Code, got.Code)
					assert.Equal(t, want.Header().Get("Content-Type"), got.Header().Get("Content-Type"))
					assert.Equal(t, want.Body.String(), got.Body.String())
				})
			}
		}
	})

	t.Run("canonical", func(t *testing.T) {
		tests := []struct {
			mode, method, target string
			want                 int
		}{
			{config.READ_WRITE, http.MethodPost, "/v1/objects", http.StatusTeapot},
			{config.READ_WRITE, http.MethodGet, "/v1/objects", http.StatusTeapot},
			{config.READ_WRITE, http.MethodPost, "/v1/search/Movie/bm25", http.StatusTeapot},
			{config.READ_WRITE, http.MethodPost, "/v1/aggregate/Movie", http.StatusTeapot},
			{config.READ_ONLY, http.MethodPost, "/v1/objects", http.StatusServiceUnavailable},
			{config.READ_ONLY, http.MethodPost, reference, http.StatusServiceUnavailable},
			{config.READ_ONLY, http.MethodGet, "/v1/objects", http.StatusTeapot},
			{config.READ_ONLY, http.MethodPost, "/v1/search/Movie/bm25", http.StatusTeapot},
			{config.READ_ONLY, http.MethodPost, "/v1/aggregate/Movie", http.StatusTeapot},
			{config.READ_ONLY, http.MethodPost, "/v1/graphql", http.StatusTeapot},
			{config.SCALE_OUT, http.MethodPost, "/v1/objects", http.StatusServiceUnavailable},
			{config.SCALE_OUT, http.MethodGet, "/v1/objects", http.StatusTeapot},
			{config.SCALE_OUT, http.MethodPost, "/v1/search/Movie/bm25", http.StatusTeapot},
			{config.SCALE_OUT, http.MethodPost, "/v1/aggregate/Movie", http.StatusTeapot},
			{config.SCALE_OUT, http.MethodPost, "/v1/replication/replicate", http.StatusTeapot},
			{config.WRITE_ONLY, http.MethodPost, "/v1/objects", http.StatusTeapot},
			{config.WRITE_ONLY, http.MethodGet, "/v1/objects", http.StatusServiceUnavailable},
			{config.WRITE_ONLY, http.MethodPost, "/v1/search/Movie/bm25", http.StatusServiceUnavailable},
			{config.WRITE_ONLY, http.MethodPost, "/v1/aggregate/Movie", http.StatusServiceUnavailable},
			{config.WRITE_ONLY, http.MethodGet, "/v1/meta", http.StatusTeapot},
			{config.WRITE_ONLY, http.MethodGet, "/v1/nodes", http.StatusTeapot},
		}
		for _, tc := range tests {
			t.Run(tc.mode+"/"+tc.method+" "+tc.target, func(t *testing.T) {
				assert.Equal(t, tc.want, serve(chains[tc.mode], tc.method, tc.target).Code)
			})
		}
	})

	t.Run("method case", func(t *testing.T) {
		const object = "/v1/objects/Movie/8c29da7a-600a-43dc-85fb-83ab2b08c294"
		// the router upper-cases the method before matching
		tests := []struct {
			mode, method, target string
			want                 int
		}{
			{config.READ_WRITE, "get", "/v1/objects", http.StatusTeapot},
			{config.WRITE_ONLY, "get", "/v1/objects", http.StatusServiceUnavailable},
			{config.WRITE_ONLY, "Get", "/v1/objects", http.StatusServiceUnavailable},
			{config.WRITE_ONLY, "post", "/v1/objects", http.StatusTeapot},
			{config.WRITE_ONLY, "get", "/v1/meta", http.StatusTeapot},
			{config.READ_ONLY, "get", "/v1/objects", http.StatusTeapot},
			{config.READ_ONLY, "post", "/v1/objects", http.StatusServiceUnavailable},
			{config.READ_ONLY, "Post", "/v1/objects", http.StatusServiceUnavailable},
			{config.READ_ONLY, "head", object, http.StatusTeapot},
			{config.SCALE_OUT, "get", "/v1/objects", http.StatusTeapot},
			{config.SCALE_OUT, "post", "/v1/objects", http.StatusServiceUnavailable},
			{config.SCALE_OUT, "Post", "/v1/objects", http.StatusServiceUnavailable},
		}
		for _, tc := range tests {
			t.Run(tc.mode+"/"+tc.method+" "+tc.target, func(t *testing.T) {
				h := chains[tc.mode]
				want := serve(h, strings.ToUpper(tc.method), tc.target)
				require.Equal(t, tc.want, want.Code)

				got := serve(h, tc.method, tc.target)
				assert.Equal(t, want.Code, got.Code)
				if tc.want == http.StatusServiceUnavailable {
					assert.Equal(t, want.Header().Get("Content-Type"), got.Header().Get("Content-Type"))
					assert.Equal(t, want.Body.String(), got.Body.String())
				}
			})
		}
	})

	t.Run("root readable in WriteOnly", func(t *testing.T) {
		assert.Equal(t, http.StatusTeapot, serve(chains[config.WRITE_ONLY], http.MethodGet, "/v1").Code)
	})

	t.Run("unmatched", func(t *testing.T) {
		tests := []struct {
			method, target string
			want           int
		}{
			{http.MethodPost, "/v1/nosuch", http.StatusNotFound},
			{http.MethodPost, "/v1/%67raphql", http.StatusNotFound},
			{http.MethodPost, "/%761/objects", http.StatusNotFound},
			{http.MethodPost, "/v1", http.StatusMethodNotAllowed},
		}
		for _, mode := range allModes {
			for _, tc := range tests {
				t.Run(mode+"/"+tc.method+" "+tc.target, func(t *testing.T) {
					assert.Equal(t, tc.want, serve(chains[mode], tc.method, tc.target).Code)
				})
			}
		}
	})

	t.Run("no panic", func(t *testing.T) {
		// net/http answers OPTIONS * itself, so only a direct handler call
		// delivers it to the chain
		targets := []struct{ name, target string }{
			{"root", "/v1"},
			{"asterisk", "*"},
			{"empty path", "http://example.com"},
		}
		for _, mode := range allModes {
			for _, tg := range targets {
				for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodOptions} {
					t.Run(mode+"/"+method+" "+tg.name, func(t *testing.T) {
						var rec *httptest.ResponseRecorder
						assert.NotPanics(t, func() { rec = serve(chains[mode], method, tg.target) })
						if rec != nil {
							assert.NotEqual(t, http.StatusServiceUnavailable, rec.Code)
						}
					})
				}
			}
		}
	})

	t.Run("body cap is wired", func(t *testing.T) {
		tests := []struct{ name, mode, target string }{
			{"encoded-slash search in ReadWrite", config.READ_WRITE, "/v1/search/A%2FB/bm25"},
			// the cap runs before the gate, so 413 wins over 503
			{"canonical search in WriteOnly", config.WRITE_ONLY, "/v1/search/Movie/bm25"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(`{}`))
				req.ContentLength = restsearch.MaxBodyBytes + 1
				rec := httptest.NewRecorder()
				chains[tc.mode].ServeHTTP(rec, req)
				assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
			})
		}
	})
}

func TestSearchServeError(t *testing.T) {
	t.Run("an encoded slash in the collection gets the search-style 404 body", func(t *testing.T) {
		spec, err := loads.Embedded(SwaggerJSON, FlatSwaggerJSON)
		require.NoError(t, err)
		api := operations.NewWeaviateAPI(spec)
		setupSearchHandlers(api, searchTestAppState(config.READ_WRITE))

		serve := func(target string) *httptest.ResponseRecorder {
			rec := httptest.NewRecorder()
			api.ServeError(rec, httptest.NewRequest(http.MethodPost, target, nil), openapierrors.NotFound("not found"))
			return rec
		}
		want := serve("/v1/search/A/nosuch")
		got := serve("/v1/search/A%2FB/nosuch")

		assert.Equal(t, http.StatusNotFound, got.Code)
		assert.Equal(t, want.Header().Get("Content-Type"), got.Header().Get("Content-Type"))
		assert.Equal(t, want.Body.String(), got.Body.String())
	})
}
