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

package grpcweb

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/cors"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// TestCORSPreflightAllowsConfiguredHeaders pins the trim bug across several
// CORS_ALLOW_HEADERS separator styles: an untrimmed split leaves " Authorization"
// for the default ", "-separated value, which rs/cors rejects, blocking
// authenticated browser grpc-web requests. Every shape must allow Authorization
// (and the grpc-web protocol headers) regardless of separator whitespace.
func TestCORSPreflightAllowsConfiguredHeaders(t *testing.T) {
	shapes := []struct {
		name         string
		allowHeaders string
	}{
		{`default ", "-separated`, config.DefaultCORSAllowHeaders},
		{"comma-only, no spaces", "Content-Type,Authorization"},
		{"irregular whitespace", "Content-Type ,  Authorization ,Batch"},
	}
	// Allowed in every shape: Content-Type/Authorization are present in all the
	// configured values above, and X-Grpc-Web is always appended by corsOptions.
	requestHeaderCases := []string{
		"authorization", // the header the bug silently dropped
		"content-type",
		// grpc-web protocol + Weaviate client headers an unmodified browser
		// client sends; appended by corsOptions, not part of CORS_ALLOW_HEADERS.
		"x-grpc-web",
		"x-user-agent",
		"grpc-timeout",
		"x-weaviate-client",
		"authorization,content-type",
		// the full unmodified-client preflight, all at once.
		"authorization,content-type,x-user-agent,grpc-timeout,x-weaviate-client",
	}
	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			handler := cors.New(corsOptions(config.CORS{
				AllowOrigin:  config.DefaultCORSAllowOrigin,
				AllowMethods: config.DefaultCORSAllowMethods,
				AllowHeaders: sh.allowHeaders,
			})).Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			for _, reqHeaders := range requestHeaderCases {
				t.Run(reqHeaders, func(t *testing.T) {
					req := httptest.NewRequest(http.MethodOptions, "/grpc-web/weaviate.v1.Weaviate/Search", nil)
					req.Header.Set("Origin", "http://localhost:3000")
					req.Header.Set("Access-Control-Request-Method", http.MethodPost)
					req.Header.Set("Access-Control-Request-Headers", reqHeaders)
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, req)

					// rs/cors signals "allowed" by echoing Access-Control-Allow-Origin;
					// a rejected preflight omits it entirely.
					require.NotEmpty(t, rec.Header().Get("Access-Control-Allow-Origin"),
						"preflight for %q rejected (ACAH=%q)", reqHeaders,
						rec.Header().Get("Access-Control-Allow-Headers"))
				})
			}
		})
	}
}

func TestMount(t *testing.T) {
	const prefix = "/grpc-web"
	tests := []struct {
		name      string
		path      string
		wantRoute string // "grpcweb" or "next"
		wantPath  string // path the grpc-web handler observes after prefix strip
	}{
		{"service method", "/grpc-web/weaviate.v1.Weaviate/Search", "grpcweb", "/weaviate.v1.Weaviate/Search"},
		{"bare prefix", "/grpc-web", "grpcweb", ""},
		{"prefix root slash", "/grpc-web/", "grpcweb", "/"},
		{"rest path", "/v1/objects", "next", ""},
		{"root", "/", "next", ""},
		{"prefix lookalike not stolen", "/grpc-web-extra", "next", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotRoute, gotPath string
			grpcWeb := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				gotRoute, gotPath = "grpcweb", r.URL.Path
			})
			next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
				gotRoute = "next"
			})

			Mount(prefix, grpcWeb, next, func() bool { return true }).ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(http.MethodPost, tt.path, nil),
			)

			if gotRoute != tt.wantRoute {
				t.Fatalf("path %q routed to %q, want %q", tt.path, gotRoute, tt.wantRoute)
			}
			if tt.wantRoute == "grpcweb" && gotPath != tt.wantPath {
				t.Fatalf("path %q: grpc-web handler saw %q, want %q", tt.path, gotPath, tt.wantPath)
			}
		})
	}
}

// TestMountRuntimeToggle pins the genuinely-dynamic behaviour: the enable switch
// is read per request off a live DynamicValue (as the grpc_web_enabled runtime
// override would be), so flipping it changes routing on a single already-built
// handler without a restart. Disabling must route grpc-web paths to next (REST)
// rather than 500/hang, and plain REST paths must be unaffected in every state.
func TestMountRuntimeToggle(t *testing.T) {
	enabled := runtime.NewDynamicValue(true)

	var gotRoute string
	grpcWeb := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { gotRoute = "grpcweb" })
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { gotRoute = "next" })
	handler := Mount("/grpc-web", grpcWeb, next, enabled.Get)

	route := func(path string) string {
		gotRoute = ""
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, path, nil))
		return gotRoute
	}

	const grpcPath = "/grpc-web/weaviate.v1.Weaviate/Search"
	const restPath = "/v1/objects"

	// default (enabled): grpc-web served, REST untouched
	require.Equal(t, "grpcweb", route(grpcPath))
	require.Equal(t, "next", route(restPath))

	// flip off on the live handler: grpc-web now falls through to REST
	require.NoError(t, enabled.SetValue(false))
	require.Equal(t, "next", route(grpcPath))
	require.Equal(t, "next", route(restPath), "plain REST unaffected while disabled")

	// flip back on: grpc-web served again, no rebuild
	require.NoError(t, enabled.SetValue(true))
	require.Equal(t, "grpcweb", route(grpcPath))
	require.Equal(t, "next", route(restPath))
}
