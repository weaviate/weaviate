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
)

// TestCORSPreflightAllowsConfiguredHeaders pins the trim bug: with the default
// ", "-separated CORS_ALLOW_HEADERS, an untrimmed split leaves " Authorization"
// which rs/cors rejects, blocking authenticated browser grpc-web requests.
func TestCORSPreflightAllowsConfiguredHeaders(t *testing.T) {
	handler := cors.New(corsOptions(config.CORS{
		AllowOrigin:  config.DefaultCORSAllowOrigin,
		AllowMethods: config.DefaultCORSAllowMethods,
		AllowHeaders: config.DefaultCORSAllowHeaders,
	})).Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	tests := []struct {
		name           string
		requestHeaders string
	}{
		{"authorization (authenticated browser request)", "authorization"},
		{"content-type", "content-type"},
		{"x-grpc-web protocol header", "x-grpc-web"},
		{"authorization + content-type (real browser preflight)", "authorization,content-type"},
		{"x-weaviate-cluster-url", "x-weaviate-cluster-url"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodOptions, "/grpc-web/weaviate.v1.Weaviate/Search", nil)
			req.Header.Set("Origin", "http://localhost:3000")
			req.Header.Set("Access-Control-Request-Method", http.MethodPost)
			req.Header.Set("Access-Control-Request-Headers", tt.requestHeaders)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			// rs/cors signals "allowed" by echoing Access-Control-Allow-Origin;
			// a rejected preflight omits it entirely.
			require.NotEmpty(t, rec.Header().Get("Access-Control-Allow-Origin"),
				"preflight for %q was rejected (ACAH=%q)", tt.requestHeaders,
				rec.Header().Get("Access-Control-Allow-Headers"))
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

			Mount(prefix, grpcWeb, next).ServeHTTP(
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
