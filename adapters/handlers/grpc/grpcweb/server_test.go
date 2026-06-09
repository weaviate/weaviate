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
)

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
