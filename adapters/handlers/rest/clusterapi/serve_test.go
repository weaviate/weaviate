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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			name:     "indices route with dynamic path",
			req:      newRequest(t, "/indices/objects/Movies"), // /indices/ paths now use indicesStaticRoute
			expected: "/indices/",                              // falls back to /indices/ for unknown patterns
		},
		{
			name:     "matched route with dynamic path",
			req:      newRequest(t, "/replicas/objects/Movies"), // matched route (not /replicas/indices/)
			expected: "/replicas/",                              // yay!
		},
		{
			name:     "matched route with dynamic path 2",
			req:      newRequest(t, "/replicas/objects/Movies2"), // matched route.
			expected: "/replicas/",                               // yay!
		},
		{
			name:     "replicas/indices route with dynamic path",
			req:      newRequest(t, "/replicas/indices/Movies/shards/shard0/objects"),
			expected: "/replicas/indices/{class}/shards/{shard}/objects",
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

func Test_indicesStaticRoute(t *testing.T) {
	cases := []struct {
		path     string
		expected string
	}{
		// Search, find, aggregations
		{"/indices/MyClass/shards/shard0/objects/_search", "/indices/{class}/shards/{shard}/objects/_search"},
		{"/indices/Article/shards/xyz123/objects/_find", "/indices/{class}/shards/{shard}/objects/_find"},
		{"/indices/Product/shards/main/objects/_aggregations", "/indices/{class}/shards/{shard}/objects/_aggregations"},

		// Objects batch operations
		{"/indices/MyClass/shards/shard0/objects", "/indices/{class}/shards/{shard}/objects"},
		{"/indices/MyClass/shards/shard0/objects:overwrite", "/indices/{class}/shards/{shard}/objects:overwrite"},
		{"/indices/MyClass/shards/shard0/objects:digest", "/indices/{class}/shards/{shard}/objects:digest"},
		{"/indices/MyClass/shards/shard0/objects:digestsInRange", "/indices/{class}/shards/{shard}/objects:digestsInRange"},

		// Hashtree
		{"/indices/MyClass/shards/shard0/objects/hashtree/0", "/indices/{class}/shards/{shard}/objects/hashtree/{level}"},
		{"/indices/MyClass/shards/shard0/objects/hashtree/5", "/indices/{class}/shards/{shard}/objects/hashtree/{level}"},
		{"/indices/MyClass/shards/shard0/objects/hashtree/123", "/indices/{class}/shards/{shard}/objects/hashtree/{level}"},

		// Single object by ID
		{"/indices/MyClass/shards/shard0/objects/550e8400-e29b-41d4-a716-446655440000", "/indices/{class}/shards/{shard}/objects/{id}"},
		{"/indices/MyClass/shards/shard0/objects/some-uuid-here", "/indices/{class}/shards/{shard}/objects/{id}"},

		// References
		{"/indices/MyClass/shards/shard0/references", "/indices/{class}/shards/{shard}/references"},

		// Shard metadata
		{"/indices/MyClass/shards/shard0/queuesize", "/indices/{class}/shards/{shard}/queuesize"},
		{"/indices/MyClass/shards/shard0/status", "/indices/{class}/shards/{shard}/status"},

		// File operations
		{"/indices/MyClass/shards/shard0/files/lsm/segment.db", "/indices/{class}/shards/{shard}/files/{path}"},
		{"/indices/MyClass/shards/shard0/files/main.hnsw.commitlog", "/indices/{class}/shards/{shard}/files/{path}"},
		{"/indices/MyClass/shards/shard0/files:metadata/lsm/segment.db", "/indices/{class}/shards/{shard}/files:metadata/{path}"},

		// Shard operations
		{"/indices/MyClass/shards/shard0", "/indices/{class}/shards/{shard}"},
		{"/indices/MyClass/shards/shard0:reinit", "/indices/{class}/shards/{shard}:reinit"},

		// Background operations
		{"/indices/MyClass/shards/shard0/background:pause", "/indices/{class}/shards/{shard}/background:pause"},
		{"/indices/MyClass/shards/shard0/background:resume", "/indices/{class}/shards/{shard}/background:resume"},
		{"/indices/MyClass/shards/shard0/background:list", "/indices/{class}/shards/{shard}/background:list"},

		// Async replication
		{"/indices/MyClass/shards/shard0/async-replication-target-node", "/indices/{class}/shards/{shard}/async-replication-target-node"},

		// Various class and shard name formats
		{"/indices/My_Class_123/shards/shard_0/objects/_search", "/indices/{class}/shards/{shard}/objects/_search"},
		{"/indices/A/shards/B/objects", "/indices/{class}/shards/{shard}/objects"},

		// Fallback for unknown patterns
		{"/indices/", "/indices/"},
		{"/indices/unknown/path/here", "/indices/"},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			got := indicesStaticRoute(tc.path)
			assert.Equal(t, tc.expected, got, "path: %s", tc.path)
		})
	}
}

func Test_replicasIndicesStaticRoute(t *testing.T) {
	cases := []struct {
		path     string
		expected string
	}{
		// Objects batch operations
		{"/replicas/indices/MyClass/shards/shard0/objects", "/replicas/indices/{class}/shards/{shard}/objects"},

		// Single object by ID
		{"/replicas/indices/MyClass/shards/shard0/objects/550e8400-e29b-41d4-a716-446655440000", "/replicas/indices/{class}/shards/{shard}/objects/{id}"},
		{"/replicas/indices/MyClass/shards/shard0/objects/some-uuid-here", "/replicas/indices/{class}/shards/{shard}/objects/{id}"},

		// Object with deletion timestamp
		{"/replicas/indices/MyClass/shards/shard0/objects/some-uuid/1234567890123", "/replicas/indices/{class}/shards/{shard}/objects/{id}/{timestamp}"},

		// References
		{"/replicas/indices/MyClass/shards/shard0/objects/references", "/replicas/indices/{class}/shards/{shard}/objects/references"},

		// Commit phases
		{"/replicas/indices/MyClass/shards/shard0:commit", "/replicas/indices/{class}/shards/{shard}:commit"},
		{"/replicas/indices/MyClass/shards/shard0:abort", "/replicas/indices/{class}/shards/{shard}:abort"},

		// Various class and shard name formats
		{"/replicas/indices/My_Class_123/shards/shard_0/objects", "/replicas/indices/{class}/shards/{shard}/objects"},
		{"/replicas/indices/A/shards/B/objects", "/replicas/indices/{class}/shards/{shard}/objects"},

		// Fallback for unknown patterns
		{"/replicas/indices/", "/replicas/indices/"},
		{"/replicas/indices/unknown/path/here", "/replicas/indices/"},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			got := replicasIndicesStaticRoute(tc.path)
			assert.Equal(t, tc.expected, got, "path: %s", tc.path)
		})
	}
}

func Test_staticRoute_indicesIntegration(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/indices/", okHandler)
	mux.HandleFunc("/replicas/indices/", okHandler)

	cases := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "indices search route",
			path:     "/indices/Movies/shards/shard0/objects/_search",
			expected: "/indices/{class}/shards/{shard}/objects/_search",
		},
		{
			name:     "indices objects route",
			path:     "/indices/Movies/shards/shard0/objects",
			expected: "/indices/{class}/shards/{shard}/objects",
		},
		{
			name:     "indices references route",
			path:     "/indices/Movies/shards/shard0/references",
			expected: "/indices/{class}/shards/{shard}/references",
		},
		{
			name:     "indices status route",
			path:     "/indices/Movies/shards/shard0/status",
			expected: "/indices/{class}/shards/{shard}/status",
		},
		{
			name:     "replicas/indices objects route",
			path:     "/replicas/indices/Movies/shards/shard0/objects",
			expected: "/replicas/indices/{class}/shards/{shard}/objects",
		},
		{
			name:     "replicas/indices single object route",
			path:     "/replicas/indices/Movies/shards/shard0/objects/some-uuid",
			expected: "/replicas/indices/{class}/shards/{shard}/objects/{id}",
		},
		{
			name:     "replicas/indices commit route",
			path:     "/replicas/indices/Movies/shards/shard0:commit",
			expected: "/replicas/indices/{class}/shards/{shard}:commit",
		},
		{
			name:     "replicas/indices references route",
			path:     "/replicas/indices/Movies/shards/shard0/objects/references",
			expected: "/replicas/indices/{class}/shards/{shard}/objects/references",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := newRequest(t, tc.path)
			_, got := staticRoute(mux)(req)
			assert.Equal(t, tc.expected, got)
		})
	}
}
