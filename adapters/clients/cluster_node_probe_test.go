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

package clients

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A 404 is the only answer that lets a probe report a node as clear without
// hearing from it, so only a node's own catch-all 404 may produce one. The
// cluster client honours HTTP_PROXY; a proxy 404ing the cluster port would
// otherwise clear every node at once.
func TestNodeProbeOnly404sFromTheNodeMeanUnsupported(t *testing.T) {
	tests := []struct {
		name            string
		handler         http.HandlerFunc
		wantUnsupported bool
	}{
		{
			name: "node without the route",
			handler: func(w http.ResponseWriter, r *http.Request) {
				// What the cluster API's catch-all handler answers.
				http.NotFound(w, r)
			},
			wantUnsupported: true,
		},
		{
			name: "proxy error page",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html><head><title>404 Not Found</title></head><body>" +
					"<center><h1>404 Not Found</h1></center><hr><center>nginx</center></body></html>"))
			},
		},
		{
			name: "proxy with an empty 404",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
		},
		{
			name: "right body, but not written by the standard library",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("404 page not found\n"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			client := NewClusterReindexCleanup(server.Client(), resolverFor(t, "node1", server.URL))
			_, err := client.CleanupInProgress(context.Background(), "node1", "Movies")
			require.Error(t, err)

			if tt.wantUnsupported {
				require.ErrorIs(t, err, ErrReindexCleanupUnsupported)
				return
			}
			require.NotErrorIs(t, err, ErrReindexCleanupUnsupported,
				"a 404 this node did not write must not read as 'no gate here'")
			assert.ErrorContains(t, err, "did not come from the node itself")
		})
	}
}

func TestNodeProbeBoundsTheResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"cleaningUp":false,"pad":%q}`, strings.Repeat("x", maxProbeResponseBytes))
	}))
	defer server.Close()

	client := NewClusterReindexCleanup(server.Client(), resolverFor(t, "node1", server.URL))
	_, err := client.CleanupInProgress(context.Background(), "node1", "Movies")
	require.ErrorContains(t, err, "response exceeds")
}
