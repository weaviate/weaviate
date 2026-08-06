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

	"github.com/weaviate/weaviate/entities/clusterprobe"
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
			// The common shape: an nginx with `add_header X-Content-Type-Options
			// nosniff` at the http{} level, which makes the header alone say
			// nothing about who wrote the 404.
			name: "proxy that adds nosniff to every answer",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Content-Type-Options", "nosniff")
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html><body><center><h1>404 Not Found</h1></center></body></html>"))
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

// A 200 saying "nothing running here" is the other answer that clears a node
// without hearing from it, so it has to prove it came from a node. Absent a
// marker, every JSON object an intermediary can return decodes to "free".
func TestNodeProbeOnly200sFromTheNodeMeanFree(t *testing.T) {
	notFromANode := []struct {
		name string
		body string
	}{
		{name: "empty object", body: `{}`},
		{name: "json null", body: `null`},
		{name: "an intermediary's error object", body: `{"error":"no route matched"}`},
		{name: "the answer's fields without the marker", body: `{"busy":false,"cleaningUp":false}`},
		{
			name: "another route's marker",
			body: `{"probe":"weaviate/something-else","busy":false,"cleaningUp":false}`,
		},
	}

	for _, tt := range notFromANode {
		t.Run("backup activity/"+tt.name, func(t *testing.T) {
			server := jsonServer(t, tt.body)
			c := NewClusterBackupActivity(server.Client(), resolverFor(t, "node1", server.URL))

			_, err := c.NodeActivity(context.Background(), "node1")
			require.Error(t, err, "a 200 that does not identify itself must not read as 'not busy'")
			assert.ErrorContains(t, err, "did not come from a Weaviate node")
			assert.NotErrorIs(t, err, ErrNodeActivityUnsupported)
		})

		t.Run("reindex cleanup/"+tt.name, func(t *testing.T) {
			server := jsonServer(t, tt.body)
			c := NewClusterReindexCleanup(server.Client(), resolverFor(t, "node1", server.URL))

			_, err := c.CleanupInProgress(context.Background(), "node1", "Movies")
			require.Error(t, err, "a 200 that does not identify itself must not read as 'no cleanup'")
			assert.ErrorContains(t, err, "did not come from a Weaviate node")
			assert.NotErrorIs(t, err, ErrReindexCleanupUnsupported)
		})
	}

	// The marker alone is not enough: a node's answer also has to say what it
	// is answering, or the missing field decodes to the permissive value.
	t.Run("backup activity marked but silent about busy", func(t *testing.T) {
		server := jsonServer(t, `{"probe":"weaviate/backup-node-activity"}`)
		c := NewClusterBackupActivity(server.Client(), resolverFor(t, "node1", server.URL))

		_, err := c.NodeActivity(context.Background(), "node1")
		require.ErrorContains(t, err, `no "busy" field`)
	})

	t.Run("reindex cleanup marked but silent about cleaningUp", func(t *testing.T) {
		server := jsonServer(t, `{"probe":"weaviate/reindex-cleanup-activity"}`)
		c := NewClusterReindexCleanup(server.Client(), resolverFor(t, "node1", server.URL))

		_, err := c.CleanupInProgress(context.Background(), "node1", "Movies")
		require.ErrorContains(t, err, `no "cleaningUp" field`)
	})
}

func jsonServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

// Both sizes are absolute, not derived from maxProbeResponseBytes: a payload
// sized from the constant moves with it, so raising the limit could never red
// this. As written, raising it past 128 KiB reds the refusal row and lowering
// it below 32 KiB reds the accepted row.
func TestNodeProbeBoundsTheResponseBody(t *testing.T) {
	tests := []struct {
		name    string
		padding int
		wantErr bool
	}{
		{name: "a 32KiB answer is read", padding: 32 << 10},
		{name: "a 128KiB answer is refused", padding: 128 << 10, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"probe":%q,"cleaningUp":false,"pad":%q}`,
					clusterprobe.ReindexCleanupMarker, strings.Repeat("x", tt.padding))
			}))
			defer server.Close()

			client := NewClusterReindexCleanup(server.Client(), resolverFor(t, "node1", server.URL))
			cleaningUp, err := client.CleanupInProgress(context.Background(), "node1", "Movies")
			if tt.wantErr {
				require.ErrorContains(t, err, "response exceeds")
				return
			}
			require.NoError(t, err, "a probe answer well under the bound must still be read")
			require.False(t, cleaningUp)
		})
	}
}
