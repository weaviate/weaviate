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
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
)

// oneNodeResolver points every node name at the test server.
type oneNodeResolver struct{ host string }

func (r oneNodeResolver) NodeHostname(string) (string, bool) { return r.host, true }

// Exercises the real mux registration against the real clients, so a route
// mounted anywhere other than where its client asks for it fails here rather
// than 404ing in production, where it would read as "older build" and silently
// disable the reindex gate. A typo in the shared path constant itself is
// caught by entities/clusterprobe/markers_test.go.
func TestProbeRoutesAnswerTheClientsThatCallThem(t *testing.T) {
	const (
		node       = "node1"
		backupID   = "backup-1"
		collection = "Movies"
	)

	mux := http.NewServeMux()
	clusterapi.RegisterProbeRoutes(mux,
		clusterapi.NewBackups(nil, busyBackupProbe(t, backupID), clusterapi.NewNoopAuthHandler()).NodeActivity(),
		clusterapi.NewReindexCleanup(
			func() clusterapi.ReindexCleanupProber { return &stubCleanupProber{cleaningUp: true} },
			clusterapi.NewNoopAuthHandler(), nil).Activity())
	// The catch-all a node really serves, so an unmatched path 404s the way a
	// build without the route would rather than the way this mux would.
	mux.Handle("/", http.NotFoundHandler())

	server := httptest.NewServer(mux)
	defer server.Close()

	host := mustHost(t, server.URL)
	resolver := oneNodeResolver{host: host}

	t.Run("node activity", func(t *testing.T) {
		activity, err := clients.NewClusterBackupActivity(server.Client(), resolver).
			NodeActivity(context.Background(), node)
		require.NoError(t, err, "the client must reach the route the mux mounts")
		require.True(t, activity.Busy)
		require.Equal(t, backupID, activity.ID)
	})

	t.Run("reindex cleanup activity", func(t *testing.T) {
		cleaningUp, err := clients.NewClusterReindexCleanup(server.Client(), resolver).
			CleanupInProgress(context.Background(), node, collection)
		require.NoError(t, err, "the client must reach the route the mux mounts")
		require.True(t, cleaningUp)
	})
}

// assertRejectsNonGET checks that a probe route mounted at path answers every
// write method with 405. Each method is its own subtest, so a route that
// answers one of them names it.
func assertRejectsNonGET(t *testing.T, server *httptest.Server, path string) {
	t.Helper()
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequest(method, server.URL+path, nil)
			require.NoError(t, err)

			res, err := server.Client().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			assert.Equal(t, http.StatusMethodNotAllowed, res.StatusCode,
				"a read-only probe must not answer writes")
		})
	}
}

func mustHost(t *testing.T, rawURL string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	return parsed.Host
}
