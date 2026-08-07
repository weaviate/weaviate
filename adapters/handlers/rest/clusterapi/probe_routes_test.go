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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
)

// oneNodeResolver points every node name at the test server.
type oneNodeResolver struct{ host string }

func (r oneNodeResolver) NodeHostname(string) (string, bool) { return r.host, true }

// A probe route mounted on one path and requested on another answers 404, and a
// 404 from a node is what every probe client reads as "this node runs an older
// build" and lets through. A typo would therefore disable the reindex gate
// rather than break it, which no other test would notice.
//
// So the real mux registration and the real clients meet here: the paths are
// never written down in this file.
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

func mustHost(t *testing.T, rawURL string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	return parsed.Host
}
