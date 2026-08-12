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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	classificationrepo "github.com/weaviate/weaviate/adapters/repos/classifications"
	"github.com/weaviate/weaviate/entities/clusterprobe"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

// everyNodeIsThisServer points every node name at the test server, so the real
// clients can be pointed at the real route table.
type everyNodeIsThisServer struct{ host string }

func (r everyNodeIsThisServer) NodeHostname(string) (string, bool) { return r.host, true }

func hostOf(t *testing.T, rawURL string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	return parsed.Host
}

// probeAppState carries the four fields newClusterMux reads while building the
// table. Everything else it only stores, so a nil is never dereferenced.
func probeAppState(t *testing.T) *state.State {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	return &state.State{
		Logger:             logger,
		ServerConfig:       &config.WeaviateConfig{},
		ClassificationRepo: &classificationrepo.DistributedRepo{},
		BackupActivity:     backup.NewNodeActivityProbe(nil),
	}
}

// The probe routes are live only if the server's real route table mounts them
// and hands the backup handler the real probe. Both are single lines in
// newClusterMux, and a node that loses either answers every peer with the one
// thing a peer may not act on: an answer that reads as "nothing running here".
func TestClusterMuxServesTheProbeRoutes(t *testing.T) {
	server := httptest.NewServer(newClusterMux(probeAppState(t), NewNoopAuthHandler()))
	defer server.Close()

	t.Run("backup node activity answers from a real probe", func(t *testing.T) {
		res, err := server.Client().Get(server.URL + clusterprobe.BackupNodeActivityPath)
		require.NoError(t, err)
		defer res.Body.Close()

		require.Equal(t, http.StatusOK, res.StatusCode,
			"503 here means the route was built without the app state's probe")
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		assert.JSONEq(t, `{"probe":"weaviate/backup-node-activity","busy":false}`, string(body))
	})

	t.Run("reindex cleanup answers not wired", func(t *testing.T) {
		res, err := server.Client().Get(server.URL + clusterprobe.ReindexCleanupActivityPath +
			"?collection=Movies")
		require.NoError(t, err)
		defer res.Body.Close()

		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		assert.Equal(t, clusterprobe.ProbeNotWiredMarker, strings.TrimSpace(string(body)))
	})
}

// The same table, read by the clients that ship with it, so a route that is
// mounted but unreadable fails here too.
func TestClusterMuxAnswersTheRealClients(t *testing.T) {
	server := httptest.NewServer(newClusterMux(probeAppState(t), NewNoopAuthHandler()))
	defer server.Close()

	resolver := everyNodeIsThisServer{host: hostOf(t, server.URL)}

	activity, err := clients.NewClusterBackupActivity(server.Client(), resolver).
		NodeActivity(context.Background(), "node1")
	require.NoError(t, err)
	assert.False(t, activity.Busy)

	_, err = clients.NewClusterReindexCleanup(server.Client(), resolver).
		CleanupInProgress(context.Background(), "node1", "Movies")
	require.ErrorIs(t, err, clients.ErrReindexCleanupUnsupported,
		"the route is mounted with no resolver behind it until 12474")
}

// A build without the probe routes falls through to the catch-all every node
// serves, and both clients have to read that as "older build, let it through".
// If the catch-all ever answers with a branded or JSON 404, every peer flips to
// a hard error and the consumer's gate fails closed for the whole upgrade.
func TestOlderBuildCatchAllReadsAsAnOlderBuild(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/", index())
	server := httptest.NewServer(mux)
	defer server.Close()

	res, err := server.Client().Get(server.URL + clusterprobe.BackupNodeActivityPath)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Equal(t, "404 page not found", strings.TrimSpace(string(body)))
	assert.Equal(t, "nosniff", res.Header.Get("X-Content-Type-Options"))

	resolver := everyNodeIsThisServer{host: hostOf(t, server.URL)}

	_, err = clients.NewClusterBackupActivity(server.Client(), resolver).
		NodeActivity(context.Background(), "node1")
	require.ErrorIs(t, err, clients.ErrNodeActivityUnsupported)

	_, err = clients.NewClusterReindexCleanup(server.Client(), resolver).
		CleanupInProgress(context.Background(), "node1", "Movies")
	require.ErrorIs(t, err, clients.ErrReindexCleanupUnsupported)
}
