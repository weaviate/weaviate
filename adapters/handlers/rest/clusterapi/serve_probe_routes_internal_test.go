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
	"strings"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	classificationrepo "github.com/weaviate/weaviate/adapters/repos/classifications"
	entbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/clusterprobe"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
)

// idleSchema is the smallest schema manager a backup.Handler can be built over.
// The probe only reads the handler's operation slots, so nothing here is called.
type idleSchema struct{}

func (idleSchema) RestoreClass(context.Context, *entbackup.ClassDescriptor, map[string]string, bool, bool) error {
	return nil
}
func (idleSchema) NodeName() string         { return "node1" }
func (idleSchema) NamespacesEnabled() bool  { return false }
func (idleSchema) ClassEqual(string) string { return "" }

// thisServer points every node name at the one test server.
type thisServer string

func (s thisServer) NodeHostname(string) (string, bool) {
	return strings.TrimPrefix(string(s), "http://"), true
}

// probeAppState carries the fields newClusterMux reads while building the
// table. The rest it only stores, so a nil is never dereferenced.
func probeAppState(t *testing.T) *state.State {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	manager := backup.NewHandler(logger, config.Backup{}, nil, idleSchema{}, nil, nil, nil, nil)
	return &state.State{
		Logger:             logger,
		ServerConfig:       &config.WeaviateConfig{},
		ClassificationRepo: &classificationrepo.DistributedRepo{},
		BackupManager:      manager,
		BackupActivity:     backup.NewNodeActivityProbe(manager),
	}
}

// The probe is live only if the server's real route table mounts the real path
// and hands the backup handler the app state's probe. Both are single lines in
// newClusterMux, and a node that loses either answers every peer with the one
// thing a peer may not act on: an answer that reads as "nothing running here".
func TestClusterMuxServesTheProbeRoute(t *testing.T) {
	server := httptest.NewServer(newClusterMux(probeAppState(t), NewNoopAuthHandler()))
	defer server.Close()

	res, err := server.Client().Get(server.URL + clusterprobe.BackupNodeActivityPath)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, res.StatusCode,
		"404 means the table does not mount the path, 503 means it mounts it without the app state's probe")
	assert.JSONEq(t, `{"probe":"weaviate/backup-node-activity","busy":false}`, string(body))
}

// The same table read by the client that ships with it, so a route that is
// mounted but unreadable fails here too.
func TestClusterMuxAnswersTheRealClient(t *testing.T) {
	server := httptest.NewServer(newClusterMux(probeAppState(t), NewNoopAuthHandler()))
	defer server.Close()
	client := clients.NewClusterBackupActivity(cluster.AuthConfig{}, time.Second, thisServer(server.URL))

	activity, err := client.NodeActivity(context.Background(), "node1")

	require.NoError(t, err)
	assert.Equal(t, backup.NodeActivity{}, activity)
}
