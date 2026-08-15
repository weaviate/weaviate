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

type thisServer string

func (s thisServer) NodeHostname(string) (string, bool) {
	return strings.TrimPrefix(string(s), "http://"), true
}

var probeAuth = cluster.AuthConfig{BasicAuth: cluster.BasicAuth{Username: "node", Password: "s3cret"}}

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

// The probe route discloses whether the cluster is mid-backup, so it must
// stay behind the cluster's own credentials.
func TestClusterMuxServesTheProbeRouteBehindAuth(t *testing.T) {
	server := httptest.NewServer(newClusterMux(probeAppState(t), NewBasicAuthHandler(probeAuth)))
	defer server.Close()

	tests := []struct {
		name       string
		user, pass string
		wantCode   int
		wantBody   string
	}{
		{
			name:     "no credentials",
			wantCode: http.StatusUnauthorized,
		},
		{
			name:     "the cluster's own credentials",
			user:     probeAuth.BasicAuth.Username,
			pass:     probeAuth.BasicAuth.Password,
			wantCode: http.StatusOK,
			wantBody: `{"probe":"weaviate/backup-node-activity","node":"node1","busy":false}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, server.URL+clusterprobe.BackupNodeActivityPath, nil)
			require.NoError(t, err)
			if tt.user != "" {
				req.SetBasicAuth(tt.user, tt.pass)
			}

			res, err := server.Client().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)

			require.Equal(t, tt.wantCode, res.StatusCode,
				"404 means the table does not mount the path, 503 means the handler was reached "+
					"with no prober, so the route no longer goes through the build-time guard, "+
					"and a 200 without credentials means it mounts it unguarded")
			if tt.wantBody != "" {
				assert.JSONEq(t, tt.wantBody, string(body))
			}
		})
	}
}

// The table snapshots the probe, and startup assigns it a dozen lines earlier.
// A reordering must stop this node here, not surface later as a nil-pointer
// panic on every peer request that reaches the route.
func TestClusterMuxRefusesToBuildBeforeTheProbeIsAssigned(t *testing.T) {
	appState := probeAppState(t)
	appState.BackupActivity = nil

	assert.PanicsWithValue(t, "clusterapi: cluster mux built before the backup node-activity probe was assigned", func() { newClusterMux(appState, NewNoopAuthHandler()) })
}

// The same table read by the client that ships with it, so a route that is
// mounted but unreadable fails here too, as does a client whose credentials are
// not the ones the table demands.
func TestClusterMuxAnswersTheRealClient(t *testing.T) {
	server := httptest.NewServer(newClusterMux(probeAppState(t), NewBasicAuthHandler(probeAuth)))
	defer server.Close()
	client := clients.NewClusterBackupActivity(probeAuth, time.Second, thisServer(server.URL))

	activity, err := client.NodeActivity(context.Background(), "node1")

	require.NoError(t, err)
	assert.Equal(t, backup.NodeActivity{Answered: true}, activity)
}
