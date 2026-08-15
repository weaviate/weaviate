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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/entities/clusterprobe"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
)

type stubProber struct {
	activity backup.NodeActivity
	calls    int
}

func (p *stubProber) Node() string { return "node1" }

func (p *stubProber) Activity() backup.NodeActivity {
	p.calls++
	return p.activity
}

func serveNodeActivity(t *testing.T, prober *stubProber, auth cluster.AuthConfig) *httptest.Server {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	backups := clusterapi.NewBackups(nil, prober, clusterapi.NewBasicAuthHandler(auth), logger)
	mux := http.NewServeMux()
	mux.Handle(clusterprobe.BackupNodeActivityPath, backups.NodeActivity())
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func TestBackupNodeActivityRoute(t *testing.T) {
	tests := []struct {
		name     string
		activity backup.NodeActivity
		method   string
		wantCode int
		wantBody string
	}{
		{
			name:     "idle names neither a kind nor an id",
			activity: backup.NodeActivity{Answered: true},
			method:   http.MethodGet,
			wantCode: http.StatusOK,
			wantBody: `{"probe":"weaviate/backup-node-activity","node":"node1","busy":false}`,
		},
		{
			// Nothing produces this today. It is here because if something ever
			// does, it must leave as busy rather than clear the node it came from.
			name:     "an activity nothing decided",
			method:   http.MethodGet,
			wantCode: http.StatusOK,
			wantBody: `{"probe":"weaviate/backup-node-activity","node":"node1","busy":true}`,
		},
		{
			name:     "busy with a backup",
			activity: backup.NodeActivity{Answered: true, Busy: true, Kind: "backup", ID: "b1"},
			method:   http.MethodGet,
			wantCode: http.StatusOK,
			wantBody: `{"probe":"weaviate/backup-node-activity","node":"node1","busy":true,"kind":"backup","id":"b1"}`,
		},
		{
			name:     "busy with a restore",
			activity: backup.NodeActivity{Answered: true, Busy: true, Kind: "restore", ID: "r1"},
			method:   http.MethodGet,
			wantCode: http.StatusOK,
			wantBody: `{"probe":"weaviate/backup-node-activity","node":"node1","busy":true,"kind":"restore","id":"r1"}`,
		},
		{name: "POST", method: http.MethodPost, wantCode: http.StatusMethodNotAllowed},
		{name: "DELETE", method: http.MethodDelete, wantCode: http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prober := &stubProber{activity: tt.activity}
			server := serveNodeActivity(t, prober, cluster.AuthConfig{})

			res := do(t, server, tt.method, "", "")
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, res.StatusCode)
			if tt.wantBody != "" {
				assert.JSONEq(t, tt.wantBody, string(body))
			}
		})
	}
}

// The answer says whether the cluster is mid-backup, so a caller that fails the
// cluster's basic auth must not learn it — and must not cost a slot read either.
func TestBackupNodeActivityRouteAuth(t *testing.T) {
	auth := cluster.AuthConfig{BasicAuth: cluster.BasicAuth{Username: "node", Password: "s3cret"}}

	tests := []struct {
		name       string
		user, pass string
		wantCode   int
		wantCalls  int
	}{
		{name: "no credentials", wantCode: http.StatusUnauthorized},
		{name: "wrong password", user: "node", pass: "guess", wantCode: http.StatusUnauthorized},
		{name: "wrong user", user: "intruder", pass: "s3cret", wantCode: http.StatusUnauthorized},
		{name: "the cluster's own credentials", user: "node", pass: "s3cret", wantCode: http.StatusOK, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prober := &stubProber{activity: backup.NodeActivity{Answered: true, Busy: true, Kind: "backup", ID: "b1"}}
			server := serveNodeActivity(t, prober, auth)

			res := do(t, server, http.MethodGet, tt.user, tt.pass)
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, res.StatusCode)
			assert.Equal(t, tt.wantCalls, prober.calls)
			if tt.wantCode == http.StatusUnauthorized {
				assert.Empty(t, body)
			}
		})
	}
}

func TestBackupNodeActivityRouteLogs(t *testing.T) {
	const probes = 5

	tests := []struct {
		name string
		// The unwired case hands NewBackups an untyped nil: a nil *stubProber
		// would still satisfy the prober interface.
		newHandler func(logrus.FieldLogger) http.Handler
		wantLevel  logrus.Level
		wantLines  int
		wantFields map[string]any
	}{
		{
			name: "a wired probe logs its verdict every time",
			newHandler: func(logger logrus.FieldLogger) http.Handler {
				prober := &stubProber{activity: backup.NodeActivity{Answered: true, Busy: true, Kind: "backup", ID: "b1"}}
				return clusterapi.NewBackups(nil, prober, clusterapi.NewNoopAuthHandler(), logger).NodeActivity()
			},
			wantLevel: logrus.DebugLevel,
			wantLines: probes,
			wantFields: map[string]any{
				"busy": true, "kind": "backup", "id": `"b1"`,
			},
		},
		{
			name: "an unwired probe is warned about once",
			newHandler: func(logger logrus.FieldLogger) http.Handler {
				return clusterapi.NewBackups(nil, nil, clusterapi.NewNoopAuthHandler(), logger).NodeActivity()
			},
			wantLevel: logrus.WarnLevel,
			wantLines: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			logger.SetLevel(logrus.DebugLevel)
			handler := tt.newHandler(logger)

			for range probes {
				req := httptest.NewRequest(http.MethodGet, clusterprobe.BackupNodeActivityPath, nil)
				handler.ServeHTTP(httptest.NewRecorder(), req)
			}

			require.Len(t, hook.AllEntries(), tt.wantLines)
			for _, entry := range hook.AllEntries() {
				assert.Equal(t, tt.wantLevel, entry.Level)
				for field, want := range tt.wantFields {
					assert.Equal(t, want, entry.Data[field])
				}
			}
		})
	}
}

func TestNewBackupsRefusesToBuildWithoutALogger(t *testing.T) {
	assert.PanicsWithValue(t, "clusterapi: NewBackups needs a logger", func() {
		clusterapi.NewBackups(nil, &stubProber{}, clusterapi.NewNoopAuthHandler(), nil)
	})
}

type oneNode string

func (n oneNode) NodeHostname(string) (string, bool) {
	return strings.TrimPrefix(string(n), "http://"), true
}

func TestBackupNodeActivityEndToEnd(t *testing.T) {
	auth := cluster.AuthConfig{BasicAuth: cluster.BasicAuth{Username: "node", Password: "s3cret"}}

	tests := []struct {
		name string
		want backup.NodeActivity
	}{
		{name: "idle", want: backup.NodeActivity{Answered: true}},
		{name: "busy", want: backup.NodeActivity{Answered: true, Busy: true, Kind: "restore", ID: "r-7"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := serveNodeActivity(t, &stubProber{activity: tt.want}, auth)
			client := clients.NewClusterBackupActivity(auth, time.Second, oneNode(server.URL))

			got, err := client.NodeActivity(context.Background(), "node1")

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func do(t *testing.T, server *httptest.Server, method, user, pass string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+clusterprobe.BackupNodeActivityPath, nil)
	require.NoError(t, err)
	if user != "" || pass != "" {
		req.SetBasicAuth(user, pass)
	}
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return res
}
