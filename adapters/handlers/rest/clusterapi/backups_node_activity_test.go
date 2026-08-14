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

func (p *stubProber) Activity() backup.NodeActivity {
	p.calls++
	return p.activity
}

func serveNodeActivity(t *testing.T, prober *stubProber, auth cluster.AuthConfig) *httptest.Server {
	t.Helper()
	backups := clusterapi.NewBackups(nil, prober, clusterapi.NewBasicAuthHandler(auth))
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
			method:   http.MethodGet,
			wantCode: http.StatusOK,
			wantBody: `{"probe":"weaviate/backup-node-activity","busy":false}`,
		},
		{
			name:     "busy with a backup",
			activity: backup.NodeActivity{Busy: true, Kind: "backup", ID: "b1"},
			method:   http.MethodGet,
			wantCode: http.StatusOK,
			wantBody: `{"probe":"weaviate/backup-node-activity","busy":true,"kind":"backup","id":"b1"}`,
		},
		{
			name:     "busy with a restore",
			activity: backup.NodeActivity{Busy: true, Kind: "restore", ID: "r1"},
			method:   http.MethodGet,
			wantCode: http.StatusOK,
			wantBody: `{"probe":"weaviate/backup-node-activity","busy":true,"kind":"restore","id":"r1"}`,
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
			prober := &stubProber{activity: backup.NodeActivity{Busy: true, Kind: "backup", ID: "b1"}}
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

type oneNode string

func (n oneNode) NodeHostname(string) (string, bool) {
	return strings.TrimPrefix(string(n), "http://"), true
}

// The handler and the client were written as two independent halves. This is
// where they meet over a real connection, agreeing on method, status, headers
// and the shape of the body.
func TestBackupNodeActivityEndToEnd(t *testing.T) {
	auth := cluster.AuthConfig{BasicAuth: cluster.BasicAuth{Username: "node", Password: "s3cret"}}

	tests := []struct {
		name string
		want backup.NodeActivity
	}{
		{name: "idle", want: backup.NodeActivity{}},
		{name: "busy", want: backup.NodeActivity{Busy: true, Kind: "restore", ID: "r-7"}},
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
