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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	"github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
)

func TestInternalBackupsNodeActivity(t *testing.T) {
	tests := []struct {
		name       string
		probe      *backup.NodeActivityProbe
		wantStatus int
		wantBody   string
	}{
		{
			name:       "idle probe",
			probe:      backup.NewNodeActivityProbe(nil),
			wantStatus: http.StatusOK,
			wantBody:   `{"busy":false}`,
		},
		{
			name:       "probe not wired",
			probe:      nil,
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := clusterapi.NewBackups(nil, tt.probe, clusterapi.NewNoopAuthHandler())
			server := httptest.NewServer(handler.NodeActivity())
			defer server.Close()

			res, err := server.Client().Get(server.URL + "/backups/node-activity")
			require.NoError(t, err)
			defer res.Body.Close()

			require.Equal(t, tt.wantStatus, res.StatusCode)
			if tt.wantBody == "" {
				return
			}

			assert.Equal(t, "application/json", res.Header.Get("Content-Type"))
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.JSONEq(t, tt.wantBody, string(body))
		})
	}
}

// The route reports whether this node is part of a backup, which is
// cluster-internal state: an unauthenticated caller must be refused rather
// than answered.
func TestInternalBackupsNodeActivityRequiresAuth(t *testing.T) {
	const (
		user = "alice"
		pass = "s3cret"
	)
	auth := clusterapi.NewBasicAuthHandler(cluster.AuthConfig{
		BasicAuth: cluster.BasicAuth{Username: user, Password: pass},
	})

	tests := []struct {
		name       string
		setAuth    bool
		user, pass string
		wantStatus int
	}{
		{name: "no credentials", wantStatus: http.StatusUnauthorized},
		{name: "wrong user", setAuth: true, user: "mallory", pass: pass, wantStatus: http.StatusUnauthorized},
		{name: "wrong password", setAuth: true, user: user, pass: "guess", wantStatus: http.StatusUnauthorized},
		{name: "correct credentials", setAuth: true, user: user, pass: pass, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := clusterapi.NewBackups(nil, backup.NewNodeActivityProbe(nil), auth)
			server := httptest.NewServer(handler.NodeActivity())
			defer server.Close()

			req, err := http.NewRequest(http.MethodGet, server.URL+"/backups/node-activity", nil)
			require.NoError(t, err)
			if tt.setAuth {
				req.SetBasicAuth(tt.user, tt.pass)
			}

			res, err := server.Client().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			require.Equal(t, tt.wantStatus, res.StatusCode)
			if tt.wantStatus != http.StatusOK {
				body, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				assert.Empty(t, body, "a refused caller must not be told what this node is doing")
			}
		})
	}
}

func TestInternalBackupsNodeActivityRejectsNonGET(t *testing.T) {
	handler := clusterapi.NewBackups(nil, backup.NewNodeActivityProbe(nil), clusterapi.NewNoopAuthHandler())
	server := httptest.NewServer(handler.NodeActivity())
	defer server.Close()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req, err := http.NewRequest(method, server.URL+"/backups/node-activity", nil)
			require.NoError(t, err)

			res, err := server.Client().Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			assert.Equal(t, http.StatusMethodNotAllowed, res.StatusCode,
				"a read-only probe must not answer writes")
		})
	}
}
