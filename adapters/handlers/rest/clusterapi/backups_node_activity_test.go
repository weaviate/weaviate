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
