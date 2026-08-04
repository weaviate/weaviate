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
)

type stubCleanupProber struct {
	cleaningUp bool
	asked      string
}

func (s *stubCleanupProber) AnyCleanupInProgressForCollection(collection string) bool {
	s.asked = collection
	return s.cleaningUp
}

func TestInternalReindexCleanupActivity(t *testing.T) {
	tests := []struct {
		name       string
		prober     *stubCleanupProber
		query      string
		wantStatus int
		wantBody   string
		wantAsked  string
	}{
		{
			name:       "gate is up",
			prober:     &stubCleanupProber{cleaningUp: true},
			query:      "?collection=Movies",
			wantStatus: http.StatusOK,
			wantBody:   `{"cleaningUp":true}`,
			wantAsked:  "Movies",
		},
		{
			name:       "gate is down",
			prober:     &stubCleanupProber{},
			query:      "?collection=Movies",
			wantStatus: http.StatusOK,
			wantBody:   `{"cleaningUp":false}`,
			wantAsked:  "Movies",
		},
		{
			name:       "collection is required",
			prober:     &stubCleanupProber{cleaningUp: true},
			query:      "",
			wantStatus: http.StatusBadRequest,
		},
		{
			// Never answer "not cleaning up" from a node that cannot tell:
			// a cancel's response depends on this.
			name:       "probe not wired",
			prober:     nil,
			query:      "?collection=Movies",
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var handler *clusterapi.ReindexCleanup
			if tt.prober == nil {
				handler = clusterapi.NewReindexCleanup(nil, clusterapi.NewNoopAuthHandler())
			} else {
				handler = clusterapi.NewReindexCleanup(tt.prober, clusterapi.NewNoopAuthHandler())
			}
			server := httptest.NewServer(handler.Activity())
			defer server.Close()

			res, err := server.Client().Get(server.URL + "/reindex/cleanup-activity" + tt.query)
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
			assert.Equal(t, tt.wantAsked, tt.prober.asked)
		})
	}
}
