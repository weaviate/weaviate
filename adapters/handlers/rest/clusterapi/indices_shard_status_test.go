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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/entities/storagestate"
)

type reportShards struct {
	shards
	report storagestate.Report
}

func (r reportShards) GetShardStatus(context.Context, string, string) (storagestate.Report, error) {
	return r.report, nil
}

// TestShardStatusReport covers a rolling upgrade from both sides: a node gets
// the conditions when it asks for them, and talks to an older node either way.
func TestShardStatusReport(t *testing.T) {
	report := storagestate.Report{
		Status:     storagestate.StatusReadOnly.String(),
		Conditions: []string{storagestate.ConditionUserReadOnly},
	}
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	current := NewIndices(reportShards{report: report}, startedDB{},
		NewNoopAuthHandler(), func() bool { return false }, logger).Indices()
	older := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		shared.IndicesPayloads.GetShardStatusResults.SetContentTypeHeader(w)
		body, _ := shared.IndicesPayloads.GetShardStatusResults.Marshal(report.Status)
		w.Write(body)
	})

	tests := []struct {
		name   string
		server http.Handler
		want   storagestate.Report
	}{
		{name: "current node", server: current, want: report},
		{name: "older node", server: older, want: storagestate.Report{Status: report.Status}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(tt.server)
			defer ts.Close()

			client := clients.NewRemoteIndex(ts.Client())
			got, err := client.GetShardStatus(t.Context(), strings.TrimPrefix(ts.URL, "http://"), "C", "S")
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	t.Run("older coordinator", func(t *testing.T) {
		rec := httptest.NewRecorder()
		current.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/indices/C/shards/S/status", nil))

		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, shared.IndicesPayloads.GetShardStatusResults.MIME(), rec.Header().Get("content-type"))
		status, err := shared.IndicesPayloads.GetShardStatusResults.Unmarshal(rec.Body.Bytes())
		require.NoError(t, err)
		require.Equal(t, report.Status, status)
	})
}
