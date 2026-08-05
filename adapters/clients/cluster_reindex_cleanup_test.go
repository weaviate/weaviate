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

package clients

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterReindexCleanup(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		want       bool
		wantErrIs  error
		wantErrMsg string
	}{
		{
			name:       "gate is closed",
			statusCode: http.StatusOK,
			body:       `{"cleaningUp":true}`,
			want:       true,
		},
		{
			name:       "gate is open",
			statusCode: http.StatusOK,
			body:       `{"cleaningUp":false}`,
		},
		{
			name:       "older build does not serve the route",
			statusCode: http.StatusNotFound,
			body:       "404 page not found",
			wantErrIs:  ErrReindexCleanupUnsupported,
		},
		{
			name:       "unwired probe",
			statusCode: http.StatusServiceUnavailable,
			body:       "reindex cleanup probe is not wired on this node",
			wantErrMsg: "unexpected status code 503",
		},
		{
			name:       "unreadable body",
			statusCode: http.StatusOK,
			body:       "{",
			wantErrMsg: "unmarshal reindex cleanup response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotCollection string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotCollection = r.URL.Query().Get("collection")
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.body))
			}))
			defer server.Close()

			host, err := url.Parse(server.URL)
			require.NoError(t, err)
			client := NewClusterReindexCleanup(server.Client(), staticResolver{"node1": host.Host})

			got, err := client.CleanupInProgress(context.Background(), "node1", "Movies")

			assert.Equal(t, "/reindex/cleanup-activity", gotPath)
			assert.Equal(t, "Movies", gotCollection, "the collection has to reach the owner")

			switch {
			case tt.wantErrIs != nil:
				require.ErrorIs(t, err, tt.wantErrIs)
			case tt.wantErrMsg != "":
				require.ErrorContains(t, err, tt.wantErrMsg)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestClusterReindexCleanupUnresolvableNode(t *testing.T) {
	client := NewClusterReindexCleanup(http.DefaultClient, staticResolver{})
	_, err := client.CleanupInProgress(context.Background(), "ghost", "Movies")
	require.ErrorContains(t, err, "unable to resolve hostname")
	require.NotErrorIs(t, err, ErrReindexCleanupUnsupported,
		"an unresolvable node is not the same as one without the route")
}
