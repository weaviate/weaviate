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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestRemoteNode_GetRebalanceMeasurement(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		want      replicationTypes.RebalanceNodeMeasurement
		wantErrAs any
	}{
		{
			name:   "200 decodes every field",
			status: http.StatusOK,
			body: `{"node":"node-1","diskTotalBytes":1000,"diskAvailableBytes":400,"diskWarningPercentage":80,` +
				`"memoryLimitBytes":2048,"replicationWorkers":5,"error":"partial"}`,
			want: replicationTypes.RebalanceNodeMeasurement{
				Node:                  "node-1",
				DiskTotalBytes:        1000,
				DiskAvailableBytes:    400,
				DiskWarningPercentage: 80,
				MemoryLimitBytes:      2048,
				ReplicationWorkers:    5,
				Error:                 "partial",
			},
		},
		{
			name:      "non-200 returns an error",
			status:    http.StatusInternalServerError,
			body:      "boom",
			wantErrAs: &enterrors.ErrUnexpectedStatusCode{},
		},
		{
			name:      "malformed 200 body returns an error",
			status:    http.StatusOK,
			body:      "{not json",
			wantErrAs: &enterrors.ErrUnmarshalBody{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodGet, r.Method)
				assert.Equal(t, "/replication/rebalance/measurement", r.URL.Path)
				w.WriteHeader(tt.status)
				_, err := w.Write([]byte(tt.body))
				assert.NoError(t, err)
			}))
			defer server.Close()

			got, err := NewRemoteNode(server.Client()).GetRebalanceMeasurement(context.Background(), server.Listener.Addr().String())
			if tt.wantErrAs != nil {
				require.ErrorAs(t, err, tt.wantErrAs)
				require.Zero(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
