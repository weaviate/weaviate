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
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
)

func TestRebalanceMeasurementHandler(t *testing.T) {
	const path = "/replication/rebalance/measurement"

	draftShards := []replicationTypes.RebalanceShardSize{
		{Collection: "Articles", Shard: "s1", Bytes: 100},
		{Collection: "Articles", Shard: "s2", Bytes: 200},
	}

	tests := []struct {
		name         string
		method       string
		query        string
		diskTotal    uint64
		diskFree     uint64
		memoryLimit  int64
		warningPct   uint64
		workers      int
		expectedCode int
		expected     replicationTypes.RebalanceNodeMeasurement
	}{
		{
			name:         "GET returns the node-level numbers",
			method:       http.MethodGet,
			diskTotal:    1 << 40,
			diskFree:     1 << 38,
			memoryLimit:  8 << 30,
			warningPct:   80,
			workers:      10,
			expectedCode: http.StatusOK,
			expected: replicationTypes.RebalanceNodeMeasurement{
				DiskTotalBytes:        1 << 40,
				DiskAvailableBytes:    1 << 38,
				DiskWarningPercentage: 80,
				MemoryLimitBytes:      8 << 30,
				ReplicationWorkers:    10,
			},
		},
		{
			name:         "GET passes an unset memory limit through as MaxInt64",
			method:       http.MethodGet,
			diskTotal:    1 << 40,
			diskFree:     1 << 38,
			memoryLimit:  math.MaxInt64,
			warningPct:   80,
			workers:      10,
			expectedCode: http.StatusOK,
			expected: replicationTypes.RebalanceNodeMeasurement{
				DiskTotalBytes:        1 << 40,
				DiskAvailableBytes:    1 << 38,
				DiskWarningPercentage: 80,
				MemoryLimitBytes:      math.MaxInt64,
				ReplicationWorkers:    10,
			},
		},
		{
			name:         "GET with draft parameters adds the draft to disk numbers read at request time",
			method:       http.MethodGet,
			query:        "?draftTask=plan-1&draftVersion=7",
			diskTotal:    1 << 40,
			diskFree:     1 << 38,
			memoryLimit:  8 << 30,
			warningPct:   80,
			workers:      10,
			expectedCode: http.StatusOK,
			expected: replicationTypes.RebalanceNodeMeasurement{
				DiskTotalBytes:        1 << 40,
				DiskAvailableBytes:    1 << 38,
				DiskWarningPercentage: 80,
				MemoryLimitBytes:      8 << 30,
				ReplicationWorkers:    10,
				Shards:                draftShards,
			},
		},
		{
			name:         "GET for a missing draft is not found",
			method:       http.MethodGet,
			query:        "?draftTask=plan-1&draftVersion=8",
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "GET with a draft task and no version is a bad request",
			method:       http.MethodGet,
			query:        "?draftTask=plan-1",
			expectedCode: http.StatusBadRequest,
		},
		{
			name:         "POST is not allowed",
			method:       http.MethodPost,
			expectedCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// diskUse counts its calls. One call per request shows the disk
			// numbers are read at request time.
			diskReads := 0
			h := clusterapi.NewRebalanceMeasurement(
				func() (uint64, uint64) {
					diskReads++
					return tt.diskTotal, tt.diskFree
				},
				func(taskID string, version uint64) ([]replicationTypes.RebalanceShardSize, error) {
					if taskID == "plan-1" && version == 7 {
						return draftShards, nil
					}
					return nil, fmt.Errorf("read draft: %w", os.ErrNotExist)
				},
				func() int64 { return tt.memoryLimit },
				tt.warningPct,
				tt.workers,
				clusterapi.NewNoopAuthHandler(),
			)

			req := httptest.NewRequest(tt.method, path+tt.query, nil)
			rec := httptest.NewRecorder()
			h.Measurement().ServeHTTP(rec, req)

			require.Equal(t, tt.expectedCode, rec.Code)
			if tt.expectedCode != http.StatusOK {
				return
			}

			assert.Equal(t, 1, diskReads)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			var got replicationTypes.RebalanceNodeMeasurement
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
			assert.Equal(t, tt.expected, got)
		})
	}
}
