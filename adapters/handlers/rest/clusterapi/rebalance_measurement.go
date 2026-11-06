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
	"encoding/json"
	"fmt"
	"net/http"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
)

type rebalanceMeasurement struct {
	diskUse               func() (total, free uint64)
	memoryLimit           func() int64
	diskWarningPercentage uint64
	replicationWorkers    int
	auth                  auth
}

// NewRebalanceMeasurement builds the handler for this node's rebalance
// measurement. It calls diskUse and memoryLimit on every request.
func NewRebalanceMeasurement(diskUse func() (total, free uint64), memoryLimit func() int64,
	diskWarningPercentage uint64, replicationWorkers int, auth auth,
) *rebalanceMeasurement {
	return &rebalanceMeasurement{
		diskUse:               diskUse,
		memoryLimit:           memoryLimit,
		diskWarningPercentage: diskWarningPercentage,
		replicationWorkers:    replicationWorkers,
		auth:                  auth,
	}
}

func (m *rebalanceMeasurement) Measurement() http.Handler {
	return m.auth.handleFunc(m.measurementHandler())
}

func (m *rebalanceMeasurement) measurementHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.Method != http.MethodGet {
			msg := fmt.Sprintf("/replication/rebalance api path %q with method %v not found", r.URL.Path, r.Method)
			http.Error(w, msg, http.StatusMethodNotAllowed)
			return
		}

		// Node and Error stay empty. The planner sets Node on every row and
		// reports failed measurements itself.
		total, free := m.diskUse()
		measurement := replicationTypes.RebalanceNodeMeasurement{
			DiskTotalBytes:        int64(total),
			DiskAvailableBytes:    int64(free),
			DiskWarningPercentage: int64(m.diskWarningPercentage),
			MemoryLimitBytes:      m.memoryLimit(),
			ReplicationWorkers:    m.replicationWorkers,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(measurement); err != nil {
			http.Error(w, "/replication/rebalance marshal response: "+err.Error(),
				http.StatusInternalServerError)
		}
	}
}
