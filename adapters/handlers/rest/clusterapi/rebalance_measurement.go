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
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
)

type rebalanceMeasurement struct {
	diskUse               func() (total, free uint64)
	readDraft             func(taskID string, version uint64) ([]replicationTypes.RebalanceShardSize, error)
	memoryLimit           func() int64
	diskWarningPercentage uint64
	replicationWorkers    int
	auth                  auth
}

// NewRebalanceMeasurement builds the handler for this node's rebalance
// measurement. It calls diskUse and memoryLimit on every request. A request
// that names a draft task and version also gets that draft's shard sizes from
// readDraft. For a missing draft, readDraft's error matches os.ErrNotExist.
func NewRebalanceMeasurement(diskUse func() (total, free uint64),
	readDraft func(taskID string, version uint64) ([]replicationTypes.RebalanceShardSize, error),
	memoryLimit func() int64, diskWarningPercentage uint64, replicationWorkers int, auth auth,
) *rebalanceMeasurement {
	return &rebalanceMeasurement{
		diskUse:               diskUse,
		readDraft:             readDraft,
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

		var shards []replicationTypes.RebalanceShardSize
		query := r.URL.Query()
		if query.Has("draftTask") || query.Has("draftVersion") {
			taskID := query.Get("draftTask")
			version, err := strconv.ParseUint(query.Get("draftVersion"), 10, 64)
			if taskID == "" || err != nil {
				http.Error(w, "/replication/rebalance draftTask and a numeric draftVersion must be set together",
					http.StatusBadRequest)
				return
			}
			shards, err = m.readDraft(taskID, version)
			if errors.Is(err, os.ErrNotExist) {
				http.Error(w, "/replication/rebalance "+err.Error(), http.StatusNotFound)
				return
			}
			if err != nil {
				http.Error(w, "/replication/rebalance "+err.Error(), http.StatusInternalServerError)
				return
			}
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
			Shards:                shards,
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(measurement); err != nil {
			http.Error(w, "/replication/rebalance marshal response: "+err.Error(),
				http.StatusInternalServerError)
		}
	}
}
