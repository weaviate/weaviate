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

package types

// RebalanceDraftNamespace is the distributed-task namespace for rebalance
// drafts. Each listed node sizes its own shards and saves them as a draft.
const RebalanceDraftNamespace = "replication_rebalance_draft"

// RebalanceShardSize is the on-disk size of one local shard replica.
type RebalanceShardSize struct {
	Collection string `json:"collection"`
	Shard      string `json:"shard"`
	Bytes      int64  `json:"bytes"`
}

// RebalanceNodeMeasurement is one node's resource measurement taken by a
// rebalance planning run.
type RebalanceNodeMeasurement struct {
	Node           string `json:"node"`
	DiskTotalBytes int64  `json:"diskTotalBytes"`
	// DiskAvailableBytes is the free bytes getDiskUse reports. On unix this is
	// statfs Bfree, which includes blocks reserved for root. On windows it is
	// the total free bytes from GetDiskFreeSpaceEx.
	DiskAvailableBytes    int64 `json:"diskAvailableBytes"`
	DiskWarningPercentage int64 `json:"diskWarningPercentage"`
	MemoryLimitBytes      int64 `json:"memoryLimitBytes"`
	ReplicationWorkers    int   `json:"replicationWorkers"`
	// Shards holds the node's drafted shard sizes. It is empty unless the
	// measurement was taken for a draft.
	Shards []RebalanceShardSize `json:"shards,omitempty"`
	// Error is set when the node could not be measured.
	Error string `json:"error,omitempty"`
}
