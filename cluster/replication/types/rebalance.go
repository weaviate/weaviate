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

// RebalanceNodeMeasurement is one node's resource measurement taken by a
// rebalance planning run.
type RebalanceNodeMeasurement struct {
	Node           string `json:"node"`
	DiskTotalBytes int64  `json:"diskTotalBytes"`
	// DiskAvailableBytes is the free bytes getDiskUse reports: statfs Bfree,
	// which includes blocks reserved for root.
	DiskAvailableBytes    int64 `json:"diskAvailableBytes"`
	DiskWarningPercentage int64 `json:"diskWarningPercentage"`
	MemoryLimitBytes      int64 `json:"memoryLimitBytes"`
	ReplicationWorkers    int   `json:"replicationWorkers"`
	// Error is set when the node could not be measured.
	Error string `json:"error,omitempty"`
}
