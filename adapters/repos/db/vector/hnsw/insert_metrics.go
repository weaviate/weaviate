//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

type insertMetrics struct {
	total                           Observer
	prepareAndInsertNode            Observer
	findEntrypoint                  Observer
	updateGlobalEntrypoint          Observer
	findAndConnectTotal             Observer
	findAndConnectSearch            Observer
	findAndConnectHeuristic         Observer
	findAndConnectUpdateConnections Observer
}

// newInsertMetrics curries the prometheus observers just once at creation time
// and therefore avoids having to make a lot of allocations on the hot path
func newInsertMetrics(metrics *Metrics) *insertMetrics {
	return &insertMetrics{
		total:                           metrics.TrackInsertObserver("total"),
		prepareAndInsertNode:            metrics.TrackInsertObserver("prepare_and_insert_node"),
		findEntrypoint:                  metrics.TrackInsertObserver("find_entrypoint"),
		updateGlobalEntrypoint:          metrics.TrackInsertObserver("update_global_entrypoint"),
		findAndConnectTotal:             metrics.TrackInsertObserver("find_and_connect_total"),
		findAndConnectSearch:            metrics.TrackInsertObserver("find_and_connect_search"),
		findAndConnectHeuristic:         metrics.TrackInsertObserver("find_and_connect_heuristic"),
		findAndConnectUpdateConnections: metrics.TrackInsertObserver("find_and_connect_update_connections"),
	}
}
