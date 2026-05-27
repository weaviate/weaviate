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

package reindex

import "time"

// defaultRuntimeReindexConfig is the shared [ReindexTaskConfig] every
// "live" runtime-reindex task variant uses. Every NewRuntime*Task
// constructor differs only in the strategy struct attached on top.
//
// Knobs intentionally identical across variants:
//   - SwapBuckets / TidyBuckets — the recovery/finalize handshake fires
//     after every successful migration regardless of strategy.
//   - Concurrency: 2 — keeps the runtime sweep within the bounded write-
//     amplification budget set during the Symptom-B fix.
//   - MemtableOptFactor / BackupMemtableOptFactor — sized for the
//     reindex sidecar buckets, not the canonical bucket.
//   - ProcessingDuration / PauseDuration / CheckProcessingEveryNoObjects
//     — the per-shard pacing loop.
//
// SelectionEnabled is always true; runtime tasks never widen beyond the
// explicit (collection, propName) tuple. Caller passes the prop set in;
// shard set is nil (`all shards on this node`).
func defaultRuntimeReindexConfig(propNames []string, collectionName string) ReindexTaskConfig {
	selected := make(map[string]struct{}, len(propNames))
	for _, p := range propNames {
		selected[p] = struct{}{}
	}
	return ReindexTaskConfig{
		SwapBuckets:                   true,
		TidyBuckets:                   true,
		Concurrency:                   2,
		MemtableOptFactor:             4,
		BackupMemtableOptFactor:       1,
		ProcessingDuration:            10 * time.Minute,
		PauseDuration:                 1 * time.Second,
		CheckProcessingEveryNoObjects: 1000,

		SelectionEnabled: true,
		SelectedPropsByCollection: map[string]map[string]struct{}{
			collectionName: selected,
		},
		SelectedShardsByCollection: map[string]map[string]struct{}{
			collectionName: nil,
		},
	}
}

// blockmaxSearchableTaskConfig keeps the original name as a thin
// wrapper for the two callers that pre-date the generalisation. Both
// resolve to the same defaultRuntimeReindexConfig.
func blockmaxSearchableTaskConfig(propNames []string, collectionName string) ReindexTaskConfig {
	return defaultRuntimeReindexConfig(propNames, collectionName)
}
