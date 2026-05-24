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

// blockmaxSearchableTaskConfig returns the shared reindexTaskConfig used
// by NewRuntimeEnableSearchableTask + NewRuntimeRebuildSearchableTask.
// Both scan objects to produce BlockMax postings; the only difference is
// the strategy attached on top.
func blockmaxSearchableTaskConfig(propNames []string, collectionName string) reindexTaskConfig {
	selected := make(map[string]struct{}, len(propNames))
	for _, p := range propNames {
		selected[p] = struct{}{}
	}
	return reindexTaskConfig{
		swapBuckets:                   true,
		tidyBuckets:                   true,
		concurrency:                   2,
		memtableOptFactor:             4,
		backupMemtableOptFactor:       1,
		processingDuration:            10 * time.Minute,
		pauseDuration:                 1 * time.Second,
		checkProcessingEveryNoObjects: 1000,

		selectionEnabled: true,
		selectedPropsByCollection: map[string]map[string]struct{}{
			collectionName: selected,
		},
		selectedShardsByCollection: map[string]map[string]struct{}{
			collectionName: nil,
		},
	}
}
