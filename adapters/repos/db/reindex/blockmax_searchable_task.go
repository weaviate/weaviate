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
func blockmaxSearchableTaskConfig(propNames []string, collectionName string) ReindexTaskConfig {
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
