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

import (
	"time"

	"github.com/sirupsen/logrus"
)

// NewRuntimeRoaringSetRefreshTask creates a ShardReindexTaskGeneric configured
// for runtime (live) RoaringSet refresh. This rebuilds the filterable property
// indexes listed in propNames from the objects bucket without changing the
// storage format.
//
// propNames must be non-empty — whole-collection rebuilds are not supported
// from the runtime reindex API and would silently widen the blast radius.
func NewRuntimeRoaringSetRefreshTask(
	logger logrus.FieldLogger,
	propNames []string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	strategy := &RoaringSetRefreshStrategy{Generation: generation}

	selectedProps := make(map[string]struct{}, len(propNames))
	for _, p := range propNames {
		selectedProps[p] = struct{}{}
	}

	cfg := ReindexTaskConfig{
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
			collectionName: selectedProps,
		},
		SelectedShardsByCollection: map[string]map[string]struct{}{
			collectionName: nil, // nil = all shards
		},
	}

	return NewShardReindexTaskGeneric(
		"RoaringSetRefresh", logger, strategy, cfg,
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}
