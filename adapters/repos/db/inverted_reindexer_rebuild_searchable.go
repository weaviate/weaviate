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

package db

import (
	"time"

	"github.com/sirupsen/logrus"
)

// NewRuntimeRebuildSearchableTask constructs a ShardReindexTaskGeneric that
// rebuilds existing BlockMax searchable buckets from the objects store.
// Caller-side dispatch in handlers_indexes.go gates this on the property
// already being BlockMax — WAND properties get routed to MapToBlockmax
// instead.
func NewRuntimeRebuildSearchableTask(
	logger logrus.FieldLogger,
	propNames []string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	strategy := &RebuildSearchableStrategy{
		propNames:  propNames,
		generation: generation,
	}

	selectedProps := make(map[string]struct{}, len(propNames))
	for _, p := range propNames {
		selectedProps[p] = struct{}{}
	}

	cfg := reindexTaskConfig{
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
			collectionName: selectedProps,
		},
		selectedShardsByCollection: map[string]map[string]struct{}{
			collectionName: nil,
		},
	}

	return NewShardReindexTaskGeneric(
		"RebuildSearchable", logger, strategy, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}
