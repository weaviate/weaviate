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
	"github.com/weaviate/weaviate/usecases/schema"
)

// NewRuntimeFilterableToRangeableTask creates a ShardReindexTaskGeneric configured
// for runtime (live) filterable→rangeable migration. It builds RoaringSetRange
// indexes from existing data, enabling indexRangeFilters on numeric properties.
func NewRuntimeFilterableToRangeableTask(
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
	propNames []string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	strategy := &FilterableToRangeableStrategy{
		SchemaManager: schemaManager,
		PropNames:     propNames,
		Generation:    generation,
	}

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
		"FilterableToRangeable", logger, strategy, cfg,
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}
