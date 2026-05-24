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

// NewRuntimeSearchableRetokenizeTask creates a ShardReindexTaskGeneric configured
// for runtime (live) retokenization of a searchable property. It rebuilds the
// searchable (BM25) index with a different tokenization strategy.
func NewRuntimeSearchableRetokenizeTask(
	logger logrus.FieldLogger,
	propName, targetTokenization, className, bucketStrategy string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	strategy := &SearchableRetokenizeStrategy{
		PropName:           propName,
		TargetTokenization: targetTokenization,
		ClassName:          className,
		BucketStrategy:     bucketStrategy,
		Generation:         generation,
	}

	selectedProps := map[string]struct{}{
		propName: {},
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
		"SearchableRetokenize", logger, strategy, cfg,
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}
