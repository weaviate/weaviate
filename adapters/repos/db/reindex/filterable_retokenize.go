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

// NewRuntimeFilterableRetokenizeTask creates a ShardReindexTaskGeneric configured
// for runtime (live) retokenization of a filterable property. It rebuilds the
// filterable (RoaringSet) index with a different tokenization strategy.
//
// The schema flip (Tokenization field) is no longer performed by this
// strategy — it now happens cluster-wide in
// [ReindexProvider.OnTaskCompleted] after every node's local
// OnGroupCompleted has run the bucket pointer swap. The strategy
// constructor therefore no longer needs the schema.Manager.
func NewRuntimeFilterableRetokenizeTask(
	logger logrus.FieldLogger,
	propName, targetTokenization, className string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	strategy := &FilterableRetokenizeStrategy{
		propName:           propName,
		targetTokenization: targetTokenization,
		className:          className,
		generation:         generation,
	}

	selectedProps := map[string]struct{}{
		propName: {},
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
			collectionName: nil, // nil = all shards
		},
	}

	return NewShardReindexTaskGeneric(
		"FilterableRetokenize", logger, strategy, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}
