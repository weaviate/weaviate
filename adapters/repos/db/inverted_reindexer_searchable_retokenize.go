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
	"github.com/weaviate/weaviate/usecases/schema"
)

// NewRuntimeSearchableRetokenizeTask creates a ShardReindexTaskGeneric configured
// for runtime (live) retokenization of a searchable property. It rebuilds the
// searchable (BM25) index with a different tokenization strategy.
//
// schemaManager is wired into the strategy so OnMigrationComplete can flip
// the schema's Tokenization. Historically the schema flip lived only in the
// FilterableRetokenize sibling, which worked when both sub-tasks ran. After
// createReindexTasks gained the defense that skips the filterable sub-task
// for properties without a filterable bucket (IndexFilterable=false), the
// searchable strategy must carry the schema flip too.
func NewRuntimeSearchableRetokenizeTask(
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
	propName, targetTokenization, className, bucketStrategy string,
	collectionName string,
) *ShardReindexTaskGeneric {
	strategy := &SearchableRetokenizeStrategy{
		schemaManager:      schemaManager,
		propName:           propName,
		targetTokenization: targetTokenization,
		className:          className,
		bucketStrategy:     bucketStrategy,
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
		"SearchableRetokenize", logger, strategy, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}
