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

// NewRuntimeMapToBlockmaxTask creates a ShardReindexTaskGeneric configured for
// runtime (live) Map→Blockmax migration of the searchable properties listed in
// propNames. The migration is performed via atomic bucket pointer swaps
// without shard restart.
//
// propNames must be non-empty. The class-level UsingBlockMaxWAND flag is
// only flipped once every searchable property on every shard has been
// migrated — see MapToBlockmaxStrategy.OnMigrationComplete.
func NewRuntimeMapToBlockmaxTask(
	logger logrus.FieldLogger,
	schemaManager *schema.Manager,
	propNames []string,
	collectionName string,
) *ShardReindexTaskGeneric {
	strategy := &MapToBlockmaxStrategy{schemaManager: schemaManager}

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
			collectionName: nil, // nil = all shards
		},
	}

	return NewShardReindexTaskGeneric(
		"MapToBlockmax", logger, strategy, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}

// NewFileMapToBlockmaxReindexTracker creates a file-based reindex tracker
// for the searchable map-to-blockmax migration. This is a backward-compatible
// wrapper around NewFileReindexTracker used by the debug handler.
func NewFileMapToBlockmaxReindexTracker(lsmPath string, keyParser indexKeyParser) *fileReindexTracker {
	return NewFileReindexTracker(lsmPath, "searchable_map_to_blockmax", keyParser)
}
