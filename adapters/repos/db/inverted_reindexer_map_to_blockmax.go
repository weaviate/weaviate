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
// runtime (live) Map→Blockmax migration. The migration is performed via atomic
// bucket pointer swaps without shard restart.
func NewRuntimeMapToBlockmaxTask(logger logrus.FieldLogger,
	schemaManager *schema.Manager,
) *ShardReindexTaskGeneric {
	strategy := &MapToBlockmaxStrategy{schemaManager: schemaManager}

	cfg := reindexTaskConfig{
		swapBuckets:                   true,
		tidyBuckets:                   true,
		concurrency:                   2,
		memtableOptFactor:             4,
		backupMemtableOptFactor:       1,
		processingDuration:            10 * time.Minute,
		pauseDuration:                 1 * time.Second,
		checkProcessingEveryNoObjects: 1000,
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
