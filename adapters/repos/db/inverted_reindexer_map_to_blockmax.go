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
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema"
)

// NewShardInvertedReindexTaskMapToBlockmax creates a ShardReindexTaskV3 for
// migrating searchable properties from MapCollection to Inverted (blockmax WAND).
// This is a thin constructor that wires MapToBlockmaxStrategy into
// ShardReindexTaskGeneric.
func NewShardInvertedReindexTaskMapToBlockmax(logger logrus.FieldLogger,
	swapBuckets, unswapBuckets, tidyBuckets, reloadShards, rollback, conditionalStart bool,
	processingDuration, pauseDuration time.Duration, perObjectDelay time.Duration, concurrency int,
	cptSelected []config.CollectionPropsTenants, schemaManager *schema.Manager,
) *ShardReindexTaskGeneric {
	strategy := &MapToBlockmaxStrategy{schemaManager: schemaManager}

	selectionEnabled := false
	var selectedPropsByCollection, selectedShardsByCollection map[string]map[string]struct{}
	if count := len(cptSelected); count > 0 {
		selectionEnabled = true
		selectedPropsByCollection = make(map[string]map[string]struct{}, count)
		selectedShardsByCollection = make(map[string]map[string]struct{}, count)

		for _, cpt := range cptSelected {
			var props, shards map[string]struct{}
			if countp := len(cpt.Props); countp > 0 {
				props = make(map[string]struct{}, countp)
				for _, prop := range cpt.Props {
					props[prop] = struct{}{}
				}
			}
			if counts := len(cpt.Tenants); counts > 0 {
				shards = make(map[string]struct{}, counts)
				for _, shard := range cpt.Tenants {
					shards[shard] = struct{}{}
				}
			}
			selectedPropsByCollection[cpt.Collection] = props
			selectedShardsByCollection[cpt.Collection] = shards
		}
	}

	cfg := reindexTaskConfig{
		swapBuckets:                   swapBuckets,
		unswapBuckets:                 unswapBuckets,
		tidyBuckets:                   tidyBuckets,
		reloadShards:                  reloadShards,
		rollback:                      rollback,
		concurrency:                   concurrency,
		conditionalStart:              conditionalStart,
		memtableOptFactor:             4,
		backupMemtableOptFactor:       1,
		processingDuration:            processingDuration,
		pauseDuration:                 pauseDuration,
		checkProcessingEveryNoObjects: 1000,
		selectionEnabled:              selectionEnabled,
		selectedPropsByCollection:     selectedPropsByCollection,
		selectedShardsByCollection:    selectedShardsByCollection,
		perObjectDelay:                perObjectDelay,
	}

	logger.WithField("config", fmt.Sprintf("%+v", cfg)).Debug("task created")

	return NewShardReindexTaskGeneric(
		"MapToBlockmax", logger, strategy, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}

// NewRuntimeMapToBlockmaxTask creates a ShardReindexTaskGeneric configured for
// runtime (live) Map→Blockmax migration. It uses reloadShards=false so the
// migration is performed via atomic bucket pointer swaps without shard restart.
func NewRuntimeMapToBlockmaxTask(logger logrus.FieldLogger,
	schemaManager *schema.Manager,
) *ShardReindexTaskGeneric {
	strategy := &MapToBlockmaxStrategy{schemaManager: schemaManager}

	cfg := reindexTaskConfig{
		swapBuckets:                   true,
		tidyBuckets:                   true,
		reloadShards:                  false,
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
