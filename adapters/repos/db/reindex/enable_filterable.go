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

// NewRuntimeEnableFilterableTask creates a ShardReindexTaskGeneric configured
// for runtime (live) creation of filterable (RoaringSet) indexes on
// properties that currently have none. The schema flip
// (IndexFilterable=true) now happens cluster-wide in
// [ReindexProvider.OnTaskCompleted], not from this strategy's per-shard
// OnMigrationComplete — so the constructor no longer needs the
// schema.Manager.
//
// propNames must be non-empty — this task is only meaningful on explicit
// properties, whole-collection usage would silently widen the blast radius.
func NewRuntimeEnableFilterableTask(
	logger logrus.FieldLogger,
	propNames []string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	strategy := &EnableFilterableStrategy{
		PropNames:  propNames,
		Generation: generation,
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
		"EnableFilterable", logger, strategy, cfg,
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}
