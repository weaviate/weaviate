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
	"github.com/sirupsen/logrus"
)

// NewRuntimeEnableFilterableTask creates a ShardReindexTaskGeneric configured
// for runtime (live) creation of filterable (RoaringSet) indexes on
// properties that currently have none. The schema flip
// (IndexFilterable=true) now happens cluster-wide in
// [AutoCleanupAfterTerminal.OnTaskCompleted], not from this strategy's per-shard
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

	cfg := defaultRuntimeReindexConfig(propNames, collectionName)

	return NewShardReindexTaskGeneric(
		"EnableFilterable", logger, strategy, cfg,
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}
