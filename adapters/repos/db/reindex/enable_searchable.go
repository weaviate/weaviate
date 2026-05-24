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

// NewRuntimeEnableSearchableTask creates a ShardReindexTaskGeneric configured
// for runtime (live) creation of blockmax searchable indexes on text/text[]
// properties that currently have none. The schema flip
// (IndexSearchable=true + Tokenization) now happens cluster-wide in
// [ReindexProvider.OnTaskCompleted], not from this strategy's per-shard
// OnMigrationComplete — so the constructor no longer needs the
// schema.Manager.
//
// propNames must be non-empty and tokenization must be a valid tokenization
// for the schema's text properties.
func NewRuntimeEnableSearchableTask(
	logger logrus.FieldLogger,
	propNames []string,
	collectionName string,
	tokenization string,
	generation int,
) *ShardReindexTaskGeneric {
	return NewShardReindexTaskGeneric(
		"EnableSearchable", logger,
		&EnableSearchableStrategy{
			PropNames:    propNames,
			Tokenization: tokenization,
			Generation:   generation,
		},
		blockmaxSearchableTaskConfig(propNames, collectionName),
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}
