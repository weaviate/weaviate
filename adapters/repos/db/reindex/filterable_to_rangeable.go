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
	"github.com/weaviate/weaviate/usecases/schema"
)

// Test-only export: relocation follow-up tracked separately; no new external callers.
//
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

	cfg := defaultRuntimeReindexConfig(propNames, collectionName)

	return NewShardReindexTaskGeneric(
		"FilterableToRangeable", logger, strategy, cfg,
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}
