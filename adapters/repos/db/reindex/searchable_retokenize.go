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

	cfg := defaultRuntimeReindexConfig([]string{propName}, collectionName)

	return NewShardReindexTaskGeneric(
		"SearchableRetokenize", logger, strategy, cfg,
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}
