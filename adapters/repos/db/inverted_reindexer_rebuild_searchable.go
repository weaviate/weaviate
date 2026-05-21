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
	"github.com/sirupsen/logrus"
)

// NewRuntimeRebuildSearchableTask constructs a task that rebuilds existing
// BlockMax searchable buckets from the objects store. Dispatch in
// handlers_indexes.go gates this on the property already being BlockMax;
// WAND properties route to MapToBlockmax instead.
func NewRuntimeRebuildSearchableTask(
	logger logrus.FieldLogger,
	propNames []string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	return NewShardReindexTaskGeneric(
		"RebuildSearchable", logger,
		&RebuildSearchableStrategy{propNames: propNames, generation: generation},
		blockmaxSearchableTaskConfig(propNames, collectionName),
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}
