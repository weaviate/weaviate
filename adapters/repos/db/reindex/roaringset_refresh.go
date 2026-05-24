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

// NewRuntimeRoaringSetRefreshTask creates a ShardReindexTaskGeneric configured
// for runtime (live) RoaringSet refresh. This rebuilds the filterable property
// indexes listed in propNames from the objects bucket without changing the
// storage format.
//
// propNames must be non-empty — whole-collection rebuilds are not supported
// from the runtime reindex API and would silently widen the blast radius.
func NewRuntimeRoaringSetRefreshTask(
	logger logrus.FieldLogger,
	propNames []string,
	collectionName string,
	generation int,
) *ShardReindexTaskGeneric {
	strategy := &RoaringSetRefreshStrategy{Generation: generation}

	cfg := defaultRuntimeReindexConfig(propNames, collectionName)

	return NewShardReindexTaskGeneric(
		"RoaringSetRefresh", logger, strategy, cfg,
		&UuidKeyParser{}, UuidObjectsIteratorAsync,
	)
}
