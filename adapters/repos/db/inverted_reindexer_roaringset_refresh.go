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
)

// NewRuntimeRoaringSetRefreshTask creates a ShardReindexTaskGeneric configured
// for runtime (live) RoaringSet refresh. This rebuilds filterable property
// indexes from the objects bucket without changing the storage format.
func NewRuntimeRoaringSetRefreshTask(logger logrus.FieldLogger) *ShardReindexTaskGeneric {
	strategy := &RoaringSetRefreshStrategy{}

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
		"RoaringSetRefresh", logger, strategy, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}
