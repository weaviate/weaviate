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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
)

// SearchableBucketStrategy returns the LSM bucket strategy for the
// named property's searchable bucket on this collection, scanning
// every shard until the first non-empty answer. Returns "" when no
// searchable bucket exists on any local shard.
//
// Caller is the reindex submit path: the migration dispatcher needs
// the strategy to decide whether to schedule a Map→BlockMax or a
// regular tokenization-only retokenize. The choice is shard-uniform
// (every shard of the same collection uses the same strategy), so
// returning the first match is correct.
func (db *DB) SearchableBucketStrategy(className schema.ClassName, propName string) string {
	idx := db.GetIndex(className)
	if idx == nil {
		return ""
	}
	bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
	var strategy string
	idx.ForEachShard(func(_ string, shard ShardLike) error {
		if strategy != "" {
			return nil
		}
		if bucket := shard.Store().Bucket(bucketName); bucket != nil {
			strategy = bucket.Strategy()
		}
		return nil
	})
	return strategy
}
