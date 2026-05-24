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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/reindex"
)

// areAllSearchableBucketsBlockMax reports whether the shard's
// searchable indices already use the BlockMax WAND format. The
// shard-init path uses this to set s.usingBlockMaxWAND so the read
// path picks the right bucket strategy without consulting the schema.
func (s *Shard) areAllSearchableBucketsBlockMax() bool {
	for name, bucket := range s.Store().GetBucketsByName() {
		_, indexType := reindex.GetPropNameAndIndexTypeFromBucketName(name)
		if bucket.Strategy() == lsmkv.StrategyMapCollection && indexType == reindex.IndexTypePropSearchableValue {
			return false
		}
	}
	return true
}
