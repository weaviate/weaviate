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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// EnableSearchableStrategy implements MigrationStrategy for creating a
// blockmax (StrategyInverted) searchable index on a text or text[] property
// that currently has none. It builds the bucket from the objects store with
// the target tokenization, then flips both IndexSearchable=true and
// Tokenization on the targeted properties in one RAFT update.
//
// New searchable buckets are created directly as blockmax; no map→blockmax
// transition is ever needed for a from-scratch enable.
type EnableSearchableStrategy struct {
	propNames    []string
	tokenization string
	generation   int // see genSuffix godoc
}

func (s *EnableSearchableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName)
}

func (s *EnableSearchableStrategy) TargetStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *EnableSearchableStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	return writeBlockmaxSearchablePostings(shard, bucket, docID, prop)
}

func (s *EnableSearchableStrategy) MakeAddCallback(bucketNamer func(string) string,
	armed armedMirror,
) onAddToPropertyValueIndex {
	return blockmaxSearchableAddCallback(bucketNamer, armed, s.SourceBucketName)
}

func (s *EnableSearchableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	armed armedMirror,
) onDeleteFromPropertyValueIndex {
	return blockmaxSearchableDeleteCallback(bucketNamer, armed, s.SourceBucketName)
}

func (s *EnableSearchableStrategy) AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay {
	if len(props) == 0 {
		return nil
	}
	out := make(map[string]inverted.PropertyOverlay, len(props))
	for _, p := range props {
		out[p] = inverted.PropertyOverlay{
			ForceSearchable: true,
			Tokenization:    s.tokenization,
		}
	}
	return out
}
