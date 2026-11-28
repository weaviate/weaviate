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

// RebuildSearchableStrategy rebuilds an existing BlockMax (StrategyInverted)
// searchable bucket from the objects store while preserving the property's
// current tokenization and BM25 algorithm. Dispatch rejects WAND properties
// before this strategy is constructed.
type RebuildSearchableStrategy struct {
	propNames  []string
	generation int
}

func (s *RebuildSearchableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName)
}

func (s *RebuildSearchableStrategy) TargetStrategy() string { return lsmkv.StrategyInverted }

func (s *RebuildSearchableStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	return writeBlockmaxSearchablePostings(shard, bucket, docID, prop)
}

func (s *RebuildSearchableStrategy) MakeAddCallback(bucketNamer func(string) string,
	armed armedMirror,
) onAddToPropertyValueIndex {
	return blockmaxSearchableAddCallback(bucketNamer, armed, s.SourceBucketName)
}

func (s *RebuildSearchableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	armed armedMirror,
) onDeleteFromPropertyValueIndex {
	return blockmaxSearchableDeleteCallback(bucketNamer, armed, s.SourceBucketName)
}

// AnalyzerOverlay returns nil — rebuild MUST NOT change tokenization
// (that's a separate operation: PUT .../index/searchable {"tokenization":X}).
func (s *RebuildSearchableStrategy) AnalyzerOverlay(_ []string) map[string]inverted.PropertyOverlay {
	return nil
}
