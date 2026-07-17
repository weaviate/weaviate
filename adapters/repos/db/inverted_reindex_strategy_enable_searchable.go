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
	"context"

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

func (s *EnableSearchableStrategy) MigrationDirName() string {
	return migrationDirWithProps(MigrationDirPrefixEnableSearchable, s.propNames) + genSuffix(s.generation)
}

func (s *EnableSearchableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName)
}

func (s *EnableSearchableStrategy) ReindexSuffix() string {
	return "__enable_searchable_reindex" + genSuffix(s.generation)
}

func (s *EnableSearchableStrategy) IngestSuffix() string {
	return "__enable_searchable_ingest" + genSuffix(s.generation)
}

func (s *EnableSearchableStrategy) BackupSuffix() string {
	return "__enable_searchable_backup" + genSuffix(s.generation)
}

func (s *EnableSearchableStrategy) SourceStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *EnableSearchableStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropSearchableValue
}

func (s *EnableSearchableStrategy) TargetStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *EnableSearchableStrategy) BackupStrategy() string {
	return lsmkv.StrategyInverted
}

func (s *EnableSearchableStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	return writeBlockmaxSearchablePostings(shard, bucket, docID, prop)
}

// ShouldProcessProperty always returns true — scope is driven by
// selectedPropsByCollection (see NewRuntimeEnableSearchableTask). The
// HasSearchableIndex schema flag is still false on targeted properties
// until OnMigrationComplete flips it.
func (s *EnableSearchableStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return true
}

func (s *EnableSearchableStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	var swapFallbackNamer func(string) string
	if forTargetStrategy {
		swapFallbackNamer = s.SourceBucketName
	}
	return blockmaxSearchableAddCallback(bucketNamer, propsByName, swapFallbackNamer)
}

func (s *EnableSearchableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	var swapFallbackNamer func(string) string
	if forTargetStrategy {
		swapFallbackNamer = s.SourceBucketName
	}
	return blockmaxSearchableDeleteCallback(bucketNamer, propsByName, swapFallbackNamer)
}

// PreReindexHook creates empty blockmax searchable buckets for the targeted
// properties and marks them as blockmax, so queries route to the new bucket
// as soon as it exists.
func (s *EnableSearchableStrategy) PreReindexHook(shard *Shard, props []string) {
	ctx := context.Background()
	for _, propName := range props {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		if shard.store.Bucket(bucketName) == nil {
			opts := shard.makeDefaultBucketOptions(lsmkv.StrategyInverted)
			if err := shard.store.CreateOrLoadBucket(ctx, bucketName, opts...); err != nil {
				shard.index.logger.WithField("bucket", bucketName).
					WithError(err).Error("PreReindexHook: failed to create searchable bucket")
			}
		}
	}
	shard.markSearchableBlockmaxProperties(props...)
}

// AnalyzerOverlay forces IndexSearchable=true and the target tokenization on
// the targeted properties while the backfill iterator scans the objects
// bucket. Without this override the analyzer would (a) skip the property
// because HasSearchableIndex returns false for IndexSearchable=nil/false on
// the live schema, and (b) even if it didn't skip, it would tokenize with
// the wrong (stored) tokenization. The live RAFT-stored schema is never
// mutated; both flags are flipped via OnMigrationComplete after backfill.
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

// OnMigrationComplete is a no-op for this semantic migration. The schema
// cutover (IndexSearchable=true + Tokenization flip via RAFT) now happens
// once cluster-wide from [ReindexProvider.OnTaskCompleted] after every
// node's local OnGroupCompleted has run the bucket pointer swap. See the
// Journey 3 canonical pattern in cluster/distributedtask/doc.go:111-137.
//
// Per-shard schema flips would re-introduce the first-shard-flips
// problem: the first shard on the first node to call RunSwapOnShard
// would flip the cluster-wide flags while other nodes / other shards
// still serve the old (searchable-disabled) state.
func (s *EnableSearchableStrategy) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	return nil
}
