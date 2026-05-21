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

// RebuildSearchableStrategy rebuilds an existing BlockMax (StrategyInverted)
// searchable bucket from the objects store while preserving the property's
// current tokenization and BM25 algorithm. Dispatch rejects WAND properties
// before this strategy is constructed.
type RebuildSearchableStrategy struct {
	propNames  []string
	generation int
}

func (s *RebuildSearchableStrategy) MigrationDirName() string {
	return migrationDirWithProps(MigrationDirPrefixRebuildSearchable, s.propNames) + genSuffix(s.generation)
}

func (s *RebuildSearchableStrategy) SourceBucketName(propName string) string {
	return helpers.BucketSearchableFromPropNameLSM(propName)
}

func (s *RebuildSearchableStrategy) ReindexSuffix() string {
	return "__rebuild_searchable_reindex" + genSuffix(s.generation)
}

func (s *RebuildSearchableStrategy) IngestSuffix() string {
	return "__rebuild_searchable_ingest" + genSuffix(s.generation)
}

func (s *RebuildSearchableStrategy) BackupSuffix() string {
	return "__rebuild_searchable_backup" + genSuffix(s.generation)
}

func (s *RebuildSearchableStrategy) SourceStrategy() string { return lsmkv.StrategyInverted }
func (s *RebuildSearchableStrategy) TargetStrategy() string { return lsmkv.StrategyInverted }
func (s *RebuildSearchableStrategy) BackupStrategy() string { return lsmkv.StrategyInverted }

func (s *RebuildSearchableStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropSearchableValue
}

func (s *RebuildSearchableStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	return writeBlockmaxSearchablePostings(shard, bucket, docID, prop)
}

// ShouldProcessProperty is true for every targeted property — selection is
// driven by selectedPropsByCollection in the task config.
func (s *RebuildSearchableStrategy) ShouldProcessProperty(_ *inverted.Property) bool { return true }

func (s *RebuildSearchableStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, _ bool,
) onAddToPropertyValueIndex {
	return blockmaxSearchableAddCallback(bucketNamer, propsByName)
}

func (s *RebuildSearchableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, _ bool,
) onDeleteFromPropertyValueIndex {
	return blockmaxSearchableDeleteCallback(bucketNamer, propsByName)
}

// PreReindexHook is a no-op — the target BlockMax bucket already exists
// (API dispatch rejects WAND properties before this strategy runs).
func (s *RebuildSearchableStrategy) PreReindexHook(_ *Shard, _ []string) {}

// AnalyzerOverlay returns nil — rebuild MUST NOT change tokenization
// (that's a separate verb: {searchable:{tokenization:X}}).
func (s *RebuildSearchableStrategy) AnalyzerOverlay(_ []string) map[string]inverted.PropertyOverlay {
	return nil
}

// OnMigrationComplete is a no-op — the property was already searchable
// + BlockMax pre-migration; no schema flag flips.
func (s *RebuildSearchableStrategy) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	return nil
}
