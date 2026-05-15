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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// FilterableRetokenizeStrategy implements MigrationStrategy for rebuilding the
// filterable (RoaringSet) index for a text property with a different tokenization
// strategy (e.g. WORD → FIELD). This runs after the searchable strategy and
// performs the schema update in OnMigrationComplete.
type FilterableRetokenizeStrategy struct {
	noAnalyzerOverlay
	propName           string
	targetTokenization string
	className          string
	generation         int // see genSuffix godoc
}

func (s *FilterableRetokenizeStrategy) MigrationDirName() string {
	return MigrationDirPrefixFilterableRetokenize + "_" + s.propName + genSuffix(s.generation)
}

func (s *FilterableRetokenizeStrategy) SourceBucketName(_ string) string {
	return helpers.BucketFromPropNameLSM(s.propName)
}

func (s *FilterableRetokenizeStrategy) ReindexSuffix() string {
	return "__filt_retokenize_reindex" + genSuffix(s.generation)
}

func (s *FilterableRetokenizeStrategy) IngestSuffix() string {
	return "__filt_retokenize_ingest" + genSuffix(s.generation)
}

func (s *FilterableRetokenizeStrategy) BackupSuffix() string {
	return "__filt_retokenize_backup" + genSuffix(s.generation)
}

func (s *FilterableRetokenizeStrategy) SourceStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *FilterableRetokenizeStrategy) SourceIndexType() PropertyIndexType {
	return IndexTypePropValue
}

func (s *FilterableRetokenizeStrategy) TargetStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *FilterableRetokenizeStrategy) BackupStrategy() string {
	return lsmkv.StrategyRoaringSet
}

func (s *FilterableRetokenizeStrategy) WriteToReindexBucket(_ ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	if len(prop.RawValues) == 0 {
		return nil
	}

	analyzer := inverted.NewAnalyzer(nil, s.className)
	items := analyzer.TextArray(s.targetTokenization, prop.RawValues, prop.Name, nil)

	for _, item := range items {
		if err := bucket.RoaringSetAddOne(item.Data, docID); err != nil {
			return fmt.Errorf("filterable retokenize prop '%s': %w", prop.Name, err)
		}
	}
	return nil
}

func (s *FilterableRetokenizeStrategy) ShouldProcessProperty(property *inverted.Property) bool {
	return property.HasFilterableIndex && property.Name == s.propName
}

// MakeAddCallback returns a callback for adding documents to the filterable (RoaringSet) index.
// forTargetStrategy controls which tokenization is used: true uses the new target
// tokenization (for the reindex bucket), false uses the existing tokenization
// (for the ingest/double-write bucket that must match the currently live index).
func (s *FilterableRetokenizeStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	// Hoist the analyzer out of the per-callback hot path; see the
	// corresponding comment in SearchableRetokenizeStrategy.MakeAddCallback.
	var analyzer *inverted.Analyzer
	if forTargetStrategy {
		analyzer = inverted.NewAnalyzer(nil, s.className)
	}
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)

		var items []inverted.Countable
		if forTargetStrategy && len(property.RawValues) > 0 {
			items = analyzer.TextArray(s.targetTokenization, property.RawValues, property.Name, nil)
		} else {
			items = property.Items
		}

		for _, item := range items {
			if err := shard.addToPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("filterable retokenize add prop '%s' to bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

// MakeDeleteCallback returns a callback for removing documents from the filterable index.
// forTargetStrategy has the same semantics as in MakeAddCallback.
func (s *FilterableRetokenizeStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	// Hoist the analyzer out of the per-callback hot path; see the
	// corresponding comment in SearchableRetokenizeStrategy.MakeAddCallback.
	var analyzer *inverted.Analyzer
	if forTargetStrategy {
		analyzer = inverted.NewAnalyzer(nil, s.className)
	}
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if !property.HasFilterableIndex {
			return nil
		}
		if _, ok := propsByName[property.Name]; !ok {
			return nil
		}

		bucketName := bucketNamer(property.Name)
		bucket := shard.store.Bucket(bucketName)

		var items []inverted.Countable
		if forTargetStrategy && len(property.RawValues) > 0 {
			items = analyzer.TextArray(s.targetTokenization, property.RawValues, property.Name, nil)
		} else {
			items = property.Items
		}

		for _, item := range items {
			if err := shard.deleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
				return fmt.Errorf("filterable retokenize delete prop '%s' from bucket '%s': %w", item.Data, bucketName, err)
			}
		}
		return nil
	}
}

func (s *FilterableRetokenizeStrategy) PreReindexHook(_ *Shard, _ []string) {
	// No-op: the filterable bucket already exists.
}

// OnMigrationComplete is a no-op for semantic migrations. The schema
// cutover (Tokenization flip via RAFT) now happens once cluster-wide
// from [ReindexProvider.OnTaskCompleted] after every node's local
// OnGroupCompleted has run the bucket pointer swap. See the Journey 3
// canonical pattern in cluster/distributedtask/doc.go:111-137.
//
// Per-shard schema flips would re-introduce the first-shard-flips
// problem: the first shard on the first node to call RunSwapOnShard
// would flip the cluster-wide schema flag while other nodes / other
// shards still serve the old bucket — producing partial results
// during the cross-node swap stagger window.
func (s *FilterableRetokenizeStrategy) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	return nil
}
