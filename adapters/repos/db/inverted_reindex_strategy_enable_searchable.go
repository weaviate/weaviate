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

// MakeAddCallback wraps the shared postings mirror with a BM25 prop-length
// tally track call for the migrating prop, closing the post-swap residual
// stranding window described in weaviate/0-weaviate-issues#322. See
// trackMigratingPropLength's godoc for why this is only safe/correct for
// EnableSearchableStrategy (not RebuildSearchableStrategy, which shares the
// postings callback but must NOT tally-track - see that strategy's
// AnalyzerOverlay).
func (s *EnableSearchableStrategy) MakeAddCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onAddToPropertyValueIndex {
	postings := blockmaxSearchableAddCallback(bucketNamer, propsByName)
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if err := postings(shard, docID, property); err != nil {
			return err
		}
		return trackMigratingPropLength(shard, propsByName, property)
	}
}

// MakeDeleteCallback is the delete-side counterpart of MakeAddCallback -
// see trackMigratingPropLength / untrackMigratingPropLength.
func (s *EnableSearchableStrategy) MakeDeleteCallback(bucketNamer func(string) string,
	propsByName map[string]struct{}, forTargetStrategy bool,
) onDeleteFromPropertyValueIndex {
	postings := blockmaxSearchableDeleteCallback(bucketNamer, propsByName)
	return func(shard *Shard, docID uint64, property *inverted.Property) error {
		if err := postings(shard, docID, property); err != nil {
			return err
		}
		return untrackMigratingPropLength(shard, propsByName, property)
	}
}

// trackMigratingPropLength / untrackMigratingPropLength feed the BM25
// prop-length tracker for a property an in-flight EnableSearchableStrategy
// migration is force-searchable-ing, closing the residual window between
// Shard.recomputeSearchableTallyForProp's rescan and this task's
// double-write callbacks being disabled: a write landing in that window
// would mirror postings into the searchable bucket but never reach avgdl,
// because SetPropertyLengths only tracks props whose HasSearchableIndex is
// already true on the live (not-yet-flipped) schema.
//
// Gating on propsByName (not property.HasSearchableIndex, which always
// reflects the live schema) is load-bearing, same reason AnalyzerOverlay
// forces the flag during backfill.
//
// These callbacks only fire for a property that already carries a live
// filterable/rangeable index (HasAnyInvertedIndex gates AnalyzeObject). A
// property with no live index at all never reaches them; those writes are
// covered by recomputeSearchableTallyForProp's flush-then-rescan instead -
// see that function's godoc for the fuller window picture.
//
// Gating on property.Length!=0 (not len(property.Items)!=0) is deliberate:
// DeltaSkipSearchable (inverted/delta_analyzer.go) can hand these callbacks
// an ITEM-LEVEL delta rather than the object's full value when the prop
// also carries another live index, but it always carries the FULL
// previous/next Length through on both delta entries - except for its
// synthetic "this side doesn't apply" placeholder, which hard-codes
// Length:0. Gating on Length!=0 fires on every entry with a real value
// while skipping exactly the placeholders that would corrupt Count.
//
// The tracked magnitude is len(property.Items), not property.Length (a
// different unit - see the Property godoc): for an in-place edit, items
// unchanged across the delta cancel out arithmetically, so
// len(next.Items)-len(prev.Items) == len(toAdd.Items)-len(toDel.Items),
// matching SetPropertyLengths/subtractPropLengths exactly.
//
// untrackMigratingPropLength uses JsonShardMetaData.UnTrackPropertyIfPresent
// (a single-lock-acquisition presence-check-and-untrack) rather than a
// separate PropertyTally pre-check + UnTrackProperty, because that
// composition races Shard.recomputeSearchableTallyForProp's ResetProperty -
// see UnTrackPropertyIfPresent's own godoc for the TOCTOU this closes.
func trackMigratingPropLength(shard *Shard, propsByName map[string]struct{}, property *inverted.Property) error {
	if _, ok := propsByName[property.Name]; !ok {
		return nil
	}
	if property.Length <= 0 {
		return nil
	}
	if err := shard.GetPropertyLengthTracker().TrackProperty(property.Name, float32(len(property.Items))); err != nil {
		return fmt.Errorf("tracking BM25 tally for migrating prop %q: %w", property.Name, err)
	}
	return nil
}

func untrackMigratingPropLength(shard *Shard, propsByName map[string]struct{}, property *inverted.Property) error {
	if _, ok := propsByName[property.Name]; !ok {
		return nil
	}
	if property.Length <= 0 {
		return nil
	}
	if _, err := shard.GetPropertyLengthTracker().UnTrackPropertyIfPresent(property.Name, float32(len(property.Items))); err != nil {
		return fmt.Errorf("untracking BM25 tally for migrating prop %q: %w", property.Name, err)
	}
	return nil
}

// PreReindexHook creates empty blockmax searchable buckets for the targeted
// properties and marks them as blockmax, so queries route to the new bucket
// as soon as it exists. Null/length buckets are created too - see
// [EnableFilterableStrategy.PreReindexHook] for why.
func (s *EnableSearchableStrategy) PreReindexHook(shard *Shard, props []string) {
	ctx := context.Background()
	for _, propName := range props {
		shard.ensureNullLengthBucketsForMigration(ctx, propName)
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
