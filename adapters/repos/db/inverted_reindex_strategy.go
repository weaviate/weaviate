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
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema"
)

// MigrationStrategy encapsulates the parts that differ per migration type
// (e.g., Map→Blockmax, Set→RoaringSet). The lifecycle logic (state machine,
// merge/swap/tidy, object iteration, progress tracking) lives in
// ShardReindexTaskGeneric.
type MigrationStrategy interface {
	// MigrationDirName returns the subdirectory name under .migrations/
	// e.g. "searchable_map_to_blockmax"
	MigrationDirName() string

	// SourceBucketName returns the original bucket name for the given property.
	// e.g. helpers.BucketSearchableFromPropNameLSM(propName)
	SourceBucketName(propName string) string

	// ReindexSuffix returns the suffix for reindex buckets.
	// Default is "__reindex"; blockmax overrides to "__blockmax_reindex" for backward compat.
	ReindexSuffix() string

	// IngestSuffix returns the suffix for ingest buckets.
	// Default is "__ingest"; blockmax overrides to "__blockmax_ingest" for backward compat.
	IngestSuffix() string

	// BackupSuffix returns the suffix for backup buckets.
	// Default is "__backup"; blockmax overrides to "__blockmax_map" for backward compat.
	BackupSuffix() string

	// SourceStrategy returns the LSM strategy of source buckets to discover.
	// e.g. lsmkv.StrategyMapCollection
	SourceStrategy() string

	// SourceIndexType returns the property index type to filter during discovery.
	// e.g. IndexTypePropSearchableValue
	SourceIndexType() PropertyIndexType

	// TargetStrategy returns the LSM strategy for reindex/ingest buckets.
	// e.g. lsmkv.StrategyInverted
	TargetStrategy() string

	// BackupStrategy returns the LSM strategy for backup buckets.
	// Usually the same as SourceStrategy.
	BackupStrategy() string

	// WriteToReindexBucket writes a single property's data for one object
	// into the reindex bucket during the async reindex loop.
	WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket, docID uint64,
		prop inverted.Property) error

	// ShouldProcessProperty returns true if this property should be handled
	// by the double-write callbacks.
	ShouldProcessProperty(property *inverted.Property) bool

	// MakeAddCallback creates a double-write callback for property additions.
	// forTargetStrategy=true during ingest phase, false during backup phase.
	MakeAddCallback(bucketNamer func(string) string, propsByName map[string]struct{},
		forTargetStrategy bool) onAddToPropertyValueIndex

	// MakeDeleteCallback creates a double-write callback for property deletions.
	// forTargetStrategy=true during ingest phase, false during backup phase.
	MakeDeleteCallback(bucketNamer func(string) string, propsByName map[string]struct{},
		forTargetStrategy bool) onDeleteFromPropertyValueIndex

	// PreReindexHook is called before the reindex/ingest phase begins on a shard.
	// e.g. shard.markSearchableBlockmaxProperties(props...)
	PreReindexHook(shard *Shard, props []string)

	// AnalyzerOverlay returns a per-property override map applied by the
	// inverted analyzer during the backfill scan. It is used by
	// "from-scratch" strategies (e.g. enable-filterable / enable-searchable)
	// that build a brand-new inverted bucket while the corresponding
	// schema flag is still false in the RAFT-stored schema. Without this
	// override the analyzer would skip the targeted property and produce
	// an empty target bucket.
	//
	// Strategies that don't need an overlay (the live schema flag is
	// already true for the targeted properties — e.g. retokenize,
	// map→blockmax, roaring-set refresh) should return nil.
	AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay

	// OnMigrationComplete is called when the migration is fully tidied on a
	// single shard. Implementations can read the shard's current bucket state
	// to decide whether collection-level finalization (e.g. flipping the
	// UsingBlockMaxWAND class flag) is safe — important for per-property
	// migrations, where the flag must only flip once every searchable property
	// has been migrated.
	OnMigrationComplete(ctx context.Context, shard ShardLike) error
}

// noAnalyzerOverlay supplies the default "no overlay" implementation of
// AnalyzerOverlay for strategies whose target properties already have the
// relevant schema flag set (retokenize, map→blockmax, roaring-set refresh).
// Embed this struct to get the nil-return default; strategies that need a
// real overlay (enable-filterable, enable-searchable, enable-rangeable)
// define their own method which shadows the embed.
type noAnalyzerOverlay struct{}

func (noAnalyzerOverlay) AnalyzerOverlay(_ []string) map[string]inverted.PropertyOverlay {
	return nil
}

// applyPerPropertySchemaUpdate is the shared body of every strategy's
// OnMigrationComplete that flips one or more property fields via
// per-property RAFT UpdateProperty commands.
//
// Concurrency: `fields` is the fieldmask passed all the way down to the
// FSM apply path (see cluster/proto/api.PropertyField* and
// cluster/schema/meta_class.go MergePropsMasked). Two strategies running
// in parallel on the same property — each touching different fields —
// no longer clobber each other on RAFT apply: the FSM merges only the
// listed fields onto the live class state, leaving the rest of the
// property untouched. An empty `fields` falls back to the legacy
// "replace every field" semantics for callers that don't care.
//
// We still re-read the class right before each per-property update.
// The mask closes the cross-strategy clobber path, but a strategy still
// has to make a decision (apply / skip) based on the current schema
// state — re-reading per-property keeps that decision fresh.
//
// The mutate callback is called with a *fresh* shallow copy of the
// property; the strategy mutates only the fields it owns. Return
// apply=false to skip the RAFT update for this property (e.g. the
// target state is already satisfied).
//
// Returns the names of properties that were not found in the class
// (e.g. dropped between task scheduling and completion). Multi-property
// strategies typically ignore this list — a missing property is the
// same as "already migrated" for them — while single-property strategies
// (FilterableRetokenize) treat it as a hard error to match the pre-helper
// behavior.
func applyPerPropertySchemaUpdate(
	ctx context.Context,
	mgr *schema.Manager,
	className string,
	propNames []string,
	fields []string,
	mutate func(prop *models.Property) (apply bool),
) (missing []string, err error) {
	for _, propName := range propNames {
		// Re-read the class right before each property update to minimize the
		// staleness window where a concurrent strategy could clobber our flag.
		class := mgr.ReadOnlyClass(className)
		if class == nil {
			return nil, fmt.Errorf("class %q not found", className)
		}

		var prop *models.Property
		for _, p := range class.Properties {
			if p.Name == propName {
				prop = p
				break
			}
		}
		if prop == nil {
			missing = append(missing, propName)
			continue
		}

		updated := *prop
		if !mutate(&updated) {
			continue
		}
		if err := schema.UpdatePropertyInternal(&mgr.Handler, ctx, className, &updated, fields...); err != nil {
			return missing, fmt.Errorf("updating property %q: %w", propName, err)
		}
	}
	return missing, nil
}

// reindexTaskConfig holds the configuration for a ShardReindexTaskGeneric.
// Renamed from mapToBlockmaxConfig to be strategy-agnostic.
type reindexTaskConfig struct {
	swapBuckets                   bool
	unswapBuckets                 bool
	tidyBuckets                   bool
	rollback                      bool
	conditionalStart              bool
	concurrency                   int
	memtableOptFactor             int
	backupMemtableOptFactor       int
	processingDuration            time.Duration
	pauseDuration                 time.Duration
	perObjectDelay                time.Duration
	checkProcessingEveryNoObjects int
	selectionEnabled              bool
	selectedPropsByCollection     map[string]map[string]struct{}
	selectedShardsByCollection    map[string]map[string]struct{}
}
