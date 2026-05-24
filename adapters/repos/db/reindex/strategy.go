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

package reindex

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema"
)

// Bucket-suffix naming convention: all reindex-track sidecar buckets carry a
// `__` (double-underscore) prefix on their per-strategy suffix
// (`__retokenize_ingest`, `__rangeable_reindex`, …). Origin is internal to this
// track — no pre-existing `__` convention in lsmkv. Kept double-underscore so
// the sidecars are visually distinct from canonical bucket names (which use
// single-underscore separators inside helpers.Bucket*FromPropNameLSM) and
// reserved namespace for any future user-defined property whose name happens
// to collide with a suffix base.

// Test-only export: relocation follow-up tracked separately; no new external callers.
//
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
		forTargetStrategy bool) OnAddToPropertyValueIndex

	// MakeDeleteCallback creates a double-write callback for property deletions.
	// forTargetStrategy=true during ingest phase, false during backup phase.
	MakeDeleteCallback(bucketNamer func(string) string, propsByName map[string]struct{},
		forTargetStrategy bool) OnDeleteFromPropertyValueIndex

	// PreReindexHook is called before the reindex/ingest phase begins on a shard.
	// e.g. shard.MarkSearchableBlockmaxProperties(props...)
	PreReindexHook(shard ShardLike, props []string)

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
	//
	// Phase contract (see inverted_reindex_task_generic.go file-level
	// godoc): OnMigrationComplete fires in Phase 2c — AFTER the per-prop
	// SwapBucketPointer tight loop (Phase 2a) and AFTER the inline
	// oldMain.Shutdown + oldMain→backup rename loop (Phase 2b), but still
	// INSIDE the per-shard tokenization-overlay window for migrations
	// that use one (change-tokenization-{searchable,filterable},
	// enable-filterable, enable-searchable). The overlay is cleared
	// later by the cluster-wide schema flip in
	// [ReindexProvider.OnTaskCompleted].
	//
	// Allowed work in this position:
	//
	//   - In-memory mutation of shard-local query-path state that MUST
	//     match the cluster-wide schema flip before that flip
	//     propagates. The canonical example is
	//     [Shard.setRangeableLocallyReady] in
	//     [FilterableToRangeableStrategy.OnMigrationComplete] — it
	//     ensures THIS shard's queries observe ready=true at the same
	//     moment they could observe the new schema flag. The overlay
	//     is the equivalent mechanism for tokenization changes; this
	//     hook is the equivalent for per-shard ready flags.
	//
	//   - RAFT calls (per-property schema updates) for non-semantic
	//     strategies whose schema flip is NOT batched in
	//     OnTaskCompleted: e.g. [MapToBlockmaxStrategy]'s
	//     updateToBlockMaxInvertedIndexConfig (class-level
	//     UsingBlockMaxWAND), [FilterableToRangeableStrategy]'s
	//     applyPerPropertySchemaUpdate (per-property IndexRangeFilters).
	//     These are slow (hundreds of ms) — correctness is preserved by
	//     the overlay covering the per-shard window — but they widen
	//     the FINALIZING duration beyond what the per-shard atomic
	//     contract intends. The long-term fix is to split this hook
	//     into "local-in-memory (atomic-safe)" and "cluster-wide-RAFT
	//     (outside-atomic)" callbacks.
	//
	// Forbidden work in this position:
	//
	//   - Heavy disk I/O on the new main bucket (the LIVE post-swap
	//     bucket). The bucket is being queried — any operation that
	//     stalls its compaction or flush pipeline propagates as query
	//     latency. Disk I/O on the OLD bucket is fine (it's been
	//     shut down in Phase 2b) but conventionally also moved to
	//     Phase 2b.
	//
	//   - Anything that requires the cluster-wide schema flip to have
	//     already happened. For semantic migrations the flip lives in
	//     OnTaskCompleted (after every shard's OnMigrationComplete);
	//     for non-semantic, this hook may itself drive the flip but
	//     must not assume it has already propagated to other replicas.
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
		// Route through the migration-aware path so the
		// FromInFlightMigration flag is set on the resulting RAFT
		// command. Without this flag the schema FSM's MutationGuard
		// would reject this update because the in-flight reindex task
		// is in STARTED/FINALIZING — the very guard the migration
		// completion depends on bypassing.
		if err := schema.UpdatePropertyInternalFromMigration(&mgr.Handler, ctx, className, &updated, fields...); err != nil {
			return missing, fmt.Errorf("updating property %q: %w", propName, err)
		}
	}
	return missing, nil
}

// Test-only export: relocation follow-up tracked separately; no new external callers.
//
// reindexTaskConfig holds the configuration for a ShardReindexTaskGeneric.
// Renamed from mapToBlockmaxConfig to be strategy-agnostic.
type ReindexTaskConfig struct {
	SwapBuckets                   bool
	UnswapBuckets                 bool
	TidyBuckets                   bool
	Rollback                      bool
	ConditionalStart              bool
	Concurrency                   int
	MemtableOptFactor             int
	BackupMemtableOptFactor       int
	ProcessingDuration            time.Duration
	PauseDuration                 time.Duration
	PerObjectDelay                time.Duration
	CheckProcessingEveryNoObjects int
	SelectionEnabled              bool
	SelectedPropsByCollection     map[string]map[string]struct{}
	SelectedShardsByCollection    map[string]map[string]struct{}
}
