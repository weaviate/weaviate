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
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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

	// OnMigrationComplete is called when the migration is fully tidied.
	// e.g. update schema to UsingBlockMaxWAND=true
	OnMigrationComplete(ctx context.Context, className string) error
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
