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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// Bucket-suffix naming convention: all reindex-track sidecar buckets carry a
// `__` (double-underscore) prefix on their per-strategy suffix
// (`__retokenize_ingest`, `__rangeable_reindex`, …). Origin is internal to this
// track — no pre-existing `__` convention in lsmkv. Kept double-underscore so
// the sidecars are visually distinct from canonical bucket names (which use
// single-underscore separators inside helpers.Bucket*FromPropNameLSM) and
// reserved namespace for any future user-defined property whose name happens
// to collide with a suffix base.

type MigrationStrategyCode string

const (
	StrategyCodeSearchableMapToBlockmax     MigrationStrategyCode = "searchable_map_to_blockmax"
	StrategyCodeFilterableRoaringsetRefresh MigrationStrategyCode = "filterable_roaringset_refresh"
	StrategyCodeFilterableToRangeable       MigrationStrategyCode = "filterable_to_rangeable"
	StrategyCodeSearchableRetokenize        MigrationStrategyCode = "searchable_retokenize"
	StrategyCodeFilterableRetokenize        MigrationStrategyCode = "filterable_retokenize"
	StrategyCodeEnableFilterable            MigrationStrategyCode = "enable_filterable"
	StrategyCodeEnableSearchable            MigrationStrategyCode = "enable_searchable"
	StrategyCodeRebuildSearchable           MigrationStrategyCode = "rebuild_searchable"
)

type MigrationStrategy interface {
	// TargetStrategy returns the LSM strategy for reindex/ingest buckets.
	// e.g. lsmkv.StrategyInverted
	TargetStrategy() string

	// WriteToReindexBucket writes a single property's data for one object
	// into the reindex bucket during the async reindex loop.
	WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket, docID uint64,
		prop inverted.Property) error

	// MakeAddCallback creates a double-write callback for property additions.
	MakeAddCallback(bucketNamer func(string) string, armed armedMirror,
	) onAddToPropertyValueIndex

	// MakeDeleteCallback creates a double-write callback for property deletions.
	MakeDeleteCallback(bucketNamer func(string) string, armed armedMirror,
	) onDeleteFromPropertyValueIndex

	// AnalyzerOverlay returns a per-property override map applied by the
	// analyzer; without it a property whose schema flag is false is skipped.
	AnalyzerOverlay(props []string) map[string]inverted.PropertyOverlay
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
