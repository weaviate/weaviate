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

// ReindexNamespace is the DTM namespace for reindex tasks.
const ReindexNamespace = "reindex"

// ReindexMigrationType identifies which migration strategy a reindex task uses.
type ReindexMigrationType string

const (
	// ReindexTypeRepairSearchable rebuilds searchable indexes from Map to Blockmax strategy.
	ReindexTypeRepairSearchable ReindexMigrationType = "repair-searchable"

	// ReindexTypeRepairFilterable refreshes filterable RoaringSet indexes.
	ReindexTypeRepairFilterable ReindexMigrationType = "repair-filterable"

	// ReindexTypeEnableRangeable adds RoaringSetRange indexes for numeric properties.
	ReindexTypeEnableRangeable ReindexMigrationType = "enable-rangeable"

	// ReindexTypeRepairRangeable rebuilds an existing RoaringSetRange index from
	// the current filterable bucket (same source-of-truth as enable-rangeable).
	// Use when a rangeable bucket is suspected corrupted or out of sync.
	ReindexTypeRepairRangeable ReindexMigrationType = "repair-rangeable"

	// ReindexTypeEnableFilterable creates a RoaringSet filterable index on a
	// property that currently has none. Flips IndexFilterable=true on completion.
	ReindexTypeEnableFilterable ReindexMigrationType = "enable-filterable"

	// ReindexTypeEnableSearchable creates a blockmax searchable index on a
	// property that currently has none. Flips IndexSearchable=true (and sets
	// Tokenization) on completion.
	ReindexTypeEnableSearchable ReindexMigrationType = "enable-searchable"

	// ReindexTypeChangeTokenization retokenizes text properties (searchable + filterable).
	ReindexTypeChangeTokenization ReindexMigrationType = "change-tokenization"

	// ReindexTypeChangeTokenizationFilterable retokenizes ONLY the filterable
	// index of a text/text[] property. Used when the property has no
	// searchable index — change-tokenization (which targets both buckets)
	// cannot run, so this filterable-scoped variant fills the gap.
	ReindexTypeChangeTokenizationFilterable ReindexMigrationType = "change-tokenization-filterable"
)

// ReindexTaskPayload is the JSON-serialized payload stored in the DTM task.
type ReindexTaskPayload struct {
	MigrationType      ReindexMigrationType `json:"migrationType"`
	Collection         string               `json:"collection"`
	Properties         []string             `json:"properties,omitempty"`
	TargetTokenization string               `json:"targetTokenization,omitempty"`
	// OriginalTokenization is the schema's `tokenization` value at task
	// submit time. Used by OnTaskCompleted to ensure the schema-flip
	// only runs when the schema is still at the pre-migration state.
	//
	// Without this guard, a node restart that replays the DTM scheduler's
	// callbacks for already-FINISHED tasks would fire OnTaskCompleted on
	// an older task whose payload says "target=field", overriding a
	// newer task's already-applied "target=word" schema. The replay does
	// not damage on-disk bucket data (the canonical bucket was promoted
	// at startup by FinalizeCompletedMigrations), but it leaves the
	// property's `tokenization` field at the stale older target,
	// breaking query tokenization on every subsequent BM25 search.
	//
	// Empty for non-change-tokenization migration types — they have no
	// pre-migration tokenization to guard against.
	OriginalTokenization string `json:"originalTokenization,omitempty"`
	BucketStrategy       string `json:"bucketStrategy,omitempty"`

	// Tenants records which tenants were targeted (informational, for MT collections).
	Tenants []string `json:"tenants,omitempty"`

	// UnitToNode maps unit IDs to the node name that should process them.
	UnitToNode map[string]string `json:"unitToNode"`
	// UnitToShard maps unit IDs to shard names.
	UnitToShard map[string]string `json:"unitToShard"`
}
