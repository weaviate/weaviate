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

import "encoding/json"

// ReindexNamespace is the DTM namespace for reindex tasks.
const ReindexNamespace = "reindex"

// ExtractReindexTaskCollection decodes the class name a reindex task is
// bound to. Registered on startup via
// [Raft.RegisterDistributedTaskCollectionExtractor] so that DELETE_CLASS
// cascades into reindex task GC (weaviate/0-weaviate-issues#231). Lives
// next to [ReindexTaskPayload] so the payload format and its
// scoping-decoder evolve together.
func ExtractReindexTaskCollection(payload []byte) (string, bool) {
	var p ReindexTaskPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return "", false
	}
	return p.Collection, p.Collection != ""
}

// ReindexMigrationType identifies which migration strategy a reindex task uses.
type ReindexMigrationType string

const (
	// ReindexTypeChangeAlgorithm migrates searchable indexes from Map (WAND)
	// to Inverted (BlockMax). Dispatched by {searchable:{algorithm:"blockmax"}}.
	ReindexTypeChangeAlgorithm ReindexMigrationType = "change-algorithm"

	// ReindexTypeRebuildSearchable rebuilds an existing BlockMax searchable
	// bucket from the objects store, preserving tokenization and algorithm.
	// Dispatched by {searchable:{rebuild:true}} on BlockMax properties.
	ReindexTypeRebuildSearchable ReindexMigrationType = "rebuild-searchable"

	// ReindexTypeRepairFilterable refreshes filterable RoaringSet indexes.
	ReindexTypeRepairFilterable ReindexMigrationType = "repair-filterable"

	// ReindexTypeEnableRangeable adds RoaringSetRange indexes for numeric properties.
	ReindexTypeEnableRangeable ReindexMigrationType = "enable-rangeable"

	// ReindexTypeRepairRangeable rebuilds an existing RoaringSetRange index by
	// re-scanning the objects bucket (same source-of-truth as enable-rangeable).
	// Use when a rangeable bucket is suspected corrupted or out of sync.
	//
	// Source-of-truth note: this rebuilds from OBJECTS, not from the filterable
	// bucket. The strategy that implements it
	// ([FilterableToRangeableStrategy]) is misleadingly named for historical
	// reasons; see the strategy's file-level godoc — it explicitly does not
	// read from the filterable bucket because filterable may not even exist
	// on a numeric property created with IndexFilterable=false. Tracked at
	// weaviate/0-weaviate-issues#227 (Gap 3 doc-bug) as a load-bearing
	// correctness assertion: callers relying on this comment to design their
	// recovery flow would otherwise assume the filterable bucket is the
	// authoritative source.
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
	// OriginalTokenization records the schema's tokenization at task submit
	// time. Retained for RAFT-log diagnostics; not consulted at runtime.
	OriginalTokenization string `json:"originalTokenization,omitempty"`
	BucketStrategy       string `json:"bucketStrategy,omitempty"`

	// Tenants records which tenants were targeted (informational, for MT collections).
	Tenants []string `json:"tenants,omitempty"`

	// UnitToNode maps unit IDs to the node name that should process them.
	UnitToNode map[string]string `json:"unitToNode"`
	// UnitToShard maps unit IDs to shard names.
	UnitToShard map[string]string `json:"unitToShard"`
}
