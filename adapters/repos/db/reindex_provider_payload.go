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
	"encoding/json"
	"errors"
)

// ReindexNamespace is the DTM namespace for reindex tasks.
const ReindexNamespace = "reindex"

// ErrReindexPayloadNamesNoCollection is [DecodeReindexTaskPayload]'s answer for
// a payload the decoder accepted but that names no collection.
var ErrReindexPayloadNamesNoCollection = errors.New("reindex task payload names no collection")

// DecodeReindexTaskPayload is the one place that decides whether a reindex
// task payload can be acted on. Every gate that has to answer "is this task
// readable enough to act on?" reads it through here so they cannot disagree.
// Call sites that only need a field out of a payload they already trust
// (conflict detection, the per-collection task count) still unmarshal directly.
//
// "Unreadable" keys on the collection being absent, not on the decoder
// erroring: a renamed field on a newer node's payload unmarshals silently
// into an empty collection (Go ignores unknown fields), which a naive
// decoder would then register as a free shard.
//
// The returned collection is the best name anything can recover, "" when
// nothing can:
//
//   - err == nil: act on the payload.
//   - err != nil, collection != "": scope the refusal to that collection.
//   - err != nil, collection == "": nothing says what the task holds, so the
//     only honest answer is cluster-wide, cancellable from any collection's
//     cancel endpoint.
func DecodeReindexTaskPayload(raw []byte) (ReindexTaskPayload, string, error) {
	var payload ReindexTaskPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ReindexTaskPayload{}, reindexTaskCollection(raw), err
	}
	if payload.Collection == "" {
		return ReindexTaskPayload{}, "", ErrReindexPayloadNamesNoCollection
	}
	return payload, payload.Collection, nil
}

// ExtractReindexTaskCollection decodes the class name a reindex task is
// bound to. Registered on startup via
// [Raft.RegisterDistributedTaskCollectionExtractor] so that DELETE_CLASS
// cascades into reindex task GC (weaviate/0-weaviate-issues#231). Lives
// next to [ReindexTaskPayload] so the payload format and its
// scoping-decoder evolve together.
//
// Reads through [DecodeReindexTaskPayload], so it recovers the collection from
// payloads the full decoder rejects. Without that fallback the one task an
// operator most needs to delete — the one no node can read — is the one
// deletion silently skips, leaving only the completed-task TTL to clear it.
func ExtractReindexTaskCollection(payload []byte) (string, bool) {
	_, collection, _ := DecodeReindexTaskPayload(payload)
	return collection, collection != ""
}

// reindexTaskCollection reads just the collection out of a task payload,
// tolerating a payload the full [ReindexTaskPayload] decoder rejects — a field
// retyped by a newer node during a rolling upgrade fails that decoder but
// leaves the collection perfectly readable. Empty when even that fails.
//
// Unexported so no gate can consult it on its own: read alone it says the
// payload is fine whenever the collection happens to be present, which is the
// judgement [DecodeReindexTaskPayload] exists to make in one place.
func reindexTaskCollection(raw []byte) string {
	var probe struct {
		Collection string `json:"collection"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return ""
	}
	return probe.Collection
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
