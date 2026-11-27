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
	"fmt"
	"slices"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// ReindexNamespace is the DTM namespace for reindex tasks.
const ReindexNamespace = "reindex"

type ReindexTaskHeader struct {
	ReindexTaskPayload
}

func (h ReindexTaskHeader) Collection() string { return h.ReindexTaskPayload.Collection }

func (h ReindexTaskHeader) VisiblePayload() map[string]any {
	return visibleFields(ReindexTaskPayload{
		MigrationType: h.MigrationType, Collection: h.ReindexTaskPayload.Collection,
		TargetTokenization: h.TargetTokenization, OriginalTokenization: h.OriginalTokenization,
		BucketStrategy: h.BucketStrategy, ViewServed: h.ViewServed, Generation: h.Generation,
	}, ReindexTaskVisiblePayloadFields)
}

// visibleFields encodes only what the caller copied into payload, so a large hidden field costs nothing.
func visibleFields(payload any, fields []string) map[string]any {
	raw, err := json.Marshal(payload)
	if err != nil {
		return map[string]any{}
	}
	var all map[string]any
	if err := json.Unmarshal(raw, &all); err != nil {
		return map[string]any{}
	}
	for key := range all {
		if !slices.Contains(fields, key) {
			delete(all, key)
		}
	}
	return all
}

var _ distributedtask.HeaderDecoder = ReindexDetector{}

func (ReindexDetector) DecodeCollection(payload []byte) (string, error) {
	return decodePayloadCollection(payload)
}

func decodePayloadCollection(payload []byte) (string, error) {
	var p struct {
		Collection string `json:"collection"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return "", err
	}
	return p.Collection, nil
}

// DecodeHeader unmarshals the whole payload, so a malformed field anywhere yields no header and the checks refuse.
func (ReindexDetector) DecodeHeader(payload []byte) (distributedtask.TaskHeader, error) {
	var p ReindexTaskPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	if p.Collection == "" || p.MigrationType == "" {
		return nil, fmt.Errorf("reindex payload missing collection or migrationType")
	}
	return ReindexTaskHeader{p}, nil
}

// ReindexHeaderOf is how every reader gets a reindex task's payload, and is false when the payload did not decode.
func ReindexHeaderOf(task *distributedtask.Task) (ReindexTaskHeader, bool) {
	h, ok := task.Header().(ReindexTaskHeader)
	return h, ok
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

	// Tenants scopes the cancel and terminal sweeps (MT only).
	Tenants []string `json:"tenants,omitempty"`

	// ReindexTaskRules.Admit gives a generation only to ViewServed runs, so v1.39.x payloads pass unchanged.
	ViewServed bool   `json:"viewServed,omitempty"`
	Generation uint64 `json:"generation,omitempty"`
}

// ReindexTaskVisiblePayloadFields are the payload fields GET /v1/tasks shows a
// caller without tenant read. Any other field could name tenants, shards or
// nodes, so it stays hidden until it is listed here.
var ReindexTaskVisiblePayloadFields = []string{
	"migrationType", "collection", "targetTokenization", "originalTokenization", "bucketStrategy",
	"viewServed", "generation",
}
