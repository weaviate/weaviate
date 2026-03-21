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

	// ReindexTypeChangeTokenization retokenizes text properties (searchable + filterable).
	ReindexTypeChangeTokenization ReindexMigrationType = "change-tokenization"
)

// ReindexTaskPayload is the JSON-serialized payload stored in the DTM task.
type ReindexTaskPayload struct {
	MigrationType      ReindexMigrationType `json:"migrationType"`
	Collection         string               `json:"collection"`
	Properties         []string             `json:"properties,omitempty"`
	TargetTokenization string               `json:"targetTokenization,omitempty"`
	BucketStrategy     string               `json:"bucketStrategy,omitempty"`

	// Tenants records which tenants were targeted (informational, for MT collections).
	Tenants []string `json:"tenants,omitempty"`

	// UnitToNode maps unit IDs to the node name that should process them.
	UnitToNode map[string]string `json:"unitToNode"`
	// UnitToShard maps unit IDs to shard names.
	UnitToShard map[string]string `json:"unitToShard"`
}
