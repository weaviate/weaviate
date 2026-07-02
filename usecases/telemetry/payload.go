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

package telemetry

import (
	"github.com/go-openapi/strfmt"
)

// PayloadType is the discrete set of statuses which indicate the type of payload sent
var PayloadType = struct {
	Init      string
	Update    string
	Terminate string
}{
	Init:      "INIT",
	Update:    "UPDATE",
	Terminate: "TERMINATE",
}

// Payload is the object transmitted for telemetry purposes
type Payload struct {
	// --- existing fields, UNCHANGED ---
	MachineID              strfmt.UUID                     `json:"machineId"`
	Type                   string                          `json:"type"`
	Version                string                          `json:"version"`
	ObjectsCount           int64                           `json:"objs"`
	OS                     string                          `json:"os"`
	Arch                   string                          `json:"arch"`
	UsedModules            []string                        `json:"usedModules,omitempty"`
	CollectionsCount       int                             `json:"collectionsCount"`
	ClientUsage            map[ClientType]map[string]int64 `json:"clientUsage,omitempty"`
	ClientIntegrationUsage map[string]map[string]int64     `json:"clientIntegrationUsage,omitempty"`
	CloudProvider          *string                         `json:"cloudProvider,omitempty"`
	UniqueID               *string                         `json:"uniqueID,omitempty"`

	// --- NEW: stable identity ---
	// NodeID is a persisted UUID tied to the data volume. Stable across restarts;
	// resets only on data-volume wipe. NOT the hostname. Empty means unknown; it
	// has no meaningful set-but-empty value, so a value type with omitempty is
	// sufficient (no unknown-vs-known-empty ambiguity).
	NodeID string `json:"nodeId,omitempty"`
	// ClusterID is a UUIDv7 committed once per cluster lifetime via raft. Empty
	// means the identity was not committed yet (best-effort failed / single-node);
	// there is no known-empty clusterId, so string+omitempty is sufficient.
	ClusterID string `json:"clusterId,omitempty"`
	// ClusterCreatedAt is unix-millis of cluster inception. Stored explicitly so
	// consumers never decode UUIDv7 timestamp bits. Pointer so nil distinguishes
	// "cluster identity not committed" from a (never-legitimate) createdAt of 0.
	ClusterCreatedAt *int64 `json:"clusterCreatedAt,omitempty"`

	// --- NEW: curated signal ---
	// These are pointers so a successfully-measured zero/false serializes (e.g.
	// nodeCount:0, replicationEnabled:false) instead of being dropped by omitempty.
	// A nil pointer means the value could not be determined; the schema-derived
	// fields below are always measured, so they are always non-nil in practice.
	NodeCount                  *int           `json:"nodeCount,omitempty"`
	MaxReplicationFactor       *int           `json:"maxReplicationFactor,omitempty"`
	ReplicationEnabled         *bool          `json:"replicationEnabled,omitempty"`
	MTCollectionCount          *int           `json:"mtCollectionCount,omitempty"`
	NamedVectorCollectionCount *int           `json:"namedVectorCollectionCount,omitempty"`
	AsyncIndexingEnabled       *bool          `json:"asyncIndexingEnabled,omitempty"`
	VectorIndexTypeCounts      map[string]int `json:"vectorIndexTypeCounts,omitempty"`
}
