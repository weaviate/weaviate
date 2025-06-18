//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package fsm

// Snapshot is the snapshot of the cluster FSMs (schema, rbac, etc)
// it is used to restore the Snapshot to a previous state,
// or to bring out-of-date followers up to a recent log index.
type Snapshot struct {
	// NodeID is the id of the node that created the snapshot
	NodeID string `json:"node_id"`
	// SnapshotID is the id of the snapshot comes from the provided Sink
	SnapshotID string `json:"snapshot_id"`
	// LegacySchema is the old schema that was used to create the snapshot
	// it is used to restore the schema if the snapshot is not compatible with the current schema
	// note: this is not used anymore, but we keep it for backwards compatibility
	LegacySchema map[string]any `json:"classes"`
	// Schema is the new schema that will be used to restore the FSM
	Schema []byte `json:"schema,omitempty"`
	// RBAC is the rbac that will be used to restore the FSM
	RBAC []byte `json:"rbac,omitempty"`
	// DistributedTasks are the tasks that will be used to restore the FSM.
	DistributedTasks []byte `json:"distributed_tasks,omitempty"`
	// ReplicationOps are the currently ongoing operation for replica replication
	ReplicationOps []byte `json:"replication_ops,omitempty"`
	// DbUsers is the state of dynamic db users that will be used to restore the FSM
	DbUsers []byte `json:"dbusers,omitempty"`
}

// Snapshotter is used to snapshot and restore any (FSM) state
type Snapshotter interface {
	Snapshot() ([]byte, error)
	Restore([]byte) error
}
