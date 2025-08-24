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

package schema

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"github.com/weaviate/weaviate/cluster/types"
)

// snapshot is the old format, we keep it for backwards compatibility
type snapshot struct {
	NodeID     string                `json:"node_id"`
	SnapshotID string                `json:"snapshot_id"`
	Classes    map[string]*metaClass `json:"classes"`
}

// LegacySnapshot returns a ready-to-use in-memory Raft snapshot based on the provided legacy schema
func LegacySnapshot(nodeID string, m map[string]types.ClassState) (*raft.SnapshotMeta, io.ReadCloser, error) {
	store := raft.NewInmemSnapshotStore()
	sink, err := store.Create(raft.SnapshotVersionMax, 0, 0, raft.Configuration{}, 0, nil)
	if err != nil {
		return nil, nil, err
	}
	defer sink.Close()
	snap := snapshot{
		NodeID:     nodeID,
		SnapshotID: sink.ID(),
		Classes:    make(map[string]*metaClass, len(m)),
	}
	for k, v := range m {
		// TODO support classTenantDataEvents here?
		snap.Classes[k] = &metaClass{Class: v.Class, Sharding: v.Shards}
	}

	if err := json.NewEncoder(sink).Encode(&snap); err != nil {
		return nil, nil, fmt.Errorf("encode: %w", err)
	}
	return store.Open(sink.ID())
}
