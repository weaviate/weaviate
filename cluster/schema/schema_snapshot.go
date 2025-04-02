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

type LegacySnapshot struct {
	NodeID     string                `json:"node_id"`
	SnapshotID string                `json:"snapshot_id"`
	Classes    map[string]*metaClass `json:"classes"`
}

// LegacySchemaSnapshot returns a ready-to-use in-memory Raft snapshot based on the provided legacy schema
// This is used when migrating from pre 1.24 to 1.25+ versions and we migrate the old schema using a handcraft raft snapshot.
// Do not reproduce this ever please
func LegacySchemaSnapshot(nodeID string, m map[string]types.ClassState) (*raft.SnapshotMeta, io.ReadCloser, error) {
	store := raft.NewInmemSnapshotStore()
	sink, err := store.Create(raft.SnapshotVersionMax, 0, 0, raft.Configuration{}, 0, nil)
	if err != nil {
		return nil, nil, err
	}
	defer sink.Close()
	snap := LegacySnapshot{
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

// V0Restore restores a raft snapshot in the V0 format (format pre-dating the inclusion of a versioned schema snapshot mechanism)
func (s *schema) V0Restore(r io.Reader, parser Parser) error {
	snap := LegacySnapshot{}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %w", err)
	}
	for _, cls := range snap.Classes {
		if err := parser.ParseClass(&cls.Class); err != nil { // should not fail
			return fmt.Errorf("parsing class %q: %w", cls.Class.Class, err) // schema might be corrupted
		}
		cls.Sharding.SetLocalName(s.nodeID)
	}

	s.replaceClasses(snap.Classes)
	return nil
}

type schemaSnapshotVersion int

const (
	schemaSnapshotVersionV0 = iota
)

type VersionedSnapshot struct {
	snapshotVersion schemaSnapshotVersion
}
