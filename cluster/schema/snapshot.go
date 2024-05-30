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

type snapshot struct {
	NodeID     string                `json:"node_id"`
	SnapshotID string                `json:"snapshot_id"`
	Classes    map[string]*metaClass `json:"classes"`
}

func (s *schema) Restore(r io.Reader, parser Parser) error {
	snap := snapshot{}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %v", err)
	}
	for _, cls := range snap.Classes {
		if err := parser.ParseClass(&cls.Class); err != nil { // should not fail
			return fmt.Errorf("parsing class %q: %w", cls.Class.Class, err) // schema might be corrupted
		}
		cls.Sharding.SetLocalName(s.nodeID)
	}

	s.Lock()
	defer s.Unlock()
	s.Classes = snap.Classes

	return nil
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s *schema) Persist(sink raft.SnapshotSink) (err error) {
	s.Lock()
	defer s.Unlock()

	defer sink.Close()
	snap := snapshot{
		NodeID:     s.nodeID,
		SnapshotID: sink.ID(),
		Classes:    s.Classes,
	}
	if err := json.NewEncoder(sink).Encode(&snap); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	return nil
}

func (s *schema) Release() {
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
		snap.Classes[k] = &metaClass{Class: v.Class, Sharding: v.Shards}
	}

	if err := json.NewEncoder(sink).Encode(&snap); err != nil {
		return nil, nil, fmt.Errorf("encode: %w", err)
	}
	return store.Open(sink.ID())
}
