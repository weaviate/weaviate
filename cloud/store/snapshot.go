//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/raft"
)

type snapshot struct {
	NodeID     string                `json:"node_id"`
	SnapshotID string                `json:"snapshot_id"`
	Classes    map[string]*metaClass `json:"classes"`
}

func (s *schema) Restore(r io.Reader) error {
	log.Println("restoring snapshot")
	snap := snapshot{}
	if err := json.NewDecoder(r).Decode(&snap); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %v", err)
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
	snap := snapshot{NodeID: s.nodeID, SnapshotID: sink.ID(), Classes: s.Classes}
	if err := json.NewEncoder(sink).Encode(&snap); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	log.Printf("snapshot persistence completed successfully: %s\n", sink.ID())
	return nil
}

// Release is invoked when we are finished with the snapshot.
func (s *schema) Release() {
}
