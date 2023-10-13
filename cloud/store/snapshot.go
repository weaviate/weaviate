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

func (s *schema) Restore(rc io.ReadCloser) error {
	s.Lock()
	defer s.Unlock()

	log.Println("restoring snapshot")

	defer func() {
		if err2 := rc.Close(); err2 != nil {
			log.Printf("restore snapshot: close reader: %v\n", err2)
		}
	}()

	//
	// TODO: restore class embedded sub types (see repo.schema.store)
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %v", err)
	}

	return nil
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s *schema) Persist(sink raft.SnapshotSink) (err error) {
	s.Lock()
	defer s.Unlock()

	defer sink.Close()

	err = json.NewEncoder(sink).Encode(s)

	// should we cal cancel if err != nil
	log.Println("persisting snapshot done", err)
	return err
}

// Release is invoked when we are finished with the snapshot.
func (s *schema) Release() {
	log.Println("snapshot has been successfully created")
}
