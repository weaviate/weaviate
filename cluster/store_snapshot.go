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

package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// FSMSnapshot is the snapshot of the cluster FSMs (schema, rbac, etc)
// it is used to restore the FSM to a previous state,
// or to bring out-of-date followers up to a recent log index.
type FSMSnapshot struct {
	// NodeID is the id of the node that created the snapshot
	NodeID string `json:"node_id"`
	// SnapshotID is the id of the snapshot comes from the provided Sink
	SnapshotID string `json:"snapshot_id"`
	// LegacySchema is the old schema that was used to create the snapshot
	// it is used to restore the schema if the snapshot is not compatible with the current schema
	// note: this is not used anymore, but we keep it for backwards compatibility
	LegacySchema map[string]any `json:"classes,omitempty"`
	// Schema is the new schema that will be used to restore the FSM
	Schema []byte `json:"schema,omitempty"`
	// RBAC is the rbac that will be used to restore the FSM
	RBAC []byte `json:"rbac,omitempty"`
}

// Snapshotter is used to snapshot and restore any (FSM) state
type Snapshotter interface {
	Snapshot() ([]byte, error)
	Restore(r io.Reader) error
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (s *Store) Persist(sink raft.SnapshotSink) (err error) {
	defer sink.Close()
	schemaSnapshot, err := s.schemaManager.Snapshot()
	if err != nil {
		return fmt.Errorf("schema snapshot: %w", err)
	}

	rbacSnapshot, err := s.authZManager.Snapshot()
	if err != nil {
		return fmt.Errorf("rbac snapshot: %w", err)
	}
	snap := FSMSnapshot{
		NodeID:     s.cfg.NodeID,
		SnapshotID: sink.ID(),
		Schema:     schemaSnapshot,
		RBAC:       rbacSnapshot,
	}
	if err := json.NewEncoder(sink).Encode(&snap); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	return nil
}

func (s *Store) Release() {
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
//
// The Snapshot implementation should return quickly, because Apply can not
// be called while Snapshot is running. Generally this means Snapshot should
// only capture a pointer to the state, and any expensive IO should happen
// as part of FSMSnapshot.Persist.
//
// Apply and Snapshot are always called from the same thread, but Apply will
// be called concurrently with FSMSnapshot.Persist. This means the FSM should
// be implemented to allow for concurrent updates while a snapshot is happening.
func (st *Store) Snapshot() (raft.FSMSnapshot, error) {
	st.log.Info("persisting snapshot")
	return st, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (st *Store) Restore(rc io.ReadCloser) error {
	f := func() error {
		st.log.Info("restoring schema from snapshot")
		defer func() {
			if err := rc.Close(); err != nil {
				st.log.WithError(err).Error("restore snapshot: close reader")
			}
		}()

		if err := st.schemaManager.Restore(rc, st.cfg.Parser); err != nil {
			st.log.WithError(err).Error("restoring schema from snapshot")
			return fmt.Errorf("restore schema from snapshot: %w", err)
		}
		st.log.Info("successfully restored schema from snapshot")

		if err := st.authZManager.Restore(rc); err != nil {
			st.log.WithError(err).Error("restoring rbac from snapshot")
			return fmt.Errorf("restore rbac from snapshot: %w", err)
		}
		st.log.Info("successfully restored rbac from snapshot")

		if st.cfg.MetadataOnlyVoters {
			return nil
		}

		snapIndex := lastSnapshotIndex(st.snapshotStore)
		if st.lastAppliedIndexToDB.Load() <= snapIndex {
			// db shall reload after snapshot applied to schema
			st.reloadDBFromSchema()
		}

		st.log.WithFields(logrus.Fields{
			"last_applied_index":           st.lastIndex(),
			"last_store_log_applied_index": st.lastAppliedIndexToDB.Load(),
			"last_snapshot_index":          snapIndex,
			"n":                            st.schemaManager.NewSchemaReader().Len(),
		}).Info("successfully reloaded indexes from snapshot")

		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	var err error
	g := func() {
		err = f()
		wg.Done()
	}
	enterrors.GoWrapper(g, st.log)
	wg.Wait()
	return err
}
