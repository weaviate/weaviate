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
	"github.com/weaviate/weaviate/cluster/schema"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type storeSnapshotVersion int

const (
	storeSnapshotVersionV0 storeSnapshotVersion = iota
	storeSnapshotVersionV1 storeSnapshotVersion = iota
)

type storeSnapshot struct {
	SnapshotVersion storeSnapshotVersion      `json:"snapshot_version,omitempty"`
	SchemaSnapshot  *schema.VersionedSnapshot `json:"schema_snapshot,omitempty"`
	// DynUsersSnapshot *schema.Snapshot
}

func (s *storeSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	if err := json.NewEncoder(sink).Encode(s); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return nil
}

func (s *storeSnapshot) Release() {
	// No-op, nothing to release
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
	snapshot := &storeSnapshot{
		SnapshotVersion: storeSnapshotVersionV1,
		SchemaSnapshot:  st.schemaManager.Snapshot(),
	}
	return snapshot, nil
}

func (st *Store) legacySnapshotRestore(rc io.ReadCloser) error {
	if err := st.schemaManager.V0Restore(rc, st.cfg.Parser); err != nil {
		st.log.WithError(err).Error("restoring schema from snapshot")
		return fmt.Errorf("restore schema from snapshot: %w", err)
	}
	st.log.Info("successfully restored schema from snapshot")

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

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (st *Store) Restore(rc io.ReadCloser) error {
	f := func() error {
		st.log.Info("restoring raft FSM from snapshot")
		defer func() {
			if err := rc.Close(); err != nil {
				st.log.WithError(err).Error("restore snapshot: close reader")
			}
		}()

		snapshot := &storeSnapshot{}
		if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
			return fmt.Errorf("restore snapshot: decode json: %w", err)
		}

		switch snapshot.SnapshotVersion {
		case storeSnapshotVersionV0:
			return st.legacySnapshotRestore(rc)
		case storeSnapshotVersionV1:
			break
		default:
			// !! CRITICAL FOR ROLLBACK !!
			// This is the case where the snapshot is newer than this code knows about.
			// This happens during a rollback where an older node receives a snapshot
			// created by a newer node. Reject it!
			// Raft will handle this by looking for an older snapshot *or* replaying older log entries
			st.log.Info("unknown raft snapshot version, not restoring from snapshot. This is expected if weaviate rolled back or is running differing versions.")
			return fmt.Errorf("unknown snapshot version %d, current code supports up to %d", snapshot.SnapshotVersion, storeSnapshotVersionV1)
		}

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
