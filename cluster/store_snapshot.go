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

	"github.com/weaviate/weaviate/cluster/fsm"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

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

	dbUserSnapshot, err := s.dynUserManager.Snapshot()
	if err != nil {
		return fmt.Errorf("db user snapshot: %w", err)
	}

	tasksSnapshot, err := s.distributedTasksManager.Snapshot()
	if err != nil {
		return fmt.Errorf("tasks snapshot: %w", err)
	}

	replicationSnapshot, err := s.replicationManager.Snapshot()
	if err != nil {
		return fmt.Errorf("replication snapshot: %w", err)
	}

	snap := fsm.Snapshot{
		NodeID:           s.cfg.NodeID,
		SnapshotID:       sink.ID(),
		Schema:           schemaSnapshot,
		RBAC:             rbacSnapshot,
		DbUsers:          dbUserSnapshot,
		DistributedTasks: tasksSnapshot,
		ReplicationOps:   replicationSnapshot,
	}
	if err := json.NewEncoder(sink).Encode(&snap); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	return nil
}

// Release is invoked when we are finished with the snapshot.
// Satisfy the interface for raft.FSMSnapshot
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

		snap := fsm.Snapshot{}
		if err := json.NewDecoder(rc).Decode(&snap); err != nil {
			return fmt.Errorf("restore snapshot: decode json: %w", err)
		}

		if snap.Schema != nil {
			if err := st.schemaManager.Restore(snap.Schema, st.cfg.Parser); err != nil {
				st.log.WithError(err).Error("restoring schema from snapshot")
				return fmt.Errorf("restore schema from snapshot: %w", err)
			}
		} else {
			// old snapshot format
			jsonBytes, err := json.Marshal(snap)
			if err != nil {
				return fmt.Errorf("restore snapshot: marshal json: %w", err)
			}

			if err := st.schemaManager.RestoreLegacy(jsonBytes, st.cfg.Parser); err != nil {
				st.log.WithError(err).Error("restoring schema from snapshot")
				return fmt.Errorf("restore schema from snapshot: %w", err)
			}
		}

		st.log.Info("successfully restored schema from snapshot")

		if snap.RBAC != nil {
			if err := st.authZManager.Restore(snap.RBAC); err != nil {
				st.log.WithError(err).Error("restoring rbac from snapshot")
				return fmt.Errorf("restore rbac from snapshot: %w", err)
			}
		}

		if snap.DistributedTasks != nil {
			if err := st.distributedTasksManager.Restore(snap.DistributedTasks); err != nil {
				st.log.WithError(err).Error("restoring distributed tasks from snapshot")
				return fmt.Errorf("restore distributed tasks from snapshot: %w", err)
			}
		}

		if snap.ReplicationOps != nil {
			if err := st.replicationManager.Restore(snap.ReplicationOps); err != nil {
				st.log.WithError(err).Error("restoring replication ops from snapshot")
				return fmt.Errorf("restore replication ops from snapshot: %w", err)
			}
		}

		if snap.DbUsers != nil {
			if err := st.dynUserManager.Restore(snap.DbUsers); err != nil {
				st.log.WithError(err).Error("restoring db user from snapshot")
				return fmt.Errorf("restore db user from snapshot: %w", err)
			}
		}

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
