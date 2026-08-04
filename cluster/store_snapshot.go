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

// fsmSnapshot is the raft.FSMSnapshot the Store hands to raft: every sub-FSM's
// state, already serialized, captured inside Store.Snapshot.
type fsmSnapshot struct {
	snap fsm.Snapshot
}

// Persist encodes the frozen state captured at Snapshot time to 'sink'. It
// runs concurrently with later applies and therefore must not read the live
// managers: doing so would produce a snapshot NEWER than its raft index and
// torn across sub-FSMs (each manager read at a different instant). Replaying
// log entries over a restored torn snapshot flips the apply verdicts that
// read DTM state (the drop-vector marker purge-refusal and removal gate) on
// the restoring node only — silent per-node schema divergence.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) (err error) {
	defer sink.Close()
	s.snap.SnapshotID = sink.ID()
	if err := json.NewEncoder(sink).Encode(&s.snap); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	return nil
}

// Release is invoked when we are finished with the snapshot.
// Satisfy the interface for raft.FSMSnapshot
func (s *fsmSnapshot) Release() {
}

// Snapshot returns an FSMSnapshot used to: support log compaction, to
// restore the FSM to a previous state, or to bring out-of-date followers up
// to a recent log index.
//
// Apply and Snapshot run on the same thread, so capturing every sub-FSM here
// yields a consistent cut at the snapshot's raft index (see fsmSnapshot.Persist
// for why deferring the capture to Persist is unsafe). Each sub-snapshot
// serializes in-memory state under its manager's lock — quick enough for the
// apply thread; the actual sink IO stays in Persist.
func (st *Store) Snapshot() (raft.FSMSnapshot, error) {
	st.log.Info("capturing snapshot")

	schemaSnapshot, err := st.schemaManager.SchemaSnapshot()
	if err != nil {
		return nil, fmt.Errorf("schema snapshot: %w", err)
	}

	aliasSnapshot, err := st.schemaManager.AliasSnapshot()
	if err != nil {
		return nil, fmt.Errorf("alias snapshot: %w", err)
	}

	rbacSnapshot, err := st.authZManager.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("rbac snapshot: %w", err)
	}

	dbUserSnapshot, err := st.dynUserManager.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("db user snapshot: %w", err)
	}

	namespacesSnapshot, err := st.namespaceManager.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("namespaces snapshot: %w", err)
	}

	tasksSnapshot, err := st.distributedTasksManager.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("tasks snapshot: %w", err)
	}

	replicationSnapshot, err := st.replicationManager.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("replication snapshot: %w", err)
	}

	return &fsmSnapshot{snap: fsm.Snapshot{
		NodeID:           st.cfg.NodeID,
		Schema:           schemaSnapshot,
		Aliases:          aliasSnapshot,
		RBAC:             rbacSnapshot,
		DbUsers:          dbUserSnapshot,
		Namespaces:       namespacesSnapshot,
		DistributedTasks: tasksSnapshot,
		ReplicationOps:   replicationSnapshot,
		ClusterID:        st.ClusterID(),
	}}, nil
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

		if snap.Aliases != nil {
			if err := st.schemaManager.RestoreAliases(snap.Aliases); err != nil {
				return fmt.Errorf("restore aliases from snapshot: %w", err)
			}
		}

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

		if snap.Namespaces != nil {
			if err := st.namespaceManager.Restore(snap.Namespaces); err != nil {
				st.log.Error(err)
				return fmt.Errorf("restore namespaces from snapshot: %w", err)
			}
		}

		if snap.ClusterID != "" {
			st.setClusterID(snap.ClusterID)
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
		defer wg.Done()
		err = f()
	}
	enterrors.GoWrapper(g, st.log)
	wg.Wait()
	return err
}
