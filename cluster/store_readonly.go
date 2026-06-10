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
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
)

// initReadOnly sets up the raft stores for a read-only follower without any
// write or network side effect: the bolt log store is opened read-only, the
// snapshot store is a read-only shim (raft.FileSnapshotStore's constructor
// write-tests the directory and cannot be used), and no TCP transport is created
// because a follower does no RAFT networking. It deliberately does NOT MkdirAll
// the work dir — the copy already contains it and the mount is read-only.
func (st *Store) initReadOnly() error {
	var err error

	st.logStore, err = raftbolt.New(raftbolt.Options{
		Path:        filepath.Join(st.cfg.WorkDir, raftDBName),
		BoltOptions: &bolt.Options{ReadOnly: true},
	})
	if err != nil {
		return fmt.Errorf("open read-only raft bolt at %s: %w", st.cfg.WorkDir, err)
	}

	st.logCache, err = raft.NewLogCache(logCacheCapacity, st.logStore)
	if err != nil {
		return fmt.Errorf("log cache: %w", err)
	}

	st.snapshotStore = newReadOnlySnapshotStore(st.cfg.WorkDir, st.log)

	st.log.WithFields(logrus.Fields{
		"action":   "raft_read_only_init",
		"work_dir": st.cfg.WorkDir,
		"node_id":  st.cfg.NodeID,
	}).Info("initialized read-only follower raft stores (no transport, no writes)")

	return nil
}

// hydrateReadOnly reconstructs the schema FSM the way raft.NewRaft would
// internally — restore the latest snapshot, then replay the committed log — but
// without ever becoming a RAFT node. It replays only up to lastAppliedIndexToDB
// (the index the writer materialized into its DB, which is exactly what the disk
// copy reflects); every replayed entry is therefore schemaOnly (an in-memory
// schema update), and the deferred reload inside Apply materializes the
// read-only shards once, at the final index.
//
// Committed-but-unapplied log entries beyond lastAppliedIndexToDB are
// intentionally skipped: the writer had not materialized them into its DB, so
// the copied shards do not reflect them and a follower must serve the schema its
// on-disk data actually matches.
//
// Crash-consistent snapshots are expected to be intact (committed bolt state),
// so a malformed entry is not anticipated; Apply panics on a proto-unmarshal
// failure, which is surfaced here as a fatal error rather than a partial schema.
func (st *Store) hydrateReadOnly(ctx context.Context) (err error) {
	snaps, err := st.snapshotStore.List()
	if err != nil {
		return fmt.Errorf("list snapshots: %w", err)
	}
	snapIndex := lastSnapshotIndex(st.snapshotStore)

	lastLog, err := st.logStore.LastIndex()
	if err != nil {
		return fmt.Errorf("last log index: %w", err)
	}

	// Every committed log in the copy was applied by the writer, so the follower
	// catches its in-memory schema up to the last committed log (or the snapshot
	// index if the log was fully compacted into the snapshot). Forcing
	// lastAppliedIndexToDB to this target makes every replayed entry schemaOnly
	// (an in-memory schema update) and lets the deferred reload in Apply
	// materialize the read-only shards exactly once, at the final index.
	target := max(lastLog, snapIndex)
	st.lastAppliedIndexToDB.Store(target)

	// 1. Restore the latest snapshot, if any. With st.raft == nil and
	//    !MetadataOnlyVoters, Restore hydrates the schema managers; when there
	//    are no post-snapshot logs (target <= snapIndex) it also reloads the
	//    read-only shards and marks the DB loaded.
	if len(snaps) > 0 {
		latest := snaps[0]
		_, rc, err := st.snapshotStore.Open(latest.ID)
		if err != nil {
			return fmt.Errorf("open snapshot %q: %w", latest.ID, err)
		}
		if err := st.Restore(rc); err != nil {
			return fmt.Errorf("restore snapshot %q: %w", latest.ID, err)
		}
	}

	// 2. Replay committed logs (snapIndex, lastLog]. Each entry is schemaOnly
	//    because its index <= lastAppliedIndexToDB(=target); the deferred reload
	//    in Apply fires at the final index to materialize the shards read-only.
	if lastLog > snapIndex {
		firstLog, err := st.logStore.FirstIndex()
		if err != nil {
			return fmt.Errorf("first log index: %w", err)
		}
		// logs before the snapshot may have been compacted away
		start := max(snapIndex+1, firstLog)
		for i := start; i <= lastLog; i++ {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("hydrate aborted while replaying log %d: %w", i, err)
			}
			if err := st.replayLog(i); err != nil {
				return err
			}
		}
	}

	// 3. Empty node (no snapshot, no logs) or a snapshot-only restore that did
	//    not reach the reload path: ensure the DB is loaded.
	if !st.dbLoaded.Load() {
		st.reloadDBFromSchema()
	}

	st.lastAppliedIndex.Store(target)
	st.dbLoaded.Store(true)

	st.log.WithFields(logrus.Fields{
		"action":              "raft_read_only_hydrated",
		"last_applied_index":  target,
		"last_snapshot_index": snapIndex,
		"classes":             st.schemaManager.NewSchemaReader().Len(),
	}).Info("read-only follower hydrated from copied raft state")

	return nil
}

// replayLog applies a single committed log entry to the in-memory schema,
// converting Apply's proto-unmarshal panic into a fatal error so a torn entry
// refuses startup rather than silently serving a partial schema.
func (st *Store) replayLog(index uint64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("apply log %d: %v", index, r)
		}
	}()

	var log raft.Log
	if getErr := st.logStore.GetLog(index, &log); getErr != nil {
		if errors.Is(getErr, raft.ErrLogNotFound) {
			// a gap is not expected within (snapIndex, target]; skip defensively
			return nil
		}
		return fmt.Errorf("get log %d: %w", index, getErr)
	}

	if resp, ok := st.Apply(&log).(Response); ok && resp.Error != nil {
		return fmt.Errorf("apply log %d: %w", index, resp.Error)
	}
	return nil
}
