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
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

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
	return st.schemaManager.Snapshot(), nil
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

		if st.reloadDBFromSnapshot() {
			st.log.WithField("n", st.schemaManager.NewSchemaReader().Len()).
				Info("successfully reloaded indexes from snapshot")
		}

		if st.raft != nil {
			st.lastAppliedIndex.Store(st.raft.AppliedIndex())
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
