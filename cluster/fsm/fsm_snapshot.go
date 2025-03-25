package fsm

import (
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
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
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.cfg.Logger.Info("persisting snapshot")
	return f.schemaManager.Snapshot(), nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	restoreFunc := func() error {
		f.cfg.Logger.Info("restoring schema from snapshot")
		defer func() {
			if err := rc.Close(); err != nil {
				f.cfg.Logger.WithError(err).Error("restore snapshot: close reader")
			}
		}()

		if err := f.schemaManager.Restore(rc); err != nil {
			f.cfg.Logger.WithError(err).Error("restoring schema from snapshot")
			return fmt.Errorf("restore schema from snapshot: %w", err)
		}
		f.cfg.Logger.Info("successfully restored schema from snapshot")

		if f.cfg.MetadataOnly {
			return nil
		}

		snapIndex := f.cfg.LastSnapshotIndex()
		if f.lastAppliedIndexToDB.Load() <= snapIndex {
			// db shall reload after snapshot applied to schema
			f.reloadDBFromSchema()
		}

		f.cfg.Logger.WithFields(logrus.Fields{
			"last_store_log_applied_index": f.lastAppliedIndexToDB.Load(),
			"last_snapshot_index":          snapIndex,
			"n":                            f.schemaManager.NewSchemaReader().Len(),
		}).Info("successfully reloaded indexes from snapshot")

		return nil
	}

	// Calling it via enterrors allows us to catch any panic happening
	wg := sync.WaitGroup{}
	wg.Add(1)
	var err error
	g := func() {
		err = restoreFunc()
		wg.Done()
	}
	enterrors.GoWrapper(g, f.cfg.Logger)
	wg.Wait()
	return err
}
