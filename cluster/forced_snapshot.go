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
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const (
	// forcedSnapshotPoll is how often the worker re-checks readiness.
	forcedSnapshotPoll = time.Second
	// forcedSnapshotRetry paces attempts after a failed snapshot.
	forcedSnapshotRetry = time.Minute
)

// forcedSnapshotter takes one raft snapshot on demand and exits.
//
// It seals a node whose RBAC state raft cannot rebuild exactly from its latest
// snapshot and log: the snapshot has no RBAC section (armed by Store.Restore), or the
// snapshot or log holds RBAC data from before the backup-principals migration
// (armed by cluster/rbac.Manager). Later boots restore RBAC from the new snapshot.
type forcedSnapshotter struct {
	// signal must have capacity one.
	signal <-chan struct{}
	poll   time.Duration
	retry  time.Duration
	// ready reports that the log from before this boot is applied and a leader is known.
	ready    func() bool
	snapshot func() error
	isLeader func() bool
	// barrier appends a log entry and waits until the FSM applies it.
	barrier func() error
	log     logrus.FieldLogger
}

// run waits for the signal, snapshots once the node is ready, and returns.
// It also returns when ctx is cancelled or raft shuts down.
func (f *forcedSnapshotter) run(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-f.signal:
	}
	t := time.NewTicker(f.poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if !f.ready() {
			continue
		}
		err := f.snapshot()
		if errors.Is(err, raft.ErrNothingNewToSnapshot) {
			err = f.snapshotAfterBarrier()
		}
		switch {
		case err == nil:
			f.log.WithField("action", "forced_raft_snapshot").
				Info("forced raft snapshot taken")
			return
		case errors.Is(err, raft.ErrRaftShutdown):
			return
		case errors.Is(err, raft.ErrNothingNewToSnapshot):
			f.log.WithField("action", "forced_raft_snapshot").
				Info("forced raft snapshot waits for a new log entry, retrying")
		default:
			f.log.WithField("action", "forced_raft_snapshot").
				Errorf("forced raft snapshot failed, retrying: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(f.retry):
		}
	}
}

// snapshotAfterBarrier handles raft's nothing-new refusal: the FSM has applied
// no entry since this raft instance started. On the leader a barrier
// applies a new entry, so the snapshot succeeds. A follower returns the
// refusal; its FSM advances once the leader's barrier or any write replicates.
func (f *forcedSnapshotter) snapshotAfterBarrier() error {
	if !f.isLeader() {
		return raft.ErrNothingNewToSnapshot
	}
	if err := f.barrier(); err != nil {
		return fmt.Errorf("barrier: %w", err)
	}
	return f.snapshot()
}

// startForcedSnapshotter starts a forced snapshotter worker. Every call to raft
// loads the current instance, since single-node recovery replaces it.
func (st *Store) startForcedSnapshotter() {
	f := &forcedSnapshotter{
		signal:   st.forcedSnapshotSignal,
		poll:     forcedSnapshotPoll,
		retry:    forcedSnapshotRetry,
		ready:    st.Ready,
		snapshot: func() error { return st.raft.Load().Snapshot().Error() },
		isLeader: st.IsLeader,
		barrier:  func() error { return st.raft.Load().Barrier(st.applyTimeout).Error() },
		log:      st.log,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	st.stopForcedSnapshots, st.forcedSnapshotsDone = cancel, done
	enterrors.GoWrapper(func() {
		defer close(done)
		f.run(ctx)
	}, st.log)
}
