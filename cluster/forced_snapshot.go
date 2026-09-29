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
type forcedSnapshotter struct {
	// signal should have capacity one
	signal <-chan struct{}
	poll   time.Duration
	retry  time.Duration
	// ready reports that the pre-boot log is applied and a leader is known
	ready    func() bool
	snapshot func() error
	log      logrus.FieldLogger
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
		switch {
		case err == nil, errors.Is(err, raft.ErrNothingNewToSnapshot):
			f.log.WithField("action", "forced_raft_snapshot").
				Info("forced raft snapshot taken")
			return
		case errors.Is(err, raft.ErrRaftShutdown):
			return
		default:
			f.log.WithField("action", "forced_raft_snapshot").
				Errorf("forced raft snapshot failed, retrying: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(f.retry):
			}
		}
	}
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
