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
	"sync"

	"github.com/weaviate/weaviate/cluster/proto/api"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
)

// lockReplicaOpPropose takes the op's propose lock for a replica add or an op
// state update and returns the op id with its release. Other commands get a
// no-op release.
//
// The add's FINALIZING check reads the leader's applied state, and a rewind
// is an UPDATE_STATE of the same op. Holding one lock across check and append
// for the add, and across the append for the update, means a rewind is either
// applied before the add is judged or appended after it, never in between.
// Execute waits for the leader's apply, so the next holder sees its effect.
func (st *Store) lockReplicaOpPropose(req *api.ApplyRequest) (uint64, func(), error) {
	var opID uint64
	switch req.Type {
	case api.ApplyRequest_TYPE_REPLICATION_REPLICATE_ADD_REPLICA_TO_SHARD:
		sub := api.ReplicationAddReplicaToShard{}
		if err := json.Unmarshal(req.SubCommand, &sub); err != nil {
			return 0, nil, fmt.Errorf("unmarshal add-replica-to-shard subcommand: %w", err)
		}
		opID = sub.OpId
	case api.ApplyRequest_TYPE_REPLICATION_REPLICATE_UPDATE_STATE:
		sub := api.ReplicationUpdateOpStateRequest{}
		if err := json.Unmarshal(req.SubCommand, &sub); err != nil {
			return 0, nil, fmt.Errorf("unmarshal update-op-state subcommand: %w", err)
		}
		opID = sub.Id
	default:
		return 0, func() {}, nil
	}
	st.replicaOpLocks.lock(opID)
	return opID, func() { st.replicaOpLocks.unlock(opID) }, nil
}

// admitReplicaAdd refuses a replica add unless its op is FINALIZING and not
// being cancelled. It is a propose-side fence for the same reason as
// admitPropose: an apply-side check would split old and new binaries on the
// same entry. A caller that timed out can have its add proposed late, after
// the op was rewound to HYDRATING; admitting it would load the target mid-copy.
// Must run after waitLeaderFSMCaughtUp and under the op's propose lock.
func (st *Store) admitReplicaAdd(opID uint64) error {
	op, ok := st.replicationManager.GetReplicationFSM().GetOpById(opID)
	if !ok {
		return fmt.Errorf("op %d: %w: %w", opID, replicationTypes.ErrAddReplicaOpNotFinalizing, replicationTypes.ErrReplicationOperationNotFound)
	}
	state := op.Status.GetCurrentState()
	if op.Status.ShouldCancel || state == api.CANCELLED {
		return fmt.Errorf("op %d: %w", opID, replicationTypes.ErrOpCancellationInFlight)
	}
	if state != api.FINALIZING {
		return fmt.Errorf("op %d is %s: %w", opID, state, replicationTypes.ErrAddReplicaOpNotFinalizing)
	}
	return nil
}

// opKeyLocks is a keyed mutex that drops a key once nobody holds or waits on
// it, so one entry per op id never accumulates.
type opKeyLocks struct {
	mu    sync.Mutex
	locks map[uint64]*opKeyLock
}

type opKeyLock struct {
	mu   sync.Mutex
	refs int
}

func newOpKeyLocks() *opKeyLocks {
	return &opKeyLocks{locks: make(map[uint64]*opKeyLock)}
}

func (l *opKeyLocks) lock(id uint64) {
	l.mu.Lock()
	k, ok := l.locks[id]
	if !ok {
		k = &opKeyLock{}
		l.locks[id] = k
	}
	k.refs++
	l.mu.Unlock()
	k.mu.Lock()
}

func (l *opKeyLocks) unlock(id uint64) {
	l.mu.Lock()
	k := l.locks[id]
	k.refs--
	if k.refs == 0 {
		delete(l.locks, id)
	}
	l.mu.Unlock()
	k.mu.Unlock()
}

func (l *opKeyLocks) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.locks)
}
