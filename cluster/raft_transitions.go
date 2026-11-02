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
	"time"

	"github.com/hashicorp/raft"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const transitionAbortInterval = 30 * time.Second

func (s *Raft) RunningTransitions() []schema.TenantTransition {
	return s.store.schemaManager.RunningTransitions()
}

// TransitionsCaughtUp is false while a restarted node may hold a record that already ended.
func (s *Raft) TransitionsCaughtUp() bool {
	return s.store.transitionsCaughtUp()
}

func (st *Store) transitionsCaughtUp() bool {
	if !st.dbLoaded.Load() {
		return false
	}
	rn := st.raft.Load()
	if rn == nil {
		return false
	}
	if st.IsLeader() {
		return st.waitLeaderFSMCaughtUp() == nil
	}
	if rn.Leader() == "" {
		return false
	}
	return st.fsmReachedIndex(st.joinCommitIndex.Load())
}

// fsmReachedIndex checks the last command at or below index: config and noop entries never advance
// lastAppliedIndex, and raft's own applied index moves on enqueue, before the FSM runs the entry.
func (st *Store) fsmReachedIndex(index uint64) bool {
	applied := st.lastAppliedIndex.Load()
	if applied >= index {
		return true
	}
	if last, found := st.lastCommandAtOrBelow(index); found {
		return applied >= last
	}
	// A snapshot covers index, and raft counts it applied only once the restore returned.
	rn := st.raft.Load()
	return rn != nil && rn.AppliedIndex() >= index
}

func (st *Store) lastCommandAtOrBelow(index uint64) (last uint64, found bool) {
	if st.logCache == nil {
		return 0, false
	}
	first, err := st.logCache.FirstIndex()
	if err != nil || first == 0 {
		return 0, false
	}
	var entry raft.Log
	for i := index; i >= first; i-- {
		if err := st.logCache.GetLog(i, &entry); err != nil {
			return 0, false
		}
		if entry.Type == raft.LogCommand {
			return i, true
		}
	}
	return 0, false
}

func (s *Raft) runTransitionAborts(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if s.store.IsLeader() {
			s.abortRemovedParticipants(ctx)
		}
	}
}

func (s *Raft) abortRemovedParticipants(ctx context.Context) {
	rn := s.store.raft.Load()
	if rn == nil {
		return
	}
	cf := rn.GetConfiguration()
	if err := cf.Error(); err != nil {
		s.log.Warnf("transition aborts: read raft configuration: %v", err)
		return
	}
	members := make(map[string]struct{})
	for _, server := range cf.Configuration().Servers {
		members[string(server.ID)] = struct{}{}
	}

	alive := nodeAliveFunc(s.store.cfg.NodeSelector)
	waiting := make(map[string]int)
	for _, record := range s.RunningTransitions() {
		removed, down := stalledParticipants(record, members, alive)
		for _, node := range removed {
			req := abortReport(record, node)
			if _, err := s.UpdateTenantsProcess(ctx, record.Class, req); err != nil {
				s.log.WithField("tenant", record.Tenant).Warnf("abort for removed node %q: %v", node, err)
			}
		}
		for _, node := range down {
			waiting[node]++
		}
	}
	for node, count := range waiting {
		s.log.WithField("action", "tenant_transition").Warnf("%d tenant transitions wait on down node %q; "+
			"they end when it returns or is removed from the cluster", count, node)
	}
}

// abortTransitionsOf waits until this node applied the aborts, so the rebalance after it sees them.
func (s *Raft) abortTransitionsOf(ctx context.Context, node string) {
	var last uint64
	for _, record := range s.RunningTransitions() {
		if p, ok := record.Transition.Participants[node]; !ok || p.Progress != sharding.ProgressStarted {
			continue
		}
		v, err := s.UpdateTenantsProcess(ctx, record.Class, abortReport(record, node))
		if err != nil {
			s.log.WithField("tenant", record.Tenant).Warnf("abort for removed node %q: %v", node, err)
			continue
		}
		last = max(last, v)
	}
	if last == 0 {
		return
	}
	if err := s.WaitForUpdate(ctx, last); err != nil {
		s.log.Warnf("wait for the aborts of removed node %q: %v", node, err)
	}
}

// nodeAliveFunc counts a node alive while the membership knows its address.
func nodeAliveFunc(selector cluster.NodeSelector) func(string) bool {
	return func(node string) bool {
		return selector == nil || selector.NodeAddress(node) != ""
	}
}

func stalledParticipants(record schema.TenantTransition, members map[string]struct{}, alive func(string) bool) (removed, down []string) {
	for node, participant := range record.Transition.Participants {
		if participant.Progress != sharding.ProgressStarted {
			continue
		}
		if _, ok := members[node]; !ok {
			removed = append(removed, node)
		} else if !alive(node) {
			down = append(down, node)
		}
	}
	return removed, down
}

func abortReport(record schema.TenantTransition, node string) *cmd.TenantProcessRequest {
	action := cmd.TenantProcessRequest_ACTION_UNFREEZING
	if record.Transition.Kind == sharding.TransitionFreeze {
		action = cmd.TenantProcessRequest_ACTION_FREEZING
	}
	return &cmd.TenantProcessRequest{
		Node:   node,
		Action: action,
		TenantsProcesses: []*cmd.TenantsProcess{{
			Op:      cmd.TenantsProcess_OP_ABORT,
			Attempt: record.Transition.Attempt,
			Tenant:  &cmd.Tenant{Name: record.Tenant},
		}},
	}
}
