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
	"strconv"
	"time"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const transitionAbortInterval = 30 * time.Second

func (s *Raft) NodeOverlay() *schema.NodeOverlay {
	return s.store.schemaManager.NodeOverlay()
}

func (s *Raft) RunningTransitions() []schema.TenantTransition {
	return s.store.schemaManager.RunningTransitions()
}

// TransitionsCaughtUp is false while a restarted node may hold a record that
// already ended.
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

// fsmReachedIndex reports whether the FSM has applied index. Config and noop
// entries never advance lastAppliedIndex, so an idle queue also counts.
func (st *Store) fsmReachedIndex(index uint64) bool {
	if st.lastAppliedIndex.Load() >= index {
		return true
	}
	rn := st.raft.Load()
	if rn == nil {
		return false
	}
	pending, err := strconv.ParseUint(rn.Stats()["fsm_pending"], 10, 64)
	return err == nil && pending == 0 && rn.AppliedIndex() >= index
}

// runTransitionAborts reports ABORT for participants that left the cluster and
// names the down members others wait on.
func (s *Raft) runTransitionAborts(ctx context.Context) {
	ticker := time.NewTicker(transitionAbortInterval)
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

	alive := func(node string) bool {
		return s.store.cfg.NodeSelector == nil || s.store.cfg.NodeSelector.NodeAddress(node) != ""
	}
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

// stalledParticipants returns the participants yet to report that left the
// cluster, and those that are members but down.
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
