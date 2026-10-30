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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
)

func (s *Raft) CreateRebalancePlan(ctx context.Context, req *replication.CreatePlanRequest) error {
	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REBALANCE_CREATE_PLAN,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return rebalancePlanError("create rebalance plan", err, replication.ErrPlanExists)
	}
	return nil
}

func (s *Raft) CompleteRebalancePlanning(ctx context.Context, req *replication.CompletePlanningRequest) error {
	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REBALANCE_COMPLETE_PLANNING,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return rebalancePlanError("complete rebalance planning", err,
			replication.ErrPlanNotFound, replication.ErrPlanStaleRun, replication.ErrPlanTransition)
	}
	return nil
}

func (s *Raft) StopRebalancePlan(ctx context.Context, req *replication.StopPlanRequest) error {
	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REBALANCE_STOP,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return rebalancePlanError("stop rebalance plan", err, replication.ErrPlanNotFound, replication.ErrPlanTransition)
	}
	return nil
}

func (s *Raft) DeleteRebalancePlan(ctx context.Context, req *replication.DeletePlanRequest) error {
	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REBALANCE_DELETE,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(ctx, command); err != nil {
		return rebalancePlanError("delete rebalance plan", err, replication.ErrPlanNotFound, replication.ErrPlanTransition)
	}
	return nil
}

// GetRebalancePlan reads this node's local replication state, which can lag
// behind the leader.
func (s *Raft) GetRebalancePlan() (replication.Plan, bool) {
	return s.ReplicationFsm().GetPlan()
}

// rebalancePlanError finds the first sentinel whose message appears in err's
// text and returns that sentinel, wrapped. If none matches, it returns err
// unchanged. Matching is by text because typed errors are lost when a
// follower forwards the request to the leader.
func rebalancePlanError(action string, err error, sentinels ...error) error {
	for _, sentinel := range sentinels {
		if strings.Contains(err.Error(), sentinel.Error()) {
			return fmt.Errorf("execute %s: %w", action, sentinel)
		}
	}
	return err
}
