//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package router

import (
	"fmt"

	"github.com/weaviate/weaviate/cluster/router/types"
)

// ReadPlanner builds a routing plan for one read request. The plan contains
// the appropriate number of replicas per shard based on the consistency level.
type ReadPlanner interface {
	Plan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error)
}

// readPlanner is the concrete, unexported implementation. It uses a Router
// to build routing plans and selects the appropriate number of replicas
// based on the consistency level requirements.
type readPlanner struct {
	router     types.Router
	collection string
}

var _ ReadPlanner = (*readPlanner)(nil)

// NewReadPlanner returns a ready-to-use read planner suitable for read operations.
// The planner uses the router's BuildReadRoutingPlan method and then selects
// the appropriate number of replicas based on consistency level requirements.
func NewReadPlanner(router types.Router, collection string) ReadPlanner {
	return &readPlanner{
		router:     router,
		collection: collection,
	}
}

// Plan gets all available replicas from the router and selects the appropriate
// number of replicas per shard based on the consistency level. It preserves
// the router's preferred node ordering while ensuring enough replicas are
// available to satisfy the consistency requirements.
func (p *readPlanner) Plan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	plan, err := p.router.BuildReadRoutingPlan(params)
	if err != nil {
		return types.ReadRoutingPlan{}, fmt.Errorf("failed to build read routing plan for collection %q: %w", p.collection, err)
	}

	shardGroups := p.groupReplicasByShard(plan.ReplicaSet.Replicas)
	selectedReplicas, err := p.selectReplicasForConsistency(shardGroups, plan.IntConsistencyLevel)
	if err != nil {
		return types.ReadRoutingPlan{}, fmt.Errorf("failed to select replicas for consistency level %d: %w", plan.IntConsistencyLevel, err)
	}

	return types.ReadRoutingPlan{
		Shard:               plan.Shard,
		Tenant:              plan.Tenant,
		ReplicaSet:          types.ReadReplicaSet{Replicas: selectedReplicas},
		ConsistencyLevel:    plan.ConsistencyLevel,
		IntConsistencyLevel: plan.IntConsistencyLevel,
	}, nil
}

// groupReplicasByShard organizes replicas by their shard name while preserving
// the original ordering (which includes preferred node logic from the router).
func (p *readPlanner) groupReplicasByShard(replicas []types.Replica) map[string][]types.Replica {
	shardGroups := make(map[string][]types.Replica)

	for _, replica := range replicas {
		shardGroups[replica.ShardName] = append(shardGroups[replica.ShardName], replica)
	}

	return shardGroups
}

// selectReplicasForConsistency selects the appropriate number of replicas
// per shard based on the consistency level requirements.
//
// This method assumes the routing plan has already been validated by the router's
// ValidateConsistencyLevel() method, ensuring that:
// - consistencyLevel is a valid positive integer
// - each shard has at least consistencyLevel replicas available
func (p *readPlanner) selectReplicasForConsistency(shardGroups map[string][]types.Replica, consistencyLevel int) ([]types.Replica, error) {
	var selectedReplicas []types.Replica

	for _, shardReplicas := range shardGroups {
		if len(shardReplicas) == 0 {
			continue
		}

		selected := shardReplicas[:consistencyLevel]
		selectedReplicas = append(selectedReplicas, selected...)
	}

	return selectedReplicas, nil
}
