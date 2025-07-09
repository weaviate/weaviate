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

// ReadPlanner builds a routing plan for one read request.  The plan contains
// exactly one replica per shard, chosen according to the configured strategy
// selected by a specific implementation of ReadReplicaStrategy.
type ReadPlanner interface {
	Plan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error)
}

// readPlanner is the concrete, unexported implementation.  It uses a Router
// to discover all available replicas and a ReadReplicaStrategy to choose one
// replica per shard to select the replica to hit for a read operation among
// all available replicas.
type readPlanner struct {
	router     types.Router
	strategy   types.ReadReplicaStrategy
	collection string
}

var _ ReadPlanner = (*readPlanner)(nil)

// NewReadPlanner returns a ready-to-use read planner suitable for read operations.
// If no ReadReplicaStrategy is provided a default one will be used which will select
// replicas based on direct candidate preference or fallback to local node if no
// direct candidate is provided.
func NewReadPlanner(router types.Router, collection string, strategy types.ReadReplicaStrategy, directCandidate, localNodeName string) ReadPlanner {
	if strategy == nil {
		strategy = types.NewDirectCandidateReadStrategy(types.NewDirectCandidate(directCandidate, localNodeName))
	}
	return &readPlanner{
		router:     router,
		strategy:   strategy,
		collection: collection,
	}
}

// Plan asks the router for candidate replicas, applies the read strategy to select a suitable subset of
// replicas, and returns a fully-formed ReadRoutingPlan with one replica per shard.
func (p *readPlanner) Plan(params types.RoutingPlanBuildOptions) (types.ReadRoutingPlan, error) {
	readReplicas, err := p.router.GetReadReplicasLocation(p.collection, params.Shard)
	if err != nil {
		return types.ReadRoutingPlan{}, fmt.Errorf("failed to get read replicas for collection %q shard %q: %w", p.collection, params.Shard, err)
	}

	if readReplicas.EmptyReplicas() {
		return types.ReadRoutingPlan{}, fmt.Errorf("no replicas available for collection %q shard %q", p.collection, params.Shard)
	}

	chosen := p.strategy.Apply(readReplicas, params)
	plan := types.ReadRoutingPlan{
		Shard:            params.Shard,
		ReplicaSet:       chosen,
		ConsistencyLevel: params.ConsistencyLevel,
	}

	cl, err := plan.ValidateConsistencyLevel()
	if err != nil {
		return types.ReadRoutingPlan{}, fmt.Errorf("consistency validation failed: %w", err)
	}
	plan.IntConsistencyLevel = cl

	return plan, nil
}
