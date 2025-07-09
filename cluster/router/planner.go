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
// selected by a specific implementation of ReadReplicaStrategy
// (random by default, or “prefer local node” when WithDirectCandidate is used).
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

// NewReadPlanner returns a ready-to-use read planner suitable for read operations. By default
// it spreads reads randomly across replicas to favor read load distribution across the cluster;
// passing option WithDirectCandidate will result in a planner that favor reading shards from the
// local node rather than making remote read operations.
//
//	// default random picker (does not use option WithDirectCandidate)
//	p := types.NewReadPlanner(router)
//
//	// prefer reads from the local node when available
//	p := types.NewReadPlanner(router, types.WithDirectCandidate(myNode))
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

// Plan asks the router for candidate replicas, applies the picker to select a suitable subset of
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
