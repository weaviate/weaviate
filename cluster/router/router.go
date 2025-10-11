//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package router

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router/types"
	schemaTypes "github.com/weaviate/weaviate/cluster/schema/types"
	"github.com/weaviate/weaviate/usecases/cluster"
)

type Router struct {
	logger               *logrus.Entry
	metadataReader       schemaTypes.SchemaReader
	replicationFSMReader replicationTypes.ReplicationFSMReader
	clusterStateReader   cluster.NodeSelector
}

func New(logger *logrus.Logger, clusterStateReader cluster.NodeSelector, metadataReader schemaTypes.SchemaReader, replicationFSMReader replicationTypes.ReplicationFSMReader) *Router {
	return &Router{
		logger:               logger.WithField("action", "router"),
		replicationFSMReader: replicationFSMReader,
		metadataReader:       metadataReader,
		clusterStateReader:   clusterStateReader,
	}
}

func (r *Router) GetReadWriteReplicasLocation(collection string, shard string) ([]string, []string, []string, error) {
	replicas, err := r.metadataReader.ShardReplicas(collection, shard)
	if err != nil {
		return nil, nil, nil, err
	}
	readReplicas := r.replicationFSMReader.FilterOneShardReplicasRead(collection, shard, replicas)
	writeReplicas, additionalWriteReplicas := r.replicationFSMReader.FilterOneShardReplicasWrite(collection, shard, replicas)
	return readReplicas, writeReplicas, additionalWriteReplicas, nil
}

func (r *Router) GetWriteReplicasLocation(collection string, shard string) ([]string, []string, error) {
	_, writeReplicasLocation, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation(collection, shard)
	if err != nil {
		return nil, nil, err
	}
	return writeReplicasLocation, additionalWriteReplicas, nil
}

func (r *Router) GetReadReplicasLocation(collection string, shard string) ([]string, error) {
	readReplicasLocation, _, _, err := r.GetReadWriteReplicasLocation(collection, shard)
	if err != nil {
		return nil, err
	}
	return readReplicasLocation, nil
}

func (r *Router) BuildReadRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	if err := params.Validate(); err != nil {
		return types.RoutingPlan{}, err
	}

	replicas, err := r.GetReadReplicasLocation(params.Collection, params.Shard)
	if err != nil {
		return types.RoutingPlan{}, fmt.Errorf("could not get read replicas location from sharding state: %w", err)
	}

	return r.routingPlanFromReplicas(params, replicas)
}

func (r *Router) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	if err := params.Validate(); err != nil {
		return types.RoutingPlan{}, err
	}

	writeReplicas, additionalWriteReplicas, err := r.GetWriteReplicasLocation(params.Collection, params.Shard)
	if err != nil {
		return types.RoutingPlan{}, fmt.Errorf("could not get read replicas location from sharding state: %w", err)
	}

	routingPlan, err := r.routingPlanFromReplicas(params, writeReplicas)
	if err != nil {
		return types.RoutingPlan{}, err
	}

	for _, replica := range additionalWriteReplicas {
		if replicaAddr, ok := r.clusterStateReader.NodeHostname(replica); ok {
			routingPlan.AdditionalHostAddrs = append(routingPlan.AdditionalHostAddrs, replicaAddr)
		}
	}

	return routingPlan, nil
}

func (r *Router) routingPlanFromReplicas(
	params types.RoutingPlanBuildOptions,
	replicas []string,
) (types.RoutingPlan, error) {
	routingPlan := types.RoutingPlan{
		Collection:        params.Collection,
		Shard:             params.Shard,
		Replicas:          make([]string, 0, len(replicas)),
		ConsistencyLevel:  params.ConsistencyLevel,
		ReplicasHostAddrs: make([]string, 0, len(replicas)),
	}

	// If there was no local replica first specified, put the local node as direct candidate. If the local node is part of the replica set
	// it will be handled as the direct candidate
	if params.DirectCandidateReplica == "" {
		params.DirectCandidateReplica = r.clusterStateReader.LocalName()
	}

	for _, replica := range replicas {
		if replicaAddr, ok := r.clusterStateReader.NodeHostname(replica); ok {
			// During rollout, skip nodes that are shutting down to prevent cascade failures
			// Check if this node is currently shutting down by looking at memberlist state
			if r.isNodeShuttingDown(replica) {
				r.logger.WithField("collection", params.Collection).
					WithField("shard", params.Shard).
					WithField("replica", replica).
					WithField("replica_addr", replicaAddr).
					Debug("skipping replica during rollout - node is shutting down")
				continue
			}

			// Local replica first is necessary due to the logic in finder where the first node is considered a "full read
			// candidate". This means that instead of a doing a digest read we will get the "full read" (whatever that means).
			// We handle the direct candidate here to ensure that the direct candidate is also part of the replica set
			if replica == params.DirectCandidateReplica {
				routingPlan.Replicas = slices.Insert(routingPlan.Replicas, 0, replica)
				routingPlan.ReplicasHostAddrs = slices.Insert(routingPlan.ReplicasHostAddrs, 0, replicaAddr)
			} else {
				routingPlan.Replicas = append(routingPlan.Replicas, replica)
				routingPlan.ReplicasHostAddrs = append(routingPlan.ReplicasHostAddrs, replicaAddr)
			}
		}
	}
	if len(routingPlan.Replicas) == 0 {
		return routingPlan, fmt.Errorf("no replicas found for class %s shard %s", routingPlan.Collection, routingPlan.Shard)
	}
	cl, err := routingPlan.ValidateConsistencyLevel()
	if err != nil {
		return types.RoutingPlan{}, err
	}
	routingPlan.IntConsistencyLevel = cl
	return routingPlan, err
}

func (r *Router) NodeHostname(nodeName string) (string, bool) {
	return r.clusterStateReader.NodeHostname(nodeName)
}

func (r *Router) AllHostnames() []string {
	return r.clusterStateReader.AllHostnames()
}

// isNodeShuttingDown checks if a node is currently shutting down during rollout
// This helps prevent routing to nodes that are in the process of graceful shutdown
func (r *Router) isNodeShuttingDown(nodeName string) bool {
	// Check if the node is still reachable via memberlist
	// If NodeHostname returns false, the node is no longer available
	if _, ok := r.clusterStateReader.NodeHostname(nodeName); !ok {
		return true
	}

	return false
}
