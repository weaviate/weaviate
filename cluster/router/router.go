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

func (r *Router) GetReadWriteReplicasLocation(collection string, shard string) ([]string, []string, error) {
	replicas, err := r.metadataReader.ShardReplicas(collection, shard)
	if err != nil {
		return nil, nil, err
	}
	readReplicas, writeReplicas := r.replicationFSMReader.FilterOneShardReplicasReadWrite(collection, shard, replicas)
	return readReplicas, writeReplicas, nil
}

func (r *Router) GetWriteReplicasLocation(collection string, shard string) ([]string, error) {
	_, writeReplicasLocation, err := r.GetReadWriteReplicasLocation(collection, shard)
	if err != nil {
		return nil, err
	}
	return writeReplicasLocation, nil
}

func (r *Router) GetReadReplicasLocation(collection string, shard string) ([]string, error) {
	readReplicasLocation, _, err := r.GetReadWriteReplicasLocation(collection, shard)
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

	routingPlan := types.RoutingPlan{
		Collection:        params.Collection,
		Shard:             params.Shard,
		Replicas:          make([]string, 0, len(replicas)),
		ConsistencyLevel:  params.ConsistencyLevel,
		ReplicasHostAddrs: make([]string, 0, len(replicas)),
	}

	// TODO: Local replica first is necessary due to the logic in finder where the first node is considered a "full read
	// candidate". This means that instead of a doing a digest read we will get the "full read" (whatever that means).
	// If there was no local replica first specified, put the local node first always
	if params.DirectCandidateReplica == "" {
		params.DirectCandidateReplica = r.clusterStateReader.LocalName()
	}
	if directCandidateAddr, ok := r.clusterStateReader.NodeHostname(params.DirectCandidateReplica); ok {
		routingPlan.Replicas = append(routingPlan.Replicas, params.DirectCandidateReplica)
		routingPlan.ReplicasHostAddrs = append(routingPlan.ReplicasHostAddrs, directCandidateAddr)
	}

	for _, replica := range replicas {
		// Skip the direct candidate as it's handled before hand and we don't want to duplicate information
		if replica == params.DirectCandidateReplica {
			continue
		}

		if replicaAddr, ok := r.clusterStateReader.NodeHostname(replica); ok {
			routingPlan.Replicas = append(routingPlan.Replicas, replica)
			routingPlan.ReplicasHostAddrs = append(routingPlan.ReplicasHostAddrs, replicaAddr)
		}
	}
	if len(routingPlan.Replicas) == 0 {
		return routingPlan, fmt.Errorf("no replicas found for class %s shard %s", routingPlan.Collection, routingPlan.Shard)
	}

	routingPlan.IntConsistencyLevel, err = routingPlan.ValidateConsistencyLevel()
	return routingPlan, err
}

func (r *Router) BuildWriteRoutingPlan(params types.RoutingPlanBuildOptions) (types.RoutingPlan, error) {
	// TODO: See if there is any sense in having writes be propagated to the "new" shard currently.
	// For now discarding that idea because we need doc id synced to avoid colisions
	return r.BuildReadRoutingPlan(params)
}

func (r *Router) NodeHostname(nodeName string) (string, bool) {
	return r.clusterStateReader.NodeHostname(nodeName)
}

func (r *Router) AllHostnames() []string {
	return r.clusterStateReader.AllHostnames()
}
