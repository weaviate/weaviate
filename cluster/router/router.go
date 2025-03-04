package router

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
)

type Router struct {
	logger             *logrus.Entry
	metadataReader     schema.SchemaReader
	replicationFSM     replication.ShardReplicationFSM
	clusterStateReader cluster.State
}

func New(logger *logrus.Logger, metadataReader schema.SchemaReader, replicationFSM replication.ShardReplicationFSM) *Router {
	return &Router{
		logger:         logger.WithField("action", "router"),
		replicationFSM: replicationFSM,
		metadataReader: metadataReader,
	}
}

func (r *Router) GetReadWriteReplicasLocation(collection string, shard string) ([]string, []string, int, error) {
	replicas, err := r.metadataReader.ShardReplicas(collection, shard)
	if err != nil {
		return nil, nil, 0, err
	}
	readReplicas, writeReplicas, writePrios := r.replicationFSM.FilterOneShardReplicasReadWrite(collection, shard, replicas)
	return readReplicas, writeReplicas, writePrios, nil
}

func (r *Router) GetWriteReplicasLocation(collection string, shard string) ([]string, int, error) {
	_, writeReplicasLocation, writePrios, err := r.GetReadWriteReplicasLocation(collection, shard)
	if err != nil {
		return nil, 0, err
	}
	return writeReplicasLocation, writePrios, nil
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

	routingPlan.IntConsistencyLevel, err = routingPlan.ValidateConsistencyLevel()
	return routingPlan, err
}

func (r *Router) BuildWriteRoutingPlan(collection string, shard string) (types.RoutingPlan, error) {

	return types.RoutingPlan{}, nil
}
