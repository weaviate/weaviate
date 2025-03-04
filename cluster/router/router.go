package router

import (
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
)

type Router struct {
	logger         *logrus.Entry
	metadataReader schema.SchemaReader
	replicationFSM replication.ShardReplicationFSM
}

func New(logger *logrus.Logger, metadataReader schema.SchemaReader, replicationFSM replication.ShardReplicationFSM) *Router {
	return &Router{
		logger:         logger.WithField("action", "router"),
		replicationFSM: replicationFSM,
		metadataReader: metadataReader,
	}
}

func (r *Router) GetReadWriteReplicasLocation(collection string, shard string) ([]string, []string, error) {
	replicas, err := r.metadataReader.ShardReplicas(collection, shard)
	if err != nil {
		return nil, nil, err
	}
	readReplicas, writeReplicas := r.replicationFSM.FilterOneShardReplicasReadWrite(collection, shard, replicas)
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
