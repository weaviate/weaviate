package scaling

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

type ScaleOutManager struct {
	// the scaleOutManager needs to read and updated the sharding state of a
	// class. It can access it through the schemaManager
	schemaManager SchemaManager

	// get information about which nodes are in the cluster
	clusterState clusterState
}

type clusterState interface {
	// AllNames() returns all the node names (not the hostnames!) including the
	// local one
	AllNames() []string
	LocalName() string
}

func NewScaleOutManager(clusterState clusterState) *ScaleOutManager {
	return &ScaleOutManager{
		clusterState: clusterState,
	}
}

type SchemaManager interface {
	ShardingState(class string) *sharding.State
}

func (som *ScaleOutManager) SetSchemaManager(sm SchemaManager) {
	som.schemaManager = sm
}

func (som *ScaleOutManager) Scale(ctx context.Context, className string,
	old, updated sharding.Config,
) error {
	// TODOs
	// =====

	ss := som.schemaManager.ShardingState(className)
	if ss == nil {
		return errors.Errorf("no sharding state for class %q", className)
	}

	// TODO: we would most likely need to create a deep copy here, otherwise our
	// mutations might be applied before we hit commit because of memory sharing
	// with the schema manager

	for name, shard := range ss.Physical {
		shard.AdjustReplicas(updated.Replicas, som.clusterState)
		ss.Physical[name] = shard
	}

	// add more nodes to associating list, for now pick any node that isn't the
	// current node, first iteration does not yet support spreading replication
	// shards evenly

	// for each shard
	//
	// - find existing local copy on current or remote node
	// TODO: what if existing shard is not on current node?
	//       in first iteration just fail, it's OK to support only 1->n for now
	//       which makes sure that the existing shard is local
	//
	// - create a snapshot
	//
	// - identify target nodes and tell them to create (empty) local shards
	//
	// - transfer all files from source shard to new shard
	//
	// - release snapshots to restart compation cycles, etc
	//

	// finally, commit sharding state back to schema manager so that everyone is
	// aware of the new associations. The schema Manager itself must make sure
	// that the updated assocation is replicated to the entire cluster

	return errors.Errorf("not implemented yet")
}
