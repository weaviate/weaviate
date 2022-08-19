package scaling

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

type ScaleOutManager struct {
	// the scaleOutManager needs to read and updated the sharding state of a
	// class. It can access it through the schemaManager
	schemaManager SchemaManager

	// get information about which nodes are in the cluster
	clusterState clusterState

	snapshotter Snapshotter
}

type clusterState interface {
	// AllNames() returns all the node names (not the hostnames!) including the
	// local one
	AllNames() []string
	LocalName() string
}

type Snapshotter interface {
	CreateSnapshot(
		ctx context.Context, className string, snap *snapshots.Snapshot,
	) (*snapshots.Snapshot, error)
}

func NewScaleOutManager(clusterState clusterState, snapshotter Snapshotter) *ScaleOutManager {
	return &ScaleOutManager{
		clusterState: clusterState,
		snapshotter:  snapshotter,
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
	if updated.Replicas > old.Replicas {
		return som.scaleOut(ctx, className, old, updated)
	}

	if updated.Replicas < old.Replicas {
		return som.scaleIn(ctx, className, old, updated)
	}

	return nil
}

func (som *ScaleOutManager) scaleOut(ctx context.Context, className string,
	old, updated sharding.Config,
) error {
	ssBefore := som.schemaManager.ShardingState(className)
	if ssBefore == nil {
		return errors.Errorf("no sharding state for class %q", className)
	}

	ssAfter := ssBefore.DeepCopy()

	for name, shard := range ssAfter.Physical {
		shard.AdjustReplicas(updated.Replicas, som.clusterState)
		ssAfter.Physical[name] = shard
	}

	for name := range ssBefore.Physical {
		if !ssBefore.IsShardLocal(name) {
			// TODO
			return errors.Errorf("scaling remote shards not supported yet, send request to node that has the shard to be scaled out")
		}
	}

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

func (som *ScaleOutManager) scaleIn(ctx context.Context, className string,
	old, updated sharding.Config,
) error {
	return errors.Errorf("scaling in (reducing replica count) not supported yet")
}
