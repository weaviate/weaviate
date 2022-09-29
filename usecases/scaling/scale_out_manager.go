package scaling

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

type ScaleOutManager struct {
	// the scaleOutManager needs to read and updated the sharding state of a
	// class. It can access it through the schemaManager
	schemaManager SchemaManager

	// get information about which nodes are in the cluster
	clusterState clusterState

	backerUpper BackerUpper

	nodes nodeClient

	persistenceRoot string
}

type clusterState interface {
	// AllNames() returns all the node names (not the hostnames!) including the
	// local one
	AllNames() []string
	LocalName() string
	NodeHostname(nodename string) (string, bool)
}

type BackerUpper interface {
	SingleShardBackup(
		ctx context.Context, bakID, className, shardName string,
	) (backup.ClassDescriptor, error)
}

func NewScaleOutManager(clusterState clusterState, backerUpper BackerUpper,
	nodeClient nodeClient, persistenceRoot string,
) *ScaleOutManager {
	return &ScaleOutManager{
		clusterState:    clusterState,
		backerUpper:     backerUpper,
		nodes:           nodeClient,
		persistenceRoot: persistenceRoot,
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

	for shardName := range ssBefore.Physical {
		bakID := fmt.Sprintf("_internal_scaleout_%s", uuid.New().String())
		bak, err := som.backerUpper.SingleShardBackup(ctx, bakID, className, shardName)
		if err != nil {
			return errors.Wrap(err, "create snapshot")
		}

		// TODO: This manual diffing is ugly, refactor!
		newNodes := ssAfter.Physical[shardName].BelongsToNodes
		previousNodes := ssBefore.Physical[shardName].BelongsToNodes

		additions := newNodes[len(previousNodes):]
		for _, targetNode := range additions {
			bakShard := bak.Shards[0]
			if bakShard.Name != shardName {
				// this sanity check is only needed because of the [0] above. If this
				// supports multi-shard, we need a better logic anyway
				return fmt.Errorf("shard name mismatch in backup: %q vs %q", bakShard.Name,
					shardName)
			}

			for _, file := range bakShard.Files {
				err := som.PutFile(ctx, file, targetNode, className, shardName)
				if err != nil {
					return fmt.Errorf("copy files to remote node: %w", err)
				}
			}
		}
	}

	// TODO: defer relaese snapshot!!!

	//
	// - identify target nodes and tell them to create (empty) local shards
	//
	// - transfer all files from source shard to new shard
	//
	// - release snapshots to restart compation cycles, etc
	//

	// finally, commit sharding state back to schema manager so that everyone is
	// aware of the new associations. The schema Manager itself must make sure
	// that the updated association is replicated to the entire cluster

	return nil
}

func (som *ScaleOutManager) scaleIn(ctx context.Context, className string,
	old, updated sharding.Config,
) error {
	return errors.Errorf("scaling in (reducing replica count) not supported yet")
}

func (som *ScaleOutManager) PutFile(ctx context.Context, sourceFileName string,
	targetNode, className, shardName string,
) error {
	absPath := filepath.Join(som.persistenceRoot, sourceFileName)

	hostname, ok := som.clusterState.NodeHostname(targetNode)
	if !ok {
		return fmt.Errorf("resolve hostname for node %q", targetNode)
	}

	f, err := os.Open(absPath)
	if err != nil {
		return fmt.Errorf("open file %q for reading: %w", absPath, err)
	}

	return som.nodes.PutFile(ctx, hostname, className, shardName, sourceFileName, f)
}

type nodeClient interface {
	PutFile(ctx context.Context, hostName, indexName,
		shardName, fileName string, payload io.ReadCloser) error
}
