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

package scaler

import (
	"context"
	"fmt"
	"runtime"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/sharding"
	"golang.org/x/sync/errgroup"
)

// TODOs: Performance
//
// 1. Improve performance of syncing a shard to multiple nodes (see rsync.Push).
// We could concurrently sync same files to different nodes  while avoiding overlapping
//
// 2. To fail fast, we might consider creating all shards at once and re-initialize them in the final step
//
// 3. implement scaler.scaleIn

var (
	// ErrUnresolvedName cannot resolve the host address of a node
	ErrUnresolvedName = errors.New("cannot resolve node name")
	_NUMCPU           = runtime.NumCPU()
)

// Scaler scales out/in class replicas.
//
// It scales out a class by replicating its shards on new replicas
type Scaler struct {
	schema          SchemaManager
	cluster         cluster
	source          BackUpper // data source
	client          client    // client for remote nodes
	logger          logrus.FieldLogger
	persistenceRoot string
}

// New returns a new instance of Scaler
func New(cl cluster, source BackUpper,
	c client, logger logrus.FieldLogger, persistenceRoot string,
) *Scaler {
	return &Scaler{
		cluster:         cl,
		source:          source,
		client:          c,
		logger:          logger,
		persistenceRoot: persistenceRoot,
	}
}

// BackUpper is used to back up shards of a specific class
type BackUpper interface {
	// ShardsBackup returns class backup descriptor for a list of shards
	ShardsBackup(_ context.Context, id, class string, shards []string) (backup.ClassDescriptor, error)
	// ReleaseBackup releases the backup specified by its id
	ReleaseBackup(ctx context.Context, id, className string) error
}

// cluster is used by the scaler to query cluster
type cluster interface {
	// Candidates returns list of existing nodes in the cluster
	Candidates() []string
	// LocalName returns name of this node
	LocalName() string
	// NodeHostname return hosts address for a specific node name
	NodeHostname(name string) (string, bool)
}

// SchemaManager is used by the scaler to get and update sharding states
type SchemaManager interface {
	CopyShardingState(class string) *sharding.State
}

func (s *Scaler) SetSchemaManager(sm SchemaManager) {
	s.schema = sm
}

// Scale increase/decrease class replicas.
//
// It returns the updated sharding state if successful. The caller must then
// make sure to broadcast that state to all nodes as part of the "update"
// transaction.
func (s *Scaler) Scale(ctx context.Context, className string,
	updated sharding.Config, prevReplFactor, newReplFactor int64,
) (*sharding.State, error) {
	// First identify what the sharding state was before this change. This is
	// mainly to be able to compare the diff later, so we know where we need to
	// make changes
	ssBefore := s.schema.CopyShardingState(className)
	if ssBefore == nil {
		return nil, fmt.Errorf("no sharding state for class %q", className)
	}
	if newReplFactor > prevReplFactor {
		return s.scaleOut(ctx, className, ssBefore, updated, newReplFactor)
	}

	if newReplFactor < prevReplFactor {
		return s.scaleIn(ctx, className, updated)
	}

	return nil, nil
}

// scaleOut replicate class shards on new replicas (nodes):
//
// * It calculates new sharding state
// * It pushes locally existing shards to new replicas
// * It delegates replication of remote shards to owner nodes
func (s *Scaler) scaleOut(ctx context.Context, className string, ssBefore *sharding.State,
	updated sharding.Config, replFactor int64,
) (*sharding.State, error) {
	// Create a deep copy of the old sharding state, so we can start building the
	// updated state. Because this is a deep copy we don't risk leaking our
	// changes to anyone else. We can return the changes in the end where the
	// caller can then make sure to broadcast the new state to the cluster.
	ssAfter := ssBefore.DeepCopy()
	ssAfter.Config = updated

	// Identify all shards of the class and adjust the replicas. After this is
	// done, the affected shards now belong to more nodes than they did before.
	for name, shard := range ssAfter.Physical {
		if err := shard.AdjustReplicas(int(replFactor), s.cluster); err != nil {
			return nil, err
		}
		ssAfter.Physical[name] = shard
	}
	lDist, nodeDist := distributions(ssBefore, &ssAfter)
	g, ctx := errgroup.WithContext(ctx)
	// resolve hosts beforehand
	nodes := nodeDist.nodes()
	hosts, err := hosts(nodes, s.cluster)
	if err != nil {
		return nil, err
	}
	for i, node := range nodes {
		dist := nodeDist[node]
		i := i
		g.Go(func() error {
			err := s.client.IncreaseReplicationFactor(ctx, hosts[i], className, dist)
			if err != nil {
				return fmt.Errorf("increase replication factor for class %q on node %q: %w", className, nodes[i], err)
			}
			return nil
		})
	}

	g.Go(func() error {
		if err := s.LocalScaleOut(ctx, className, lDist); err != nil {
			return fmt.Errorf("increase local replication factor: %w", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Finally, return sharding state back to schema manager. The schema manager
	// will then broadcast this updated state to the cluster. This is essentially
	// what will take the new replication shards live: On the new nodes, if
	// traffic is incoming, IsShardLocal() would have returned false before. But
	// now that a copy of the local shard is present it will return true and
	// serve the traffic.
	return &ssAfter, nil
}

// LocalScaleOut syncs local shards with new replicas.
//
// This is the meat&bones of this implementation.
// For each shard, we're roughly doing the following:
//   - Create shards backup, so the shards are safe to copy
//   - Figure out the copy targets (i.e. each node that is part of the after
//     state, but wasn't part of the before state yet)
//   - Create an empty shard on the target node
//   - Copy over all files from the backup
//   - ReInit the shard to recognize the copied files
//   - Release the single-shard backup
func (s *Scaler) LocalScaleOut(ctx context.Context,
	className string, dist ShardDist,
) error {
	if len(dist) < 1 {
		return nil
	}
	// Create backup of the sin
	bakID := fmt.Sprintf("_internal_scaler_%s", uuid.New().String()) // todo better name
	bak, err := s.source.ShardsBackup(ctx, bakID, className, dist.shards())
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	defer func() {
		err := s.source.ReleaseBackup(context.Background(), bakID, className)
		if err != nil {
			s.logger.WithField("scaler", "releaseBackup").WithField("class", className).Error(err)
		}
	}()
	rsync := newRSync(s.client, s.cluster, s.persistenceRoot)
	return rsync.Push(ctx, bak.Shards, dist, className)
}

func (s *Scaler) scaleIn(ctx context.Context, className string,
	updated sharding.Config,
) (*sharding.State, error) {
	return nil, errors.Errorf("scaling in not supported yet")
}
