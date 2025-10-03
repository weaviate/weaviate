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

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/sharding/config"
)

type ShardDist map[string][]string

// client the client interface is used to communicate with remote nodes
type client interface {
	// CreateShard creates an empty shard on the remote node.
	CreateShard(ctx context.Context, hostName, indexName, shardName string) error
}

// ErrUnresolvedName cannot resolve the host address of a node
var ErrUnresolvedName = errors.New("cannot resolve node name")

// Scaler scales out/in class replicas.
//
// It scales out a class by replicating its shards on new replicas
type Scaler struct {
	schemaReader SchemaReader
	cluster      cluster.NodeSelector
	client       client // client for remote nodes
	logger       logrus.FieldLogger
}

// New returns a new instance of Scaler
func New(cl cluster.NodeSelector, c client, logger logrus.FieldLogger) *Scaler {
	return &Scaler{
		cluster: cl,
		client:  c,
		logger:  logger,
	}
}

// SchemaReader is used by the scaler to get and update sharding states
type SchemaReader interface {
	CopyShardingState(class string) *sharding.State
}

func (s *Scaler) SetSchemaReader(sr SchemaReader) {
	s.schemaReader = sr
}

// Scale increase/decrease class replicas.
//
// It returns the updated sharding state if successful. The caller must then
// make sure to broadcast that state to all nodes as part of the "update"
// transaction.
func (s *Scaler) Scale(ctx context.Context, className string,
	updated config.Config, prevReplFactor, newReplFactor int64,
) (*sharding.State, error) {
	// First identify what the sharding state was before this change. This is
	// mainly to be able to compare the diff later, so we know where we need to
	// make changes
	ssBefore := s.schemaReader.CopyShardingState(className)
	if ssBefore == nil {
		return nil, fmt.Errorf("no sharding state for class %q", className)
	}
	if newReplFactor > prevReplFactor {
		return s.scaleOut(ctx, className, ssBefore, updated, newReplFactor)
	}

	if newReplFactor < prevReplFactor {
		return nil, errors.Errorf("scaling in not supported yet")
	}

	return nil, nil
}

// scaleOut replicate class shards on new replicas (nodes):
//
// * It calculates new sharding state
// * It pushes locally existing shards to new replicas
func (s *Scaler) scaleOut(ctx context.Context, className string, ssBefore *sharding.State,
	updated config.Config, replFactor int64,
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

	newNodesPerShard := newNodesPerShard(ssBefore, &ssAfter)
	g, ctx := enterrors.NewErrorGroupWithContextWrapper(s.logger, ctx)

	g.Go(func() error {
		for shard, nodes := range newNodesPerShard {
			for _, node := range nodes {
				host, ok := s.cluster.NodeHostname(node)
				if !ok {
					return fmt.Errorf("%w, %q", ErrUnresolvedName, node)
				}

				if err := s.client.CreateShard(ctx, host, className, shard); err != nil {
					return fmt.Errorf("create shard %q at %q: %w", shard, host, err)
				}
			}
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

func newNodesPerShard(before, after *sharding.State) ShardDist {
	res := make(ShardDist, len(before.Physical))

	for name := range before.Physical {
		res[name] = difference(after.Physical[name].BelongsToNodes, before.Physical[name].BelongsToNodes)
	}

	return res
}

func (s *Scaler) LocalScaleOut(ctx context.Context,
	className string, dist ShardDist,
) error {
	return fmt.Errorf("not supported anymore")
}
