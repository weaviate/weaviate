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

package copier

import (
	"context"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// Copier for shard replicas, can copy a shard replica from one node to another.
type Copier struct {
	// nodeSelector converts node IDs to hostnames
	nodeSelector cluster.NodeSelector
	// remoteIndex allows you to "call" methods on other nodes, in this case, we'll be "calling"
	// methods on the source node to perform the copy
	remoteIndex types.RemoteIndex
	// rootDataPath is the local path to the root data directory for the shard, we'll copy files
	// to this path
	rootDataPath string
	// indexGetter is used to load the index for the collection so that we can create/interact
	// with the shard on this node
	indexGetter types.IndexGetter
}

// New creates a new shard replica Copier.
func New(t types.RemoteIndex, nodeSelector cluster.NodeSelector, rootPath string, indexGetter types.IndexGetter) *Copier {
	return &Copier{
		remoteIndex:  t,
		nodeSelector: nodeSelector,
		rootDataPath: rootPath,
		indexGetter:  indexGetter,
	}
}

// CopyReplica copies a shard replica from the source node to this node.
func (c *Copier) CopyReplica(ctx context.Context, srcNodeId, collectionName, shardName string) error {
	// No nesescito

	return nil
}
