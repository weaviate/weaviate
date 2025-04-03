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
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/entities/schema"
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
	sourceNodeHostname, ok := c.nodeSelector.NodeHostname(srcNodeId)
	if !ok {
		return fmt.Errorf("sourceNodeName not found for node %s", srcNodeId)
	}
	relativeFilePaths, err := c.remoteIndex.PauseAndListFiles(ctx, sourceNodeHostname, collectionName, shardName)
	if err != nil {
		return err
	}
	for _, relativeFilePath := range relativeFilePaths {
		reader, err := c.remoteIndex.GetFile(ctx, sourceNodeHostname, collectionName, shardName, relativeFilePath)
		if err != nil {
			return err
		}
		defer reader.Close()

		finalPath := filepath.Join(c.rootDataPath, relativeFilePath)
		dir := path.Dir(finalPath)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("create parent folder for %s: %w", relativeFilePath, err)
		}

		f, err := os.Create(finalPath)
		if err != nil {
			return fmt.Errorf("open file %q for writing: %w", relativeFilePath, err)
		}

		defer f.Close()
		_, err = io.Copy(f, reader)
		if err != nil {
			return err
		}
	}

	// nate, not jero
	index := c.indexGetter.GetIndex(schema.ClassName(collectionName))
	err = index.LoadLocalShard(ctx, shardName)
	if err != nil {
		return err
	}

	shardLike, release, err := index.GetShard(ctx, shardName)
	if err != nil {
		return err
	}
	defer release()

	// TODO do we need to disable "other" replicas sending async updates to source node?
	//  do we get into an "infinite" node3 keeps trying to write objects that get ignored?

	// update class/collection config to set directed async replication
	// poll the source node until async replication to this node is "done"
	// commit copy and reset async replication config to default

	shard, ok := shardLike.(*db.Shard)
	if !ok {
		return fmt.Errorf("shard %s is not a db.Shard", shardName)
	}
	fmt.Println("NATEE initing async replication")
	err = shard.InitAsyncReplication()
	if err != nil {
		return err
	}
	fmt.Println("NATEE done initing async replication", err)
	// config, err := shard.GetAsyncReplicationConfig()
	// if err != nil {
	// 	return err
	// }
	// fmt.Println("NATEE starting hashbeat")
	// stats, err := shard.HashBeat(ctx, config)
	// fmt.Println("NATEE done hashbeat", stats, err)
	// if err != nil {
	// 	return err
	// }
	return nil
}
