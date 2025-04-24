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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/integrity"
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
	indexGetter types.DbInt
}

// TODO indexGetter name
// New creates a new shard replica Copier.
func New(t types.RemoteIndex, nodeSelector cluster.NodeSelector, rootPath string, indexGetter types.DbInt) *Copier {
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
		return fmt.Errorf("source node address not found in cluster membership for node %s", srcNodeId)
	}

	err := c.remoteIndex.PauseFileActivity(ctx, sourceNodeHostname, collectionName, shardName)
	if err != nil {
		return err
	}
	// TODO remove
	// fmt.Println("NATEE copy replica starting sleep")
	// time.Sleep(25 * time.Second)
	// fmt.Println("NATEE copy replica sleep done")
	defer c.remoteIndex.ResumeFileActivity(ctx, sourceNodeHostname, collectionName, shardName)

	relativeFilePaths, err := c.remoteIndex.ListFiles(ctx, sourceNodeHostname, collectionName, shardName)
	if err != nil {
		return err
	}

	for _, relativeFilePath := range relativeFilePaths {
		md, err := c.remoteIndex.GetFileMetadata(ctx, sourceNodeHostname, collectionName, shardName, relativeFilePath)
		if err != nil {
			return err
		}

		finalLocalPath := filepath.Join(c.rootDataPath, relativeFilePath)

		_, checksum, err := integrity.CRC32(finalLocalPath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if checksum == md.CRC32 {
			// local file matches remote one, no need to download it
			continue
		}

		reader, err := c.remoteIndex.GetFile(ctx, sourceNodeHostname, collectionName, shardName, relativeFilePath)
		if err != nil {
			return err
		}
		defer reader.Close()

		dir := path.Dir(finalLocalPath)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("create parent folder for %s: %w", relativeFilePath, err)
		}

		err = func() error {
			f, err := os.Create(finalLocalPath)
			if err != nil {
				return fmt.Errorf("open file %q for writing: %w", relativeFilePath, err)
			}
			defer f.Close()

			_, err = io.Copy(f, reader)
			if err != nil {
				return err
			}

			err = f.Sync()
			if err != nil {
				return fmt.Errorf("fsyncing file %q for writing: %w", relativeFilePath, err)
			}

			_, checksum, err = integrity.CRC32(finalLocalPath)
			if err != nil {
				return err
			}

			if checksum != md.CRC32 {
				return fmt.Errorf("checksum validation of file %q failed", relativeFilePath)
			}

			return nil
		}()
		if err != nil {
			return err
		}
	}

	err = c.indexGetter.GetIndex(schema.ClassName(collectionName)).LoadLocalShard(ctx, shardName)
	if err != nil {
		return err
	}

	return nil
}

// TODO objects propagated, start diff time, err
func (c *Copier) AsyncReplicationStatus(ctx context.Context, srcNodeId, targetNodeId, collectionName, shardName string) (uint64, int64, error) {
	// TODO can this blow up if node has many shards/tenants? i could add a new method to get only one shard?
	status, err := c.indexGetter.GetOneNodeStatus(ctx, srcNodeId, collectionName, "verbose")
	if err != nil {
		return 0, 0, err
	}

	for _, shard := range status.Shards {
		if shard.Name == shardName && shard.Class == collectionName {
			for _, asyncReplicationStatus := range shard.AsyncReplicationStatus {
				if asyncReplicationStatus.TargetNode == targetNodeId {
					return asyncReplicationStatus.ObjectsPropagated, asyncReplicationStatus.StartDiffTimeUnixMillis, nil
				}
			}
		}
	}

	return 0, 0, fmt.Errorf("shard %s or collection %s not found in node %s or stats are nil", shardName, collectionName, srcNodeId)
}
