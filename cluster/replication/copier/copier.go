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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/integrity"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const concurrency = 10

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
	// dbWrapper is used to load the index for the collection so that we can create/interact
	// with the shard on this node
	dbWrapper types.DbWrapper

	logger logrus.FieldLogger
}

// New creates a new shard replica Copier.
func New(t types.RemoteIndex, nodeSelector cluster.NodeSelector, rootPath string, dbWrapper types.DbWrapper, logger logrus.FieldLogger) *Copier {
	return &Copier{
		remoteIndex:  t,
		nodeSelector: nodeSelector,
		rootDataPath: rootPath,
		dbWrapper:    dbWrapper,
		logger:       logger,
	}
}

// RemoveLocalReplica removes the local replica of a shard on this node.
func (c *Copier) RemoveLocalReplica(ctx context.Context, collectionName, shardName string) {
	index := c.dbWrapper.GetIndex(schema.ClassName(collectionName))
	if index == nil {
		return // no index found, nothing to do
	}

	err := index.DropShard(shardName)
	if err != nil {
		return // no shard found, nothing to do
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
	defer c.remoteIndex.ResumeFileActivity(ctx, sourceNodeHostname, collectionName, shardName)

	relativeFilePaths, err := c.remoteIndex.ListFiles(ctx, sourceNodeHostname, collectionName, shardName)
	if err != nil {
		return err
	}

	// TODO remove this once we have a passing test that constantly inserts in parallel
	// during shard replica movement
	// if WEAVIATE_TEST_COPY_REPLICA_SLEEP is set, sleep for that amount of time
	// this is only used for testing purposes
	if os.Getenv("WEAVIATE_TEST_COPY_REPLICA_SLEEP") != "" {
		sleepTime, err := time.ParseDuration(os.Getenv("WEAVIATE_TEST_COPY_REPLICA_SLEEP"))
		if err != nil {
			return fmt.Errorf("invalid WEAVIATE_TEST_COPY_REPLICA_SLEEP: %w", err)
		}
		time.Sleep(sleepTime)
	}

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(concurrency)

	for _, relativeFilePath := range relativeFilePaths {
		filePath := relativeFilePath

		eg.Go(func() error {
			return c.syncFile(gctx, sourceNodeHostname, collectionName, shardName, filePath)
		})
	}

	err = eg.Wait()
	if err != nil {
		return err
	}

	err = c.dbWrapper.GetIndex(schema.ClassName(collectionName)).LoadLocalShard(ctx, shardName)
	if err != nil {
		return err
	}

	return nil
}

func (c *Copier) syncFile(ctx context.Context, sourceNodeHostname, collectionName, shardName, relativeFilePath string) error {
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
		return nil
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
}

// AddAsyncReplicationTargetNode adds a target node override for a shard.
func (c *Copier) AddAsyncReplicationTargetNode(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	srcNodeHostname, ok := c.nodeSelector.NodeHostname(targetNodeOverride.SourceNode)
	if !ok {
		return fmt.Errorf("source node address not found in cluster membership for node %s", targetNodeOverride.SourceNode)
	}

	return c.remoteIndex.AddAsyncReplicationTargetNode(ctx, srcNodeHostname, targetNodeOverride.CollectionID, targetNodeOverride.ShardID, targetNodeOverride)
}

// RemoveAsyncReplicationTargetNode removes a target node override for a shard.
func (c *Copier) RemoveAsyncReplicationTargetNode(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	srcNodeHostname, ok := c.nodeSelector.NodeHostname(targetNodeOverride.SourceNode)
	if !ok {
		return fmt.Errorf("source node address not found in cluster membership for node %s", targetNodeOverride.SourceNode)
	}

	return c.remoteIndex.RemoveAsyncReplicationTargetNode(ctx, srcNodeHostname, targetNodeOverride.CollectionID, targetNodeOverride.ShardID, targetNodeOverride)
}

func (c *Copier) InitAsyncReplicationLocally(ctx context.Context, collectionName, shardName string) error {
	index := c.dbWrapper.GetIndex(schema.ClassName(collectionName))
	if index == nil {
		return fmt.Errorf("index for collection %s not found", collectionName)
	}

	shard, release, err := index.GetShard(ctx, shardName)
	if err != nil || shard == nil {
		return fmt.Errorf("get shard %s: %w", shardName, err)
	}
	defer release()

	return shard.UpdateAsyncReplicationConfig(ctx, true)
}

func (c *Copier) RevertAsyncReplicationLocally(ctx context.Context, collectionName, shardName string) error {
	index := c.dbWrapper.GetIndex(schema.ClassName(collectionName))
	if index == nil {
		return fmt.Errorf("index for collection %s not found", collectionName)
	}

	shard, release, err := index.GetShard(ctx, shardName)
	if err != nil || shard == nil {
		return fmt.Errorf("get shard %s: %w", shardName, err)
	}
	defer release()

	return shard.UpdateAsyncReplicationConfig(ctx, shard.Index().Config.AsyncReplicationEnabled)
}

// AsyncReplicationStatus returns the async replication status for a shard.
// The first two return values are the number of objects propagated and the start diff time in unix milliseconds.
func (c *Copier) AsyncReplicationStatus(ctx context.Context, srcNodeId, targetNodeId, collectionName, shardName string) (models.AsyncReplicationStatus, error) {
	// TODO can using verbose here blow up if the node has many shards/tenants? i could add a new method to get only one shard?
	status, err := c.dbWrapper.GetOneNodeStatus(ctx, srcNodeId, collectionName, "verbose")
	if err != nil {
		return models.AsyncReplicationStatus{}, err
	}

	for _, shard := range status.Shards {
		if shard.Name == shardName && shard.Class == collectionName {
			for _, asyncReplicationStatus := range shard.AsyncReplicationStatus {
				if asyncReplicationStatus.TargetNode == targetNodeId {
					return models.AsyncReplicationStatus{
						ObjectsPropagated:       asyncReplicationStatus.ObjectsPropagated,
						StartDiffTimeUnixMillis: asyncReplicationStatus.StartDiffTimeUnixMillis,
						TargetNode:              asyncReplicationStatus.TargetNode,
					}, nil
				}
			}
		}
	}

	return models.AsyncReplicationStatus{}, fmt.Errorf("shard %s or collection %s not found in node %s or stats are nil", shardName, collectionName, srcNodeId)
}
