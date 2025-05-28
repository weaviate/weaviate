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
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/diskio"
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

// CopyReplicaFiles copies a shard replica from the source node to this node.
func (c *Copier) CopyReplicaFiles(ctx context.Context, srcNodeId, collectionName, shardName string, schemaVersion uint64) error {
	sourceNodeHostname, ok := c.nodeSelector.NodeHostname(srcNodeId)
	if !ok {
		return fmt.Errorf("source node address not found in cluster membership for node %s", srcNodeId)
	}

	err := c.remoteIndex.PauseFileActivity(ctx, sourceNodeHostname, collectionName, shardName, schemaVersion)
	if err != nil {
		return fmt.Errorf("failed to pause file activity: %w", err)
	}
	defer c.remoteIndex.ResumeFileActivity(ctx, sourceNodeHostname, collectionName, shardName)

	relativeFilePaths, err := c.remoteIndex.ListFiles(ctx, sourceNodeHostname, collectionName, shardName)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
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

	err = c.prepareLocalFolder(relativeFilePaths)
	if err != nil {
		return fmt.Errorf("failed to prepare local folder: %w", err)
	}

	eg, gctx := enterrors.NewErrorGroupWithContextWrapper(c.logger, ctx)
	eg.SetLimit(concurrency)
	for _, relativeFilePath := range relativeFilePaths {
		relativeFilePath := relativeFilePath
		eg.Go(func() error {
			return c.syncFile(gctx, sourceNodeHostname, collectionName, shardName, relativeFilePath)
		})
	}

	err = eg.Wait()
	if err != nil {
		return fmt.Errorf("failed to sync files: %w", err)
	}

	err = diskio.Fsync(c.rootDataPath)
	if err != nil {
		return fmt.Errorf("failed to fsync local folder: %w", err)
	}

	err = c.validateLocalFolder(relativeFilePaths)
	if err != nil {
		return fmt.Errorf("failed to validate local folder: %w", err)
	}

	return nil
}

func (c *Copier) LoadLocalShard(ctx context.Context, collectionName, shardName string) error {
	idx := c.dbWrapper.GetIndex(schema.ClassName(collectionName))
	if idx == nil {
		return fmt.Errorf("index for collection %s not found", collectionName)
	}

	return idx.LoadLocalShard(ctx, shardName)
}

func (c *Copier) prepareLocalFolder(relativeFilePaths []string) error {
	relativeFilePathsMap := make(map[string]struct{}, len(relativeFilePaths))
	for _, path := range relativeFilePaths {
		relativeFilePathsMap[path] = struct{}{}
	}

	filepath.WalkDir(c.rootDataPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walking local folder: %w", err)
		}

		if d.IsDir() {
			return nil
		}

		relativeFilePath := filepath.Join(c.rootDataPath, d.Name())

		if _, ok := relativeFilePathsMap[relativeFilePath]; !ok {
			err := os.Remove(relativeFilePath)
			if err != nil {
				return fmt.Errorf("removing local file %q not present in source node: %w", d.Name(), err)
			}
		}

		return nil
	})

	return nil
}

func (c *Copier) validateLocalFolder(relativeFilePaths []string) error {
	i := 0

	filepath.WalkDir(c.rootDataPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walking local folder: %w", err)
		}

		if d.IsDir() {
			return nil
		}

		localRelFilePath := filepath.Join(c.rootDataPath, d.Name())

		if relativeFilePaths[i] != localRelFilePath {
			return fmt.Errorf("unexpected: local folder contains unexpected content")
		}

		i++
		// exit WalkDir early if we have more files in the local folder than expected
		if len(relativeFilePaths) < i {
			return fmt.Errorf("unexpected: local folder has more files than source node")
		}

		return nil
	})

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
func (c *Copier) AddAsyncReplicationTargetNode(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride, schemaVersion uint64) error {
	srcNodeHostname, ok := c.nodeSelector.NodeHostname(targetNodeOverride.SourceNode)
	if !ok {
		return fmt.Errorf("source node address not found in cluster membership for node %s", targetNodeOverride.SourceNode)
	}

	return c.remoteIndex.AddAsyncReplicationTargetNode(ctx, srcNodeHostname, targetNodeOverride.CollectionID, targetNodeOverride.ShardID, targetNodeOverride, schemaVersion)
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
	if err != nil {
		return fmt.Errorf("get shard %s err: %w", shardName, err)
	}
	if shard == nil {
		return fmt.Errorf("get shard %s: not found", shardName)
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
	if err != nil {
		return fmt.Errorf("get shard %s err: %w", shardName, err)
	}
	if shard == nil {
		return fmt.Errorf("get shard %s: not found", shardName)
	}
	defer release()

	return shard.UpdateAsyncReplicationConfig(ctx, shard.Index().Config.AsyncReplicationEnabled)
}

// AsyncReplicationStatus returns the async replication status for a shard.
// The first two return values are the number of objects propagated and the start diff time in unix milliseconds.
func (c *Copier) AsyncReplicationStatus(ctx context.Context, srcNodeId, targetNodeId, collectionName, shardName string) (models.AsyncReplicationStatus, error) {
	status, err := c.dbWrapper.GetOneNodeStatus(ctx, srcNodeId, collectionName, shardName, "verbose")
	if err != nil {
		return models.AsyncReplicationStatus{}, err
	}

	if len(status.Shards) == 0 {
		return models.AsyncReplicationStatus{}, fmt.Errorf("stats are empty for node %s", srcNodeId)
	}

	shardFound := false
	for _, shard := range status.Shards {
		if shard.Name != shardName || shard.Class != collectionName {
			continue
		}

		shardFound = true
		if len(shard.AsyncReplicationStatus) == 0 {
			return models.AsyncReplicationStatus{}, fmt.Errorf("async replication status empty for shard %s in node %s", shardName, srcNodeId)
		}

		for _, asyncReplicationStatus := range shard.AsyncReplicationStatus {
			if asyncReplicationStatus.TargetNode != targetNodeId {
				continue
			}

			return models.AsyncReplicationStatus{
				ObjectsPropagated:       asyncReplicationStatus.ObjectsPropagated,
				StartDiffTimeUnixMillis: asyncReplicationStatus.StartDiffTimeUnixMillis,
				TargetNode:              asyncReplicationStatus.TargetNode,
			}, nil
		}
	}

	if !shardFound {
		return models.AsyncReplicationStatus{}, fmt.Errorf("shard %s not found in node %s", shardName, srcNodeId)
	}

	return models.AsyncReplicationStatus{}, fmt.Errorf("async replication status not found for shard %s in node %s", shardName, srcNodeId)
}
