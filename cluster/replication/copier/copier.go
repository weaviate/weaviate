//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package copier

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/diskio"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/integrity"

	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type FileReplicationServiceClient pbv1.FileReplicationServiceClient

type FileReplicationServiceClientFactory func(ctx context.Context, address string) (FileReplicationServiceClient, error)

// Copier for shard replicas, can copy a shard replica from one node to another.
type Copier struct {
	// clientFactory is a factory function to create a gRPC client for the remote node
	clientFactory FileReplicationServiceClientFactory
	// nodeSelector converts node IDs to hostnames
	nodeSelector cluster.NodeSelector
	// remoteIndex allows you to "call" methods on other nodes, in this case, we'll be "calling"
	// methods on the source node to perform the copy
	remoteIndex types.RemoteIndex
	// concurrentWorkers is the number of concurrent workers to use for copying files
	concurrentWorkers int
	// rootDataPath is the local path to the root data directory for the shard, we'll copy files
	// to this path
	rootDataPath string
	// dbWrapper is used to load the index for the collection so that we can create/interact
	// with the shard on this node
	dbWrapper types.DbWrapper
	// nodeName is the name of this node
	nodeName string

	logger logrus.FieldLogger
}

// New creates a new shard replica Copier.
func New(clientFactory FileReplicationServiceClientFactory, remoteIndex types.RemoteIndex, nodeSelector cluster.NodeSelector,
	concurrentWorkers int, rootPath string, dbWrapper types.DbWrapper, nodeName string, logger logrus.FieldLogger,
) *Copier {
	return &Copier{
		clientFactory:     clientFactory,
		remoteIndex:       remoteIndex,
		nodeSelector:      nodeSelector,
		concurrentWorkers: concurrentWorkers,
		rootDataPath:      rootPath,
		dbWrapper:         dbWrapper,
		nodeName:          nodeName,
		logger:            logger,
	}
}

// CopyReplicaFiles copies a shard replica from the source node to this node.
func (c *Copier) CopyReplicaFiles(ctx context.Context, srcNodeId, collectionName, shardName string, schemaVersion uint64) error {
	sourceNodeAddress := c.nodeSelector.NodeAddress(srcNodeId)

	sourceNodeGRPCPort, err := c.nodeSelector.NodeGRPCPort(srcNodeId)
	if err != nil {
		return fmt.Errorf("failed to get gRPC port for source node: %w", err)
	}

	client, err := c.clientFactory(ctx, fmt.Sprintf("%s:%d", sourceNodeAddress, sourceNodeGRPCPort))
	if err != nil {
		return fmt.Errorf("failed to create gRPC client connection: %w", err)
	}

	_, err = client.PauseFileActivity(ctx, &pbv1.PauseFileActivityRequest{
		IndexName:     collectionName,
		ShardName:     shardName,
		SchemaVersion: schemaVersion,
	})
	if err != nil {
		return fmt.Errorf("failed to pause file activity: %w", err)
	}
	defer client.ResumeFileActivity(ctx, &pbv1.ResumeFileActivityRequest{
		IndexName: collectionName,
		ShardName: shardName,
	})

	fileListResp, err := client.ListFiles(ctx, &pbv1.ListFilesRequest{
		IndexName: collectionName,
		ShardName: shardName,
	})
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	fileNameChan := make(chan string, 1000)

	enterrors.GoWrapper(func() {
		defer close(fileNameChan)

		for _, name := range fileListResp.FileNames {
			fileNameChan <- name
		}
	}, c.logger)

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

	err = c.prepareLocalFolder(collectionName, shardName, fileListResp.FileNames)
	if err != nil {
		return fmt.Errorf("failed to prepare local folder: %w", err)
	}

	metadataChan := make(chan *pbv1.FileMetadata, 1000)
	var metaWG sync.WaitGroup

	for range c.concurrentWorkers {
		metaWG.Add(1)

		enterrors.GoWrapper(func() {
			err := c.metadataWorker(ctx, client, collectionName, shardName, fileNameChan, metadataChan, &metaWG)
			if err != nil {
				c.logger.WithError(err).Error("failed to get files metadata")
				return
			}
		}, c.logger)
	}

	var dlWG sync.WaitGroup

	for range c.concurrentWorkers {
		dlWG.Add(1)

		enterrors.GoWrapper(func() {
			err := c.downloadWorker(ctx, client, metadataChan, &dlWG)
			if err != nil {
				c.logger.WithError(err).Error("failed to download files")
				return
			}
		}, c.logger)
	}

	// wait for all metadata workers to finish
	metaWG.Wait()
	close(metadataChan)

	// wait for all download workers to finish
	dlWG.Wait()

	err = c.validateLocalFolder(collectionName, shardName, fileListResp.FileNames)
	if err != nil {
		return fmt.Errorf("failed to validate local folder: %w", err)
	}

	return nil
}

func (c *Copier) shardPath(collectionName, shardName string) string {
	return path.Join(c.rootDataPath, strings.ToLower(collectionName), shardName)
}

func (c *Copier) prepareLocalFolder(collectionName, shardName string, fileNames []string) error {
	fileNamesMap := make(map[string]struct{}, len(fileNames))
	for _, fileName := range fileNames {
		fileNamesMap[fileName] = struct{}{}
	}

	var dirs []string

	// remove files that are not in the source node
	basePath := c.shardPath(collectionName, shardName)

	err := filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("preparing local folder: %w", err)
		}

		if d.IsDir() {
			dirs = append(dirs, path)
			return nil
		}

		localRelFilePath, err := filepath.Rel(c.rootDataPath, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		if _, ok := fileNamesMap[localRelFilePath]; !ok {
			err := os.Remove(path)
			if err != nil {
				return fmt.Errorf("removing local file %q not present in source node: %w", path, err)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("preparing local folder: %w", err)
	}

	// sort dirs by depth, so that we delete the deepest directories first
	sortPathsByDepthDescending(dirs)

	for _, dir := range dirs {
		isEmpty, err := diskio.IsDirEmpty(dir)
		if err != nil {
			return fmt.Errorf("checking if local folder is empty: %s: %w", dir, err)
		}
		if !isEmpty {
			continue
		}

		err = os.Remove(dir)
		if err != nil {
			return fmt.Errorf("failed to remove empty local folder: %s: %w", dir, err)
		}
	}

	return nil
}

func (c *Copier) metadataWorker(ctx context.Context, client FileReplicationServiceClient,
	collectionName, shardName string, fileNameChan <-chan string, metadataChan chan<- *pbv1.FileMetadata,
	wg *sync.WaitGroup,
) error {
	defer wg.Done()

	stream, err := client.GetFileMetadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GetFileMetadata stream: %w", err)
	}
	defer func() {
		err := stream.CloseSend()

		// drain stream
		for err == nil {
			_, err = stream.Recv()
		}
	}()

	for fileName := range fileNameChan {
		err := stream.Send(&pbv1.GetFileMetadataRequest{
			IndexName: collectionName,
			ShardName: shardName,
			FileName:  fileName,
		})
		if err != nil {
			return fmt.Errorf("failed to send GetFileMetadata request for %q: %w", fileName, err)
		}

		meta, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive file metadata for %q: %w", fileName, err)
		}

		metadataChan <- meta
	}

	return nil
}

func (c *Copier) downloadWorker(ctx context.Context, client FileReplicationServiceClient,
	metadataChan <-chan *pbv1.FileMetadata, wg *sync.WaitGroup,
) error {
	defer wg.Done()

	stream, err := client.GetFile(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GetFile stream: %w", err)
	}
	defer func() {
		err := stream.CloseSend()

		// drain stream
		for err == nil {
			_, err = stream.Recv()
		}
	}()

	for meta := range metadataChan {
		localFilePath := filepath.Join(c.rootDataPath, meta.FileName)

		_, checksum, err := integrity.CRC32(localFilePath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if checksum == meta.Crc32 {
			// local file matches remote one, no need to download it
			return nil
		}

		err = stream.Send(&pbv1.GetFileRequest{
			IndexName: meta.IndexName,
			ShardName: meta.ShardName,
			FileName:  meta.FileName,
		})
		if err != nil {
			return fmt.Errorf("failed to send GetFile request for %s: %w", meta.FileName, err)
		}

		dir := path.Dir(localFilePath)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("create parent folder for %s: %w", localFilePath, err)
		}

		if err := func() error {
			f, err := os.Create(localFilePath + ".tmp")
			if err != nil {
				return fmt.Errorf("open file %q for writing: %w", localFilePath, err)
			}
			defer f.Close()

			wbuf := bufio.NewWriter(f)

			for {
				chunk, err := stream.Recv()
				if err != nil {
					return fmt.Errorf("failed to receive file chunk for %s: %w", meta.FileName, err)
				}

				if len(chunk.Data) > 0 {
					_, err = wbuf.Write(chunk.Data)
					if err != nil {
						return fmt.Errorf("writing chunk to file %q: %w", localFilePath+".tmp", err)
					}
				}

				if chunk.Eof {
					break
				}
			}

			err = wbuf.Flush()
			if err != nil {
				return fmt.Errorf("flushing buffer to file %q: %w", localFilePath+".tmp", err)
			}

			err = f.Sync()
			if err != nil {
				return fmt.Errorf("fsyncing file %q for writing: %w", localFilePath+".tmp", err)
			}

			_, checksum, err = integrity.CRC32(localFilePath + ".tmp")
			if err != nil {
				return fmt.Errorf("calculating checksum for file %q: %w", localFilePath+".tmp", err)
			}

			if checksum != meta.Crc32 {
				defer os.Remove(localFilePath + ".tmp")
				return fmt.Errorf("checksum validation of file %q failed, expected %d, got %d", localFilePath+".tmp", meta.Crc32, checksum)
			}

			err = os.Rename(localFilePath+".tmp", localFilePath)
			if err != nil {
				return fmt.Errorf("renaming temporary file %q to final path %q: %w", localFilePath+".tmp", localFilePath, err)
			}

			return nil
		}(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Copier) LoadLocalShard(ctx context.Context, collectionName, shardName string) error {
	idx := c.dbWrapper.GetIndex(schema.ClassName(collectionName))
	if idx == nil {
		return fmt.Errorf("index for collection %s not found", collectionName)
	}

	return idx.LoadLocalShard(ctx, shardName, false)
}

func (c *Copier) validateLocalFolder(collectionName, shardName string, fileNames []string) error {
	fileNamesMap := make(map[string]struct{}, len(fileNames))
	for _, fileName := range fileNames {
		fileNamesMap[fileName] = struct{}{}
	}

	var dirs []string

	basePath := c.shardPath(collectionName, shardName)

	err := filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("validating local folder: %w", err)
		}

		if d.IsDir() {
			dirs = append(dirs, path)
			return nil
		}

		localRelFilePath, err := filepath.Rel(c.rootDataPath, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		if _, ok := fileNamesMap[localRelFilePath]; !ok {
			return fmt.Errorf("file %q not found in source node, but exists locally", localRelFilePath)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("validating local folder: %w", err)
	}

	// sort dirs by depth, so that we fsync the deepest directories first
	sortPathsByDepthDescending(dirs)

	for _, dir := range dirs {
		if err := diskio.Fsync(dir); err != nil {
			return fmt.Errorf("failed to fsync local folder: %s: %w", dir, err)
		}
	}

	return nil
}

// sortPathsByDepthDescending sorts paths by depth in descending order.
// Paths with the same depth may be sorted in any order.
// For example:
//
//	/a/b
//	/a/b/c
//	/a/b/d
//	/a
//
// may be sorted to:
//
//	/a/b/d
//	/a/b/c
//	/a/b
//	/a
func sortPathsByDepthDescending(paths []string) {
	sort.Slice(paths, func(i, j int) bool {
		return depth(paths[i]) > depth(paths[j])
	})
}

func depth(path string) int {
	return strings.Count(filepath.Clean(path), string(filepath.Separator))
}

// AddAsyncReplicationTargetNode adds a target node override for a shard.
func (c *Copier) AddAsyncReplicationTargetNode(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride, schemaVersion uint64) error {
	if targetNodeOverride.SourceNode == c.nodeName {
		index := c.dbWrapper.GetIndex(schema.ClassName(targetNodeOverride.CollectionID))
		if index == nil {
			return nil
		}
		return index.IncomingAddAsyncReplicationTargetNode(ctx, targetNodeOverride.ShardID, targetNodeOverride)
	}

	srcNodeHostname, ok := c.nodeSelector.NodeHostname(targetNodeOverride.SourceNode)
	if !ok {
		return fmt.Errorf("source node address not found in cluster membership for node %s", targetNodeOverride.SourceNode)
	}

	return c.remoteIndex.AddAsyncReplicationTargetNode(ctx, srcNodeHostname, targetNodeOverride.CollectionID, targetNodeOverride.ShardID, targetNodeOverride, schemaVersion)
}

// RemoveAsyncReplicationTargetNode removes a target node override for a shard.
func (c *Copier) RemoveAsyncReplicationTargetNode(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error {
	if targetNodeOverride.SourceNode == c.nodeName {
		index := c.dbWrapper.GetIndex(schema.ClassName(targetNodeOverride.CollectionID))
		if index == nil {
			return nil
		}
		return index.IncomingRemoveAsyncReplicationTargetNode(ctx, targetNodeOverride.ShardID, targetNodeOverride)
	}

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

	return shard.SetAsyncReplicationEnabled(ctx, true)
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

	return shard.SetAsyncReplicationEnabled(ctx, shard.Index().Config.AsyncReplicationEnabled)
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
