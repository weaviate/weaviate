//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package copier

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/integrity"
)

var _NUMCPU = runtime.GOMAXPROCS(0)

type FileReplicationServiceClient protocol.FileReplicationServiceClient

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
	return c.CopyReplicaFilesToLocalShard(ctx, srcNodeId, collectionName, shardName, "", schemaVersion)
}

// CopyReplicaFilesToLocalShard is like CopyReplicaFiles but lands files
// in a different local shard directory when localShardOverride is set
// (used by SELF_RECOVERY to write into "<shard>.recovering/"; the caller
// then renames via PromoteRecoveryFolder). CRC32 per-file resume still
// works across crashes since it operates on the local destination paths.
func (c *Copier) CopyReplicaFilesToLocalShard(ctx context.Context, srcNodeId, collectionName, shardName, localShardOverride string, schemaVersion uint64) error {
	sourceNodeAddress := c.nodeSelector.NodeAddress(srcNodeId)

	sourceNodeGRPCPort, err := c.nodeSelector.NodeGRPCPort(srcNodeId)
	if err != nil {
		return fmt.Errorf("failed to get gRPC port for source node: %w", err)
	}

	client, err := c.clientFactory(ctx, net.JoinHostPort(sourceNodeAddress, fmt.Sprintf("%d", sourceNodeGRPCPort)))
	if err != nil {
		return fmt.Errorf("failed to create gRPC client connection: %w", err)
	}

	_, err = client.PauseFileActivity(ctx, &protocol.PauseFileActivityRequest{
		IndexName:     collectionName,
		ShardName:     shardName,
		SchemaVersion: schemaVersion,
	})
	if err != nil {
		return fmt.Errorf("failed to pause file activity: %w", err)
	}
	defer client.ResumeFileActivity(ctx, &protocol.ResumeFileActivityRequest{
		IndexName: collectionName,
		ShardName: shardName,
	})

	fileListResp, err := client.ListFiles(ctx, &protocol.ListFilesRequest{
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

	localShard := c.localShardName(shardName, localShardOverride)
	err = c.prepareLocalFolder(collectionName, shardName, localShard, fileListResp.FileNames)
	if err != nil {
		return fmt.Errorf("failed to prepare local folder: %w", err)
	}

	metadataChan := make(chan *protocol.FileMetadata, 1000)
	mWg := enterrors.NewErrorGroupWrapper(c.logger)
	for range c.concurrentWorkers {
		mWg.Go(func() error {
			err := c.metadataWorker(ctx, client, collectionName, shardName, fileNameChan, metadataChan)
			if err != nil {
				c.logger.WithError(err).Error("failed to get files metadata")
			}
			return err
		})
	}

	dWg := enterrors.NewErrorGroupWrapper(c.logger)
	for range c.concurrentWorkers {
		dWg.Go(func() error {
			err := c.downloadWorker(ctx, client, metadataChan, shardName, localShard)
			if err != nil {
				c.logger.WithError(err).Error("failed to download files")
			}
			return err
		})
	}

	// wait for all metadata workers to finish
	if err := mWg.Wait(); err != nil {
		return fmt.Errorf("failed to get files metadata: %w", err)
	}
	close(metadataChan)

	// wait for all download workers to finish
	if err := dWg.Wait(); err != nil {
		return fmt.Errorf("failed to download files: %w", err)
	}

	err = c.validateLocalFolder(collectionName, shardName, localShard, fileListResp.FileNames)
	if err != nil {
		return fmt.Errorf("failed to validate local folder: %w", err)
	}

	return nil
}

func (c *Copier) localShardName(srcShard, override string) string {
	if override == "" {
		return srcShard
	}
	return override
}

// rewriteRelPathToLocalShard rewrites the shard segment of a
// source-relative path (e.g. "coll/shard/.../file") to the local
// destination shard. No-op when srcShard == localShard. Wire-protocol
// paths from the file-replication gRPC service are slash-separated
// regardless of the source's OS, so split/join on '/' explicitly
// rather than filepath.Separator. The caller re-joins with
// filepath.Join when materialising the result on the local FS.
func (c *Copier) rewriteRelPathToLocalShard(srcRelPath, srcShard, localShard string) string {
	if localShard == srcShard {
		return srcRelPath
	}
	parts := strings.SplitN(srcRelPath, "/", 3)
	if len(parts) >= 2 && parts[1] == srcShard {
		parts[1] = localShard
		return strings.Join(parts, "/")
	}
	return srcRelPath
}

func (c *Copier) shardPath(collectionName, shardName string) string {
	return path.Join(c.rootDataPath, strings.ToLower(collectionName), shardName)
}

func (c *Copier) prepareLocalFolder(collectionName, shardName, localShard string, fileNames []string) error {
	// Keyed by LOCAL relative path so we can compare against on-disk
	// files (which live under localShard, not srcShard, for SELF_RECOVERY).
	fileNamesMap := make(map[string]struct{}, len(fileNames))
	for _, fileName := range fileNames {
		fileNamesMap[c.rewriteRelPathToLocalShard(fileName, shardName, localShard)] = struct{}{}
	}

	var dirs []string

	// remove files in the local destination not in the source manifest.
	basePath := c.shardPath(collectionName, localShard)

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
	collectionName, shardName string, fileNameChan <-chan string, metadataChan chan<- *protocol.FileMetadata,
) error {
	for fileName := range fileNameChan {
		meta, err := client.GetFileMetadata(ctx, &protocol.GetFileMetadataRequest{
			IndexName: collectionName,
			ShardName: shardName,
			FileName:  fileName,
		})
		if err != nil {
			return fmt.Errorf("failed to send GetFileMetadata request for %q: %w", fileName, err)
		}

		metadataChan <- meta
	}

	return nil
}

func (c *Copier) downloadWorker(ctx context.Context, client FileReplicationServiceClient,
	metadataChan <-chan *protocol.FileMetadata, srcShard, localShard string,
) error {
	for meta := range metadataChan {
		// Rewrite the shard segment so SELF_RECOVERY writes land in
		// "<shard>.recovering/" rather than the live "<shard>/".
		localFilePath := filepath.Join(c.rootDataPath, c.rewriteRelPathToLocalShard(meta.FileName, srcShard, localShard))

		_, checksum, err := integrity.CRC32(localFilePath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if checksum == meta.Crc32 {
			// local file matches remote one, no need to download it
			continue
		}

		stream, err := client.GetFile(ctx, &protocol.GetFileRequest{
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

		tmpPath := localFilePath + ".tmp"

		if err := func() error {
			f, err := os.Create(tmpPath)
			if err != nil {
				return fmt.Errorf("open file %q for writing: %w", tmpPath, err)
			}
			defer func() {
				if f != nil {
					f.Close()
				}
			}()
			if err := f.Truncate(meta.Size); err != nil {
				return fmt.Errorf("failed to preallocate file: %w", err)
			}

			eg := enterrors.NewErrorGroupWrapper(c.logger)
			eg.SetLimit(_NUMCPU)
			for {
				chunk, err := stream.Recv()
				if err != nil {
					return fmt.Errorf("failed to receive file chunk for %s: %w", meta.FileName, err)
				}
				if len(chunk.Data) > 0 {
					eg.Go(func() error {
						if _, err := f.WriteAt(chunk.Data, chunk.Offset); err != nil {
							return fmt.Errorf("writing chunk to file %q: %w", tmpPath, err)
						}
						return nil
					})
				}
				if chunk.Eof {
					break
				}
			}

			if err = eg.Wait(); err != nil {
				return fmt.Errorf("writing chunks to file %q: %w", tmpPath, err)
			}

			err = f.Sync()
			if err != nil {
				return fmt.Errorf("fsyncing file %q for writing: %w", tmpPath, err)
			}

			err = f.Close()
			f = nil // prevent deferred close
			if err != nil {
				return fmt.Errorf("closing file: %w", err)
			}

			_, checksum, err = integrity.CRC32(tmpPath)
			if err != nil {
				return fmt.Errorf("calculating checksum for file %q: %w", tmpPath, err)
			}

			if checksum != meta.Crc32 {
				return fmt.Errorf("checksum validation of file %q failed, expected %d, got %d", tmpPath, meta.Crc32, checksum)
			}

			err = os.Rename(tmpPath, localFilePath)
			if err != nil {
				return fmt.Errorf("renaming temporary file %q to final path %q: %w", tmpPath, localFilePath, err)
			}

			return nil
		}(); err != nil {
			rerr := os.Remove(tmpPath)
			if rerr != nil {
				c.logger.Warnf("failed to remove temporary file %q after error", tmpPath)
			}
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

// PromoteRecoveryFolder atomically renames "<shard>.recovering/" into
// "<shard>/" after a SELF_RECOVERY copy completes. Idempotent across
// crashes: if the live dir already exists the recovery dir (if any) is
// erased and the call is a no-op; if both dirs are missing it errors.
// On rename/fsync error the recovery dir is left in place so the next
// attempt resumes via CRC32 per-file logic.
func (c *Copier) PromoteRecoveryFolder(collectionName, shardName string) error {
	recoveryPath := c.shardPath(collectionName, api.RecoveryFolderName(shardName))
	livePath := c.shardPath(collectionName, shardName)

	liveExists, err := dirExists(livePath)
	if err != nil {
		return fmt.Errorf("promote recovery folder: stat %q: %w", livePath, err)
	}
	recoveryExists, err := dirExists(recoveryPath)
	if err != nil {
		return fmt.Errorf("promote recovery folder: stat %q: %w", recoveryPath, err)
	}

	switch {
	case liveExists && recoveryExists:
		// Stale recovery dir from a crash before cleanup; live is canonical.
		if err := os.RemoveAll(recoveryPath); err != nil {
			return fmt.Errorf("promote recovery folder: live dir already exists, but failed to remove stale %q: %w", recoveryPath, err)
		}
		return nil
	case liveExists:
		// Rename succeeded on a previous attempt and the caller crashed
		// before observing READY; nothing to do.
		return nil
	case !recoveryExists:
		return fmt.Errorf("promote recovery folder: neither %q nor %q exists", recoveryPath, livePath)
	}

	if err := os.Rename(recoveryPath, livePath); err != nil {
		return fmt.Errorf("promote recovery folder: rename %q -> %q: %w", recoveryPath, livePath, err)
	}

	// fsync parent so the rename is durable across a crash (POSIX).
	parent := filepath.Dir(livePath)
	if err := diskio.Fsync(parent); err != nil {
		return fmt.Errorf("promote recovery folder: fsync parent %q: %w", parent, err)
	}
	return nil
}

// dirExists is (true, nil) when path is a directory, (false, nil) on
// ENOENT, and (false, err) for any other stat failure or when path
// exists but is not a directory (caller should surface the error
// rather than silently treat a stray file as a present shard dir).
func dirExists(path string) (bool, error) {
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if !fi.IsDir() {
		return false, fmt.Errorf("expected directory at %q, found %s", path, fi.Mode().String())
	}
	return true, nil
}

func (c *Copier) validateLocalFolder(collectionName, shardName, localShard string, fileNames []string) error {
	// Keyed by LOCAL relative path; see prepareLocalFolder.
	fileNamesMap := make(map[string]struct{}, len(fileNames))
	for _, fileName := range fileNames {
		fileNamesMap[c.rewriteRelPathToLocalShard(fileName, shardName, localShard)] = struct{}{}
	}

	var dirs []string

	basePath := c.shardPath(collectionName, localShard)

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
	return index.InitAsyncReplicationOnShard(ctx, shardName)
}

func (c *Copier) RevertAsyncReplicationLocally(ctx context.Context, collectionName, shardName string) error {
	index := c.dbWrapper.GetIndex(schema.ClassName(collectionName))
	if index == nil {
		return fmt.Errorf("index for collection %s not found", collectionName)
	}
	return index.RevertAsyncReplicationOnShard(ctx, shardName)
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
