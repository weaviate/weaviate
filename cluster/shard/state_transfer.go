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

package shard

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/integrity"

	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
)

// ShardReinitializer reinitializes a shard from files on disk. This is called
// after downloading shard data from the leader during state transfer.
type ShardReinitializer interface {
	ReinitShard(ctx context.Context, className, shardName string) error
}

// StateTransfer downloads shard data from the current leader when a follower
// needs a full state transfer (e.g. after falling too far behind and having
// its RAFT log entries truncated).
type StateTransfer struct {
	RpcClientMaker rpcClientMaker
	Reinitializer  ShardReinitializer
	LeaderFunc     func(className, shardName string) string // returns leader node ID
	RootDataPath   string
	Log            logrus.FieldLogger
}

// TransferState downloads all shard data from the current leader and
// reinitializes the local shard. It is called from FSM.Restore() when a
// foreign snapshot is detected.
func (st *StateTransfer) TransferState(ctx context.Context, className, shardName string) error {
	log := st.Log.WithFields(logrus.Fields{
		"action": "state_transfer",
		"class":  className,
		"shard":  shardName,
	})

	// 1. Determine the current leader with retry/backoff.
	leaderNodeID, err := st.resolveLeaderNodeID(ctx, className, shardName, log)
	if err != nil {
		return fmt.Errorf("resolve leader: %w", err)
	}

	// 2. Create gRPC client to the leader (rpcClientMaker resolves nodeID → gRPC address).
	client, err := st.RpcClientMaker(ctx, leaderNodeID)
	if err != nil {
		return fmt.Errorf("create RPC client to leader %s: %w", leaderNodeID, err)
	}

	// 3. Create a transfer snapshot on the leader.
	snapResp, err := client.CreateTransferSnapshot(ctx, &shardproto.CreateTransferSnapshotRequest{
		Class: className,
		Shard: shardName,
	})
	if err != nil {
		return fmt.Errorf("create transfer snapshot: %w", err)
	}
	snapshotID := snapResp.SnapshotId

	log.WithFields(logrus.Fields{
		"snapshot_id": snapshotID,
		"file_count":  len(snapResp.Files),
	}).Info("transfer snapshot created on leader")

	// 4. Ensure snapshot is released on the leader when done (even on error).
	defer func() {
		_, releaseErr := client.ReleaseTransferSnapshot(ctx, &shardproto.ReleaseTransferSnapshotRequest{
			SnapshotId: snapshotID,
		})
		if releaseErr != nil {
			log.WithError(releaseErr).Warn("failed to release transfer snapshot on leader")
		}
	}()

	// 5. Download all files from the leader using a worker pool.
	if err := st.downloadFiles(ctx, client, snapshotID, snapResp.Files, log); err != nil {
		return fmt.Errorf("download files: %w", err)
	}

	// 6. Remove local files not present on the leader (stale/compacted).
	if err := st.cleanupLocalFiles(className, shardName, snapResp.Files); err != nil {
		return fmt.Errorf("cleanup stale files: %w", err)
	}

	log.Info("all files downloaded, reinitializing shard")

	// 7. Reinitialize the local shard from the downloaded files.
	if err := st.Reinitializer.ReinitShard(ctx, className, shardName); err != nil {
		return fmt.Errorf("reinit shard: %w", err)
	}

	log.Info("state transfer complete")
	return nil
}

// resolveLeaderNodeID determines the current leader's node ID with
// exponential backoff retry (the leader may not be elected yet).
func (st *StateTransfer) resolveLeaderNodeID(ctx context.Context, className, shardName string, log logrus.FieldLogger) (string, error) {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 2 * time.Second
	bo.MaxElapsedTime = 30 * time.Second
	bo.Reset()

	ctxBackoff := backoff.WithContext(bo, ctx)

	var leaderNodeID string
	err := backoff.Retry(func() error {
		nodeID := st.LeaderFunc(className, shardName)
		if nodeID == "" {
			log.Debug("no leader found, will retry")
			return fmt.Errorf("no leader found for %s/%s", className, shardName)
		}

		leaderNodeID = nodeID
		return nil
	}, ctxBackoff)
	if err != nil {
		return "", err
	}

	log.WithField("leader_node_id", leaderNodeID).Info("resolved leader for state transfer")
	return leaderNodeID, nil
}

// downloadFiles downloads all snapshot files from the leader using a pool of
// concurrent workers. Each file is written to a temp file, CRC32-validated,
// then renamed to its final path.
func (st *StateTransfer) downloadFiles(
	ctx context.Context,
	client shardproto.ShardReplicationServiceClient,
	snapshotID string,
	files []*shardproto.SnapshotFileInfo,
	log logrus.FieldLogger,
) error {
	fileChan := make(chan *shardproto.SnapshotFileInfo, len(files))
	for _, f := range files {
		fileChan <- f
	}
	close(fileChan)

	numWorkers := runtime.GOMAXPROCS(0)
	eg := enterrors.NewErrorGroupWrapper(log)
	eg.SetLimit(numWorkers)

	for range numWorkers {
		eg.Go(func() error {
			return st.downloadWorker(ctx, client, snapshotID, fileChan, log)
		})
	}

	return eg.Wait()
}

// downloadWorker processes files from the channel, downloading each from the
// leader via streaming RPC. Follows the same pattern as copier.go: write to
// temp file, CRC32 validate, rename to final path.
func (st *StateTransfer) downloadWorker(
	ctx context.Context,
	client shardproto.ShardReplicationServiceClient,
	snapshotID string,
	fileChan <-chan *shardproto.SnapshotFileInfo,
	log logrus.FieldLogger,
) error {
	for meta := range fileChan {
		if err := st.downloadFile(ctx, client, snapshotID, meta, log); err != nil {
			return err
		}
	}
	return nil
}

// downloadFile downloads a single file from the leader, validates its CRC32
// checksum, and moves it into place.
func (st *StateTransfer) downloadFile(
	ctx context.Context,
	client shardproto.ShardReplicationServiceClient,
	snapshotID string,
	meta *shardproto.SnapshotFileInfo,
	log logrus.FieldLogger,
) error {
	localFilePath := filepath.Join(st.RootDataPath, meta.Name)
	tmpPath := localFilePath + ".tmp"

	// Check if the local file already matches the leader's version.
	_, checksum, err := integrity.CRC32(localFilePath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if checksum == meta.Crc32 {
		log.WithField("file", meta.Name).Debug("file already up-to-date, skipping download")
		return nil
	}

	stream, err := client.GetSnapshotFile(ctx, &shardproto.GetSnapshotFileRequest{
		SnapshotId: snapshotID,
		FileName:   meta.Name,
	})
	if err != nil {
		return fmt.Errorf("get snapshot file %s: %w", meta.Name, err)
	}

	dir := path.Dir(localFilePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return fmt.Errorf("create parent dir for %s: %w", localFilePath, err)
	}

	if err := func() error {
		f, err := os.Create(tmpPath)
		if err != nil {
			return fmt.Errorf("create temp file %s: %w", tmpPath, err)
		}
		defer func() {
			if f != nil {
				f.Close()
			}
		}()

		// Pre-allocate the file to the expected size.
		if err := f.Truncate(meta.Size); err != nil {
			return fmt.Errorf("preallocate file %s: %w", tmpPath, err)
		}

		for {
			chunk, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("receive chunk for %s: %w", meta.Name, err)
			}

			if len(chunk.Data) > 0 {
				if _, err := f.WriteAt(chunk.Data, chunk.Offset); err != nil {
					return fmt.Errorf("write chunk to %s: %w", tmpPath, err)
				}
			}

			if chunk.Eof {
				break
			}
		}

		if err := f.Sync(); err != nil {
			return fmt.Errorf("fsync %s: %w", tmpPath, err)
		}

		if err := f.Close(); err != nil {
			f = nil
			return fmt.Errorf("close %s: %w", tmpPath, err)
		}
		f = nil // prevent deferred close

		// Validate CRC32.
		_, checksum, err := integrity.CRC32(tmpPath)
		if err != nil {
			return fmt.Errorf("compute CRC32 for %s: %w", tmpPath, err)
		}
		if checksum != meta.Crc32 {
			return fmt.Errorf("CRC32 mismatch for %s: expected %d, got %d", meta.Name, meta.Crc32, checksum)
		}

		// Atomically move into place.
		if err := os.Rename(tmpPath, localFilePath); err != nil {
			return fmt.Errorf("rename %s to %s: %w", tmpPath, localFilePath, err)
		}

		return nil
	}(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}

	log.WithFields(logrus.Fields{
		"file": meta.Name,
		"size": meta.Size,
	}).Debug("downloaded file")

	return nil
}

// cleanupLocalFiles removes local files that are not present in the leader's
// file list (e.g. files that were compacted away on the leader). The raft/
// subdirectory is excluded since RAFT state is managed by the RAFT library.
func (st *StateTransfer) cleanupLocalFiles(className, shardName string, remoteFiles []*shardproto.SnapshotFileInfo) error {
	remoteSet := make(map[string]struct{}, len(remoteFiles))
	for _, f := range remoteFiles {
		remoteSet[f.Name] = struct{}{}
	}

	basePath := filepath.Join(st.RootDataPath, strings.ToLower(className), shardName)

	var dirs []string

	err := filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return fmt.Errorf("walking local shard dir: %w", err)
		}

		// Skip the raft/ subdirectory entirely.
		if d.IsDir() && d.Name() == "raft" {
			return fs.SkipDir
		}

		if d.IsDir() {
			dirs = append(dirs, path)
			return nil
		}

		localRelPath, err := filepath.Rel(st.RootDataPath, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		if _, ok := remoteSet[localRelPath]; !ok {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("removing stale local file %q: %w", path, err)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("cleanup local files: %w", err)
	}

	// Remove empty directories deepest-first.
	sort.Slice(dirs, func(i, j int) bool {
		return strings.Count(dirs[i], string(filepath.Separator)) > strings.Count(dirs[j], string(filepath.Separator))
	})

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		if len(entries) == 0 {
			_ = os.Remove(dir)
		}
	}

	return nil
}
