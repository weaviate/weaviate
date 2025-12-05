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

package db

import (
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// GetNodeStatus returns the status of all Weaviate nodes.
func (db *DB) GetNodeStatus(ctx context.Context, className, shardName string, verbosity string) ([]*models.NodeStatus, error) {
	nodeStatuses := make([]*models.NodeStatus, len(db.schemaGetter.Nodes()))
	eg := enterrors.NewErrorGroupWrapper(db.logger)
	eg.SetLimit(_NUMCPU)
	for i, nodeName := range db.schemaGetter.Nodes() {
		i, nodeName := i, nodeName
		eg.Go(func() error {
			status, err := db.GetOneNodeStatus(ctx, nodeName, className, shardName, verbosity)
			if err != nil {
				return fmt.Errorf("node: %v: %w", nodeName, err)
			}
			if status.Status == nil {
				return enterrors.NewErrNotFound(
					fmt.Errorf("class %q not found", className))
			}
			nodeStatuses[i] = status

			return nil
		}, nodeName)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	sort.Slice(nodeStatuses, func(i, j int) bool {
		return nodeStatuses[i].Name < nodeStatuses[j].Name
	})
	return nodeStatuses, nil
}

func (db *DB) GetOneNodeStatus(ctx context.Context, nodeName, className, shardName, output string) (*models.NodeStatus, error) {
	if db.schemaGetter.NodeName() == nodeName {
		return db.LocalNodeStatus(ctx, className, shardName, output), nil
	}
	status, err := db.remoteNode.GetNodeStatus(ctx, nodeName, className, shardName, output)
	if err != nil {
		var errSendHttpRequest *enterrors.ErrSendHttpRequest
		switch {
		case errors.As(err, &errSendHttpRequest):
			if errors.Is(errSendHttpRequest.Unwrap(), context.DeadlineExceeded) {
				nodeTimeout := models.NodeStatusStatusTIMEOUT
				return &models.NodeStatus{Name: nodeName, Status: &nodeTimeout}, nil
			}

			nodeUnavailable := models.NodeStatusStatusUNAVAILABLE
			return &models.NodeStatus{Name: nodeName, Status: &nodeUnavailable}, nil
		case errors.As(err, &enterrors.ErrOpenHttpRequest{}):
			nodeUnavailable := models.NodeStatusStatusUNAVAILABLE
			return &models.NodeStatus{Name: nodeName, Status: &nodeUnavailable}, nil
		default:
			return nil, err
		}
	}
	return status, nil
}

// IncomingGetNodeStatus returns the index if it exists or nil if it doesn't
func (db *DB) IncomingGetNodeStatus(ctx context.Context, className, shardName, verbosity string) (*models.NodeStatus, error) {
	return db.LocalNodeStatus(ctx, className, shardName, verbosity), nil
}

func (db *DB) LocalNodeStatus(ctx context.Context, className, shardName, output string) *models.NodeStatus {
	if className != "" && db.GetIndex(schema.ClassName(className)) == nil {
		// class not found
		return &models.NodeStatus{}
	}

	var (
		shards    []*models.NodeShardStatus
		nodeStats *models.NodeStats
	)
	if output == verbosity.OutputVerbose {
		nodeStats, shards = db.localNodeShardStats(ctx, className, shardName)
	}

	clusterHealthStatus := models.NodeStatusStatusHEALTHY
	if db.schemaGetter.ClusterHealthScore() > 0 {
		clusterHealthStatus = models.NodeStatusStatusUNHEALTHY
	}

	status := models.NodeStatus{
		Name:            db.schemaGetter.NodeName(),
		Version:         db.config.ServerVersion,
		GitHash:         db.config.GitHash,
		Status:          &clusterHealthStatus,
		Shards:          shards,
		Stats:           nodeStats,
		BatchStats:      db.localNodeBatchStats(),
		OperationalMode: db.config.OperationalMode.Get(),
	}

	return &status
}

func (db *DB) localNodeShardStats(ctx context.Context,
	className, shardName string,
) (*models.NodeStats, []*models.NodeShardStatus) {
	var objectCount, shardCount int64
	if className == "" {
		// Snapshot the indices rather than holding the index lock while collecting:
		// collection does I/O per shard and would otherwise block every index
		// creation and deletion for its entire duration. dropIndex.RLock is taken
		// under indexLock so an index cannot be dropped once it is in the snapshot.
		db.indexLock.RLock()
		indices := make([]*Index, 0, len(db.indices))
		for name, idx := range db.indices {
			if idx == nil {
				db.logger.WithField("action", "local_node_status_for_all").
					Warningf("no resource found for index %q", name)
				continue
			}
			idx.dropIndex.RLock()
			indices = append(indices, idx)
		}
		db.indexLock.RUnlock()
		defer func() {
			for _, idx := range indices {
				idx.dropIndex.RUnlock()
			}
		}()

		type indexStats struct {
			objects, shards int64
			status          []*models.NodeShardStatus
		}
		results := make([]indexStats, len(indices))

		eg := enterrors.NewErrorGroupWrapper(db.logger)
		eg.SetLimit(_NUMCPU)
		for i, idx := range indices {
			eg.Go(func() error {
				objects, shards, status := idx.getShardsNodeStatus(ctx, shardName)
				results[i] = indexStats{objects: objects, shards: shards, status: status}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			db.logger.WithField("action", "local_node_status_for_all").Error(err)
		}

		var status []*models.NodeShardStatus
		for _, res := range results {
			objectCount, shardCount = objectCount+res.objects, shardCount+res.shards
			status = append(status, res.status...)
		}
		return &models.NodeStats{
			ObjectCount: objectCount,
			ShardCount:  shardCount,
		}, status
	}
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		db.logger.WithField("action", "local_node_status_for_class").
			Warningf("no index found for class %q", className)
		return nil, nil
	}
	objectCount, shardCount, status := idx.getShardsNodeStatus(ctx, shardName)
	return &models.NodeStats{
		ObjectCount: objectCount,
		ShardCount:  shardCount,
	}, status
}

func (db *DB) localNodeBatchStats() *models.BatchStats {
	rate := db.ratePerSecond.Load()
	stats := &models.BatchStats{RatePerSecond: rate}
	if !db.AsyncIndexingEnabled {

		ql := int64(len(db.jobQueueCh))
		stats.QueueLength = &ql
	}
	return stats
}

// getShardsNodeStatus returns the status of the index's shards, along with the
// total object count and the number of shards.
// If shardName is provided, it will only get the status of the specific shard.
// Otherwise, it will get the status of all shards.
// If an error occurs, this method may return a partial result.
func (i *Index) getShardsNodeStatus(ctx context.Context, shardName string,
) (totalCount, shardCount int64, status []*models.NodeShardStatus) {
	replicationFactor, replicasPerShard := i.getShardsReplicationDetails(shardName)

	i.ForEachShard(func(name string, shard ShardLike) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// if shardName is provided, only return the status for the specified shard
		if shardName != "" && shardName != name {
			return nil
		}

		// Don't force load a lazy shard to get nodes status
		className := i.Config.ClassName.String()
		if lazy, ok := shard.(*LazyLoadShard); ok {
			if !lazy.isLoaded() {
				shardStatus := &models.NodeShardStatus{
					Name:                 name,
					Class:                className,
					VectorIndexingStatus: shard.GetStatus().String(),
					Loaded:               false,
					ReplicationFactor:    replicationFactor,
					NumberOfReplicas:     replicasPerShard[name],
					// don't add compression status as this would trigger loading the shard
				}
				status = append(status, shardStatus)
				shardCount++
				return nil
			}
		}

		objectCount, err := shard.ObjectCountAsync(ctx)
		if err != nil {
			i.logger.Warnf("error while getting object count for shard %s: %w", shard.Name(), err)
		}

		totalCount += int64(objectCount)

		// FIXME stats of target vectors
		var queueLen int64
		_ = shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
			queueLen += queue.Size()
			return nil
		})
		_ = shard.ForEachGeoQueue(func(_ string, queue *VectorIndexQueue) error {
			queueLen += queue.Size()
			return nil
		})

		var compressed bool
		_ = shard.ForEachVectorIndex(func(_ string, index VectorIndex) error {
			compressed = compressed || index.Compressed()
			return nil
		})

		shardStatus := &models.NodeShardStatus{
			Name:                   name,
			Class:                  className,
			ObjectCount:            objectCount,
			VectorIndexingStatus:   shard.GetStatus().String(),
			VectorQueueLength:      queueLen,
			Compressed:             isAnyVectorIndexCompressed(shard),
			Loaded:                 true,
			AsyncReplicationStatus: shard.getAsyncReplicationStats(ctx),
			ReplicationFactor:      replicationFactor,
			NumberOfReplicas:       replicasPerShard[name],
		}
		status = append(status, shardStatus)
		shardCount++
		return nil
	})
	return totalCount, shardCount, status
}

// getShardsReplicationDetails resolves the replication factor and the number of
// replicas per shard in a single schema read. All shards of an index share the
// same sharding state, so reading it per shard only adds contention on the
// schema lock. If shardName is set, only that shard is resolved.
func (i *Index) getShardsReplicationDetails(shardName string) (int64, map[string]int64) {
	var replicationFactor int64
	replicasPerShard := map[string]int64{}
	class := i.Config.ClassName.String()
	err := i.schemaReader.Read(class, true, func(class *models.Class, state *sharding.State) error {
		replicationFactor = state.ReplicationFactor
		if shardName != "" {
			numberOfReplicas, err := state.NumberOfReplicas(shardName)
			if err != nil {
				return fmt.Errorf("unable to retrieve number of replicas for class %s: %w", class.Class, err)
			}
			replicasPerShard[shardName] = numberOfReplicas
			return nil
		}
		for name, physical := range state.Physical {
			replicasPerShard[name] = int64(len(physical.BelongsToNodes))
		}
		return nil
	})
	if err != nil {
		i.logger.Errorf("error while getting replication details for class %s: %v", class, err)
	}
	return replicationFactor, replicasPerShard
}

func isAnyVectorIndexCompressed(shard ShardLike) bool {
	var compressed bool
	shard.ForEachVectorIndex(func(_ string, index VectorIndex) error {
		compressed = compressed || index.Compressed()
		return nil
	})
	return compressed
}

func (db *DB) GetNodeStatistics(ctx context.Context) ([]*models.Statistics, error) {
	nodeStatistics := make([]*models.Statistics, len(db.schemaGetter.Nodes()))
	eg := enterrors.NewErrorGroupWrapper(db.logger)
	eg.SetLimit(_NUMCPU)
	for i, nodeName := range db.schemaGetter.Nodes() {
		i, nodeName := i, nodeName
		eg.Go(func() error {
			statistics, err := db.getNodeStatistics(ctx, nodeName)
			if err != nil {
				return fmt.Errorf("node: %v: %w", nodeName, err)
			}
			nodeStatistics[i] = statistics

			return nil
		}, nodeName)
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	sort.Slice(nodeStatistics, func(i, j int) bool {
		return nodeStatistics[i].Name < nodeStatistics[j].Name
	})
	return nodeStatistics, nil
}

func (db *DB) IncomingGetNodeStatistics() (*models.Statistics, error) {
	return db.localNodeStatistics()
}

func (db *DB) localNodeStatistics() (*models.Statistics, error) {
	stats := db.schemaGetter.Statistics()
	var raft *models.RaftStatistics
	raftStats, ok := stats["raft"].(map[string]string)
	if ok {
		raft = &models.RaftStatistics{
			AppliedIndex:             raftStats["applied_index"],
			CommitIndex:              raftStats["commit_index"],
			FsmPending:               raftStats["fsm_pending"],
			LastContact:              raftStats["last_contact"],
			LastLogIndex:             raftStats["last_log_index"],
			LastLogTerm:              raftStats["last_log_term"],
			LastSnapshotIndex:        raftStats["last_snapshot_index"],
			LastSnapshotTerm:         raftStats["last_snapshot_term"],
			LatestConfiguration:      stats["raft_latest_configuration_servers"],
			LatestConfigurationIndex: raftStats["latest_configuration_index"],
			NumPeers:                 raftStats["num_peers"],
			ProtocolVersion:          raftStats["protocol_version"],
			ProtocolVersionMax:       raftStats["protocol_version_max"],
			ProtocolVersionMin:       raftStats["protocol_version_min"],
			SnapshotVersionMax:       raftStats["snapshot_version_max"],
			SnapshotVersionMin:       raftStats["snapshot_version_min"],
			State:                    raftStats["state"],
			Term:                     raftStats["term"],
		}
	}
	status := models.StatisticsStatusHEALTHY
	if db.schemaGetter.ClusterHealthScore() > 0 {
		status = models.StatisticsStatusUNHEALTHY
	}
	statistics := &models.Statistics{
		Status:                  &status,
		Name:                    stats["id"].(string),
		LeaderAddress:           stats["leader_address"],
		LeaderID:                stats["leader_id"],
		Ready:                   stats["ready"].(bool),
		IsVoter:                 stats["is_voter"].(bool),
		Open:                    stats["open"].(bool),
		Bootstrapped:            stats["bootstrapped"].(bool),
		InitialLastAppliedIndex: stats["last_store_log_applied_index"].(uint64),
		DbLoaded:                stats["db_loaded"].(bool),
		Candidates:              stats["candidates"],
		Raft:                    raft,
	}
	return statistics, nil
}

func (db *DB) getNodeStatistics(ctx context.Context, nodeName string) (*models.Statistics, error) {
	if db.schemaGetter.NodeName() == nodeName {
		return db.localNodeStatistics()
	}
	statistics, err := db.remoteNode.GetStatistics(ctx, nodeName)
	if err != nil {
		var errSendHttpRequest *enterrors.ErrSendHttpRequest
		switch {
		case errors.As(err, &errSendHttpRequest):
			if errors.Is(errSendHttpRequest.Unwrap(), context.DeadlineExceeded) {
				nodeTimeout := models.StatisticsStatusTIMEOUT
				return &models.Statistics{Name: nodeName, Status: &nodeTimeout}, nil
			}

			nodeUnavailable := models.StatisticsStatusUNAVAILABLE
			return &models.Statistics{Name: nodeName, Status: &nodeUnavailable}, nil
		case errors.As(err, &enterrors.ErrOpenHttpRequest{}):
			nodeUnavailable := models.StatisticsStatusUNAVAILABLE
			return &models.Statistics{Name: nodeName, Status: &nodeUnavailable}, nil
		default:
			return nil, err
		}
	}
	return statistics, nil
}
