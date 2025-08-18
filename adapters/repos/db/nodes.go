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
		nodeStats = db.localNodeShardStats(ctx, &shards, className, shardName)
	}

	clusterHealthStatus := models.NodeStatusStatusHEALTHY
	if db.schemaGetter.ClusterHealthScore() > 0 {
		clusterHealthStatus = models.NodeStatusStatusUNHEALTHY
	}

	status := models.NodeStatus{
		Name:       db.schemaGetter.NodeName(),
		Version:    db.config.ServerVersion,
		GitHash:    db.config.GitHash,
		Status:     &clusterHealthStatus,
		Shards:     shards,
		Stats:      nodeStats,
		BatchStats: db.localNodeBatchStats(),
	}

	return &status
}

func (db *DB) localNodeShardStats(ctx context.Context,
	status *[]*models.NodeShardStatus, className, shardName string,
) *models.NodeStats {
	var objectCount, shardCount int64
	if className == "" {
		db.indexLock.RLock()
		defer db.indexLock.RUnlock()
		for name, idx := range db.indices {
			if idx == nil {
				db.logger.WithField("action", "local_node_status_for_all").
					Warningf("no resource found for index %q", name)
				continue
			}
			objects, shards := idx.getShardsNodeStatus(ctx, status, shardName)
			objectCount, shardCount = objectCount+objects, shardCount+shards
		}
		return &models.NodeStats{
			ObjectCount: objectCount,
			ShardCount:  shardCount,
		}
	}
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		db.logger.WithField("action", "local_node_status_for_class").
			Warningf("no index found for class %q", className)
		return nil
	}
	objectCount, shardCount = idx.getShardsNodeStatus(ctx, status, shardName)
	return &models.NodeStats{
		ObjectCount: objectCount,
		ShardCount:  shardCount,
	}
}

func (db *DB) localNodeBatchStats() *models.BatchStats {
	rate := db.ratePerSecond.Load()
	stats := &models.BatchStats{RatePerSecond: rate}
	if !asyncEnabled() {
		ql := int64(len(db.jobQueueCh))
		stats.QueueLength = &ql
	}
	return stats
}

// getShardsNodeStatus modifies the status slice to include the shard statuses.
// If shardName is provided, it will only get the status of the specific shard.
// Otherwise, it will get the status of all shards.
// Returns the total object count and the number of shards.
// If an error occurs, the status slice may have been modified and this method
// may return a partial result.
func (i *Index) getShardsNodeStatus(ctx context.Context,
	status *[]*models.NodeShardStatus, shardName string,
) (totalCount, shardCount int64) {
	i.ForEachShard(func(name string, shard ShardLike) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// if shardName is provided, only return the status for the specified shard
		if shardName != "" && shardName != name {
			return nil
		}

		// Don't force load a lazy shard to get nodes status
		if lazy, ok := shard.(*LazyLoadShard); ok {
			if !lazy.isLoaded() {
				class := shard.Index().Config.ClassName.String()
				shardingState := shard.Index().getSchema.CopyShardingState(class)
				numberOfReplicas, err := shardingState.NumberOfReplicas(shard.Name())
				if err != nil {
					i.logger.Errorf("error while getting number of replicas for shard %s: %w", shard.Name(), err)
				}
				shardStatus := &models.NodeShardStatus{
					Name:                 name,
					Class:                class,
					VectorIndexingStatus: shard.GetStatus().String(),
					Loaded:               false,
					ReplicationFactor:    shardingState.ReplicationFactor,
					NumberOfReplicas:     numberOfReplicas,
				}
				*status = append(*status, shardStatus)
				shardCount++
				return nil
			}
		}

		objectCount := int64(shard.ObjectCountAsync())
		totalCount += objectCount

		// FIXME stats of target vectors
		var queueLen int64
		_ = shard.ForEachVectorQueue(func(_ string, queue *VectorIndexQueue) error {
			queueLen += queue.Size()
			return nil
		})

		var compressed bool
		_ = shard.ForEachVectorIndex(func(_ string, index VectorIndex) error {
			compressed = compressed || index.Compressed()
			return nil
		})

		class := shard.Index().Config.ClassName.String()
		shardingState := shard.Index().getSchema.CopyShardingState(class)
		numberOfReplicas, err := shardingState.NumberOfReplicas(shard.Name())
		if err != nil {
			i.logger.Errorf("error while getting number of replicas for shard %s: %w", shard.Name(), err)
		}

		shardStatus := &models.NodeShardStatus{
			Name:                   name,
			Class:                  class,
			ObjectCount:            objectCount,
			VectorIndexingStatus:   shard.GetStatus().String(),
			VectorQueueLength:      queueLen,
			Compressed:             compressed,
			Loaded:                 true,
			AsyncReplicationStatus: shard.getAsyncReplicationStats(ctx),
			ReplicationFactor:      shardingState.ReplicationFactor,
			NumberOfReplicas:       numberOfReplicas,
		}
		*status = append(*status, shardStatus)
		shardCount++
		return nil
	})
	return
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
