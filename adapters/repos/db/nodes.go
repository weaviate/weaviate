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
	// nodes join and leave while this runs, so the same read has to size the
	// slice and drive the loop
	nodeNames := db.schemaGetter.Nodes()
	nodeStatuses := make([]*models.NodeStatus, len(nodeNames))
	eg := enterrors.NewErrorGroupWrapper(db.logger)
	eg.SetLimit(_NUMCPU)
	for i, nodeName := range nodeNames {
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
		status, err := db.LocalNodeStatus(ctx, className, shardName, output)
		if errors.Is(err, context.DeadlineExceeded) {
			// a local scan that ran out of time times out this node alone,
			// instead of failing the status of every node
			return timedOutNodeStatus(nodeName), nil
		}
		return status, err
	}
	status, err := db.remoteNode.GetNodeStatus(ctx, nodeName, className, shardName, output)
	if err != nil {
		// the reported status carries no reason, so the cause is only visible here
		db.logger.Warnf("node %q did not report its status: %v", nodeName, err)

		// errors.As needs a value target: the client returns these errors by value
		var errSendHttpRequest enterrors.ErrSendHttpRequest
		switch {
		case errors.As(err, &errSendHttpRequest):
			if errors.Is(errSendHttpRequest.Unwrap(), context.DeadlineExceeded) {
				return timedOutNodeStatus(nodeName), nil
			}

			return unavailableNodeStatus(nodeName), nil
		case errors.As(err, &enterrors.ErrOpenHttpRequest{}):
			return unavailableNodeStatus(nodeName), nil
		case errors.As(err, &enterrors.ErrUnexpectedStatusCode{}):
			// a node that answers with an error, e.g. because it is shutting down,
			// is reported as unavailable instead of failing the status of every node
			return unavailableNodeStatus(nodeName), nil
		default:
			return nil, err
		}
	}
	return status, nil
}

func timedOutNodeStatus(nodeName string) *models.NodeStatus {
	timeout := models.NodeStatusStatusTIMEOUT
	return &models.NodeStatus{Name: nodeName, Status: &timeout}
}

func unavailableNodeStatus(nodeName string) *models.NodeStatus {
	unavailable := models.NodeStatusStatusUNAVAILABLE
	return &models.NodeStatus{Name: nodeName, Status: &unavailable}
}

// IncomingGetNodeStatus returns the index if it exists or nil if it doesn't.
// A scan that ran out of time surfaces as an error: the node that asked has
// timed out on its own request by then and reports this node as timed out.
func (db *DB) IncomingGetNodeStatus(ctx context.Context, className, shardName, verbosity string) (*models.NodeStatus, error) {
	return db.LocalNodeStatus(ctx, className, shardName, verbosity)
}

func (db *DB) LocalNodeStatus(ctx context.Context, className, shardName, output string) (*models.NodeStatus, error) {
	if className != "" && db.GetIndex(schema.ClassName(className)) == nil {
		// class not found
		return &models.NodeStatus{}, nil
	}

	var (
		shards    []*models.NodeShardStatus
		nodeStats *models.NodeStats
	)
	if output == verbosity.OutputVerbose {
		var err error
		nodeStats, err = db.localNodeShardStats(ctx, &shards, className, shardName)
		if err != nil {
			return nil, err
		}
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

	return &status, nil
}

func (db *DB) localNodeShardStats(ctx context.Context,
	status *[]*models.NodeShardStatus, className, shardName string,
) (*models.NodeStats, error) {
	var objectCount, shardCount int64
	if className == "" {
		// scanning every shard takes far too long to hold indexLock
		for name, idx := range db.copyIndices() {
			if idx == nil {
				db.logger.WithField("action", "local_node_status_for_all").
					Warningf("no resource found for index %q", name)
				continue
			}
			objects, shards, err := scanIndexShards(ctx, idx, status, shardName)
			if err != nil {
				return nil, err
			}
			objectCount, shardCount = objectCount+objects, shardCount+shards
		}
		return &models.NodeStats{
			ObjectCount: objectCount,
			ShardCount:  shardCount,
		}, nil
	}

	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		db.logger.WithField("action", "local_node_status_for_class").
			Warningf("no index found for class %q", className)
		return nil, nil
	}
	objectCount, shardCount, err := scanIndexShards(ctx, idx, status, shardName)
	if err != nil {
		return nil, err
	}
	return &models.NodeStats{
		ObjectCount: objectCount,
		ShardCount:  shardCount,
	}, nil
}

// errIndexClosed stands in for a close whose cause was never signalled, which
// drop() leaves behind.
var errIndexClosed = errors.New("collection is closed")

// scanIndexShards appends the shard statuses of one index to status. A closed
// index and an unfinished scan contribute nothing, but only a collection being
// deleted may be left out without an error: anything else would report a live
// collection as empty.
func scanIndexShards(ctx context.Context, idx *Index,
	status *[]*models.NodeShardStatus, shardName string,
) (objectCount, shardCount int64, err error) {
	idx.dropIndex.RLock()
	defer idx.dropIndex.RUnlock()

	idx.closeLock.RLock()
	defer idx.closeLock.RUnlock()

	var shards []*models.NodeShardStatus
	if idx.closed {
		err = context.Cause(idx.closeRequestedCtx)
		if err == nil {
			err = errIndexClosed
		}
	} else {
		scanCtx, done := idx.cancelOnCloseRequested(ctx)
		defer done()

		objectCount, shardCount, err = idx.getShardsNodeStatus(scanCtx, &shards, shardName)
	}
	if err != nil {
		// the collection is named only here: the error reaches callers that this
		// endpoint does not check a read permission against
		idx.logger.Warnf("node status scan of collection %q stopped: %v", idx.Config.ClassName, err)
		if errors.Is(err, errIndexDropped) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	*status = append(*status, shards...)
	return objectCount, shardCount, nil
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

// getShardsNodeStatus modifies the status slice to include the shard statuses.
// If shardName is provided, it will only get the status of the specific shard.
// Otherwise, it will get the status of all shards.
// Returns the total object count and the number of shards.
// If an error is returned, the counts and the status slice hold a partial result.
func (i *Index) getShardsNodeStatus(ctx context.Context,
	status *[]*models.NodeShardStatus, shardName string,
) (totalCount, shardCount int64, err error) {
	if ctx.Err() != nil {
		return 0, 0, context.Cause(ctx)
	}

	shards := map[string]ShardLike{}
	if err = i.ForEachShard(func(name string, shard ShardLike) error {
		if shardName == "" || shardName == name {
			shards[name] = shard
		}
		return nil
	}); err != nil {
		return 0, 0, err
	}

	// the state is read once the shard list is fixed, so a shard created while
	// the scan runs is left out instead of reported with a guessed replica count
	className := i.Config.ClassName.String()
	replicationFactor, replicaCounts := i.readReplicationDetails()
	replicaCountOf := func(name string) int64 {
		if count, ok := replicaCounts[name]; ok {
			return count
		}
		// a shard the read did not cover holds as many replicas as the collection asks for
		return replicationFactor
	}

	for name, shard := range shards {
		if ctx.Err() != nil {
			return totalCount, shardCount, context.Cause(ctx)
		}

		// Don't force load a lazy shard to get nodes status
		if lazy, ok := shard.(*LazyLoadShard); ok && !lazy.isLoaded() {
			shardStatus := &models.NodeShardStatus{
				Name:                 name,
				Class:                className,
				VectorIndexingStatus: shard.GetStatus().String(),
				Loaded:               false,
				ReplicationFactor:    replicationFactor,
				NumberOfReplicas:     replicaCountOf(name),
				// don't add compression status as this would trigger loading the shard
			}
			*status = append(*status, shardStatus)
			shardCount++
			continue
		}

		objectCount, err := shard.ObjectCountAsync(ctx)
		if err != nil {
			i.logger.Warnf("error while getting object count for shard %s: %v", shard.Name(), err)
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
			NumberOfReplicas:       replicaCountOf(name),
		}
		*status = append(*status, shardStatus)
		shardCount++
	}
	return totalCount, shardCount, nil
}

// readReplicationDetails reads the sharding state once for the whole collection,
// so that scanning it cannot cost a schema read per shard. The map covers only
// the shards the schema holds when the read runs. A collection the schema does
// not hold returns a zero factor and a nil map.
//
// A collection whose local index outlives its schema entry has been deleted, and
// does not come back by waiting, so the read does not retry.
func (i *Index) readReplicationDetails() (replicationFactor int64, replicaCounts map[string]int64) {
	class := i.Config.ClassName.String()
	err := i.schemaReader.Read(class, false, func(_ *models.Class, state *sharding.State) error {
		replicationFactor = state.ReplicationFactor
		replicaCounts = make(map[string]int64, len(state.Physical))
		for shardName, physical := range state.Physical {
			replicaCounts[shardName] = int64(len(physical.BelongsToNodes))
		}
		return nil
	})
	if err != nil {
		i.logger.Errorf("error while getting replication details of collection %s: %v", class, err)
	}
	return replicationFactor, replicaCounts
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
	// nodes join and leave while this runs, so the same read has to size the
	// slice and drive the loop
	nodeNames := db.schemaGetter.Nodes()
	nodeStatistics := make([]*models.Statistics, len(nodeNames))
	eg := enterrors.NewErrorGroupWrapper(db.logger)
	eg.SetLimit(_NUMCPU)
	for i, nodeName := range nodeNames {
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
		// the reported status carries no reason, so the cause is only visible here
		db.logger.Warnf("node %q did not report its statistics: %v", nodeName, err)

		// errors.As needs a value target: the client returns these errors by value
		var errSendHttpRequest enterrors.ErrSendHttpRequest
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
