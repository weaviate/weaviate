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

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
)

// GetNodeStatus returns the status of all Weaviate nodes.
func (db *DB) GetNodeStatus(ctx context.Context, className string, verbosity string) ([]*models.NodeStatus, error) {
	nodeStatuses := make([]*models.NodeStatus, len(db.schemaGetter.Nodes()))
	for i, nodeName := range db.schemaGetter.Nodes() {
		status, err := db.getNodeStatus(ctx, nodeName, className, verbosity)
		if err != nil {
			return nil, fmt.Errorf("node: %v: %w", nodeName, err)
		}
		if status.Status == nil {
			return nil, enterrors.NewErrNotFound(
				fmt.Errorf("class %q not found", className))
		}
		nodeStatuses[i] = status
	}

	sort.Slice(nodeStatuses, func(i, j int) bool {
		return nodeStatuses[i].Name < nodeStatuses[j].Name
	})
	return nodeStatuses, nil
}

func (db *DB) getNodeStatus(ctx context.Context, nodeName string, className, output string) (*models.NodeStatus, error) {
	if db.schemaGetter.NodeName() == nodeName {
		return db.localNodeStatus(className, output), nil
	}
	status, err := db.remoteNode.GetNodeStatus(ctx, nodeName, className, output)
	if err != nil {
		switch err.(type) {
		case enterrors.ErrOpenHttpRequest, enterrors.ErrSendHttpRequest:
			nodeUnavailable := models.NodeStatusStatusUNAVAILABLE
			return &models.NodeStatus{Name: nodeName, Status: &nodeUnavailable}, nil
		default:
			return nil, err
		}
	}
	return status, nil
}

// IncomingGetNodeStatus returns the index if it exists or nil if it doesn't
func (db *DB) IncomingGetNodeStatus(ctx context.Context, className, verbosity string) (*models.NodeStatus, error) {
	return db.localNodeStatus(className, verbosity), nil
}

func (db *DB) localNodeStatus(className, output string) *models.NodeStatus {
	if className != "" && db.GetIndex(schema.ClassName(className)) == nil {
		// class not found
		return &models.NodeStatus{}
	}

	var (
		shards     []*models.NodeShardStatus
		nodeStats  *models.NodeStats
		batchStats *models.BatchStats
	)
	if output == verbosity.OutputVerbose {
		nodeStats = db.localNodeShardStats(&shards, className)
		batchStats = db.localNodeBatchStats()
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
		BatchStats: batchStats,
	}

	if !asyncEnabled() && output == verbosity.OutputVerbose {
		ql := int64(len(db.jobQueueCh))
		status.BatchStats.QueueLength = &ql
	}

	return &status
}

func (db *DB) localNodeShardStats(status *[]*models.NodeShardStatus, className string) *models.NodeStats {
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
			objects, shards := idx.getShardsNodeStatus(status)
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
	objectCount, shardCount = idx.getShardsNodeStatus(status)
	return &models.NodeStats{
		ObjectCount: objectCount,
		ShardCount:  shardCount,
	}
}

func (db *DB) localNodeBatchStats() *models.BatchStats {
	db.batchMonitorLock.Lock()
	rate := db.ratePerSecond
	db.batchMonitorLock.Unlock()
	return &models.BatchStats{RatePerSecond: int64(rate)}
}

func (i *Index) getShardsNodeStatus(status *[]*models.NodeShardStatus) (totalCount, shardCount int64) {
	i.ForEachShard(func(name string, shard ShardLike) error {
		objectCount := int64(shard.ObjectCount())
		totalCount += objectCount
		shardStatus := &models.NodeShardStatus{
			Name:                 name,
			Class:                shard.Index().Config.ClassName.String(),
			ObjectCount:          objectCount,
			VectorIndexingStatus: shard.GetStatus().String(),
			VectorQueueLength:    shard.Queue().Size(),
			Compressed:           shard.VectorIndex().Compressed(),
		}
		*status = append(*status, shardStatus)
		shardCount++
		return nil
	})
	return
}
