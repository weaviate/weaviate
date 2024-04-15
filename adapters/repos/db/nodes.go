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
func (db *DB) GetNodeStatus(ctx context.Context, className string, verbosity string) ([]*models.NodeStatus, error) {
	nodeStatuses := make([]*models.NodeStatus, len(db.schemaGetter.Nodes()))
	eg := enterrors.NewErrorGroupWrapper(db.logger)
	eg.SetLimit(_NUMCPU)
	for i, nodeName := range db.schemaGetter.Nodes() {
		i, nodeName := i, nodeName
		eg.Go(func() error {
			status, err := db.getNodeStatus(ctx, nodeName, className, verbosity)
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

func (db *DB) getNodeStatus(ctx context.Context, nodeName string, className, output string) (*models.NodeStatus, error) {
	if db.schemaGetter.NodeName() == nodeName {
		return db.LocalNodeStatus(ctx, className, output), nil
	}
	status, err := db.remoteNode.GetNodeStatus(ctx, nodeName, className, output)
	if err != nil {
		switch typed := err.(type) {
		case enterrors.ErrSendHttpRequest:
			if errors.Is(typed.Unwrap(), context.DeadlineExceeded) {
				nodeTimeout := models.NodeStatusStatusTIMEOUT
				return &models.NodeStatus{Name: nodeName, Status: &nodeTimeout}, nil
			}

			nodeUnavailable := models.NodeStatusStatusUNAVAILABLE
			return &models.NodeStatus{Name: nodeName, Status: &nodeUnavailable}, nil
		case enterrors.ErrOpenHttpRequest:
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
	return db.LocalNodeStatus(ctx, className, verbosity), nil
}

func (db *DB) LocalNodeStatus(ctx context.Context, className, output string) *models.NodeStatus {
	if className != "" && db.GetIndex(schema.ClassName(className)) == nil {
		// class not found
		return &models.NodeStatus{}
	}

	var (
		shards    []*models.NodeShardStatus
		nodeStats *models.NodeStats
	)
	if output == verbosity.OutputVerbose {
		nodeStats = db.localNodeShardStats(ctx, &shards, className)
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
	status *[]*models.NodeShardStatus, className string,
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
			objects, shards := idx.getShardsNodeStatus(ctx, status)
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
	objectCount, shardCount = idx.getShardsNodeStatus(ctx, status)
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

func (i *Index) getShardsNodeStatus(ctx context.Context,
	status *[]*models.NodeShardStatus,
) (totalCount, shardCount int64) {
	i.ForEachShard(func(name string, shard ShardLike) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Don't force load a lazy shard to get nodes status
		if lazy, ok := shard.(*LazyLoadShard); ok {
			if !lazy.isLoaded() {
				shardStatus := &models.NodeShardStatus{
					Name:                 name,
					Class:                shard.Index().Config.ClassName.String(),
					VectorIndexingStatus: shard.GetStatus().String(),
					Loaded:               false,
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
		var compressed bool
		if shard.hasTargetVectors() {
			for _, queue := range shard.Queues() {
				queueLen += queue.Size()
			}
			for _, vectorIndex := range shard.VectorIndexes() {
				if vectorIndex.Compressed() {
					compressed = true
					break
				}
			}
		} else {
			queueLen = shard.Queue().Size()
			compressed = shard.VectorIndex().Compressed()
		}

		shardStatus := &models.NodeShardStatus{
			Name:                 name,
			Class:                shard.Index().Config.ClassName.String(),
			ObjectCount:          objectCount,
			VectorIndexingStatus: shard.GetStatus().String(),
			VectorQueueLength:    queueLen,
			Compressed:           compressed,
			Loaded:               true,
		}
		*status = append(*status, shardStatus)
		shardCount++
		return nil
	})
	return
}
