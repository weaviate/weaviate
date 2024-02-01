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
	"golang.org/x/sync/errgroup"
)

// GetNodeStatus returns the status of all Weaviate nodes.
func (db *DB) GetNodeStatus(ctx context.Context, className string, verbosity string) ([]*models.NodeStatus, error) {
	nodeStatuses := make([]*models.NodeStatus, len(db.schemaGetter.Nodes()))
	eg := errgroup.Group{}
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
		})
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
		return db.localNodeStatus(ctx, className, output), nil
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
	return db.localNodeStatus(ctx, className, verbosity), nil
}

func (db *DB) localNodeStatus(ctx context.Context, className, output string) *models.NodeStatus {
	var (
		objectCount int64
		shardCount  int64
		shards      []*models.NodeShardStatus
	)

	if className != "" && db.GetIndex(schema.ClassName(className)) == nil {
		// class not found
		return &models.NodeStatus{}
	}

	if className == "" {
		objectCount, shardCount = db.localNodeStatusAll(ctx, &shards, output)
	} else {
		objectCount, shardCount = db.localNodeStatusForClass(ctx, &shards, className, output)
	}

	clusterHealthStatus := models.NodeStatusStatusHEALTHY
	if db.schemaGetter.ClusterHealthScore() > 0 {
		clusterHealthStatus = models.NodeStatusStatusUNHEALTHY
	}
	db.batchMonitorLock.Lock()
	rate := db.ratePerSecond
	db.batchMonitorLock.Unlock()

	status := models.NodeStatus{
		Name:    db.schemaGetter.NodeName(),
		Version: db.config.ServerVersion,
		GitHash: db.config.GitHash,
		Status:  &clusterHealthStatus,
		Shards:  shards,
		Stats: &models.NodeStats{
			ShardCount:  shardCount,
			ObjectCount: objectCount,
		},
		BatchStats: &models.BatchStats{
			RatePerSecond: int64(rate),
		},
	}

	if !asyncEnabled() {
		ql := int64(len(db.jobQueueCh))
		status.BatchStats.QueueLength = &ql
	}

	return &status
}

func (db *DB) localNodeStatusAll(ctx context.Context, status *[]*models.NodeShardStatus,
	output string,
) (totalCount, shardCount int64) {
	db.indexLock.RLock()
	defer db.indexLock.RUnlock()
	for name, idx := range db.indices {
		if idx == nil {
			db.logger.WithField("action", "local_node_status_for_all").
				Warningf("no resource found for index %q", name)
			continue
		}
		total, shard := idx.getShardsNodeStatus(ctx, status, output)
		totalCount, shardCount = totalCount+total, shardCount+shard
	}
	return
}

func (db *DB) localNodeStatusForClass(ctx context.Context, status *[]*models.NodeShardStatus,
	className, output string,
) (totalCount, shardCount int64) {
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		db.logger.WithField("action", "local_node_status_for_class").
			Warningf("no index found for class %q", className)
		return 0, 0
	}
	return idx.getShardsNodeStatus(ctx, status, output)
}

func (i *Index) getShardsNodeStatus(ctx context.Context,
	status *[]*models.NodeShardStatus, output string,
) (totalCount, shardCount int64) {
	i.ForEachShard(func(name string, shard ShardLike) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		objectCount := int64(shard.ObjectCount())
		totalCount += objectCount
		if output == verbosity.OutputVerbose {
			shardStatus := &models.NodeShardStatus{
				Name:                 name,
				Class:                shard.Index().Config.ClassName.String(),
				ObjectCount:          objectCount,
				VectorIndexingStatus: shard.GetStatus().String(),
				VectorQueueLength:    shard.Queue().Size(),
				Compressed:           shard.VectorIndex().Compressed(),
			}
			*status = append(*status, shardStatus)
		}
		shardCount++
		return nil
	})
	return
}
