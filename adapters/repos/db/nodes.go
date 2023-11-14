//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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
)

// GetNodeStatus returns the status of all Weaviate nodes.
func (db *DB) GetNodeStatus(ctx context.Context, className string) ([]*models.NodeStatus, error) {
	nodeStatuses := make([]*models.NodeStatus, len(db.schemaGetter.Nodes()))
	for i, nodeName := range db.schemaGetter.Nodes() {
		status, err := db.getNodeStatus(ctx, nodeName, className)
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

func (db *DB) getNodeStatus(ctx context.Context, nodeName string, className string) (*models.NodeStatus, error) {
	if db.schemaGetter.NodeName() == nodeName {
		return db.localNodeStatus(className), nil
	}
	status, err := db.remoteNode.GetNodeStatus(ctx, nodeName, className)
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
func (db *DB) IncomingGetNodeStatus(ctx context.Context, className string) (*models.NodeStatus, error) {
	return db.localNodeStatus(className), nil
}

func (db *DB) localNodeStatus(className string) *models.NodeStatus {
	var (
		objectCount int64
		shards      []*models.NodeShardStatus
	)

	if className != "" && db.GetIndex(schema.ClassName(className)) == nil {
		// class not found
		return &models.NodeStatus{}
	}

	if className == "" {
		objectCount = db.localNodeStatusAll(&shards)
	} else {
		objectCount = db.localNodeStatusForClass(&shards, className)
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
			ShardCount:  int64(len(shards)),
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

func (db *DB) localNodeStatusAll(status *[]*models.NodeShardStatus) (totalCount int64) {
	db.indexLock.RLock()
	defer db.indexLock.RUnlock()
	for name, idx := range db.indices {
		if idx == nil {
			db.logger.WithField("action", "local_node_status_for_all").
				Warningf("no resource found for index %q", name)
			continue
		}
		totalCount += idx.getShardsNodeStatus(status)
	}
	return
}

func (db *DB) localNodeStatusForClass(status *[]*models.NodeShardStatus,
	className string,
) (totalCount int64) {
	idx := db.GetIndex(schema.ClassName(className))
	if idx == nil {
		db.logger.WithField("action", "local_node_status_for_class").
			Warningf("no index found for class %q", className)
		return 0
	}
	return idx.getShardsNodeStatus(status)
}

func (i *Index) getShardsNodeStatus(status *[]*models.NodeShardStatus) (totalCount int64) {
	i.ForEachShard(func(name string, shard ShardInterface) error {
		objectCount := int64(shard.ObjectCount())
		shardStatus := &models.NodeShardStatus{
			Name:                 name,
			Class:                shard.Index().Config.ClassName.String(),
			ObjectCount:          objectCount,
			VectorIndexingStatus: shard.GetStatus().String(),
			VectorQueueLength:    shard.Queue().Size(),
		}
		totalCount += objectCount
		*status = append(*status, shardStatus)
		return nil
	})
	return
}
