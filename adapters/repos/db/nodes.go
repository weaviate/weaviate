//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
)

// GetNodeStatuses returns the status of all Weaviate nodes.
func (db *DB) GetNodeStatuses(ctx context.Context) ([]*models.NodeStatus, error) {
	var nodeStatuses []*models.NodeStatus
	for _, nodeName := range db.schemaGetter.Nodes() {
		status, err := db.getNodeStatus(ctx, nodeName)
		if err != nil {
			return nil, fmt.Errorf("node: %v: %w", nodeName, err)
		}
		nodeStatuses = append(nodeStatuses, status)
	}

	sort.Slice(nodeStatuses, func(i, j int) bool {
		return nodeStatuses[i].Name < nodeStatuses[j].Name
	})
	return nodeStatuses, nil
}

func (db *DB) getNodeStatus(ctx context.Context, nodeName string) (*models.NodeStatus, error) {
	if db.schemaGetter.NodeName() == nodeName {
		return db.localNodeStatus(), nil
	}
	status, err := db.remoteNode.GetNodeStatus(ctx, nodeName)
	if err != nil {
		if strings.HasPrefix(err.Error(), "open http request") || strings.HasPrefix(err.Error(), "send http request") {
			nodeUnavailable := models.NodeStatusStatusUNAVAILABLE
			return &models.NodeStatus{Name: nodeName, Status: &nodeUnavailable}, nil
		}
		return nil, err
	}
	return status, nil
}

// IncomingGetNodeStatus returns the index if it exists or nil if it doesn't
func (db *DB) IncomingGetNodeStatus(ctx context.Context) (*models.NodeStatus, error) {
	return db.localNodeStatus(), nil
}

func (db *DB) localNodeStatus() *models.NodeStatus {
	var totalObjectCount int64
	var shardCount int64

	shards := []*models.NodeShardStatus{}
	for _, index := range db.indices {
		for shardName, shard := range index.Shards {
			objectCount := int64(shard.counter.Get())
			shardStatus := &models.NodeShardStatus{
				Name:        shardName,
				Class:       shard.index.Config.ClassName.String(),
				ObjectCount: objectCount,
			}
			totalObjectCount += objectCount
			shardCount++
			shards = append(shards, shardStatus)
		}
	}

	clusterHealthStatus := models.NodeStatusStatusHEALTHY
	if db.schemaGetter.ClusterHealthScore() > 0 {
		clusterHealthStatus = models.NodeStatusStatusUNHEALTHY
	}

	status := &models.NodeStatus{
		Name:    db.schemaGetter.NodeName(),
		Version: db.config.ServerVersion,
		GitHash: db.config.GitHash,
		Status:  &clusterHealthStatus,
		Shards:  shards,
		Stats: &models.NodeStats{
			ShardCount:  shardCount,
			ObjectCount: totalObjectCount,
		},
	}
	return status
}
