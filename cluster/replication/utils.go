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

package replication

import "github.com/sirupsen/logrus"

func getLoggerForOp(logger *logrus.Logger, op ShardReplicationOp) *logrus.Entry {
	return logger.WithFields(logrus.Fields{
		"op":                op.ID,
		"source_node":       op.SourceShard.NodeId,
		"target_node":       op.TargetShard.NodeId,
		"source_shard":      op.SourceShard.ShardId,
		"target_shard":      op.TargetShard.ShardId,
		"source_collection": op.SourceShard.CollectionId,
		"target_collection": op.TargetShard.CollectionId,
	})
}
