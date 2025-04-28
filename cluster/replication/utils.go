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
