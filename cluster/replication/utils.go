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

func logFieldsForOp(op ShardReplicationOp) logrus.Fields {
	return logrus.Fields{
		"op_uuid":           op.UUID,
		"op_id":             op.ID,
		"source_node":       op.SourceShard.NodeId,
		"target_node":       op.TargetShard.NodeId,
		"source_shard":      op.SourceShard.ShardId,
		"target_shard":      op.TargetShard.ShardId,
		"source_collection": op.SourceShard.CollectionId,
		"target_collection": op.TargetShard.CollectionId,
		"transfer_type":     op.TransferType,
	}
}

func logFieldsForStatus(state ShardReplicationOpStatus) logrus.Fields {
	return logrus.Fields{
		"state": state.GetCurrentState(),
	}
}

func getLoggerForOpAndStatus(logger *logrus.Entry, op ShardReplicationOp, state ShardReplicationOpStatus) *logrus.Entry {
	return logger.WithFields(logFieldsForOp(op)).WithFields(logFieldsForStatus(state))
}
