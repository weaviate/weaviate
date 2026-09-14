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

package replication

import (
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type replicationHandler struct {
	authorizer         authorization.Authorizer
	replicationManager replicationTypes.Manager

	logger  logrus.FieldLogger
	metrics *monitoring.PrometheusMetrics
	enabled *runtime.DynamicValue[bool]
}

func SetupHandlers(enabled *runtime.DynamicValue[bool], api *operations.WeaviateAPI, replicationManager replicationTypes.Manager, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger,
) {
	h := &replicationHandler{
		authorizer:         authorizer,
		replicationManager: replicationManager,
		logger:             logger,
		metrics:            metrics,
		enabled:            enabled,
	}
	api.ReplicationReplicateHandler = replication.ReplicateHandlerFunc(h.replicate)
	api.ReplicationReplicationDetailsHandler = replication.ReplicationDetailsHandlerFunc(h.getReplicationDetailsByReplicationId)
	api.ReplicationCancelReplicationHandler = replication.CancelReplicationHandlerFunc(h.cancelReplication)
	api.ReplicationDeleteReplicationHandler = replication.DeleteReplicationHandlerFunc(h.deleteReplication)
	api.ReplicationDeleteAllReplicationsHandler = replication.DeleteAllReplicationsHandlerFunc(h.deleteAllReplications)
	api.ReplicationForceDeleteReplicationsHandler = replication.ForceDeleteReplicationsHandlerFunc(h.forceDeleteReplications)

	// Sharding state query handlers
	api.ReplicationGetCollectionShardingStateHandler = replication.GetCollectionShardingStateHandlerFunc(h.getCollectionShardingState)
	api.ReplicationGetReplicationScalePlanHandler = replication.GetReplicationScalePlanHandlerFunc(h.getReplicationScalePlan)
	api.ReplicationApplyReplicationScalePlanHandler = replication.ApplyReplicationScalePlanHandlerFunc(h.applyReplicationScalePlan)

	// Replication node details query handlers
	api.ReplicationListReplicationHandler = replication.ListReplicationHandlerFunc(h.listReplication)
}
