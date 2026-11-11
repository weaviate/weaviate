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

	rebalanceManager rebalanceManager
	nodes            rebalanceNodes

	logger  logrus.FieldLogger
	metrics *monitoring.PrometheusMetrics
	// enabled is REPLICA_MOVEMENT_ENABLED.
	enabled          *runtime.DynamicValue[bool]
	rebalanceEnabled *runtime.DynamicValue[bool]
	// dataPath is the node's persistence data root, probed for hardlink support.
	dataPath string
}

func SetupHandlers(enabled, rebalanceEnabled *runtime.DynamicValue[bool], api *operations.WeaviateAPI, replicationManager replicationTypes.Manager,
	rebalanceManager rebalanceManager, nodes rebalanceNodes, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger,
	dataPath string,
) {
	h := &replicationHandler{
		authorizer:         authorizer,
		replicationManager: replicationManager,
		rebalanceManager:   rebalanceManager,
		nodes:              nodes,
		logger:             logger,
		metrics:            metrics,
		enabled:            enabled,
		rebalanceEnabled:   rebalanceEnabled,
		dataPath:           dataPath,
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

	// Rebalance plan handlers
	api.ReplicationCreateRebalancePlanHandler = replication.CreateRebalancePlanHandlerFunc(h.createRebalancePlan)
	api.ReplicationGetRebalancePlanHandler = replication.GetRebalancePlanHandlerFunc(h.getRebalancePlan)
	api.ReplicationStopRebalancePlanHandler = replication.StopRebalancePlanHandlerFunc(h.stopRebalancePlan)
	api.ReplicationApproveRebalancePlanHandler = replication.ApproveRebalancePlanHandlerFunc(h.approveRebalancePlan)
	api.ReplicationDeleteRebalancePlanHandler = replication.DeleteRebalancePlanHandlerFunc(h.deleteRebalancePlan)
}
