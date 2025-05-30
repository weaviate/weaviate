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

import (
	"net/http"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type replicationHandler struct {
	authorizer         authorization.Authorizer
	replicationManager replicationTypes.Manager

	logger  logrus.FieldLogger
	metrics *monitoring.PrometheusMetrics
}

func SetupHandlers(enabled bool, api *operations.WeaviateAPI, replicationManager replicationTypes.Manager, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger,
) {
	if !enabled {
		setupUnimplementedHandlers(api)
		return
	}

	h := &replicationHandler{
		authorizer:         authorizer,
		replicationManager: replicationManager,
		logger:             logger,
		metrics:            metrics,
	}
	api.ReplicationReplicateHandler = replication.ReplicateHandlerFunc(h.replicate)
	api.ReplicationReplicationDetailsHandler = replication.ReplicationDetailsHandlerFunc(h.getReplicationDetailsByReplicationId)
	api.ReplicationCancelReplicationHandler = replication.CancelReplicationHandlerFunc(h.cancelReplication)
	api.ReplicationDeleteReplicationHandler = replication.DeleteReplicationHandlerFunc(h.deleteReplication)
	api.ReplicationDeleteAllReplicationsHandler = replication.DeleteAllReplicationsHandlerFunc(h.deleteAllReplications)

	// Sharding state query handlers
	api.ReplicationGetCollectionShardingStateHandler = replication.GetCollectionShardingStateHandlerFunc(h.getCollectionShardingState)

	// Replication node details query handlers
	api.ReplicationListReplicationHandler = replication.ListReplicationHandlerFunc(h.listReplication)
}

type unimplementedResponseWriter struct {
}

func (w *unimplementedResponseWriter) WriteResponse(rw http.ResponseWriter, p runtime.Producer) {
	rw.WriteHeader(http.StatusNotImplemented)
}

func setupUnimplementedHandlers(api *operations.WeaviateAPI) {
	api.ReplicationReplicateHandler = replication.ReplicateHandlerFunc(func(replication.ReplicateParams, *models.Principal) middleware.Responder {
		return &unimplementedResponseWriter{}
	})
	api.ReplicationReplicationDetailsHandler = replication.ReplicationDetailsHandlerFunc(func(replication.ReplicationDetailsParams, *models.Principal) middleware.Responder {
		return &unimplementedResponseWriter{}
	})
	api.ReplicationCancelReplicationHandler = replication.CancelReplicationHandlerFunc(func(replication.CancelReplicationParams, *models.Principal) middleware.Responder {
		return &unimplementedResponseWriter{}
	})
	api.ReplicationDeleteReplicationHandler = replication.DeleteReplicationHandlerFunc(func(replication.DeleteReplicationParams, *models.Principal) middleware.Responder {
		return &unimplementedResponseWriter{}
	})
	api.ReplicationDeleteAllReplicationsHandler = replication.DeleteAllReplicationsHandlerFunc(func(replication.DeleteAllReplicationsParams, *models.Principal) middleware.Responder {
		return &unimplementedResponseWriter{}
	})
	api.ReplicationGetCollectionShardingStateHandler = replication.GetCollectionShardingStateHandlerFunc(func(replication.GetCollectionShardingStateParams, *models.Principal) middleware.Responder {
		return &unimplementedResponseWriter{}
	})
	api.ReplicationListReplicationHandler = replication.ListReplicationHandlerFunc(func(replication.ListReplicationParams, *models.Principal) middleware.Responder {
		return &unimplementedResponseWriter{}
	})
}
