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
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type replicationHandler struct {
	authorizer         authorization.Authorizer
	replicationManager replicationTypes.Manager

	logger  logrus.FieldLogger
	metrics *monitoring.PrometheusMetrics
}

func SetupHandlers(api *operations.WeaviateAPI, replicationManager replicationTypes.Manager, metrics *monitoring.PrometheusMetrics, authorizer authorization.Authorizer, logger logrus.FieldLogger,
) {
	h := &replicationHandler{
		authorizer: authorizer,
		logger:     logger,
		metrics:    metrics,
	}
	api.ReplicationReplicateHandler = replication.ReplicateHandlerFunc(h.replicate)
	api.ReplicationReplicationDetailsHandler = replication.ReplicationDetailsHandlerFunc(h.getReplicationOperationDetails)
}
