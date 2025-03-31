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
	"errors"
	"fmt"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (h *replicationHandler) replicate(params replication.ReplicateParams, principal *models.Principal) middleware.Responder {
	if err := params.Body.Validate(nil /* pass nil as we don't validate formatting here*/); err != nil {
		return replication.NewReplicateBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.ShardsMetadata(*params.Body.CollectionID, *params.Body.ShardID)...); err != nil {
		return replication.NewReplicateForbidden()
	}

	if err := h.replicationManager.ReplicationReplicateReplica(*params.Body.SourceNodeName, *params.Body.CollectionID, *params.Body.ShardID, *params.Body.DestinationNodeName); err != nil {
		if errors.Is(err, replicationTypes.ErrInvalidRequest) {
			return replication.NewReplicateUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		} else {
			return replication.NewReplicateInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
	}

	h.logger.WithFields(logrus.Fields{
		"action":       "replication",
		"op":           "replicate",
		"collection":   *params.Body.CollectionID,
		"shardId":      *params.Body.ShardID,
		"sourceNodeId": *params.Body.SourceNodeName,
		"destNodeId":   *params.Body.DestinationNodeName,
	}).Info("replicate operation registered")

	return replication.NewReplicateOK()
}

func (h *replicationHandler) getReplicationOperationDetails(params replication.ReplicationDetailsParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.CollectionsMetadata()...); err != nil {
		return h.handleForbiddenError()
	}

	return h.handleNotImplementedError(params)
}

func (h *replicationHandler) handleForbiddenError() middleware.Responder {
	return replication.NewReplicationDetailsForbidden()
}

func (h *replicationHandler) handleNotImplementedError(params replication.ReplicationDetailsParams) middleware.Responder {
	return replication.NewReplicationDetailsNotImplemented().WithPayload(cerrors.ErrPayloadFromSingleErr(
		fmt.Errorf("fetching replication details for operation id %s is not yet implemented", params.ID)))
}
