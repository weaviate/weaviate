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
	"strconv"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
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
		"action":       "replication_engine",
		"op":           "replicate",
		"collection":   *params.Body.CollectionID,
		"shardId":      *params.Body.ShardID,
		"sourceNodeId": *params.Body.SourceNodeName,
		"destNodeId":   *params.Body.DestinationNodeName,
	}).Info("replicate operation registered")

	return replication.NewReplicateOK()
}

func (h *replicationHandler) getReplicationDetailsByReplicationId(params replication.ReplicationDetailsParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.CollectionsMetadata()...); err != nil {
		return h.handleForbiddenError(err)
	}

	id, err := strconv.ParseUint(params.ID, 10, 64)
	if err != nil {
		return h.handleMalformedRequestError(params.ID, err)
	}

	response, err := h.replicationManager.GetReplicationDetailsByReplicationId(id)
	if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
		return h.handleOperationNotFoundError(params.ID, err)
	} else if err != nil {
		return h.handleInternalServerError(params.ID, err)
	}

	return h.handleReplicationDetailsResponse(response)
}

func (h *replicationHandler) handleReplicationDetailsResponse(response api.ReplicationDetailsResponse) *replication.ReplicationDetailsOK {
	idAsString := strconv.FormatUint(response.Id, 10)
	return replication.NewReplicationDetailsOK().WithPayload(&models.ReplicationReplicateDetailsReplicaResponse{
		ID:           &idAsString,
		Collection:   &response.Collection,
		ShardID:      &response.ShardId,
		SourceNodeID: &response.SourceNodeId,
		TargetNodeID: &response.TargetNodeId,
		Status:       &response.Status,
	})
}

func (h *replicationHandler) handleForbiddenError(err error) middleware.Responder {
	return replication.NewReplicationDetailsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("access denied: %w", err)))
}

func (h *replicationHandler) handleMalformedRequestError(id string, err error) middleware.Responder {
	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "replication_details",
		"id":     id,
		"error":  err,
	}).Debug("malformed request for replication operation")

	return replication.NewReplicationDetailsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(
		fmt.Errorf("malformed request for replication operation with id '%s'", id)))
}

func (h *replicationHandler) handleOperationNotFoundError(id string, err error) middleware.Responder {
	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "replication_details",
		"id":     id,
		"error":  err,
	}).Debug("replication operation not found")

	return replication.NewReplicationDetailsNotFound()
}

func (h *replicationHandler) handleInternalServerError(id string, err error) middleware.Responder {
	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "replication_details",
		"id":     id,
		"error":  err,
	}).Error("error while retrieving replication operation details")

	return replication.NewReplicationDetailsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(
		fmt.Errorf("error while retrieving details for replication operation id '%s': %w", id, err)))
}
