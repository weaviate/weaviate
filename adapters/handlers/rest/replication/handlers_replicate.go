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
	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
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

	id, err := uuid.NewRandom()
	if err != nil {
		return replication.NewReplicateInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("could not generate uuid v4: %w", err)))
	}
	uuid := strfmt.UUID(id.String())

	if err := h.replicationManager.ReplicationReplicateReplica(uuid, *params.Body.SourceNodeName, *params.Body.CollectionID, *params.Body.ShardID, *params.Body.DestinationNodeName); err != nil {
		if errors.Is(err, replicationTypes.ErrInvalidRequest) {
			return replication.NewReplicateUnprocessableEntity().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
		return replication.NewReplicateInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":       "replication_engine",
		"op":           "replicate",
		"id":           id,
		"collection":   *params.Body.CollectionID,
		"shardId":      *params.Body.ShardID,
		"sourceNodeId": *params.Body.SourceNodeName,
		"destNodeId":   *params.Body.DestinationNodeName,
	}).Info("replicate operation registered")

	return h.handleReplicationReplicateResponse(uuid)
}

func (h *replicationHandler) getReplicationDetailsByReplicationId(params replication.ReplicationDetailsParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.CollectionsMetadata()...); err != nil {
		return h.handleForbiddenError(err)
	}

	response, err := h.replicationManager.GetReplicationDetailsByReplicationId(params.ID)
	if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
		return h.handleOperationNotFoundError(params.ID, err)
	} else if err != nil {
		return h.handleInternalServerError(params.ID, err)
	}

	includeHistory := false
	if params.IncludeHistory != nil {
		includeHistory = *params.IncludeHistory
	}
	return h.handleReplicationDetailsResponse(includeHistory, response)
}

func (h *replicationHandler) handleReplicationReplicateResponse(id strfmt.UUID) *replication.ReplicateOK {
	return replication.NewReplicateOK().WithPayload(&models.ReplicationReplicateReplicaResponse{ID: &id})
}

func (h *replicationHandler) handleReplicationDetailsResponse(withHistory bool, response api.ReplicationDetailsResponse) *replication.ReplicationDetailsOK {
	// Compute history only if requested
	var history []*models.ReplicationReplicateDetailsReplicaStatus
	if withHistory {
		history = make([]*models.ReplicationReplicateDetailsReplicaStatus, len(response.StatusHistory))
		for i, status := range response.StatusHistory {
			history[i] = &models.ReplicationReplicateDetailsReplicaStatus{
				State:  status.State,
				Errors: status.Errors,
			}
		}
	}

	return replication.NewReplicationDetailsOK().WithPayload(&models.ReplicationReplicateDetailsReplicaResponse{
		Collection:   &response.Collection,
		ID:           &response.Uuid,
		ShardID:      &response.ShardId,
		SourceNodeID: &response.SourceNodeId,
		TargetNodeID: &response.TargetNodeId,
		Status: &models.ReplicationReplicateDetailsReplicaStatus{
			State:  response.Status.State,
			Errors: response.Status.Errors,
		},
		StatusHistory: history,
	})
}

func (h *replicationHandler) handleForbiddenError(err error) middleware.Responder {
	return replication.NewReplicationDetailsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("access denied: %w", err)))
}

func (h *replicationHandler) handleOperationNotFoundError(id strfmt.UUID, err error) middleware.Responder {
	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "replication_details",
		"id":     id,
		"error":  err,
	}).Debug("replication operation not found")

	return replication.NewReplicationDetailsNotFound()
}

func (h *replicationHandler) handleInternalServerError(id strfmt.UUID, err error) middleware.Responder {
	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "replication_details",
		"id":     id,
		"error":  err,
	}).Error("error while retrieving replication operation details")

	return replication.NewReplicationDetailsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(
		fmt.Errorf("error while retrieving details for replication operation id '%s': %w", id, err)))
}

func (h *replicationHandler) deleteReplication(params replication.DeleteReplicationParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.CollectionsMetadata()...); err != nil {
		return replication.NewDeleteReplicationForbidden()
	}

	if err := h.replicationManager.DeleteReplication(params.ID); err != nil {
		if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
			return h.handleOperationNotFoundError(params.ID, err)
		}
		return h.handleInternalServerError(params.ID, err)
	}

	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "delete_replication",
		"id":     params.ID,
	}).Info("replication operation stopped")

	return replication.NewDeleteReplicationNoContent()
}

func (h *replicationHandler) cancelReplication(params replication.CancelReplicationParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.CollectionsMetadata()...); err != nil {
		return replication.NewCancelReplicationForbidden()
	}

	if err := h.replicationManager.CancelReplication(params.ID); err != nil {
		if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
			return h.handleOperationNotFoundError(params.ID, err)
		}
		return h.handleInternalServerError(params.ID, err)
	}

	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "cancel_replication",
		"id":     params.ID,
	}).Info("replication operation cancelled")

	return replication.NewCancelReplicationNoContent()
}
