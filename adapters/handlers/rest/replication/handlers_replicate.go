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

	transferType := models.ReplicationReplicateReplicaRequestTransferTypeCOPY
	if params.Body.TransferType != nil {
		transferType = *params.Body.TransferType
	}
	if err := h.replicationManager.ReplicationReplicateReplica(uuid, *params.Body.SourceNodeName, *params.Body.CollectionID, *params.Body.ShardID, *params.Body.DestinationNodeName, transferType); err != nil {
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
		"transferType": params.Body.TransferType,
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

func (h *replicationHandler) generateReplicationDetailsResponse(withHistory bool, response api.ReplicationDetailsResponse) *models.ReplicationReplicateDetailsReplicaResponse {
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

	return &models.ReplicationReplicateDetailsReplicaResponse{
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
		TransferType:  &response.TransferType,
	}
}

func (h *replicationHandler) generateArrayReplicationDetailsResponse(withHistory bool, response []api.ReplicationDetailsResponse) []*models.ReplicationReplicateDetailsReplicaResponse {
	responses := make([]*models.ReplicationReplicateDetailsReplicaResponse, len(response))
	for i, r := range response {
		responses[i] = h.generateReplicationDetailsResponse(withHistory, r)
	}
	return responses
}

func (h *replicationHandler) handleReplicationDetailsResponse(withHistory bool, response api.ReplicationDetailsResponse) *replication.ReplicationDetailsOK {
	return replication.NewReplicationDetailsOK().WithPayload(h.generateReplicationDetailsResponse(withHistory, response))
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
	if err := h.authorizer.Authorize(principal, authorization.DELETE, authorization.CollectionsMetadata()...); err != nil {
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

func (h *replicationHandler) deleteAllReplications(_ replication.DeleteAllReplicationsParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.DELETE, authorization.CollectionsMetadata()...); err != nil {
		return replication.NewDeleteAllReplicationsForbidden()
	}

	if err := h.replicationManager.DeleteAllReplications(); err != nil {
		return replication.NewDeleteAllReplicationsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "delete_all_operations",
	}).Info("delete all replication operations")

	return replication.NewDeleteAllReplicationsNoContent()
}

func (h *replicationHandler) cancelReplication(params replication.CancelReplicationParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.CollectionsMetadata()...); err != nil {
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

func (h *replicationHandler) listReplication(params replication.ListReplicationParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.CollectionsMetadata()...); err != nil {
		return replication.NewListReplicationForbidden()
	}

	var response []api.ReplicationDetailsResponse
	var err error

	if params.Collection == nil && params.Shard == nil && params.NodeID == nil {
		response, err = h.replicationManager.GetAllReplicationDetails()
	} else if params.Collection != nil {
		if params.Shard != nil {
			response, err = h.replicationManager.GetReplicationDetailsByCollectionAndShard(*params.Collection, *params.Shard)
		} else {
			response, err = h.replicationManager.GetReplicationDetailsByCollection(*params.Collection)
		}
	} else if params.NodeID != nil {
		response, err = h.replicationManager.GetReplicationDetailsByTargetNode(*params.NodeID)
	} else {
		// This can happen if the user provides only a shard id without a collection id
		return replication.NewListReplicationBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("shard id provided without collection id")))
	}

	// Handle error if any
	if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
		return replication.NewListReplicationNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	} else if err != nil {
		return replication.NewListReplicationInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	// Parse response into the correct format and return
	includeHistory := false
	if params.IncludeHistory != nil {
		includeHistory = *params.IncludeHistory
	}
	return replication.NewListReplicationOK().WithPayload(h.generateArrayReplicationDetailsResponse(includeHistory, response))
}

func (h *replicationHandler) generateShardingStateResponse(collection string, shards map[string][]string) *models.ReplicationShardingStateResponse {
	shardsResponse := make([]*models.ReplicationShardReplicas, 0, len(shards))
	for shard, replicas := range shards {
		shardsResponse = append(shardsResponse, &models.ReplicationShardReplicas{
			Shard:    shard,
			Replicas: replicas,
		})
	}
	return &models.ReplicationShardingStateResponse{
		ShardingState: &models.ReplicationShardingState{
			Collection: collection,
			Shards:     shardsResponse,
		},
	}
}

func (h *replicationHandler) getCollectionShardingState(params replication.GetCollectionShardingStateParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizer.Authorize(principal, authorization.READ, authorization.CollectionsMetadata()...); err != nil {
		return replication.NewGetCollectionShardingStateForbidden()
	}

	if params.Collection == nil {
		return replication.NewGetCollectionShardingStateBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("collection is required")))
	}

	var shardingState api.ShardingState
	var err error
	if params.Shard != nil {
		shardingState, err = h.replicationManager.QueryShardingStateByCollectionAndShard(*params.Collection, *params.Shard)
	} else {
		shardingState, err = h.replicationManager.QueryShardingStateByCollection(*params.Collection)
	}

	if errors.Is(err, replicationTypes.ErrNotFound) {
		return replication.NewGetCollectionShardingStateNotFound()
	} else if err != nil {
		return replication.NewGetCollectionShardingStateInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	return replication.NewGetCollectionShardingStateOK().WithPayload(h.generateShardingStateResponse(shardingState.Collection, shardingState.Shards))
}
