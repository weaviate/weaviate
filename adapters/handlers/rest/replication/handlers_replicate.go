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
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (h *replicationHandler) replicate(params replication.ReplicateParams, principal *models.Principal) middleware.Responder {
	if err := params.Body.Validate(nil /* pass nil as we don't validate formatting here*/); err != nil {
		return replication.NewReplicateBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	collection := schema.UppercaseClassName(*params.Body.CollectionID)
	ctx := params.HTTPRequest.Context()

	if err := h.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.Replications(collection, *params.Body.ShardID)); err != nil {
		return replication.NewReplicateForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
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
	if err := h.replicationManager.ReplicationReplicateReplica(params.HTTPRequest.Context(), uuid, *params.Body.SourceNodeName, collection, *params.Body.ShardID, *params.Body.DestinationNodeName, transferType); err != nil {
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
	ctx := params.HTTPRequest.Context()

	response, err := h.replicationManager.GetReplicationDetailsByReplicationId(params.HTTPRequest.Context(), params.ID)
	if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
		if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Replications("*", "*")); err != nil {
			return replication.NewReplicationDetailsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
		return h.handleOperationNotFoundError(params.ID, err)
	} else if err != nil {
		return h.handleInternalServerError(params.ID, err)
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Replications(response.Collection, response.ShardId)); err != nil {
		return replication.NewReplicationDetailsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
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
		Collection:         &response.Collection,
		ID:                 &response.Uuid,
		ShardID:            &response.ShardId,
		SourceNodeID:       &response.SourceNodeId,
		TargetNodeID:       &response.TargetNodeId,
		Uncancelable:       response.Uncancelable,
		ScheduledForCancel: response.ScheduledForCancel,
		ScheduledForDelete: response.ScheduledForDelete,
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
	ctx := params.HTTPRequest.Context()

	response, err := h.replicationManager.GetReplicationDetailsByReplicationId(ctx, params.ID)
	if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
		return replication.NewDeleteReplicationNoContent()
	} else if err != nil {
		return replication.NewDeleteReplicationInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Replications(response.Collection, response.ShardId)); err != nil {
		return replication.NewDeleteReplicationForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.replicationManager.DeleteReplication(ctx, params.ID); err != nil {
		if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
			return replication.NewDeleteReplicationNoContent()
		}
		if errors.Is(err, replicationTypes.ErrDeletionImpossible) {
			return replication.NewDeleteReplicationConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
		return replication.NewDeleteReplicationInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "delete_replication",
		"id":     params.ID,
	}).Info("replication operation stopped")

	return replication.NewDeleteReplicationNoContent()
}

func (h *replicationHandler) deleteAllReplications(params replication.DeleteAllReplicationsParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	if err := h.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Replications("*", "*")); err != nil {
		return replication.NewDeleteAllReplicationsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.replicationManager.DeleteAllReplications(ctx); err != nil {
		return replication.NewDeleteAllReplicationsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "delete_all_operations",
	}).Info("delete all replication operations")

	return replication.NewDeleteAllReplicationsNoContent()
}

func (h *replicationHandler) forceDeleteReplications(params replication.ForceDeleteReplicationsParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if err := h.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Replications("*", "*")); err != nil {
		return replication.NewForceDeleteReplicationsForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	all := params.Body == nil || (params.Body.Collection == "" && params.Body.Shard == "" && params.Body.ID == "" && params.Body.Node == "")
	byCollection := params.Body != nil && params.Body.Collection != ""
	byShard := params.Body != nil && params.Body.Shard != ""
	byId := params.Body != nil && params.Body.ID != ""
	byNode := params.Body != nil && params.Body.Node != ""
	dryRun := params.Body != nil && params.Body.DryRun != nil && *params.Body.DryRun

	var err error
	if dryRun {
		var details []api.ReplicationDetailsResponse

		if all {
			details, err = h.replicationManager.GetAllReplicationDetails(params.HTTPRequest.Context())
		} else if byCollection {
			if byShard {
				details, err = h.replicationManager.GetReplicationDetailsByCollectionAndShard(params.HTTPRequest.Context(), params.Body.Collection, params.Body.Shard)
			} else {
				details, err = h.replicationManager.GetReplicationDetailsByCollection(params.HTTPRequest.Context(), params.Body.Collection)
			}
		} else if byId {
			detail, innerErr := h.replicationManager.GetReplicationDetailsByReplicationId(params.HTTPRequest.Context(), params.Body.ID)
			if errors.Is(innerErr, replicationTypes.ErrReplicationOperationNotFound) {
				return replication.NewForceDeleteReplicationsOK().WithPayload(&models.ReplicationReplicateForceDeleteResponse{
					Deleted: []strfmt.UUID{params.Body.ID},
					DryRun:  dryRun,
				})
			}
			details = []api.ReplicationDetailsResponse{detail}
			err = innerErr
		} else if byNode {
			details, err = h.replicationManager.GetReplicationDetailsByTargetNode(params.HTTPRequest.Context(), params.Body.Node)
		} else {
			// This can happen if the user provides only a shard id without a collection id
			return replication.NewForceDeleteReplicationsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("shard id provided without collection id")))
		}
		if err != nil {
			return replication.NewForceDeleteReplicationsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}

		uuids := make([]strfmt.UUID, len(details))
		for i, detail := range details {
			uuids[i] = detail.Uuid
		}

		h.logger.WithFields(logrus.Fields{
			"action": "replication",
			"op":     "force_delete_operations",
		}).Info("dry run of force delete replication operations")

		return replication.NewForceDeleteReplicationsOK().WithPayload(&models.ReplicationReplicateForceDeleteResponse{
			Deleted: uuids,
			DryRun:  true,
		})
	}

	if all {
		err = h.replicationManager.ForceDeleteAllReplications(params.HTTPRequest.Context())
	} else if byCollection {
		if byShard {
			err = h.replicationManager.ForceDeleteReplicationsByCollectionAndShard(params.HTTPRequest.Context(), params.Body.Collection, params.Body.Shard)
		} else {
			err = h.replicationManager.ForceDeleteReplicationsByCollection(params.HTTPRequest.Context(), params.Body.Collection)
		}
	} else if byId {
		innerErr := h.replicationManager.ForceDeleteReplicationByUuid(params.HTTPRequest.Context(), params.Body.ID)
		if errors.Is(innerErr, replicationTypes.ErrReplicationOperationNotFound) {
			return replication.NewForceDeleteReplicationsOK().WithPayload(&models.ReplicationReplicateForceDeleteResponse{
				Deleted: []strfmt.UUID{params.Body.ID},
				DryRun:  dryRun,
			})
		}
		err = innerErr
	} else if byNode {
		err = h.replicationManager.ForceDeleteReplicationsByTargetNode(params.HTTPRequest.Context(), params.Body.Node)
	} else {
		// This can happen if the user provides only a shard id without a collection id
		return replication.NewForceDeleteReplicationsBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("shard id provided without collection id")))
	}
	if err != nil {
		return replication.NewForceDeleteReplicationsInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "force_delete_operations",
	}).Info("force delete replication operations")

	return replication.NewForceDeleteReplicationsOK().WithPayload(&models.ReplicationReplicateForceDeleteResponse{})
}

func (h *replicationHandler) cancelReplication(params replication.CancelReplicationParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	response, err := h.replicationManager.GetReplicationDetailsByReplicationId(ctx, params.ID)
	if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
		return replication.NewCancelReplicationNoContent()
	} else if err != nil {
		return replication.NewCancelReplicationInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Replications(response.Collection, response.ShardId)); err != nil {
		return replication.NewCancelReplicationForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	if err := h.replicationManager.CancelReplication(ctx, params.ID); err != nil {
		if errors.Is(err, replicationTypes.ErrReplicationOperationNotFound) {
			return replication.NewCancelReplicationNoContent()
		}
		if errors.Is(err, replicationTypes.ErrCancellationImpossible) {
			return replication.NewCancelReplicationConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
		}
		return replication.NewCancelReplicationInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	h.logger.WithFields(logrus.Fields{
		"action": "replication",
		"op":     "cancel_replication",
		"id":     params.ID,
	}).Info("replication operation cancelled")

	return replication.NewCancelReplicationNoContent()
}

func (h *replicationHandler) listReplication(params replication.ListReplicationParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()

	if err := h.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Replications("*", "*")); err != nil {
		return replication.NewListReplicationForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	var response []api.ReplicationDetailsResponse
	var err error

	if params.Collection == nil && params.Shard == nil && params.NodeID == nil {
		response, err = h.replicationManager.GetAllReplicationDetails(params.HTTPRequest.Context())
	} else if params.Collection != nil {
		if params.Shard != nil {
			response, err = h.replicationManager.GetReplicationDetailsByCollectionAndShard(params.HTTPRequest.Context(), *params.Collection, *params.Shard)
		} else {
			response, err = h.replicationManager.GetReplicationDetailsByCollection(params.HTTPRequest.Context(), *params.Collection)
		}
	} else if params.NodeID != nil {
		response, err = h.replicationManager.GetReplicationDetailsByTargetNode(params.HTTPRequest.Context(), *params.NodeID)
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
	ctx := params.HTTPRequest.Context()
	if params.Collection == nil {
		return replication.NewGetCollectionShardingStateBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(fmt.Errorf("collection is required")))
	}
	collection := *params.Collection

	shard := "*"
	if params.Shard != nil {
		shard = *params.Shard
	}

	if err := h.authorizer.Authorize(ctx, principal, authorization.READ, collection, shard); err != nil {
		return replication.NewGetCollectionShardingStateForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	var shardingState api.ShardingState
	var err error
	if params.Shard != nil {
		shardingState, err = h.replicationManager.QueryShardingStateByCollectionAndShard(params.HTTPRequest.Context(), collection, shard)
	} else {
		shardingState, err = h.replicationManager.QueryShardingStateByCollection(params.HTTPRequest.Context(), collection)
	}

	if errors.Is(err, replicationTypes.ErrNotFound) {
		return replication.NewGetCollectionShardingStateNotFound()
	} else if err != nil {
		return replication.NewGetCollectionShardingStateInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(err))
	}

	return replication.NewGetCollectionShardingStateOK().WithPayload(h.generateShardingStateResponse(shardingState.Collection, shardingState.Shards))
}
