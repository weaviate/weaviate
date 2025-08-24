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
	"fmt"
	"math/rand"
	"net/http"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
)

func createReplicationHandlerWithMocks(t *testing.T, logger *logrus.Logger) (*replicationHandler, *authorization.MockAuthorizer, *types.MockManager) {
	t.Helper()
	mockAuthorizer := authorization.NewMockAuthorizer(t)
	mockReplicationManager := types.NewMockManager(t)

	handler := &replicationHandler{
		authorizer:         mockAuthorizer,
		replicationManager: mockReplicationManager,
		logger:             logger,
	}

	return handler, mockAuthorizer, mockReplicationManager
}

func TestReplicationReplicate(t *testing.T) {
	t.Run("successful replication", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		transferType := randomTransferType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
				TransferType:        &transferType,
			},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.EXPECT().ReplicationReplicateReplica(mock.Anything, mock.AnythingOfType("strfmt.UUID"), sourceNodeId, collection, shardId, targetNodeId, transferType).Return(nil)

		// WHEN
		response := handler.replicate(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicateOK{}, response)
		assert.NotNil(t, response.(*replication.ReplicateOK).Payload.ID)
		mockAuthorizer.AssertExpectations(t)
		mockReplicationManager.AssertExpectations(t)
	})

	t.Run("missing collection in request body", func(t *testing.T) {
		// GIVEN
		handler, _, _ := createReplicationHandlerWithMocks(t, createNullLogger(t))

		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		transferType := randomTransferType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
				TransferType:        &transferType,
			},
		}

		// WHEN
		response := handler.replicate(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicateBadRequest{}, response)
	})

	t.Run("missing target node id in request body", func(t *testing.T) {
		// GIVEN
		handler, _, _ := createReplicationHandlerWithMocks(t, createNullLogger(t))

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		transferType := randomTransferType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:   &collection,
				ShardID:        &shardId,
				SourceNodeName: &sourceNodeId,
				TransferType:   &transferType,
			},
		}

		// WHEN
		response := handler.replicate(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicateBadRequest{}, response)
	})

	t.Run("missing shard id in request body", func(t *testing.T) {
		// GIVEN
		handler, _, _ := createReplicationHandlerWithMocks(t, createNullLogger(t))

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		transferType := randomTransferType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				SourceNodeName:      &sourceNodeId,
				TransferType:        &transferType,
			},
		}

		// WHEN
		response := handler.replicate(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicateBadRequest{}, response)
	})

	t.Run("missing source node id in request body", func(t *testing.T) {
		// GIVEN
		handler, _, _ := createReplicationHandlerWithMocks(t, createNullLogger(t))

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		transferType := randomTransferType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				ShardID:             &shardId,
				DestinationNodeName: &targetNodeId,
				TransferType:        &transferType,
			},
		}

		// WHEN
		response := handler.replicate(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicateBadRequest{}, response)
	})

	t.Run("unprocessable entity error", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		transferType := randomTransferType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
				TransferType:        &transferType,
			},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.EXPECT().ReplicationReplicateReplica(mock.Anything, mock.AnythingOfType("strfmt.UUID"), sourceNodeId, collection, shardId, targetNodeId, transferType).Return(types.ErrInvalidRequest)

		// WHEN
		response := handler.replicate(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicateUnprocessableEntity{}, response)
		mockAuthorizer.AssertExpectations(t)
		mockReplicationManager.AssertExpectations(t)
	})

	t.Run("internal server error", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		transferType := randomTransferType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
				TransferType:        &transferType,
			},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.EXPECT().ReplicationReplicateReplica(mock.Anything, mock.AnythingOfType("strfmt.UUID"), sourceNodeId, collection, shardId, targetNodeId, transferType).Return(errors.New("target node does not exist"))

		// WHEN
		response := handler.replicate(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicateInternalServerError{}, response)
		mockAuthorizer.AssertExpectations(t)
		mockReplicationManager.AssertExpectations(t)
	})

	t.Run("authorization error", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, _ := createReplicationHandlerWithMocks(t, createNullLogger(t))

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		transferType := randomTransferType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
				TransferType:        &transferType,
			},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("authorization error"))

		// WHEN
		response := handler.replicate(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicateForbidden{}, response)
		mockAuthorizer.AssertExpectations(t)
	})
}

func TestGetReplicationDetailsByReplicationId(t *testing.T) {
	t.Run("successful retrieval", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))
		id := uuid4()
		params := replication.ReplicationDetailsParams{
			ID:          id,
			HTTPRequest: &http.Request{},
		}

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		statusOptions := []string{
			models.ReplicationReplicateDetailsReplicaStatusStateREGISTERED,
			models.ReplicationReplicateDetailsReplicaStatusStateHYDRATING,
			models.ReplicationReplicateDetailsReplicaStatusStateFINALIZING,
			models.ReplicationReplicateDetailsReplicaStatusStateDEHYDRATING,
			models.ReplicationReplicateDetailsReplicaStatusStateREADY,
			models.ReplicationReplicateDetailsReplicaStatusStateCANCELLED,
		}
		status := randomString(statusOptions)
		transferType := randomTransferType()

		expectedResponse := api.ReplicationDetailsResponse{
			Uuid:         id,
			Collection:   collection,
			ShardId:      shardId,
			SourceNodeId: sourceNodeId,
			TargetNodeId: targetNodeId,
			Status: api.ReplicationDetailsState{
				State:  status,
				Errors: []string{},
			},
			StatusHistory: []api.ReplicationDetailsState{},
			TransferType:  transferType,
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.EXPECT().GetReplicationDetailsByReplicationId(mock.Anything, id).Return(expectedResponse, nil)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsOK{}, response)
		mockAuthorizer.AssertExpectations(t)
		mockReplicationManager.AssertExpectations(t)

		replicationDetails := response.(*replication.ReplicationDetailsOK)
		assert.Equal(t, id, *replicationDetails.Payload.ID)
		assert.Equal(t, collection, *replicationDetails.Payload.Collection)
		assert.Equal(t, shardId, *replicationDetails.Payload.ShardID)
		assert.Equal(t, sourceNodeId, *replicationDetails.Payload.SourceNodeID)
		assert.Equal(t, targetNodeId, *replicationDetails.Payload.TargetNodeID)
		assert.Equal(t, status, replicationDetails.Payload.Status.State)
		assert.Equal(t, 0, len(replicationDetails.Payload.StatusHistory))
	})

	t.Run("successful retrieval with history", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))
		uuid := uuid4()
		id := uint64(randomInt(100))
		params := replication.ReplicationDetailsParams{
			ID:             uuid,
			HTTPRequest:    &http.Request{},
			IncludeHistory: &[]bool{true}[0],
		}

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		statusOptions := []string{
			models.ReplicationReplicateDetailsReplicaStatusStateREGISTERED,
			models.ReplicationReplicateDetailsReplicaStatusStateHYDRATING,
			models.ReplicationReplicateDetailsReplicaStatusStateFINALIZING,
			models.ReplicationReplicateDetailsReplicaStatusStateDEHYDRATING,
			models.ReplicationReplicateDetailsReplicaStatusStateREADY,
			models.ReplicationReplicateDetailsReplicaStatusStateCANCELLED,
		}
		status := randomString(statusOptions)
		historyStatus := randomString(statusOptions)

		expectedResponse := api.ReplicationDetailsResponse{
			Uuid:         uuid,
			Id:           id,
			Collection:   collection,
			ShardId:      shardId,
			SourceNodeId: sourceNodeId,
			TargetNodeId: targetNodeId,
			Status: api.ReplicationDetailsState{
				State:  status,
				Errors: []string{},
			},
			StatusHistory: []api.ReplicationDetailsState{
				{
					State:  historyStatus,
					Errors: []string{"error1", "error2"},
				},
			},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.EXPECT().GetReplicationDetailsByReplicationId(mock.Anything, mock.AnythingOfType("strfmt.UUID")).Return(expectedResponse, nil)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsOK{}, response)
		mockAuthorizer.AssertExpectations(t)
		mockReplicationManager.AssertExpectations(t)

		replicationDetails := response.(*replication.ReplicationDetailsOK)
		assert.Equal(t, uuid, *replicationDetails.Payload.ID)
		assert.Equal(t, collection, *replicationDetails.Payload.Collection)
		assert.Equal(t, shardId, *replicationDetails.Payload.ShardID)
		assert.Equal(t, sourceNodeId, *replicationDetails.Payload.SourceNodeID)
		assert.Equal(t, targetNodeId, *replicationDetails.Payload.TargetNodeID)
		assert.Equal(t, status, replicationDetails.Payload.Status.State)
		assert.Equal(t, historyStatus, replicationDetails.Payload.StatusHistory[0].State)
		assert.Equal(t, []string{"error1", "error2"}, replicationDetails.Payload.StatusHistory[0].Errors)
	})

	t.Run("request id not found authorized", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))
		id := uuid4()
		params := replication.ReplicationDetailsParams{
			ID:          id,
			HTTPRequest: &http.Request{},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Replications("*", "*")).Return(nil)
		mockReplicationManager.EXPECT().GetReplicationDetailsByReplicationId(mock.Anything, id).Return(api.ReplicationDetailsResponse{}, types.ErrReplicationOperationNotFound)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsNotFound{}, response)
		mockAuthorizer.AssertExpectations(t)
	})

	t.Run("request id not found forbidden", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))
		id := uuid4()
		params := replication.ReplicationDetailsParams{
			ID:          id,
			HTTPRequest: &http.Request{},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, authorization.READ, authorization.Replications("*", "*")).Return(fmt.Errorf("forbidden access"))
		mockReplicationManager.EXPECT().GetReplicationDetailsByReplicationId(mock.Anything, id).Return(api.ReplicationDetailsResponse{}, types.ErrReplicationOperationNotFound)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsForbidden{}, response)
		mockAuthorizer.AssertExpectations(t)
	})

	t.Run("internal server error", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))
		id := uuid4()
		params := replication.ReplicationDetailsParams{
			ID:          id,
			HTTPRequest: &http.Request{},
		}

		mockReplicationManager.EXPECT().GetReplicationDetailsByReplicationId(mock.Anything, id).Return(api.ReplicationDetailsResponse{}, errors.New("internal error"))

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsInternalServerError{}, response)
		mockAuthorizer.AssertExpectations(t)
	})

	t.Run("authorization error", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))
		id := uuid4()
		params := replication.ReplicationDetailsParams{
			ID:          id,
			HTTPRequest: &http.Request{},
		}

		// Retrieves details first by ID then authorizes on the collection/shard of the replication
		mockReplicationManager.EXPECT().GetReplicationDetailsByReplicationId(mock.Anything, id).Return(api.ReplicationDetailsResponse{}, nil)
		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("forbidden access"))

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsForbidden{}, response)
		mockAuthorizer.AssertExpectations(t)
	})
}

func createNullLogger(t *testing.T) *logrus.Logger {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	return logger
}

func randomInt(max int64) int64 {
	if max <= 0 {
		panic(fmt.Sprintf("max parameter must be positive, received %d", max))
	}

	return rand.Int63n(max)
}

func uuid4() strfmt.UUID {
	id, err := uuid.NewRandom()
	if err != nil {
		panic(fmt.Sprintf("failed to generate UUID: %v", err))
	}
	return strfmt.UUID(id.String())
}

func randomString(candidates []string) string {
	if len(candidates) == 0 {
		panic("candidates slice cannot be empty")
	}

	return candidates[randomInt(int64(len(candidates)))]
}

func randomTransferType() string {
	if rand.Uint64()%2 == 0 {
		return api.COPY.String()
	}
	return api.MOVE.String()
}
