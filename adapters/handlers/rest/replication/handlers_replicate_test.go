//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"fmt"
	"math/rand"
	"net/http"
	"testing"
	"time"

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
		shard := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNode := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNode := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		replicationType := randomReplicationType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				Collection: &collection,
				TargetNode: &targetNode,
				Shard:      &shard,
				SourceNode: &sourceNode,
				Type:       &replicationType,
			},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.EXPECT().ReplicationReplicateReplica(mock.Anything, mock.AnythingOfType("strfmt.UUID"), sourceNode, collection, shard, targetNode, replicationType).Return(nil)

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

		shard := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNode := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNode := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		replicationType := randomReplicationType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				TargetNode: &targetNode,
				Shard:      &shard,
				SourceNode: &sourceNode,
				Type:       &replicationType,
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
		shard := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNode := fmt.Sprintf("node-%d", randomInt(5)*2)
		replicationType := randomReplicationType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				Collection: &collection,
				Shard:      &shard,
				SourceNode: &sourceNode,
				Type:       &replicationType,
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
		sourceNode := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNode := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		replicationType := randomReplicationType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				Collection: &collection,
				TargetNode: &targetNode,
				SourceNode: &sourceNode,
				Type:       &replicationType,
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
		shard := fmt.Sprintf("shard-%d", randomInt(10))
		targetNode := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		replicationType := randomReplicationType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				Collection: &collection,
				Shard:      &shard,
				TargetNode: &targetNode,
				Type:       &replicationType,
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
		shard := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNode := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNode := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		replicationType := randomReplicationType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				Collection: &collection,
				TargetNode: &targetNode,
				Shard:      &shard,
				SourceNode: &sourceNode,
				Type:       &replicationType,
			},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.EXPECT().ReplicationReplicateReplica(mock.Anything, mock.AnythingOfType("strfmt.UUID"), sourceNode, collection, shard, targetNode, replicationType).Return(types.ErrInvalidRequest)

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
		shard := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNode := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNode := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		replicationType := randomReplicationType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				Collection: &collection,
				TargetNode: &targetNode,
				Shard:      &shard,
				SourceNode: &sourceNode,
				Type:       &replicationType,
			},
		}

		mockAuthorizer.EXPECT().Authorize(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.EXPECT().ReplicationReplicateReplica(mock.Anything, mock.AnythingOfType("strfmt.UUID"), sourceNode, collection, shard, targetNode, replicationType).Return(errors.New("target node does not exist"))

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
		shard := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNode := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNode := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		replicationType := randomReplicationType()
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				Collection: &collection,
				TargetNode: &targetNode,
				Shard:      &shard,
				SourceNode: &sourceNode,
				Type:       &replicationType,
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
		replicationType := randomReplicationType()

		startTime := time.Now().UnixMilli()
		expectedResponse := api.ReplicationDetailsResponse{
			Uuid:         id,
			Collection:   collection,
			ShardId:      shardId,
			SourceNodeId: sourceNodeId,
			TargetNodeId: targetNodeId,
			Status: api.ReplicationDetailsState{
				State:           status,
				Errors:          []api.ReplicationDetailsError{},
				StartTimeUnixMs: startTime,
			},
			StatusHistory:   []api.ReplicationDetailsState{},
			TransferType:    replicationType,
			StartTimeUnixMs: startTime,
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
		assert.Equal(t, shardId, *replicationDetails.Payload.Shard)
		assert.Equal(t, sourceNodeId, *replicationDetails.Payload.SourceNode)
		assert.Equal(t, targetNodeId, *replicationDetails.Payload.TargetNode)
		assert.Equal(t, status, replicationDetails.Payload.Status.State)
		assert.Equal(t, 0, len(replicationDetails.Payload.Status.Errors))
		assert.Equal(t, startTime, replicationDetails.Payload.Status.WhenStartedUnixMs)
		assert.Equal(t, 0, len(replicationDetails.Payload.StatusHistory))
		assert.Equal(t, replicationType, *replicationDetails.Payload.Type)
		assert.Equal(t, startTime, replicationDetails.Payload.WhenStartedUnixMs)
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

		startTime := time.Now().Add(-time.Hour).UnixMilli()
		firstErrorTime := time.Now().Add(-time.Hour).UnixMilli()
		secondErrorTime := time.Now().Add(-time.Hour).Add(time.Minute).UnixMilli()

		expectedResponse := api.ReplicationDetailsResponse{
			Uuid:         uuid,
			Id:           id,
			Collection:   collection,
			ShardId:      shardId,
			SourceNodeId: sourceNodeId,
			TargetNodeId: targetNodeId,
			Status: api.ReplicationDetailsState{
				State:  status,
				Errors: []api.ReplicationDetailsError{},
			},
			StatusHistory: []api.ReplicationDetailsState{
				{
					State:           historyStatus,
					Errors:          []api.ReplicationDetailsError{{Message: "error1", ErroredTimeUnixMs: firstErrorTime}, {Message: "error2", ErroredTimeUnixMs: secondErrorTime}},
					StartTimeUnixMs: startTime,
				},
			},
			StartTimeUnixMs: startTime,
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
		assert.Equal(t, shardId, *replicationDetails.Payload.Shard)
		assert.Equal(t, sourceNodeId, *replicationDetails.Payload.SourceNode)
		assert.Equal(t, targetNodeId, *replicationDetails.Payload.TargetNode)
		assert.Equal(t, status, replicationDetails.Payload.Status.State)
		assert.Equal(t, 0, len(replicationDetails.Payload.Status.Errors))
		assert.Equal(t, startTime, replicationDetails.Payload.Status.WhenStartedUnixMs)
		assert.Equal(t, historyStatus, replicationDetails.Payload.StatusHistory[0].State)
		assert.Equal(t, "error1", replicationDetails.Payload.StatusHistory[0].Errors[0].Message)
		assert.Equal(t, "error2", replicationDetails.Payload.StatusHistory[0].Errors[1].Message)
		assert.Equal(t, firstErrorTime, replicationDetails.Payload.StatusHistory[0].Errors[0].WhenErroredUnixMs)
		assert.Equal(t, secondErrorTime, replicationDetails.Payload.StatusHistory[0].Errors[1].WhenErroredUnixMs)
		assert.Equal(t, startTime, replicationDetails.Payload.WhenStartedUnixMs)
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

func randomReplicationType() string {
	if rand.Uint64()%2 == 0 {
		return api.COPY.String()
	}
	return api.MOVE.String()
}
