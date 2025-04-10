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
	"crypto/rand"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"testing"

	authorizationMocks "github.com/weaviate/weaviate/mocks/usecases/auth/authorization"

	replicationMocks "github.com/weaviate/weaviate/mocks/cluster/replication/types"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
)

func createReplicationHandlerWithMocks(t *testing.T, logger *logrus.Logger) (*replicationHandler, *authorizationMocks.Authorizer, *replicationMocks.Manager) {
	t.Helper()
	mockAuthorizer := new(authorizationMocks.Authorizer)
	mockReplicationManager := new(replicationMocks.Manager)

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
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
			},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.On("ReplicationReplicateReplica", sourceNodeId, collection, shardId, targetNodeId).Return(nil)

		// WHEN
		response := handler.replicate(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicateOK{}, response)
		mockAuthorizer.AssertExpectations(t)
		mockReplicationManager.AssertExpectations(t)
	})

	t.Run("missing collection in request body", func(t *testing.T) {
		// GIVEN
		handler, _, _ := createReplicationHandlerWithMocks(t, createNullLogger(t))

		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
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
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:   &collection,
				ShardID:        &shardId,
				SourceNodeName: &sourceNodeId,
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
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				SourceNodeName:      &sourceNodeId,
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
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				ShardID:             &shardId,
				DestinationNodeName: &targetNodeId,
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
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
			},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.On("ReplicationReplicateReplica", sourceNodeId, collection, shardId, targetNodeId).Return(types.ErrInvalidRequest)

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
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
			},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.On("ReplicationReplicateReplica", sourceNodeId, collection, shardId, targetNodeId).Return(errors.New("target node does not exist"))

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
		params := replication.ReplicateParams{
			HTTPRequest: &http.Request{},
			Body: &models.ReplicationReplicateReplicaRequest{
				CollectionID:        &collection,
				DestinationNodeName: &targetNodeId,
				ShardID:             &shardId,
				SourceNodeName:      &sourceNodeId,
			},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("authorization error"))

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
		id := randomUint64()
		params := replication.ReplicationDetailsParams{
			ID:          strconv.FormatUint(id, 10),
			HTTPRequest: &http.Request{},
		}

		collection := fmt.Sprintf("Collection%d", randomInt(10))
		shardId := fmt.Sprintf("shard-%d", randomInt(10))
		sourceNodeId := fmt.Sprintf("node-%d", randomInt(5)*2)
		targetNodeId := fmt.Sprintf("node-%d", randomInt(5)*2+1)
		statusOptions := []string{
			models.ReplicationReplicateDetailsReplicaResponseStatusREADY,
			models.ReplicationReplicateDetailsReplicaResponseStatusINDEXING,
			models.ReplicationReplicateDetailsReplicaResponseStatusREPLICATIONDEHYDRATING,
			models.ReplicationReplicateDetailsReplicaResponseStatusREPLICATIONFINALIZING,
			models.ReplicationReplicateDetailsReplicaResponseStatusREPLICATIONHYDRATING,
		}
		status := randomString(statusOptions)

		expectedResponse := api.ReplicationDetailsResponse{
			Id:           id,
			Collection:   collection,
			ShardId:      shardId,
			SourceNodeId: sourceNodeId,
			TargetNodeId: targetNodeId,
			Status:       status,
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.On("GetReplicationDetailsByReplicationId", id).Return(expectedResponse, nil)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsOK{}, response)
		mockAuthorizer.AssertExpectations(t)
		mockReplicationManager.AssertExpectations(t)

		replicationDetails := response.(*replication.ReplicationDetailsOK)
		assert.Equal(t, strconv.FormatUint(id, 10), *replicationDetails.Payload.ID)
		assert.Equal(t, collection, *replicationDetails.Payload.Collection)
		assert.Equal(t, shardId, *replicationDetails.Payload.ShardID)
		assert.Equal(t, sourceNodeId, *replicationDetails.Payload.SourceNodeID)
		assert.Equal(t, targetNodeId, *replicationDetails.Payload.TargetNodeID)
		assert.Equal(t, status, *replicationDetails.Payload.Status)
	})

	t.Run("malformed request id", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, _ := createReplicationHandlerWithMocks(t, createNullLogger(t))
		params := replication.ReplicationDetailsParams{
			ID:          "foo",
			HTTPRequest: &http.Request{},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsBadRequest{}, response)
		mockAuthorizer.AssertExpectations(t)
	})

	t.Run("empty request id", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, _ := createReplicationHandlerWithMocks(t, createNullLogger(t))
		params := replication.ReplicationDetailsParams{
			ID:          "",
			HTTPRequest: &http.Request{},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsBadRequest{}, response)
		mockAuthorizer.AssertExpectations(t)
	})

	t.Run("request id not found", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))
		id := randomUint64()
		params := replication.ReplicationDetailsParams{
			ID:          strconv.FormatUint(id, 10),
			HTTPRequest: &http.Request{},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.On("GetReplicationDetailsByReplicationId", id).Return(api.ReplicationDetailsResponse{}, types.ErrReplicationOperationNotFound)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsNotFound{}, response)
		mockAuthorizer.AssertExpectations(t)
	})

	t.Run("internal server error", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationManager := createReplicationHandlerWithMocks(t, createNullLogger(t))
		id := randomUint64()
		params := replication.ReplicationDetailsParams{
			ID:          strconv.FormatUint(id, 10),
			HTTPRequest: &http.Request{},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationManager.On("GetReplicationDetailsByReplicationId", id).Return(api.ReplicationDetailsResponse{}, errors.New("internal error"))

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsInternalServerError{}, response)
		mockAuthorizer.AssertExpectations(t)
	})

	t.Run("authorization error", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, _ := createReplicationHandlerWithMocks(t, createNullLogger(t))
		id := randomUint64()
		params := replication.ReplicationDetailsParams{
			ID:          strconv.FormatUint(id, 10),
			HTTPRequest: &http.Request{},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("forbidden access"))

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

	n, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		panic(fmt.Sprintf("failed to generate random number in range [0, %d): %v", max, err))
	}
	return n.Int64()
}

func randomUint64() uint64 {
	maxUint64 := new(big.Int).SetUint64(^uint64(0))
	n, err := rand.Int(rand.Reader, maxUint64)
	if err != nil {
		panic(fmt.Sprintf("failed to generate random number in range [0, %s): %v", maxUint64.String(), err))
	}
	return n.Uint64()
}

func randomString(candidates []string) string {
	if len(candidates) == 0 {
		panic("candidates slice cannot be empty")
	}

	return candidates[randomInt(int64(len(candidates)))]
}
