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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
)

type MockAuthorizer struct {
	mock.Mock
}

type MockReplicationStatusProvider struct {
	mock.Mock
}

func (m *MockAuthorizer) Authorize(principal *models.Principal, verb string, resources ...string) error {
	args := m.Called(principal, verb, resources)
	return args.Error(0)
}

func (m *MockAuthorizer) AuthorizeSilent(principal *models.Principal, verb string, resources ...string) error {
	return m.Authorize(principal, verb, resources...)
}

func (m *MockAuthorizer) FilterAuthorizedResources(principal *models.Principal, verb string, resources ...string) ([]string, error) {
	args := m.Called(principal, verb)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockReplicationStatusProvider) GetReplicationDetailsByReplicationId(id uint64) (api.ReplicationDetailsResponse, error) {
	args := m.Called(id)
	return args.Get(0).(api.ReplicationDetailsResponse), args.Error(1)
}

func createReplicationHandlerWithMocks(t *testing.T) (*replicationHandler, *MockAuthorizer, *MockReplicationStatusProvider) {
	t.Helper()
	mockAuthorizer := new(MockAuthorizer)
	mockReplicationStatusProvider := new(MockReplicationStatusProvider)

	logger := logrus.New()
	logger.SetOutput(logrus.StandardLogger().Out)

	handler := &replicationHandler{
		authorizer:                 mockAuthorizer,
		replicationManager:         nil, // not used at the moment
		replicationDetailsProvider: mockReplicationStatusProvider,
		logger:                     logger,
	}

	return handler, mockAuthorizer, mockReplicationStatusProvider
}

func TestGetReplicationDetailsByReplicationId(t *testing.T) {
	t.Run("successful retrieval", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationStatusProvider := createReplicationHandlerWithMocks(t)
		var id = randomUint64()
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
		mockReplicationStatusProvider.On("GetReplicationDetailsByReplicationId", id).Return(expectedResponse, nil)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsOK{}, response)
		mockAuthorizer.AssertExpectations(t)
		mockReplicationStatusProvider.AssertExpectations(t)

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
		handler, mockAuthorizer, _ := createReplicationHandlerWithMocks(t)
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
		handler, mockAuthorizer, _ := createReplicationHandlerWithMocks(t)
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
		handler, mockAuthorizer, mockReplicationStatusProvider := createReplicationHandlerWithMocks(t)
		var id = randomUint64()
		params := replication.ReplicationDetailsParams{
			ID:          strconv.FormatUint(id, 10),
			HTTPRequest: &http.Request{},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationStatusProvider.On("GetReplicationDetailsByReplicationId", id).Return(api.ReplicationDetailsResponse{}, types.ErrReplicationOperationNotFound)

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsNotFound{}, response)
		mockAuthorizer.AssertExpectations(t)
	})

	t.Run("internal server error", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, mockReplicationStatusProvider := createReplicationHandlerWithMocks(t)
		var id = randomUint64()
		params := replication.ReplicationDetailsParams{
			ID:          strconv.FormatUint(id, 10),
			HTTPRequest: &http.Request{},
		}

		mockAuthorizer.On("Authorize", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockReplicationStatusProvider.On("GetReplicationDetailsByReplicationId", id).Return(api.ReplicationDetailsResponse{}, errors.New("internal error"))

		// WHEN
		response := handler.getReplicationDetailsByReplicationId(params, &models.Principal{})

		// THEN
		assert.IsType(t, &replication.ReplicationDetailsInternalServerError{}, response)
		mockAuthorizer.AssertExpectations(t)
	})

	t.Run("authorization error", func(t *testing.T) {
		// GIVEN
		handler, mockAuthorizer, _ := createReplicationHandlerWithMocks(t)
		var id = randomUint64()
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
