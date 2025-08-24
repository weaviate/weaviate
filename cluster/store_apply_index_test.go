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

package cluster

import (
	"errors"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// setupTestData creates common test data for store apply index tests
func setupTestData(t *testing.T, initialIndex uint64) (MockStore, *raft.Log) {
	mockStore := NewMockStore(t, "Node-1", 0)
	mockStore.store.lastAppliedIndex.Store(initialIndex)
	mockStore.store.metrics = newStoreMetrics("Node-1", prometheus.NewPedanticRegistry())
	mockStore.store.metrics.fsmLastAppliedIndex.Set(float64(initialIndex))

	cls := &models.Class{
		Class: "TestClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	ss := &sharding.State{
		Physical: map[string]sharding.Physical{
			"T1": {
				Name:           "T1",
				BelongsToNodes: []string{"Node-1"},
				Status:         "HOT",
			},
		},
	}

	log := &raft.Log{
		Index: initialIndex + 1,
		Type:  raft.LogCommand,
		Data:  cmdAsBytes("TestClass", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{Class: cls, State: ss}, nil),
	}

	return mockStore, log
}

func TestStore_ApplyIndex(t *testing.T) {
	// Test case 1: Apply index is greater than last applied index
	t.Run("Apply index is greater than last applied index", func(t *testing.T) {
		mockStore, log := setupTestData(t, 100)
		mockStore.parser.On("ParseClass", mock.Anything).Return(nil)
		mockStore.indexer.On("AddClass", mock.Anything).Return(nil)
		mockStore.indexer.On("TriggerSchemaUpdateCallbacks").Return()

		result := mockStore.store.Apply(log)

		// Verify that the result contains no error
		resp, ok := result.(Response)
		assert.True(t, ok)
		assert.NoError(t, resp.Error)

		// Verify that lastAppliedIndex was updated
		currentIndex := mockStore.store.lastAppliedIndex.Load()
		assert.Equal(t, uint64(101), log.Index)
		assert.Equal(t, uint64(101), currentIndex, "lastAppliedIndex should be updated on success")
	})

	// Test case 2: Apply index fails due to parse error
	t.Run("Apply index fails due to parse error", func(t *testing.T) {
		mockStore, log := setupTestData(t, 100)
		mockStore.parser.On("ParseClass", mock.Anything).Return(errors.New("parse error"))
		result := mockStore.store.Apply(log)

		// Verify that the result contains an error
		resp, ok := result.(Response)
		assert.True(t, ok)
		assert.Error(t, resp.Error)

		// Verify that lastAppliedIndex was not updated
		assert.Equal(t, uint64(101), log.Index)
		// we depend on metrics to check if the index was updated
		// as we allow lastAppliedIndex to be updated even when there's an error
		// to handle edge cases in the DB layer for already released versions
		assert.Equal(t, float64(100), testutil.ToFloat64(mockStore.store.metrics.fsmLastAppliedIndex), "lastAppliedIndex should not be updated when there's an error")
	})
}
