//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	"github.com/stretchr/testify/require"

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

// testNamespace is the namespace the state-change cases seed and flip.
const testNamespace = "customer1"

// namespaceStateLog builds a real CHANGE_NAMESPACE_STATE entry. The
// sub-command carries the command policy version, so only store.Apply's own
// l.Index can reach StateChangeIndex.
func namespaceStateLog(index uint64, target api.NamespaceState) *raft.Log {
	return &raft.Log{
		Index: index,
		Type:  raft.LogCommand,
		Data: cmdAsBytes("", api.ApplyRequest_TYPE_CHANGE_NAMESPACE_STATE,
			api.ChangeNamespaceStateRequest{
				Name:        testNamespace,
				TargetState: target,
				Version:     api.NamespaceLatestCommandPolicyVersion,
			}, nil),
	}
}

// applyNamespaceState drives log through store.Apply and reads the namespace back.
func applyNamespaceState(t *testing.T, ms MockStore, log *raft.Log) api.Namespace {
	t.Helper()
	resp, ok := ms.store.Apply(log).(Response)
	require.True(t, ok)
	require.NoError(t, resp.Error)
	ns, ok := ms.cfg.NamespacesController.GetNamespace(testNamespace)
	require.True(t, ok)
	return ns
}

// TestStore_ApplyIndex_NamespaceCreate pins that a namespace is born
// carrying the log index of its own create command, so StateChangeIndex is
// never left unset. The sub-command carries the command policy version, so
// only store.Apply's own l.Index can reach the field.
func TestStore_ApplyIndex_NamespaceCreate(t *testing.T) {
	const initialIndex uint64 = 100

	ms, _ := setupTestData(t, initialIndex)
	log := &raft.Log{
		Index: initialIndex + 1,
		Type:  raft.LogCommand,
		Data: cmdAsBytes("", api.ApplyRequest_TYPE_ADD_NAMESPACE,
			api.AddNamespaceRequest{
				Namespace: api.Namespace{Name: testNamespace, HomeNodes: []string{"Node-1"}},
				Version:   api.NamespaceLatestCommandPolicyVersion,
			}, nil),
	}

	ns := applyNamespaceState(t, ms, log)

	assert.Equal(t, api.NamespaceStateActive, ns.State)
	assert.Equal(t, initialIndex+1, ns.StateChangeIndex,
		"the recorded index must be the RAFT log index, not the command policy version")
}

func TestStore_ApplyIndex_NamespaceStateChange(t *testing.T) {
	const initialIndex uint64 = 100

	seed := func(t *testing.T) MockStore {
		t.Helper()
		ms, _ := setupTestData(t, initialIndex)
		require.NoError(t, ms.cfg.NamespacesController.Create(
			api.Namespace{Name: testNamespace, HomeNodes: []string{"Node-1"}}, initialIndex))
		return ms
	}

	t.Run("accepted flip records the applied log index", func(t *testing.T) {
		ms := seed(t)
		ns := applyNamespaceState(t, ms, namespaceStateLog(initialIndex+1, api.NamespaceStateSuspended))

		assert.Equal(t, api.NamespaceStateSuspended, ns.State)
		assert.Equal(t, initialIndex+1, ns.StateChangeIndex,
			"the recorded index must be the RAFT log index, not the command policy version")
	})

	t.Run("successive flips record increasing indexes", func(t *testing.T) {
		ms := seed(t)
		first := applyNamespaceState(t, ms, namespaceStateLog(initialIndex+1, api.NamespaceStateSuspended))
		second := applyNamespaceState(t, ms, namespaceStateLog(initialIndex+5, api.NamespaceStateResuming))

		assert.Equal(t, initialIndex+5, second.StateChangeIndex)
		assert.Greater(t, second.StateChangeIndex, first.StateChangeIndex)
	})

	t.Run("same-state re-apply at a higher index leaves the recorded index alone", func(t *testing.T) {
		ms := seed(t)
		flipped := applyNamespaceState(t, ms, namespaceStateLog(initialIndex+1, api.NamespaceStateSuspended))
		reapplied := applyNamespaceState(t, ms, namespaceStateLog(initialIndex+9, api.NamespaceStateSuspended))

		assert.Equal(t, flipped.StateChangeIndex, reapplied.StateChangeIndex,
			"a re-applied command must not advance the recorded index")
	})

	t.Run("metadata-only voter still records the flip", func(t *testing.T) {
		ms := seed(t)
		ms.store.cfg.MetadataOnlyVoters = true

		// Namespace state is metadata, so a metadata-only voter must track
		// it. The indexer has no expectations set: any call to it fails.
		ns := applyNamespaceState(t, ms, namespaceStateLog(initialIndex+1, api.NamespaceStateSuspended))

		assert.Equal(t, api.NamespaceStateSuspended, ns.State)
		assert.Equal(t, initialIndex+1, ns.StateChangeIndex)
	})
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
