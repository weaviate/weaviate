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

package router_test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	schemaTypes "github.com/weaviate/weaviate/cluster/schema/types"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/sharding/config"
)

func createShardingStateWithShards(shards []string) *sharding.State {
	state := &sharding.State{
		Physical: make(map[string]sharding.Physical),
		Config:   config.Config{},
	}

	for _, shard := range shards {
		state.Physical[shard] = sharding.Physical{
			Name:           shard,
			BelongsToNodes: []string{"node1", "node2"},
			OwnsPercentage: 1.0 / float64(len(shards)),
		}
	}

	return state
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_NoShards(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")

	emptyState := createShardingStateWithShards([]string{})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(emptyState)

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "")

	require.NoError(t, err, "unexpected error while getting read/write replica locations")
	require.Empty(t, rs.Replicas, "read replica locations should be empty")
	require.Empty(t, ws.Replicas, "write replica locations should be empty")
	require.Empty(t, ws.AdditionalReplicas, "additional write replica locations should be empty")
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_OneShard(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3")

	shards := []string{"shard1"}
	state := createShardingStateWithShards(shards)
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "")

	expectedReadReplicas := types.ReadReplicaSet{
		Replicas: []types.Replica{
			{
				NodeName:  "node1",
				ShardName: "shard1",
				HostAddr:  "node1",
			},
			{
				NodeName:  "node2",
				ShardName: "shard1",
				HostAddr:  "node2",
			},
		},
	}

	expectedWriteReplicas := types.WriteReplicaSet{
		Replicas: []types.Replica{
			{
				NodeName:  "node1",
				ShardName: "shard1",
				HostAddr:  "node1",
			},
		},
		AdditionalReplicas: []types.Replica{
			{
				NodeName:  "node2",
				ShardName: "shard1",
				HostAddr:  "node2",
			},
		},
	}

	require.NoError(t, err, "unexpected error while getting read/write replica locations")
	require.Equal(t, expectedReadReplicas, rs)
	require.Equal(t, expectedWriteReplicas, ws)
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_MultipleShards(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3")

	shards := []string{"shard1", "shard2", "shard3"}
	state := createShardingStateWithShards(shards)
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard2").Return([]string{"node2", "node3"}, nil)
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard3").Return([]string{"node3", "node1"}, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard2", []string{"node2", "node3"}).
		Return([]string{"node2", "node3"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard2", []string{"node2", "node3"}).
		Return([]string{"node2"}, []string{"node3"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard3", []string{"node3", "node1"}).
		Return([]string{"node3", "node1"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard3", []string{"node3", "node1"}).
		Return([]string{"node3"}, []string{"node1"})

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "")

	expectedReadReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"},
		{NodeName: "node2", ShardName: "shard1", HostAddr: "node2"},
		{NodeName: "node2", ShardName: "shard2", HostAddr: "node2"},
		{NodeName: "node3", ShardName: "shard2", HostAddr: "node3"},
		{NodeName: "node3", ShardName: "shard3", HostAddr: "node3"},
		{NodeName: "node1", ShardName: "shard3", HostAddr: "node1"},
	}

	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"},
		{NodeName: "node2", ShardName: "shard2", HostAddr: "node2"},
		{NodeName: "node3", ShardName: "shard3", HostAddr: "node3"},
	}

	expectedAdditionalWriteReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "shard1", HostAddr: "node2"},
		{NodeName: "node3", ShardName: "shard2", HostAddr: "node3"},
		{NodeName: "node1", ShardName: "shard3", HostAddr: "node1"},
	}

	require.NoError(t, err, "unexpected error while getting read/write replica locations")
	require.ElementsMatch(t, expectedReadReplicas, rs.Replicas)
	require.ElementsMatch(t, expectedWriteReplicas, ws.Replicas)
	require.ElementsMatch(t, expectedAdditionalWriteReplicas, ws.AdditionalReplicas)
}

func TestSingleTenantRouter_GetWriteReplicasLocation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	ws, err := r.GetWriteReplicasLocation("TestClass", "", "")
	require.NoError(t, err, "unexpected error while getting write replicas")

	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"},
	}

	expectedAdditionalWriteReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "shard1", HostAddr: "node2"},
	}

	require.ElementsMatch(t, expectedWriteReplicas, ws.Replicas)
	require.ElementsMatch(t, expectedAdditionalWriteReplicas, ws.AdditionalReplicas)
}

func TestSingleTenantRouter_GetReadReplicasLocation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	readReplicas, err := r.GetReadReplicasLocation("TestClass", "", "")
	expectedReadReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"},
		{NodeName: "node2", ShardName: "shard1", HostAddr: "node2"},
	}

	require.NoError(t, err)
	require.ElementsMatch(t, expectedReadReplicas, readReplicas.Replicas)
}

func TestSingleTenantRouter_ErrorInMiddleOfShardProcessing(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")

	state := createShardingStateWithShards([]string{"shard1", "shard2", "shard3"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)

	// First shard success
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	// Second shard failure
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard2").Return([]string{}, errors.New("shard2 error"))

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "shard2 error")
	require.Empty(t, rs.Replicas)
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_Success(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	mockSchemaReader.EXPECT().
		ShardReplicas("TestClass", "luke").
		Return([]string{"node1"}, nil)

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().
		OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1", "node2"})

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{"node2"})

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "luke", "")

	expectedReadReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "luke", HostAddr: "node1"},
		{NodeName: "node2", ShardName: "luke", HostAddr: "node2"},
	}
	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "luke", HostAddr: "node1"},
	}
	expectedAdditionalWriteReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "luke", HostAddr: "node2"},
	}

	require.NoError(t, err)
	require.ElementsMatch(t, expectedReadReplicas, rs.Replicas)
	require.ElementsMatch(t, expectedWriteReplicas, ws.Replicas)
	require.ElementsMatch(t, expectedAdditionalWriteReplicas, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_TenantNotFound(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	tenantStatus := map[string]string{}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, errors.New("tenant not found: \"luke\""))

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "luke", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not found: \"luke\"")
	require.Empty(t, rs.Replicas)
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_TenantNotActive(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusCOLD,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "luke", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while checking tenant active status: \"luke\"")
	require.Empty(t, rs.Replicas)
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_NonTenantRequestForMultiTenant(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	metadataReader := schemaTypes.NewMockSchemaReader(t)

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		metadataReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "class TestClass has multi-tenancy enabled, but request was without tenant")
	require.Empty(t, rs.Replicas)
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_GetWriteReplicasLocation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().
		OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{"node2"})

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	ws, err := r.GetWriteReplicasLocation("TestClass", "luke", "")
	require.NoError(t, err)

	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "luke", HostAddr: "node1"},
	}
	expectedAdditionalWriteReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "luke", HostAddr: "node2"},
	}

	require.Equal(t, expectedWriteReplicas, ws.Replicas)
	require.Equal(t, expectedAdditionalWriteReplicas, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_GetReadReplicasLocation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().
		OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"})
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	readReplicas, err := r.GetReadReplicasLocation("TestClass", "luke", "")
	require.NoError(t, err)

	expected := []types.Replica{
		{NodeName: "node1", ShardName: "luke", HostAddr: "node1"},
	}
	require.Equal(t, expected, readReplicas.Replicas)
}

func TestMultiTenantRouter_TenantStatusChangeDuringOperation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)
	tenantStatusFirst := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	tenantStatusSecond := map[string]string{
		"luke": models.TenantActivityStatusFREEZING,
	}

	mockSchemaGetter.EXPECT().
		OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatusFirst, nil).Once()
	mockSchemaGetter.EXPECT().
		OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatusSecond, nil).Once()

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}).Once()
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{}).Once()

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "luke", "")
	require.NoError(t, err)

	expected := []types.Replica{
		{NodeName: "node1", ShardName: "luke", HostAddr: "node1"},
	}
	require.Equal(t, expected, rs.Replicas)
	require.Equal(t, expected, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)

	rs, ws, err = r.GetReadWriteReplicasLocation("TestClass", "luke", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error while checking tenant active status: \"luke\"")
	require.Empty(t, rs.Replicas)
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_VariousTenantStatuses(t *testing.T) {
	statusTests := []struct {
		status    string
		shouldErr bool
		errMsg    string
	}{
		{models.TenantActivityStatusHOT, false, ""},
		{models.TenantActivityStatusCOLD, true, "error while checking tenant active status"},
		{models.TenantActivityStatusFROZEN, true, "error while checking tenant active status"},
		{models.TenantActivityStatusFREEZING, true, "error while checking tenant active status"},
		{"UNKNOWN_STATUS", true, "error while checking tenant active status"},
	}

	for _, test := range statusTests {
		t.Run("status_"+test.status, func(t *testing.T) {
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
			mockNodeSelector := mocks.NewMockNodeSelector("node1")
			mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

			tenantStatus := map[string]string{
				"luke": test.status,
			}
			mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
				Return(tenantStatus, nil)

			var expectedReplicas []types.Replica
			if !test.shouldErr {
				mockSchemaReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)
				mockReplicationFSM.EXPECT().
					FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
					Return([]string{"node1"})
				mockReplicationFSM.EXPECT().
					FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
					Return([]string{"node1"}, []string{})
				expectedReplicas = []types.Replica{
					{NodeName: "node1", ShardName: "luke", HostAddr: "node1"},
				}
			}

			r := router.NewBuilder(
				"TestClass",
				true,
				mockNodeSelector,
				mockSchemaGetter,
				mockSchemaReader,
				mockReplicationFSM,
			).Build()

			rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "luke", "")

			if test.shouldErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errMsg)
				require.Empty(t, rs.Replicas)
				require.Empty(t, ws.Replicas)
				require.Empty(t, ws.AdditionalReplicas)
			} else {
				require.NoError(t, err)
				require.Equal(t, expectedReplicas, rs.Replicas)
				require.Equal(t, expectedReplicas, ws.Replicas)
				require.Empty(t, ws.AdditionalReplicas)
			}
		})
	}
}

func TestSingleTenantRouter_BuildReadRoutingPlan_NoReplicas(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")
	emptyState := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(emptyState)
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{}, nil)
	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shard1", []string{}).
		Return([]string{})

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "shard1", []string{}).
		Return([]string{}, []string{})

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()
	rs, err := r.GetReadReplicasLocation("TestClass", "", "shard1")

	require.NoError(t, err)
	require.Equal(t, []types.Replica(nil), rs.Replicas)
}

func TestMultiTenantRouter_BuildReadRoutingPlan_NoReplicas(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{}, nil)

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().
		OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "luke", []string{}).
		Return([]string{})

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "luke", []string{}).
		Return([]string{}, []string{})

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()
	rs, err := r.GetReadReplicasLocation("TestClass", "luke", "")
	require.NoError(t, err)
	require.Empty(t, rs.Replicas, "should have empty replicas when no replicas available")
}

func TestMultiTenantRouter_BuildReadRoutingPlan_Success(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)
	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()
	rs, err := r.GetReadReplicasLocation("TestClass", "luke", "")
	require.NoError(t, err)
	require.Equal(t, []types.Replica{{
		NodeName:  "node1",
		ShardName: "luke",
		HostAddr:  "node1",
	}}, rs.Replicas)
}

func TestMultiTenantRouter_BuildRoutingPlan_TenantNotFoundDuringBuild(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	metadataReader := schemaTypes.NewMockSchemaReader(t)

	tenantStatus := map[string]string{}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "nonexistent").
		Return(tenantStatus, nil)

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		metadataReader,
		mockReplicationFSM,
	).Build()
	rs, err := r.GetReadReplicasLocation("TestClass", "nonexistent", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error while checking tenant existence: \"nonexistent\"")
	require.Empty(t, rs.Replicas)
}

func TestRouter_NodeHostname(t *testing.T) {
	tests := []struct {
		name         string
		partitioning bool
	}{
		{"single-tenant", false},
		{"multi-tenant", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockNodeSelector := cluster.NewMockNodeSelector(t)
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
			mockNodeSelector.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
			mockNodeSelector.EXPECT().NodeHostname("node2").Return("", false)

			var mockSchemaReader schemaTypes.SchemaReader
			if !tt.partitioning {
				mockSchemaReader = schemaTypes.NewMockSchemaReader(t)
			}

			r := router.NewBuilder(
				"TestClass",
				tt.partitioning,
				mockNodeSelector,
				mockSchemaGetter,
				mockSchemaReader,
				mockReplicationFSM,
			).Build()

			hostname, ok := r.NodeHostname("node1")
			require.True(t, ok)
			require.Equal(t, "host1.example.com", hostname)

			hostname, ok = r.NodeHostname("node2")
			require.False(t, ok)
			require.Empty(t, hostname)
		})
	}
}

func TestRouter_AllHostnames(t *testing.T) {
	tests := []struct {
		name         string
		partitioning bool
	}{
		{"single-tenant", false},
		{"multi-tenant", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3")
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)

			var mockSchemaReader schemaTypes.SchemaReader
			if !tt.partitioning {
				mockSchemaReader = schemaTypes.NewMockSchemaReader(t)
			}

			expectedHostnames := []string{"node1", "node2", "node3"}

			r := router.NewBuilder(
				"TestClass",
				tt.partitioning,
				mockNodeSelector,
				mockSchemaGetter,
				mockSchemaReader,
				mockReplicationFSM,
			).Build()

			hostnames := r.AllHostnames()
			require.Equal(t, expectedHostnames, hostnames)
		})
	}
}

func TestMultiTenantRouter_MultipleTenantsSameCollection(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3", "node4")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	tenants := map[string][]string{
		"alice":   {"node1", "node2"},
		"bob":     {"node2", "node3"},
		"charlie": {"node1", "node3"},
		"diana":   {"node3", "node4"},
	}

	for tenant, replicas := range tenants {
		tenantStatus := map[string]string{tenant: models.TenantActivityStatusHOT}
		mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", tenant).
			Return(tenantStatus, nil)
		mockSchemaReader.EXPECT().ShardReplicas("TestClass", tenant).Return(replicas, nil)
		mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", tenant, replicas).
			Return(replicas)
		mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", tenant, replicas).
			Return([]string{replicas[0]}, replicas[1:])
	}

	r := router.NewBuilder("TestClass", true, mockNodeSelector,
		mockSchemaGetter, mockSchemaReader, mockReplicationFSM).Build()

	for tenant, expected := range tenants {
		rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", tenant, "")

		require.NoError(t, err, "unexpected error for tenant %s", tenant)

		require.Equal(t, sorted(expected), sorted(rs.NodeNames()), "read replicas mismatch for tenant %s", tenant)
		require.Equal(t, []string{expected[0]}, ws.NodeNames(), "write replicas mismatch for tenant %s", tenant)

		if len(expected) > 1 {
			require.Equal(t, sorted(expected[1:]), sorted(ws.AdditionalNodeNames()), "additional writes mismatch for tenant %s", tenant)
		} else {
			require.Empty(t, ws.AdditionalReplicas, "additional writes should be empty for tenant %s", tenant)
		}
	}
}

// sorted returns a sorted copy of a string slice
func sorted(input []string) []string {
	cp := append([]string(nil), input...)
	sort.Strings(cp)
	return cp
}

func TestMultiTenantRouter_MixedTenantStates(t *testing.T) {
	tenants := map[string]struct {
		status      string
		shouldWork  bool
		description string
	}{
		"active-tenant-1": {models.TenantActivityStatusHOT, true, "router error for active-tenant-1"},
		"active-tenant-2": {models.TenantActivityStatusHOT, true, "router error for active-tenant-2"},
		"cold-tenant":     {models.TenantActivityStatusCOLD, false, "router error for cold-tenant"},
		"frozen-tenant":   {models.TenantActivityStatusFROZEN, false, "router error for frozen tenant"},
		"freezing-tenant": {models.TenantActivityStatusFREEZING, false, "router error  for freezing tenant"},
	}

	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	for tenantName, tenantsStatus := range tenants {
		tenantStatus := map[string]string{tenantName: tenantsStatus.status}
		mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", tenantName).
			Return(tenantStatus, nil)

		if tenantsStatus.shouldWork {
			mockSchemaReader.EXPECT().ShardReplicas("TestClass", tenantName).Return([]string{"node1", "node2"}, nil)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", tenantName, []string{"node1", "node2"}).
				Return([]string{"node1", "node2"})
			mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", tenantName, []string{"node1", "node2"}).
				Return([]string{"node1"}, []string{"node2"})
		}
	}

	r := router.NewBuilder("TestClass", true, mockNodeSelector,
		mockSchemaGetter, mockSchemaReader, mockReplicationFSM).Build()

	for tenantName, tenantsStatus := range tenants {
		t.Run(tenantName, func(t *testing.T) {
			rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", tenantName, "")

			if tenantsStatus.shouldWork {
				require.NoError(t, err, "%s: should work", tenantsStatus.description)
				require.ElementsMatch(t, []string{"node1", "node2"}, rs.NodeNames())
				require.Equal(t, []string{"node1"}, ws.NodeNames())
				require.Equal(t, []string{"node2"}, ws.AdditionalNodeNames())
			} else {
				require.Error(t, err, "%s: should fail", tenantsStatus.description)
				require.Contains(t, err.Error(), "router error 'tenant status'", "error should mention tenant not active")
				require.Empty(t, rs.Replicas)
				require.Empty(t, ws.Replicas)
				require.Empty(t, ws.AdditionalReplicas)
			}
		})
	}
}

func TestMultiTenantRouter_SameTenantDifferentCollections(t *testing.T) {
	collections := []string{"Articles", "Users", "Products"}
	tenantName := "alice"

	for _, collection := range collections {
		t.Run(collection, func(t *testing.T) {
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
			mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3")
			mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

			var expectedReplicas []string
			switch collection {
			case "Articles":
				expectedReplicas = []string{"node1", "node2"}
			case "Users":
				expectedReplicas = []string{"node2", "node3"}
			case "Products":
				expectedReplicas = []string{"node1", "node3"}
			}

			tenantStatus := map[string]string{tenantName: models.TenantActivityStatusHOT}
			mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, collection, tenantName).
				Return(tenantStatus, nil)
			mockSchemaReader.EXPECT().ShardReplicas(collection, tenantName).Return(expectedReplicas, nil)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasRead(collection, tenantName, expectedReplicas).
				Return(expectedReplicas)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite(collection, tenantName, expectedReplicas).
				Return([]string{expectedReplicas[0]}, expectedReplicas[1:])

			r := router.NewBuilder(collection, true, mockNodeSelector,
				mockSchemaGetter, mockSchemaReader, mockReplicationFSM).Build()

			rs, ws, err := r.GetReadWriteReplicasLocation(collection, tenantName, "")

			require.NoError(t, err, "unexpected error for collection %s", collection)
			require.ElementsMatch(t, expectedReplicas, rs.NodeNames(), "read replicas mismatch for collection %s", collection)
			require.Equal(t, []string{expectedReplicas[0]}, ws.NodeNames(), "write replicas mismatch for collection %s", collection)
			require.ElementsMatch(t, expectedReplicas[1:], ws.AdditionalNodeNames(), "additional writes mismatch for collection %s", collection)
		})
	}
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_SpecificRandomShard(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3", "node4", "node5")

	allShards := []string{"shard1", "shard2", "shard3", "shard4", "shard5"}
	state := createShardingStateWithShards(allShards)
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
	targetShard := allShards[rand.Intn(len(allShards))]
	shardToNodes := map[string][]string{
		"shard1": {"node1", "node2"},
		"shard2": {"node2", "node3"},
		"shard3": {"node3", "node4"},
		"shard4": {"node4", "node5"},
		"shard5": {"node5", "node1"},
	}

	targetNodes := shardToNodes[targetShard]
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", targetShard).Return(targetNodes, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", targetShard, targetNodes).
		Return(targetNodes)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", targetShard, targetNodes).
		Return([]string{targetNodes[0]}, targetNodes[1:])

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", targetShard)

	var expectedReadReplicas []types.Replica
	var expectedWriteReplicas []types.Replica
	var expectedAdditionalWriteReplicas []types.Replica

	for _, node := range targetNodes {
		expectedReadReplicas = append(expectedReadReplicas, types.Replica{
			NodeName: node, ShardName: targetShard, HostAddr: node,
		})
	}

	expectedWriteReplicas = []types.Replica{
		{NodeName: targetNodes[0], ShardName: targetShard, HostAddr: targetNodes[0]},
	}

	for _, node := range targetNodes[1:] {
		expectedAdditionalWriteReplicas = append(expectedAdditionalWriteReplicas, types.Replica{
			NodeName: node, ShardName: targetShard, HostAddr: node,
		})
	}

	require.NoError(t, err)
	require.Equal(t, expectedReadReplicas, rs.Replicas)
	require.Equal(t, expectedWriteReplicas, ws.Replicas)
	require.Equal(t, expectedAdditionalWriteReplicas, ws.AdditionalReplicas)
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_InvalidShard(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")

	shards := []string{"shard1", "shard2"}
	state := createShardingStateWithShards(shards)
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	rs, ws, err := r.GetReadWriteReplicasLocation("TestClass", "", "invalid_shard")

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while trying to find shard: invalid_shard in collection: TestClass")
	require.Empty(t, rs.Replicas)
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestSingleTenantRouter_BroadcastVsTargeted(t *testing.T) {
	allShards := []string{"shard1", "shard2", "shard3", "shard4", "shard5"}
	randomShard := allShards[rand.Intn(len(allShards))]

	testCases := []struct {
		name         string
		shard        string
		expectShards []string
		description  string
	}{
		{
			name:         "broadcast_empty_shard",
			shard:        "",
			expectShards: allShards,
			description:  "empty shard should target all shards",
		},
		{
			name:         "targeted_random_shard",
			shard:        randomShard,
			expectShards: []string{randomShard},
			description:  fmt.Sprintf("specific shard %s should target only that shard", randomShard),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
			mockNodeSelector := cluster.NewMockNodeSelector(t)

			state := createShardingStateWithShards(allShards)
			mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)

			for _, shard := range testCase.expectShards {
				mockSchemaReader.EXPECT().ShardReplicas("TestClass", shard).Return([]string{"node1"}, nil)
				mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", shard, []string{"node1"}).
					Return([]string{"node1"})
				mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", shard, []string{"node1"}).
					Return([]string{"node1"}, []string{})
				mockNodeSelector.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
			}

			r := router.NewBuilder(
				"TestClass",
				false,
				mockNodeSelector,
				mockSchemaGetter,
				mockSchemaReader,
				mockReplicationFSM,
			).Build()
			rs, err := r.GetReadReplicasLocation("TestClass", "", testCase.shard)
			require.NoError(t, err, "unexpected error for %s", testCase.description)
			actualShards := rs.Shards()
			require.ElementsMatch(t, testCase.expectShards, actualShards, "shard targeting mismatch for %s", testCase.description)
		})
	}
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_NoWriteReplicas(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)

	emptyState := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(emptyState)
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2", "node3"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2", "node3"}).
		Return([]string{"node1", "node2", "node3"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2", "node3"}).
		Return([]string{}, []string{})
	mockNodeSelector.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockNodeSelector.EXPECT().NodeHostname("node2").Return("host2.example.com", true)
	mockNodeSelector.EXPECT().NodeHostname("node3").Return("host3.example.com", true)

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()
	ws, err := r.GetWriteReplicasLocation("TestClass", "", "shard1")
	require.NoError(t, err)
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_MultipleShards(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)

	shards := []string{"shard1", "shard2", "shard3"}
	state := createShardingStateWithShards(shards)
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)

	// Setup expectations for all shards
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	for _, node := range []string{"node1", "node2", "node3"} {
		mockNodeSelector.EXPECT().NodeHostname(node).Return(node+".example.com", true).Maybe()
	}

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()
	ws, err := r.GetWriteReplicasLocation("TestClass", "", "shard1")
	require.NoError(t, err)

	// Should contain write replicas from all shards
	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "shard1", HostAddr: "node1.example.com"},
	}

	expectedAdditionalReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "shard1", HostAddr: "node2.example.com"},
	}

	require.ElementsMatch(t, expectedWriteReplicas, ws.Replicas)
	require.ElementsMatch(t, expectedAdditionalReplicas, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_Success(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "alice").Return([]string{"node1", "node2"}, nil)

	tenantStatus := map[string]string{
		"alice": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "alice").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "alice", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "alice", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	mockNodeSelector.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockNodeSelector.EXPECT().NodeHostname("node2").Return("host2.example.com", true)

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()
	ws, err := r.GetWriteReplicasLocation("TestClass", "alice", "")
	require.NoError(t, err)
	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "alice", HostAddr: "host1.example.com"},
	}
	expectedAdditionalWriteReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "alice", HostAddr: "host2.example.com"},
	}
	// Note: AdditionalReplicaSet should be empty in multi-tenant write plan based on the code
	require.Equal(t, expectedWriteReplicas, ws.Replicas)
	require.Equal(t, expectedAdditionalWriteReplicas, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_NoWriteReplicas(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "alice").Return([]string{}, nil)

	tenantStatus := map[string]string{
		"alice": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "alice").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "alice", []string{}).
		Return([]string{})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "alice", []string{}).
		Return([]string{}, []string{})

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()
	ws, err := r.GetWriteReplicasLocation("TestClass", "alice", "")
	require.NoError(t, err)
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_TenantValidation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()
	ws, err := r.GetWriteReplicasLocation("TestClass", "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "class TestClass has multi-tenancy enabled, but request was without tenant")
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_TenantNotActive(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	tenantStatus := map[string]string{
		"alice": models.TenantActivityStatusCOLD,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "alice").
		Return(tenantStatus, nil)

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()
	ws, err := r.GetWriteReplicasLocation("TestClass", "alice", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error while checking tenant active status")
	require.Empty(t, ws.Replicas)
	require.Empty(t, ws.AdditionalReplicas)
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_SpecifiedShard(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)

	state := createShardingStateWithShards([]string{"shardA"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shardA").
		Return([]string{"node1", "node2"}, nil)

	mockReplFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shardA", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "shardA", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	mockNodeSelector.EXPECT().NodeHostname("node1").Return("host1", true)
	mockNodeSelector.EXPECT().NodeHostname("node2").Return("host2", true)
	mockNodeSelector.EXPECT().LocalName().Return("node1")

	r := router.NewBuilder("TestClass", false, mockNodeSelector,
		mockSchemaGetter, mockSchemaReader, mockReplFSM).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:              "",
		Shard:               "shardA",
		ConsistencyLevel:    types.ConsistencyLevelOne,
		DirectCandidateNode: "",
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.NoError(t, err)

	want := []types.Replica{{NodeName: "node1", ShardName: "shardA", HostAddr: "host1"}}
	require.Equal(t, want, plan.ReplicaSet.Replicas)
	require.Equal(t, []types.Replica{{NodeName: "node2", ShardName: "shardA", HostAddr: "host2"}},
		plan.ReplicaSet.AdditionalReplicas)
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_DefaultShard(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)

	tenant := "luke"

	mockSchemaGetter.EXPECT().
		OptimisticTenantStatus(mock.Anything, "TestClass", tenant).
		Return(map[string]string{tenant: models.TenantActivityStatusHOT}, nil)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", tenant).
		Return([]string{"node1", "node2"}, nil)

	mockReplFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", tenant, []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", tenant, []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	mockNodeSelector.EXPECT().NodeHostname("node1").Return("host1", true)
	mockNodeSelector.EXPECT().NodeHostname("node2").Return("host2", true)
	mockNodeSelector.EXPECT().LocalName().Return("node1")

	r := router.NewBuilder("TestClass", true, mockNodeSelector,
		mockSchemaGetter, mockSchemaReader, mockReplFSM).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:              tenant,
		Shard:               "",
		ConsistencyLevel:    types.ConsistencyLevelOne,
		DirectCandidateNode: "",
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.NoError(t, err)

	want := []types.Replica{{NodeName: "node1", ShardName: tenant, HostAddr: "host1"}}
	require.Equal(t, want, plan.ReplicaSet.Replicas)
	require.Equal(t, []types.Replica{{NodeName: "node2", ShardName: tenant, HostAddr: "host2"}},
		plan.ReplicaSet.AdditionalReplicas)
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_NoReplicas(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")

	emptyState := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(emptyState)
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1"}).
		Return([]string{}, []string{}) // No write replicas

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:           "",
		Shard:            "shard1",
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no replica found")
	require.Empty(t, plan.ReplicaSet.Replicas)
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_TenantValidation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:           "some-tenant",
		Shard:            "shard1",
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "class TestClass has multi-tenancy disabled, but request was with tenant")
	require.Empty(t, plan.ReplicaSet.Replicas)
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_ConsistencyLevelValidation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:           "",
		Shard:            "shard1",
		ConsistencyLevel: "INVALID_LEVEL", // Invalid consistency level
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.NoError(t, err)
	require.Equal(t, 1, plan.IntConsistencyLevel)
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_ReplicaOrdering(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3")

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2", "node3"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2", "node3"}).
		Return([]string{"node1", "node2", "node3"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2", "node3"}).
		Return([]string{"node1", "node2"}, []string{"node3"})

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:              "",
		Shard:               "shard1",
		ConsistencyLevel:    types.ConsistencyLevelOne,
		DirectCandidateNode: "node2", // Should be ordered first
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.NoError(t, err)
	require.Equal(t, "node2", plan.ReplicaSet.Replicas[0].NodeName, "DirectCandidateNode should be first")
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_NoReplicas(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "alice").Return([]string{"node1"}, nil)

	tenantStatus := map[string]string{
		"alice": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "alice").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "alice", []string{"node1"}).
		Return([]string{"node1"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "alice", []string{"node1"}).
		Return([]string{}, []string{}) // No write replicas

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:           "alice",
		Shard:            "",
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no replica found")
	require.Empty(t, plan.ReplicaSet.Replicas)
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_ConsistencyLevelValidation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "alice").Return([]string{"node1", "node2"}, nil)

	tenantStatus := map[string]string{
		"alice": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "alice").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "alice", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "alice", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:           "alice",
		Shard:            "",
		ConsistencyLevel: "INVALID_LEVEL",
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.NoError(t, err)
	require.Equal(t, 1, plan.IntConsistencyLevel)
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_ReplicaOrdering(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2", "node3")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "alice").Return([]string{"node1", "node2", "node3"}, nil)

	tenantStatus := map[string]string{
		"alice": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "alice").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "alice", []string{"node1", "node2", "node3"}).
		Return([]string{"node1", "node2", "node3"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "alice", []string{"node1", "node2", "node3"}).
		Return([]string{"node1", "node3"}, []string{"node2"})

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:              "alice",
		Shard:               "",
		ConsistencyLevel:    types.ConsistencyLevelOne,
		DirectCandidateNode: "node3", // Should be ordered first
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.NoError(t, err)
	require.Equal(t, "node3", plan.ReplicaSet.Replicas[0].NodeName, "DirectCandidateNode should be first")
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_TenantNotFound(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	tenantStatus := map[string]string{}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "nonexistent").
		Return(tenantStatus, errors.New("tenant not found: \"nonexistent\""))

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:           "nonexistent",
		Shard:            "",
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	plan, err := r.BuildWriteRoutingPlan(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not found: \"nonexistent\"")
	require.Empty(t, plan.ReplicaSet.Replicas)
}

// You should also check if BuildReadRoutingPlan has similar gaps in coverage
func TestSingleTenantRouter_BuildReadRoutingPlan_TenantValidation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:           "some-tenant", // Single tenant should reject non-empty tenant
		Shard:            "shard1",
		ConsistencyLevel: types.ConsistencyLevelOne,
	}

	plan, err := r.BuildReadRoutingPlan(opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "class TestClass has multi-tenancy disabled, but request was with tenant")
	require.Empty(t, plan.ReplicaSet.Replicas)
}

func TestMultiTenantRouter_BuildReadRoutingPlan_ConsistencyLevelValidation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1", "node2")
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "").Return([]string{"node1", "node2"}, nil)

	tenantStatus := map[string]string{
		"alice": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "alice").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	opts := types.RoutingPlanBuildOptions{
		Tenant:           "alice",
		Shard:            "",
		ConsistencyLevel: "INVALID_LEVEL",
	}

	plan, err := r.BuildReadRoutingPlan(opts)
	require.NoError(t, err)
	require.Equal(t, 1, plan.IntConsistencyLevel)
}
