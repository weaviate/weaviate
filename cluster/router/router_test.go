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

func createRoutingPlanBuildOptions(partitioningKey string) types.RoutingPlanBuildOptions {
	return types.RoutingPlanBuildOptions{
		Shard:               partitioningKey,
		ConsistencyLevel:    types.ConsistencyLevelOne,
		DirectCandidateNode: "",
	}
}

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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.NoError(t, err, "unexpected error while getting read/write replica locations")
	require.Empty(t, readReplicas, "read replica locations should be empty")
	require.Empty(t, writeReplicas, "write replica locations should be empty")
	require.Empty(t, additionalWriteReplicas, "additional write replica locations should be empty")
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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	expectedReadReplicas := types.ReplicaSet{
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

	expectedWriteReplicas := types.ReplicaSet{
		Replicas: []types.Replica{
			{
				NodeName:  "node1",
				ShardName: "shard1",
				HostAddr:  "node1",
			},
		},
	}

	expectedAdditionalWriteReplicas := types.ReplicaSet{
		Replicas: []types.Replica{
			{
				NodeName:  "node2",
				ShardName: "shard1",
				HostAddr:  "node2",
			},
		},
	}

	require.NoError(t, err, "unexpected error while getting read/write replica locations")
	require.Equal(t, expectedReadReplicas, readReplicas)
	require.Equal(t, expectedWriteReplicas, writeReplicas)
	require.Equal(t, expectedAdditionalWriteReplicas, additionalWriteReplicas)
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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

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
	require.ElementsMatch(t, expectedReadReplicas, readReplicas.Replicas)
	require.ElementsMatch(t, expectedWriteReplicas, writeReplicas.Replicas)
	require.ElementsMatch(t, expectedAdditionalWriteReplicas, additionalWriteReplicas.Replicas)
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

	writeReplicas, additionalWriteReplicas, err := r.GetWriteReplicasLocation("TestClass", "")
	require.NoError(t, err, "unexpected error while getting write replicas")

	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "shard1", HostAddr: "node1"},
	}

	expectedAdditionalWriteReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "shard1", HostAddr: "node2"},
	}

	require.ElementsMatch(t, expectedWriteReplicas, writeReplicas.Replicas)
	require.ElementsMatch(t, expectedAdditionalWriteReplicas, additionalWriteReplicas.Replicas)
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

	readReplicas, err := r.GetReadReplicasLocation("TestClass", "")
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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "shard2 error")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

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
	require.ElementsMatch(t, expectedReadReplicas, readReplicas.Replicas)
	require.ElementsMatch(t, expectedWriteReplicas, writeReplicas.Replicas)
	require.ElementsMatch(t, expectedAdditionalWriteReplicas, additionalWriteReplicas.Replicas)
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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not found: \"luke\"")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while checking tenant active status: \"luke\"")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant is required for multi-tenant collections")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
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

	writeReplicas, additionalWriteReplicas, err := r.GetWriteReplicasLocation("TestClass", "luke")
	require.NoError(t, err)

	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "luke", HostAddr: "node1"},
	}
	expectedAdditionalWriteReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "luke", HostAddr: "node2"},
	}

	require.Equal(t, expectedWriteReplicas, writeReplicas.Replicas)
	require.Equal(t, expectedAdditionalWriteReplicas, additionalWriteReplicas.Replicas)
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

	readReplicas, err := r.GetReadReplicasLocation("TestClass", "luke")
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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")
	require.NoError(t, err)

	expected := []types.Replica{
		{NodeName: "node1", ShardName: "luke", HostAddr: "node1"},
	}
	require.Equal(t, expected, readReplicas.Replicas)
	require.Equal(t, expected, writeReplicas.Replicas)
	require.Empty(t, additionalWriteReplicas.Replicas)

	readReplicas, writeReplicas, additionalWriteReplicas, err = r.GetReadWriteReplicasLocation("TestClass", "luke")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error while checking tenant status for tenant \"luke\"")
	require.Empty(t, readReplicas.Replicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
}

func TestMultiTenantRouter_VariousTenantStatuses(t *testing.T) {
	statusTests := []struct {
		status    string
		shouldErr bool
		errMsg    string
	}{
		{models.TenantActivityStatusHOT, false, ""},
		{models.TenantActivityStatusCOLD, true, "error while checking tenant status for tenant \"luke\""},
		{models.TenantActivityStatusFROZEN, true, "error while checking tenant status for tenant \"luke\""},
		{models.TenantActivityStatusFREEZING, true, "error while checking tenant status for tenant \"luke\""},
		{"UNKNOWN_STATUS", true, "error while checking tenant status for tenant \"luke\""},
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

			readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

			if test.shouldErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errMsg)
				require.Empty(t, readReplicas.Replicas)
				require.Empty(t, writeReplicas)
				require.Empty(t, additionalWriteReplicas)
			} else {
				require.NoError(t, err)
				require.Equal(t, expectedReplicas, readReplicas.Replicas)
				require.Equal(t, expectedReplicas, writeReplicas.Replicas)
				require.Empty(t, additionalWriteReplicas.Replicas)
			}
		})
	}
}

func TestSingleTenantRouter_BuildReadRoutingPlan_NoReplicas(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := mocks.NewMockNodeSelector("node1")

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

	params := createRoutingPlanBuildOptions("shard1")
	plan, err := r.BuildReadRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while getting read replicas for collection \"TestClass\" shard \"shard1\"")
	require.Equal(t, []types.Replica(nil), plan.Replicas())
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

	params := createRoutingPlanBuildOptions("luke")
	plan, err := r.BuildReadRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while checking read replica availability for collection \"TestClass\" shard \"luke\"")
	require.Equal(t, []types.Replica(nil), plan.Replicas())
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

	params := createRoutingPlanBuildOptions("luke")
	plan, err := r.BuildReadRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, "luke", plan.Shard)
	require.Equal(t, types.ReplicaSet{Replicas: []types.Replica{{
		NodeName:  "node1",
		ShardName: "luke",
		HostAddr:  "node1",
	}}}, plan.ReplicaSet)
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

	params := createRoutingPlanBuildOptions("nonexistent")
	plan, err := r.BuildReadRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while building read routing plan for collection \"TestClass\" shard \"nonexistent\"")
	require.Empty(t, plan.Replicas())
}

func TestSingleTenantRouter_BuildRoutingPlan_WithDirectCandidate(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
	mockNodeSelector.EXPECT().LocalName().Return("node1")

	directCandidateNode := "node2"
	mockSchemaReader.EXPECT().
		ShardReplicas("TestClass", "shard1").
		Return([]string{"node1", directCandidateNode, "node3"}, nil)

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", directCandidateNode, "node3"}).
		Return([]string{"node1", directCandidateNode, "node3"})

	mockReplicationFSM.EXPECT().
		FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", directCandidateNode, "node3"}).
		Return([]string{"node1", directCandidateNode, "node3"}, []string{})

	mockNodeSelector.EXPECT().NodeHostname(directCandidateNode).Return("host2.example.com", true)
	mockNodeSelector.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockNodeSelector.EXPECT().NodeHostname("node3").Return("host3.example.com", true)

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	params := types.RoutingPlanBuildOptions{
		Shard:               "shard1",
		ConsistencyLevel:    types.ConsistencyLevelOne,
		DirectCandidateNode: directCandidateNode,
	}

	plan, err := r.BuildReadRoutingPlan(params)
	require.NoError(t, err)

	expectedReplicas := []types.Replica{
		{NodeName: directCandidateNode, ShardName: "shard1", HostAddr: "host2.example.com"},
		{NodeName: "node1", ShardName: "shard1", HostAddr: "host1.example.com"},
		{NodeName: "node3", ShardName: "shard1", HostAddr: "host3.example.com"},
	}
	require.Equal(t, expectedReplicas, plan.Replicas())
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
		read, write, additional, err := r.GetReadWriteReplicasLocation("TestClass", tenant)

		require.NoError(t, err, "unexpected error for tenant %s", tenant)

		require.Equal(t, sorted(expected), sorted(read.NodeNames()), "read replicas mismatch for tenant %s", tenant)
		require.Equal(t, []string{expected[0]}, write.NodeNames(), "write replicas mismatch for tenant %s", tenant)

		if len(expected) > 1 {
			require.Equal(t, sorted(expected[1:]), sorted(additional.NodeNames()), "additional writes mismatch for tenant %s", tenant)
		} else {
			require.Empty(t, additional, "additional writes should be empty for tenant %s", tenant)
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
		"active-tenant-1": {models.TenantActivityStatusHOT, true, "error while checking tenant status for tenant \"active-tenant-1\""},
		"active-tenant-2": {models.TenantActivityStatusHOT, true, "error while checking tenant status for tenant \"active-tenant-2\""},
		"cold-tenant":     {models.TenantActivityStatusCOLD, false, "error while checking tenant status for tenant \"cold-tenant\""},
		"frozen-tenant":   {models.TenantActivityStatusFROZEN, false, "error while checking tenant status for tenant \"frozen-tenant\""},
		"freezing-tenant": {models.TenantActivityStatusFREEZING, false, "error while checking tenant status for tenant \"freezing-tenant\""},
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
			readReplicas, writeReplicas, additionalWrites, err := r.GetReadWriteReplicasLocation("TestClass", tenantName)

			if tenantsStatus.shouldWork {
				require.NoError(t, err, "%s: should work", tenantsStatus.description)
				require.ElementsMatch(t, []string{"node1", "node2"}, readReplicas.NodeNames())
				require.Equal(t, []string{"node1"}, writeReplicas.NodeNames())
				require.Equal(t, []string{"node2"}, additionalWrites.NodeNames())
			} else {
				require.Error(t, err, "%s: should fail", tenantsStatus.description)
				require.Contains(t, err.Error(), "error while checking tenant status", "error should mention tenant not active")
				require.Empty(t, readReplicas)
				require.Empty(t, writeReplicas)
				require.Empty(t, additionalWrites)
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

			readReplicas, writeReplicas, additionalWrites, err := r.GetReadWriteReplicasLocation(collection, tenantName)

			require.NoError(t, err, "unexpected error for collection %s", collection)
			require.ElementsMatch(t, expectedReplicas, readReplicas.NodeNames(), "read replicas mismatch for collection %s", collection)
			require.Equal(t, []string{expectedReplicas[0]}, writeReplicas.NodeNames(), "write replicas mismatch for collection %s", collection)
			require.ElementsMatch(t, expectedReplicas[1:], additionalWrites.NodeNames(), "additional writes mismatch for collection %s", collection)
		})
	}
}

func TestMultiTenantRouter_RoutingPlanConstruction_DirectCandidate(t *testing.T) {
	testCases := []struct {
		name            string
		tenant          string
		replicas        []string
		directCandidate string
		expectedFirst   string
		expectedTotal   int
		description     string
	}{
		{
			name:            "no_direct_candidate",
			tenant:          "tenant1",
			replicas:        []string{"node1", "node2", "node3"},
			directCandidate: "",
			expectedFirst:   "node1", // LocalName() should return node1
			expectedTotal:   3,
			description:     "local node should be first when no direct candidate",
		},
		{
			name:            "valid_direct_candidate",
			tenant:          "tenant2",
			replicas:        []string{"node1", "node2", "node3"},
			directCandidate: "node2",
			expectedFirst:   "node2",
			expectedTotal:   3,
			description:     "direct candidate should be first",
		},
		{
			name:            "direct_candidate_not_in_replicas",
			tenant:          "tenant3",
			replicas:        []string{"node1", "node2"},
			directCandidate: "node4",
			expectedFirst:   "node1", // Falls back to first replica
			expectedTotal:   2,
			description:     "non-replica direct candidate should be ignored",
		},
		{
			name:            "single_replica",
			tenant:          "tenant4",
			replicas:        []string{"node1"},
			directCandidate: "node1",
			expectedFirst:   "node1",
			expectedTotal:   1,
			description:     "single replica with matching direct candidate",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
			mockNodeSelector := cluster.NewMockNodeSelector(t)
			mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

			tenantStatus := map[string]string{testCase.tenant: models.TenantActivityStatusHOT}
			mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", testCase.tenant).
				Return(tenantStatus, nil)
			mockSchemaReader.EXPECT().ShardReplicas("TestClass", testCase.tenant).Return(testCase.replicas, nil)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", testCase.tenant, testCase.replicas).
				Return(testCase.replicas)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", testCase.tenant, testCase.replicas).
				Return(testCase.replicas[:1], testCase.replicas[1:])

			for _, replica := range testCase.replicas {
				mockNodeSelector.EXPECT().NodeHostname(replica).Return(replica+".example.com", true)
			}

			if testCase.directCandidate != "" {
				mockNodeSelector.EXPECT().LocalName().Return(testCase.directCandidate)
			} else {
				mockNodeSelector.EXPECT().LocalName().Return("node1")
			}

			r := router.NewBuilder("TestClass", true, mockNodeSelector,
				mockSchemaGetter, mockSchemaReader, mockReplicationFSM).Build()

			params := types.RoutingPlanBuildOptions{
				Shard:               testCase.tenant,
				ConsistencyLevel:    types.ConsistencyLevelOne,
				DirectCandidateNode: testCase.directCandidate,
			}

			plan, err := r.BuildReadRoutingPlan(params)

			require.NoError(t, err, "unexpected error for %s", testCase.description)
			require.Equal(t, testCase.tenant, plan.Shard)
			require.Len(t, plan.Replicas(), testCase.expectedTotal, "replica count mismatch for %s", testCase.description)
			require.Equal(t, testCase.expectedFirst, plan.Replicas()[0].NodeName, "first replica mismatch for %s", testCase.description)
			require.Equal(t, testCase.expectedFirst+".example.com", plan.Replicas()[0].HostAddr, "first host address mismatch for %s", testCase.description)
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
	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", targetShard)

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
	require.Equal(t, expectedReadReplicas, readReplicas.Replicas)
	require.Equal(t, expectedWriteReplicas, writeReplicas.Replicas)
	require.Equal(t, expectedAdditionalWriteReplicas, additionalWriteReplicas.Replicas)
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

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "invalid_shard")

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while getting target shards for collection \"TestClass\" shard \"invalid_shard\"")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
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
			mockNodeSelector.EXPECT().LocalName().Return("node1")

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

			params := types.RoutingPlanBuildOptions{
				Shard:            testCase.shard,
				ConsistencyLevel: types.ConsistencyLevelOne,
			}

			plan, err := r.BuildReadRoutingPlan(params)

			require.NoError(t, err, "unexpected error for %s", testCase.description)
			require.Equal(t, testCase.shard, plan.Shard)

			actualShards := plan.ReplicaSet.Shards()
			require.ElementsMatch(t, testCase.expectShards, actualShards, "shard targeting mismatch for %s", testCase.description)
		})
	}
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_Success(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
	mockNodeSelector.EXPECT().LocalName().Return("node1")
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2", "node3"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2", "node3"}).
		Return([]string{"node1", "node2", "node3"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2", "node3"}).
		Return([]string{"node1", "node2"}, []string{"node3"})
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

	params := createRoutingPlanBuildOptions("shard1")
	plan, err := r.BuildWriteRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, "shard1", plan.Shard)

	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "shard1", HostAddr: "host1.example.com"},
		{NodeName: "node2", ShardName: "shard1", HostAddr: "host2.example.com"},
	}
	expectedAdditionalReplicas := []types.Replica{
		{NodeName: "node3", ShardName: "shard1", HostAddr: "host3.example.com"},
	}

	require.Equal(t, expectedWriteReplicas, plan.ReplicaSet.Replicas)
	require.Equal(t, expectedAdditionalReplicas, plan.AdditionalReplicaSet.Replicas)
	require.Equal(t, types.ConsistencyLevelOne, plan.ConsistencyLevel)
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

	params := createRoutingPlanBuildOptions("shard1")
	plan, err := r.BuildWriteRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while checking replica availability for collection \"TestClass\" shard \"shard1\"")
	require.Empty(t, plan.Replicas())
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_WithDirectCandidate(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
	mockNodeSelector.EXPECT().LocalName().Return("node1")

	directCandidateNode := "node3"
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").
		Return([]string{"node1", "node2", directCandidateNode}, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2", directCandidateNode}).
		Return([]string{"node1", "node2", directCandidateNode})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2", directCandidateNode}).
		Return([]string{"node1", "node2", directCandidateNode}, []string{})

	mockNodeSelector.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockNodeSelector.EXPECT().NodeHostname("node2").Return("host2.example.com", true)
	mockNodeSelector.EXPECT().NodeHostname(directCandidateNode).Return("host3.example.com", true)

	r := router.NewBuilder(
		"TestClass",
		false,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	params := types.RoutingPlanBuildOptions{
		Shard:               "shard1",
		ConsistencyLevel:    types.ConsistencyLevelOne,
		DirectCandidateNode: directCandidateNode,
	}

	plan, err := r.BuildWriteRoutingPlan(params)
	require.NoError(t, err)

	// Direct candidate should be first
	expectedReplicas := []types.Replica{
		{NodeName: directCandidateNode, ShardName: "shard1", HostAddr: "host3.example.com"},
		{NodeName: "node1", ShardName: "shard1", HostAddr: "host1.example.com"},
		{NodeName: "node2", ShardName: "shard1", HostAddr: "host2.example.com"},
	}
	require.Equal(t, expectedReplicas, plan.Replicas())
}

func TestSingleTenantRouter_BuildWriteRoutingPlan_MultipleShards(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)

	shards := []string{"shard1", "shard2", "shard3"}
	state := createShardingStateWithShards(shards)
	mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
	mockNodeSelector.EXPECT().LocalName().Return("node1")

	// Setup expectations for all shards
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

	// Test broadcast write (empty shard)
	params := createRoutingPlanBuildOptions("")
	plan, err := r.BuildWriteRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, "", plan.Shard)

	// Should contain write replicas from all shards
	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "shard1", HostAddr: "node1.example.com"},
		{NodeName: "node2", ShardName: "shard2", HostAddr: "node2.example.com"},
		{NodeName: "node3", ShardName: "shard3", HostAddr: "node3.example.com"},
	}

	expectedAdditionalReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "shard1", HostAddr: "node2.example.com"},
		{NodeName: "node3", ShardName: "shard2", HostAddr: "node3.example.com"},
		{NodeName: "node1", ShardName: "shard3", HostAddr: "node1.example.com"},
	}

	require.ElementsMatch(t, expectedWriteReplicas, plan.ReplicaSet.Replicas)
	require.ElementsMatch(t, expectedAdditionalReplicas, plan.AdditionalReplicaSet.Replicas)
}

// Multi-Tenant Write Routing Plan Tests

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
	mockNodeSelector.EXPECT().LocalName().Return("node1")

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	params := createRoutingPlanBuildOptions("alice")
	plan, err := r.BuildWriteRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, "alice", plan.Shard)

	expectedWriteReplicas := []types.Replica{
		{NodeName: "node1", ShardName: "alice", HostAddr: "host1.example.com"},
	}
	expectedAdditionalWriteReplicas := []types.Replica{
		{NodeName: "node2", ShardName: "alice", HostAddr: "host2.example.com"},
	}
	// Note: AdditionalReplicaSet should be empty in multi-tenant write plan based on the code
	require.Equal(t, expectedWriteReplicas, plan.ReplicaSet.Replicas)
	require.Equal(t, expectedAdditionalWriteReplicas, plan.AdditionalReplicaSet.Replicas)
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

	params := createRoutingPlanBuildOptions("alice")
	plan, err := r.BuildWriteRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while checking write replica availability for collection \"TestClass\" shard \"alice\"")
	require.Empty(t, plan.Replicas())
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

	params := createRoutingPlanBuildOptions("")
	plan, err := r.BuildWriteRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while creating routing plan for collection \"TestClass\"")
	require.Empty(t, plan.Replicas())
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

	params := createRoutingPlanBuildOptions("alice")
	plan, err := r.BuildWriteRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "error while getting write replicas for collection TestClass shard alice")
	require.Empty(t, plan.Replicas())
}

func TestMultiTenantRouter_BuildWriteRoutingPlan_WithDirectCandidate(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockSchemaReader := schemaTypes.NewMockSchemaReader(t)

	directCandidateNode := "node3"
	mockSchemaReader.EXPECT().ShardReplicas("TestClass", "alice").
		Return([]string{"node1", "node2", directCandidateNode}, nil)

	tenantStatus := map[string]string{
		"alice": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "alice").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "alice", []string{"node1", "node2", directCandidateNode}).
		Return([]string{"node1", "node2", directCandidateNode})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "alice", []string{"node1", "node2", directCandidateNode}).
		Return([]string{"node1", "node2", directCandidateNode}, []string{})

	mockNodeSelector.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockNodeSelector.EXPECT().NodeHostname("node2").Return("host2.example.com", true)
	mockNodeSelector.EXPECT().NodeHostname(directCandidateNode).Return("host3.example.com", true)
	mockNodeSelector.EXPECT().LocalName().Return("node1")

	r := router.NewBuilder(
		"TestClass",
		true,
		mockNodeSelector,
		mockSchemaGetter,
		mockSchemaReader,
		mockReplicationFSM,
	).Build()

	params := types.RoutingPlanBuildOptions{
		Shard:               "alice",
		ConsistencyLevel:    types.ConsistencyLevelOne,
		DirectCandidateNode: directCandidateNode,
	}

	plan, err := r.BuildWriteRoutingPlan(params)
	require.NoError(t, err)

	// Direct candidate should be first
	expectedReplicas := []types.Replica{
		{NodeName: directCandidateNode, ShardName: "alice", HostAddr: "host3.example.com"},
		{NodeName: "node1", ShardName: "alice", HostAddr: "host1.example.com"},
		{NodeName: "node2", ShardName: "alice", HostAddr: "host2.example.com"},
	}
	require.Equal(t, expectedReplicas, plan.Replicas())
}

// Consistency Level Tests for Write Plans

func TestWriteRoutingPlan_ConsistencyLevelValidation(t *testing.T) {
	testCases := []struct {
		name             string
		consistencyLevel types.ConsistencyLevel
		expectError      bool
		description      string
	}{
		{
			name:             "valid_consistency_one",
			consistencyLevel: types.ConsistencyLevelOne,
			expectError:      false,
			description:      "ConsistencyLevelOne should be valid",
		},
		{
			name:             "valid_consistency_quorum",
			consistencyLevel: types.ConsistencyLevelQuorum,
			expectError:      false,
			description:      "ConsistencyLevelQuorum should be valid",
		},
		{
			name:             "valid_consistency_all",
			consistencyLevel: types.ConsistencyLevelAll,
			expectError:      false,
			description:      "ConsistencyLevelAll should be valid",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockSchemaReader := schemaTypes.NewMockSchemaReader(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
			mockNodeSelector := cluster.NewMockNodeSelector(t)

			state := createShardingStateWithShards([]string{"shard1"})
			mockSchemaReader.EXPECT().CopyShardingState("TestClass").Return(state)
			mockNodeSelector.EXPECT().LocalName().Return("node1")

			mockSchemaReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2", "node3"}, nil)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2", "node3"}).
				Return([]string{"node1", "node2", "node3"})
			mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2", "node3"}).
				Return([]string{"node1", "node2"}, []string{"node3"})

			for _, node := range []string{"node1", "node2", "node3"} {
				mockNodeSelector.EXPECT().NodeHostname(node).Return(node+".example.com", true)
			}

			r := router.NewBuilder(
				"TestClass",
				false,
				mockNodeSelector,
				mockSchemaGetter,
				mockSchemaReader,
				mockReplicationFSM,
			).Build()

			params := types.RoutingPlanBuildOptions{
				Shard:            "shard1",
				ConsistencyLevel: testCase.consistencyLevel,
			}

			plan, err := r.BuildWriteRoutingPlan(params)

			if testCase.expectError {
				require.Error(t, err, testCase.description)
			} else {
				require.NoError(t, err, testCase.description)
				require.Equal(t, testCase.consistencyLevel, plan.ConsistencyLevel)
				require.NotZero(t, plan.IntConsistencyLevel, "IntConsistencyLevel should be set")
			}
		})
	}
}
