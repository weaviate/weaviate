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

package router_test

import (
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

func createRoutingPlanBuildOptions(collection, tenant string) types.RoutingPlanBuildOptions {
	return types.RoutingPlanBuildOptions{
		Collection:             collection,
		Shard:                  tenant,
		ConsistencyLevel:       types.ConsistencyLevelOne,
		DirectCandidateReplica: "",
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
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	emptyState := createShardingStateWithShards([]string{})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err, "unexpected error building router")

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.NoError(t, err, "unexpected error while getting read/write replica locations")
	require.Empty(t, readReplicas, "read replica locations should be empty")
	require.Empty(t, writeReplicas, "write replica locations should be empty")
	require.Empty(t, additionalWriteReplicas, "additional write replica locations should be empty")
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_MultipleShards(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2", "node3")

	shards := []string{"shard1", "shard2", "shard3"}
	state := createShardingStateWithShards(shards)
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(state)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)
	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard2").Return([]string{"node2", "node3"}, nil)
	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard3").Return([]string{"node3", "node1"}, nil)

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

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err, "unexpected error building router")

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.NoError(t, err, "unexpected error while getting read/write replica locations")
	require.ElementsMatch(t, []string{"node1", "node2", "node3"}, readReplicas)
	require.ElementsMatch(t, []string{"node1", "node2", "node3"}, writeReplicas)
	require.ElementsMatch(t, []string{"node1", "node2", "node3"}, additionalWriteReplicas)
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_ErrorFromDependencies(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(state)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard1").
		Return([]string{}, errors.New("shard metadata error"))

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "shard metadata error")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_TenantRequestForSingleTenant(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := cluster.NewMockNodeSelector(t)

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant must be empty for single-tenant collections")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
}

func TestSingleTenantRouter_GetWriteReplicasLocation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(state)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	writeReplicas, additionalWriteReplicas, err := r.GetWriteReplicasLocation("TestClass", "")
	require.NoError(t, err)
	require.Equal(t, []string{"node1"}, writeReplicas)
	require.Equal(t, []string{"node2"}, additionalWriteReplicas)
}

func TestSingleTenantRouter_GetReadReplicasLocation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(state)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1"}, []string{"node2"})

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	readReplicas, err := r.GetReadReplicasLocation("TestClass", "")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"node1", "node2"}, readReplicas)
}

func TestSingleTenantRouter_ErrorInMiddleOfShardProcessing(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1")

	state := createShardingStateWithShards([]string{"shard1", "shard2", "shard3"})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(state)

	// First shard success
	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	// Second shard failure
	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard2").Return([]string{}, errors.New("shard2 error"))

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

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
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{"node2"})

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.NoError(t, err)
	require.ElementsMatch(t, []string{"node1", "node2"}, readReplicas)
	require.Equal(t, []string{"node1"}, writeReplicas)
	require.Equal(t, []string{"node2"}, additionalWriteReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_TenantNotFound(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	tenantStatus := map[string]string{}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, errors.New("tenant not found: \"luke\""))

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

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
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusCOLD,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not active: \"luke\"")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_ErrorFromDependencies(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(map[string]string{}, errors.New("schema service error"))

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.Error(t, err)
	require.Contains(t, err.Error(), "schema service error")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
	require.Empty(t, additionalWriteReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_NonTenantRequestForMultiTenant(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

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
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{"node2"})

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	writeReplicas, additionalWriteReplicas, err := r.GetWriteReplicasLocation("TestClass", "luke")
	require.NoError(t, err)
	require.Equal(t, []string{"node1"}, writeReplicas)
	require.Equal(t, []string{"node2"}, additionalWriteReplicas)
}

func TestMultiTenantRouter_GetReadReplicasLocation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	readReplicas, err := r.GetReadReplicasLocation("TestClass", "luke")
	require.NoError(t, err)
	require.Equal(t, []string{"node1"}, readReplicas)
}

func TestMultiTenantRouter_TenantStatusChangeDuringOperation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1")
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

	tenantStatusFirst := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	tenantStatusSecond := map[string]string{
		"luke": models.TenantActivityStatusFREEZING,
	}

	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatusFirst, nil).Once()
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatusSecond, nil).Once()

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}).Once()
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{}).Once()

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")
	require.NoError(t, err)
	require.Equal(t, []string{"node1"}, readReplicas)
	require.Equal(t, []string{"node1"}, writeReplicas)
	require.Empty(t, additionalWriteReplicas)

	readReplicas, writeReplicas, additionalWriteReplicas, err = r.GetReadWriteReplicasLocation("TestClass", "luke")
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not active")
	require.Empty(t, readReplicas)
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
		{models.TenantActivityStatusCOLD, true, "tenant not active"},
		{models.TenantActivityStatusFROZEN, true, "tenant not active"},
		{models.TenantActivityStatusFREEZING, true, "tenant not active"},
		{"UNKNOWN_STATUS", true, "tenant not active"},
	}

	for _, test := range statusTests {
		t.Run("status_"+test.status, func(t *testing.T) {
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
			mockClusterReader := mocks.NewMockNodeSelector("node1")
			mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

			tenantStatus := map[string]string{
				"luke": test.status,
			}
			mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
				Return(tenantStatus, nil)

			if !test.shouldErr {
				mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)
				mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
					Return([]string{"node1"})
				mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
					Return([]string{"node1"}, []string{})
			}

			r, err := router.NewBuilder(
				"TestClass",
				true,
				mockClusterReader,
				mockSchemaGetter,
				mockMetadataReader,
				mockReplicationFSM,
			).Build()
			require.NoError(t, err)

			readReplicas, writeReplicas, additionalWriteReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

			if test.shouldErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errMsg)
				require.Empty(t, readReplicas)
				require.Empty(t, writeReplicas)
				require.Empty(t, additionalWriteReplicas)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"node1"}, readReplicas)
				require.Equal(t, []string{"node1"}, writeReplicas)
				require.Empty(t, additionalWriteReplicas)
			}
		})
	}
}

func TestSingleTenantRouter_BuildReadRoutingPlan_NoReplicas(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1")

	emptyState := createShardingStateWithShards([]string{})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	params := createRoutingPlanBuildOptions("TestClass", "")
	plan, err := r.BuildReadRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "no replicas found")
	require.Empty(t, plan.Replicas)
}

func TestMultiTenantRouter_BuildReadRoutingPlan_Success(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)
	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{})

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	params := createRoutingPlanBuildOptions("TestClass", "luke")
	plan, err := r.BuildReadRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, "TestClass", plan.Collection)
	require.Equal(t, "luke", plan.Shard)
	require.Equal(t, []string{"node1"}, plan.Replicas)
	require.Equal(t, []string{"node1"}, plan.ReplicasHostAddrs)
}

func TestMultiTenantRouter_BuildRoutingPlan_TenantNotFoundDuringBuild(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	tenantStatus := map[string]string{}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "nonexistent").
		Return(tenantStatus, nil)

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	params := createRoutingPlanBuildOptions("TestClass", "nonexistent")
	plan, err := r.BuildReadRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not found: \"nonexistent\"")
	require.Empty(t, plan.Replicas)
}

func TestSingleTenantRouter_BuildRoutingPlan_WithDirectCandidate(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := cluster.NewMockNodeSelector(t)

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(state)

	directCandidateNode := "node2"
	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", directCandidateNode, "node3"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", directCandidateNode, "node3"}).
		Return([]string{"node1", directCandidateNode, "node3"})

	// node2 should be first due to direct candidate expected to be the first node
	mockClusterReader.EXPECT().NodeHostname(directCandidateNode).Return("host2.example.com", true)
	mockClusterReader.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockClusterReader.EXPECT().NodeHostname("node3").Return("host3.example.com", true)

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	params := types.RoutingPlanBuildOptions{
		Collection:             "TestClass",
		Shard:                  "",
		ConsistencyLevel:       types.ConsistencyLevelOne,
		DirectCandidateReplica: directCandidateNode,
	}

	plan, err := r.BuildReadRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, directCandidateNode, plan.Replicas[0])
	require.Equal(t, "host2.example.com", plan.ReplicasHostAddrs[0])
	require.Contains(t, plan.Replicas, "node1")
	require.Contains(t, plan.Replicas, "node3")
}

func TestRouter_BuildWriteRoutingPlan_DelegatesCorrectly(t *testing.T) {
	t.Run("single-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1")

		emptyState := createShardingStateWithShards([]string{})
		mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

		r, err := router.NewBuilder(
			"TestClass",
			false,
			mockClusterReader,
			mockSchemaGetter,
			mockMetadataReader,
			mockReplicationFSM,
		).Build()
		require.NoError(t, err)

		params := createRoutingPlanBuildOptions("TestClass", "")
		plan, err := r.BuildWriteRoutingPlan(params)

		require.Error(t, err)
		require.Contains(t, err.Error(), "no replicas found")
		require.Empty(t, plan.Replicas)
	})

	t.Run("multi-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
		mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
		mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

		tenantStatus := map[string]string{
			"luke": models.TenantActivityStatusHOT,
		}
		mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
			Return(tenantStatus, nil)

		mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
			Return([]string{"node1", "node2"})
		mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
			Return([]string{"node1"}, []string{"node2"})

		r, err := router.NewBuilder(
			"TestClass",
			true,
			mockClusterReader,
			mockSchemaGetter,
			mockMetadataReader,
			mockReplicationFSM,
		).Build()
		require.NoError(t, err)

		params := createRoutingPlanBuildOptions("TestClass", "luke")
		plan, err := r.BuildWriteRoutingPlan(params)

		require.NoError(t, err)
		require.Equal(t, "TestClass", plan.Collection)
		require.Equal(t, "luke", plan.Shard)
		require.Equal(t, []string{"node1"}, plan.Replicas) // "node2" discarded as additional write not included
		require.Equal(t, []string{"node1"}, plan.ReplicasHostAddrs)
	})
}

func TestRouter_GetWriteReplicasLocation_DelegatesCorrectly(t *testing.T) {
	t.Run("single-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

		emptyState := createShardingStateWithShards([]string{})
		mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

		r, err := router.NewBuilder(
			"TestClass",
			false,
			mockClusterReader,
			mockSchemaGetter,
			mockMetadataReader,
			mockReplicationFSM,
		).Build()
		require.NoError(t, err)

		writeReplicas, additionalWriteReplicas, err := r.GetWriteReplicasLocation("TestClass", "")
		require.NoError(t, err)
		require.Empty(t, writeReplicas)
		require.Empty(t, additionalWriteReplicas)
	})

	t.Run("multi-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
		mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
		mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

		tenantStatus := map[string]string{
			"luke": models.TenantActivityStatusHOT,
		}
		mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
			Return(tenantStatus, nil)

		mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
			Return([]string{"node1"})
		mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
			Return([]string{"node1"}, []string{"node2"})

		r, err := router.NewBuilder(
			"TestClass",
			true,
			mockClusterReader,
			mockSchemaGetter,
			mockMetadataReader,
			mockReplicationFSM,
		).Build()
		require.NoError(t, err)

		writeReplicas, additionalWriteReplicas, err := r.GetWriteReplicasLocation("TestClass", "luke")
		require.NoError(t, err)
		require.Equal(t, []string{"node1"}, writeReplicas)
		require.Equal(t, []string{"node2"}, additionalWriteReplicas)
	})
}

func TestRouter_GetReadReplicasLocation_DelegatesCorrectly(t *testing.T) {
	t.Run("single-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

		emptyState := createShardingStateWithShards([]string{})
		mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

		r, err := router.NewBuilder(
			"TestClass",
			false,
			mockClusterReader,
			mockSchemaGetter,
			mockMetadataReader,
			mockReplicationFSM,
		).Build()
		require.NoError(t, err)

		readReplicas, err := r.GetReadReplicasLocation("TestClass", "")
		require.NoError(t, err)
		require.Empty(t, readReplicas)
	})

	t.Run("multi-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
		mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
		mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

		tenantStatus := map[string]string{
			"luke": models.TenantActivityStatusHOT,
		}
		mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
			Return(tenantStatus, nil)

		mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
			Return([]string{"node1"})
		mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
			Return([]string{"node1"}, []string{"node2"})

		r, err := router.NewBuilder(
			"TestClass",
			true,
			mockClusterReader,
			mockSchemaGetter,
			mockMetadataReader,
			mockReplicationFSM,
		).Build()
		require.NoError(t, err)

		readReplicas, err := r.GetReadReplicasLocation("TestClass", "luke")
		require.NoError(t, err)
		require.Equal(t, []string{"node1"}, readReplicas)
	})
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
			mockClusterReader := cluster.NewMockNodeSelector(t)
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)

			mockClusterReader.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
			mockClusterReader.EXPECT().NodeHostname("node2").Return("", false)

			var mockMetadataReader schemaTypes.SchemaReader
			if !tt.partitioning {
				mockMetadataReader = schemaTypes.NewMockSchemaReader(t)
			}

			r, err := router.NewBuilder(
				"TestClass",
				tt.partitioning,
				mockClusterReader,
				mockSchemaGetter,
				mockMetadataReader,
				mockReplicationFSM,
			).Build()
			require.NoError(t, err)

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
			mockClusterReader := mocks.NewMockNodeSelector("node1", "node2", "node3")
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)

			var mockMetadataReader schemaTypes.SchemaReader
			if !tt.partitioning {
				mockMetadataReader = schemaTypes.NewMockSchemaReader(t)
			}

			expectedHostnames := []string{"node1", "node2", "node3"}

			r, err := router.NewBuilder(
				"TestClass",
				tt.partitioning,
				mockClusterReader,
				mockSchemaGetter,
				mockMetadataReader,
				mockReplicationFSM,
			).Build()
			require.NoError(t, err)

			hostnames := r.AllHostnames()
			require.Equal(t, expectedHostnames, hostnames)
		})
	}
}

func TestSingleTenantRouter_HostnameNotFound(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := cluster.NewMockNodeSelector(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(state)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"})

	mockClusterReader.EXPECT().LocalName().Return("node1")
	mockClusterReader.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockClusterReader.EXPECT().NodeHostname("node2").Return("", false)

	r, err := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	params := createRoutingPlanBuildOptions("TestClass", "")
	plan, err := r.BuildReadRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, []string{"node1"}, plan.Replicas)
	require.Equal(t, []string{"host1.example.com"}, plan.ReplicasHostAddrs)
}

func TestMultiTenantRouter_HostnameNotFound(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := cluster.NewMockNodeSelector(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "luke").Return([]string{"node1"}, nil)

	tenantStatus := map[string]string{"luke": models.TenantActivityStatusHOT}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1", "node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", "luke", []string{"node1"}).
		Return([]string{"node1"}, []string{"node2"})

	mockClusterReader.EXPECT().LocalName().Return("node1")
	mockClusterReader.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockClusterReader.EXPECT().NodeHostname("node2").Return("", false)

	r, err := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	).Build()
	require.NoError(t, err)

	params := createRoutingPlanBuildOptions("TestClass", "luke")
	plan, err := r.BuildReadRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, []string{"node1"}, plan.Replicas)
	require.Equal(t, []string{"host1.example.com"}, plan.ReplicasHostAddrs)
}

func TestMultiTenantRouter_MultipleTenantsSameCollection(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2", "node3", "node4")
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

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
		mockMetadataReader.EXPECT().ShardReplicas("TestClass", tenant).Return(replicas, nil)
		mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", tenant, replicas).
			Return(replicas)
		mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", tenant, replicas).
			Return([]string{replicas[0]}, replicas[1:])
	}

	r, err := router.NewBuilder("TestClass", true, mockClusterReader,
		mockSchemaGetter, mockMetadataReader, mockReplicationFSM).Build()
	require.NoError(t, err)

	for tenant, expectedReplicas := range tenants {
		readReplicas, writeReplicas, additionalWrites, err := r.GetReadWriteReplicasLocation("TestClass", tenant)

		require.NoError(t, err, "unexpected error for tenant %s", tenant)
		require.ElementsMatch(t, expectedReplicas, readReplicas, "read replicas mismatch for tenant %s", tenant)
		require.Equal(t, []string{expectedReplicas[0]}, writeReplicas, "write replicas mismatch for tenant %s", tenant)
		if len(expectedReplicas) > 1 {
			require.ElementsMatch(t, expectedReplicas[1:], additionalWrites, "additional writes mismatch for tenant %s", tenant)
		} else {
			require.Empty(t, additionalWrites, "additional writes should be empty for tenant %s", tenant)
		}
	}
}

func TestMultiTenantRouter_MixedTenantStates(t *testing.T) {
	tenants := map[string]struct {
		status      string
		shouldWork  bool
		description string
	}{
		"active-tenant-1": {models.TenantActivityStatusHOT, true, "primary active tenant"},
		"active-tenant-2": {models.TenantActivityStatusHOT, true, "secondary active tenant"},
		"cold-tenant":     {models.TenantActivityStatusCOLD, false, "deactivated tenant"},
		"frozen-tenant":   {models.TenantActivityStatusFROZEN, false, "archived tenant"},
		"freezing-tenant": {models.TenantActivityStatusFREEZING, false, "tenant being migrated"},
	}

	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2", "node3")
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	for tenantName, tenantsStatus := range tenants {
		tenantStatus := map[string]string{tenantName: tenantsStatus.status}
		mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", tenantName).
			Return(tenantStatus, nil)

		if tenantsStatus.shouldWork {
			mockMetadataReader.EXPECT().ShardReplicas("TestClass", tenantName).Return([]string{"node1", "node2"}, nil)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", tenantName, []string{"node1", "node2"}).
				Return([]string{"node1", "node2"})
			mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", tenantName, []string{"node1", "node2"}).
				Return([]string{"node1"}, []string{"node2"})
		}
	}

	r, err := router.NewBuilder("TestClass", true, mockClusterReader,
		mockSchemaGetter, mockMetadataReader, mockReplicationFSM).Build()
	require.NoError(t, err)

	for tenantName, tenantsStatus := range tenants {
		t.Run(tenantName, func(t *testing.T) {
			readReplicas, writeReplicas, additionalWrites, err := r.GetReadWriteReplicasLocation("TestClass", tenantName)

			if tenantsStatus.shouldWork {
				require.NoError(t, err, "%s: should work", tenantsStatus.description)
				require.ElementsMatch(t, []string{"node1", "node2"}, readReplicas)
				require.Equal(t, []string{"node1"}, writeReplicas)
				require.Equal(t, []string{"node2"}, additionalWrites)
			} else {
				require.Error(t, err, "%s: should fail", tenantsStatus.description)
				require.Contains(t, err.Error(), "tenant not active", "error should mention tenant not active")
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
			mockClusterReader := mocks.NewMockNodeSelector("node1", "node2", "node3")
			mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

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
			mockMetadataReader.EXPECT().ShardReplicas(collection, tenantName).Return(expectedReplicas, nil)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasRead(collection, tenantName, expectedReplicas).
				Return(expectedReplicas)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite(collection, tenantName, expectedReplicas).
				Return([]string{expectedReplicas[0]}, expectedReplicas[1:])

			r, err := router.NewBuilder(collection, true, mockClusterReader,
				mockSchemaGetter, mockMetadataReader, mockReplicationFSM).Build()
			require.NoError(t, err)

			readReplicas, writeReplicas, additionalWrites, err := r.GetReadWriteReplicasLocation(collection, tenantName)

			require.NoError(t, err, "unexpected error for collection %s", collection)
			require.ElementsMatch(t, expectedReplicas, readReplicas, "read replicas mismatch for collection %s", collection)
			require.Equal(t, []string{expectedReplicas[0]}, writeReplicas, "write replicas mismatch for collection %s", collection)
			require.ElementsMatch(t, expectedReplicas[1:], additionalWrites, "additional writes mismatch for collection %s", collection)
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
			mockClusterReader := cluster.NewMockNodeSelector(t)
			mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

			tenantStatus := map[string]string{testCase.tenant: models.TenantActivityStatusHOT}
			mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", testCase.tenant).
				Return(tenantStatus, nil)
			mockMetadataReader.EXPECT().ShardReplicas("TestClass", testCase.tenant).Return(testCase.replicas, nil)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasRead("TestClass", testCase.tenant, testCase.replicas).
				Return(testCase.replicas)
			mockReplicationFSM.EXPECT().FilterOneShardReplicasWrite("TestClass", testCase.tenant, testCase.replicas).
				Return(testCase.replicas[:1], testCase.replicas[1:])

			for _, replica := range testCase.replicas {
				mockClusterReader.EXPECT().NodeHostname(replica).Return(replica+".example.com", true)
			}

			if testCase.directCandidate == "" {
				mockClusterReader.EXPECT().LocalName().Return("node1")
			}

			r, err := router.NewBuilder("TestClass", true, mockClusterReader,
				mockSchemaGetter, mockMetadataReader, mockReplicationFSM).Build()
			require.NoError(t, err)

			params := types.RoutingPlanBuildOptions{
				Collection:             "TestClass",
				Shard:                  testCase.tenant,
				ConsistencyLevel:       types.ConsistencyLevelOne,
				DirectCandidateReplica: testCase.directCandidate,
			}

			plan, err := r.BuildReadRoutingPlan(params)

			require.NoError(t, err, "unexpected error for %s", testCase.description)
			require.Equal(t, "TestClass", plan.Collection)
			require.Equal(t, testCase.tenant, plan.Shard)
			require.Len(t, plan.Replicas, testCase.expectedTotal, "replica count mismatch for %s", testCase.description)
			require.Len(t, plan.ReplicasHostAddrs, testCase.expectedTotal, "host address count mismatch for %s", testCase.description)
			require.Equal(t, testCase.expectedFirst, plan.Replicas[0], "first replica mismatch for %s", testCase.description)
			require.Equal(t, testCase.expectedFirst+".example.com", plan.ReplicasHostAddrs[0], "first host address mismatch for %s", testCase.description)
		})
	}
}
