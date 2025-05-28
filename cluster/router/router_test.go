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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/router"
	"github.com/weaviate/weaviate/cluster/router/types"
	schemaTypes "github.com/weaviate/weaviate/cluster/schema/types"
	"github.com/weaviate/weaviate/entities/models"
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

func TestRouterBuilder_Build_MissingDependencies(t *testing.T) {
	tests := []struct {
		name          string
		className     string
		partitioning  bool
		setupMocks    func() (cluster.NodeSelector, schema.SchemaGetter, schemaTypes.SchemaReader, replicationTypes.ReplicationFSMReader)
		expectedError string
	}{
		{
			name:         "missing className",
			className:    "",
			partitioning: false,
			setupMocks: func() (cluster.NodeSelector, schema.SchemaGetter, schemaTypes.SchemaReader, replicationTypes.ReplicationFSMReader) {
				return mocks.NewMockNodeSelector("node1", "node2"), schema.NewMockSchemaGetter(t), schemaTypes.NewMockSchemaReader(t), replicationTypes.NewMockReplicationFSMReader(t)
			},
			expectedError: "className is required",
		},
		{
			name:         "missing schemaGetter",
			className:    "TestClass",
			partitioning: false,
			setupMocks: func() (cluster.NodeSelector, schema.SchemaGetter, schemaTypes.SchemaReader, replicationTypes.ReplicationFSMReader) {
				return mocks.NewMockNodeSelector("node1", "node2"), nil, schemaTypes.NewMockSchemaReader(t), replicationTypes.NewMockReplicationFSMReader(t)
			},
			expectedError: "schemaGetter is required",
		},
		{
			name:         "missing replicationFSMReader",
			className:    "TestClass",
			partitioning: false,
			setupMocks: func() (cluster.NodeSelector, schema.SchemaGetter, schemaTypes.SchemaReader, replicationTypes.ReplicationFSMReader) {
				return mocks.NewMockNodeSelector("node1", "node2"), schema.NewMockSchemaGetter(t), schemaTypes.NewMockSchemaReader(t), nil
			},
			expectedError: "replicationFSMReader is required",
		},
		{
			name:         "missing clusterStateReader",
			className:    "TestClass",
			partitioning: false,
			setupMocks: func() (cluster.NodeSelector, schema.SchemaGetter, schemaTypes.SchemaReader, replicationTypes.ReplicationFSMReader) {
				return nil, schema.NewMockSchemaGetter(t), schemaTypes.NewMockSchemaReader(t), replicationTypes.NewMockReplicationFSMReader(t)
			},
			expectedError: "clusterStateReader is required",
		},
		{
			name:         "missing metadataReader for single-tenant",
			className:    "TestClass",
			partitioning: false,
			setupMocks: func() (cluster.NodeSelector, schema.SchemaGetter, schemaTypes.SchemaReader, replicationTypes.ReplicationFSMReader) {
				return mocks.NewMockNodeSelector("node1", "node2"), schema.NewMockSchemaGetter(t), nil, replicationTypes.NewMockReplicationFSMReader(t)
			},
			expectedError: "metadataReader is required for single-tenant router",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterReader, schemaGetter, metadataReader, replicationFSM := tt.setupMocks()

			builder := router.NewBuilder(
				tt.className,
				tt.partitioning,
				clusterReader,
				schemaGetter,
				metadataReader,
				replicationFSM,
			)

			r, err := builder.Build()

			require.Nil(t, r, "unexpected nil router")
			require.Error(t, err, "missing expected error while building router")
			require.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestRouterBuilder_Build_Success(t *testing.T) {
	tests := []struct {
		name         string
		partitioning bool
	}{
		{
			name:         "single-tenant router",
			partitioning: false,
		},
		{
			name:         "multi-tenant router",
			partitioning: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")
			mockSchemaGetter := schema.NewMockSchemaGetter(t)
			mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)

			var mockMetadataReader schemaTypes.SchemaReader
			if !tt.partitioning {
				mockMetadataReader = schemaTypes.NewMockSchemaReader(t)
			}

			builder := router.NewBuilder(
				"TestClass",
				tt.partitioning,
				mockClusterReader,
				mockSchemaGetter,
				mockMetadataReader,
				mockReplicationFSM,
			)

			r, err := builder.Build()

			require.NoError(t, err, "unexpected error building router")
			require.NotNil(t, r, "unexpected nil router")
		})
	}
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_NoShards(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	emptyState := createShardingStateWithShards([]string{})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

	builder := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err, "unexpected error building router")

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.NoError(t, err, "unexpected error while getting read/write replica locations")
	require.Empty(t, readReplicas, "read/write replica locations should be empty")
	require.Empty(t, writeReplicas, "write/read replica locations should be empty")
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

	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"}, []string{"node1"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "shard2", []string{"node2", "node3"}).
		Return([]string{"node2", "node3"}, []string{"node2"})
	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "shard3", []string{"node3", "node1"}).
		Return([]string{"node3", "node1"}, []string{"node3"})

	builder := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err, "unexpected error building router")

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.NoError(t, err, "unexpected error while getting read/write replica locations")
	require.ElementsMatch(t, []string{"node1", "node2", "node3"}, readReplicas)
	require.ElementsMatch(t, []string{"node1", "node2", "node3"}, writeReplicas)
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

	builder := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "shard metadata error")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_TenantRequestInNonMultiTenant(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	emptyState := createShardingStateWithShards([]string{})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

	builder := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.NoError(t, err)
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_Success(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "luke", []string{"luke"}).
		Return([]string{"node1", "node2"}, []string{"node1"})

	builder := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.NoError(t, err)
	require.Equal(t, []string{"node1", "node2"}, readReplicas)
	require.Equal(t, []string{"node1"}, writeReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_TenantNotFound(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	tenantStatus := map[string]string{}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	builder := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not found: \"luke\"")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_TenantNotActive(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusCOLD,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	builder := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not active: \"luke\"")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_ErrorFromDependencies(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(map[string]string{}, errors.New("schema service error"))

	builder := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.Error(t, err)
	require.Contains(t, err.Error(), "schema service error")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
}

func TestMultiTenantRouter_GetReadWriteReplicasLocation_NonTenantRequestForMultiTenant(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	builder := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant is required for multi-tenant collections")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
}

func TestSingleTenantRouter_GetReadWriteReplicasLocation_TenantRequestForSingleTenant(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := cluster.NewMockNodeSelector(t)

	builder := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant must be empty for single-tenant collections")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
}

func TestSingleTenantRouter_BuildReadRoutingPlan_NoReplicas(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1")

	emptyState := createShardingStateWithShards([]string{})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

	builder := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	)

	r, err := builder.Build()
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

	tenantStatus := map[string]string{
		"luke": models.TenantActivityStatusHOT,
	}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)

	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "luke", []string{"luke"}).
		Return([]string{"node1", "node2"}, []string{"node1"})

	builder := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	params := createRoutingPlanBuildOptions("TestClass", "luke")
	plan, err := r.BuildReadRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, "TestClass", plan.Collection)
	require.Equal(t, "luke", plan.Shard)
	require.Equal(t, []string{"node1", "node2"}, plan.Replicas)
	require.Equal(t, []string{"node1", "node2"}, plan.ReplicasHostAddrs)
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

			builder := router.NewBuilder(
				"TestClass",
				tt.partitioning,
				mockClusterReader,
				mockSchemaGetter,
				mockMetadataReader,
				mockReplicationFSM,
			)

			r, err := builder.Build()
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

			builder := router.NewBuilder(
				"TestClass",
				tt.partitioning,
				mockClusterReader,
				mockSchemaGetter,
				mockMetadataReader,
				mockReplicationFSM,
			)

			r, err := builder.Build()
			require.NoError(t, err)

			hostnames := r.AllHostnames()
			require.Equal(t, expectedHostnames, hostnames)
		})
	}
}

func TestRouter_GetWriteReplicasLocation_DelegatesCorrectly(t *testing.T) {
	t.Run("single-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

		emptyState := createShardingStateWithShards([]string{})
		mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

		builder := router.NewBuilder(
			"TestClass",
			false,
			mockClusterReader,
			mockSchemaGetter,
			mockMetadataReader,
			mockReplicationFSM,
		)

		r, err := builder.Build()
		require.NoError(t, err)

		writeReplicas, err := r.GetWriteReplicasLocation("TestClass", "")
		require.NoError(t, err)
		require.Empty(t, writeReplicas)
	})

	t.Run("multi-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

		tenantStatus := map[string]string{
			"luke": models.TenantActivityStatusHOT,
		}
		mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
			Return(tenantStatus, nil)

		mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "luke", []string{"luke"}).
			Return([]string{"node1", "node2"}, []string{"node1"})

		builder := router.NewBuilder(
			"TestClass",
			true,
			mockClusterReader,
			mockSchemaGetter,
			nil,
			mockReplicationFSM,
		)

		r, err := builder.Build()
		require.NoError(t, err)

		writeReplicas, err := r.GetWriteReplicasLocation("TestClass", "luke")
		require.NoError(t, err)
		require.Equal(t, []string{"node1"}, writeReplicas)
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

		builder := router.NewBuilder(
			"TestClass",
			false,
			mockClusterReader,
			mockSchemaGetter,
			mockMetadataReader,
			mockReplicationFSM,
		)

		r, err := builder.Build()
		require.NoError(t, err)

		readReplicas, err := r.GetReadReplicasLocation("TestClass", "")
		require.NoError(t, err)
		require.Empty(t, readReplicas)
	})

	t.Run("multi-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

		tenantStatus := map[string]string{
			"luke": models.TenantActivityStatusHOT,
		}
		mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
			Return(tenantStatus, nil)

		mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "luke", []string{"luke"}).
			Return([]string{"node1", "node2"}, []string{"node1"})

		builder := router.NewBuilder(
			"TestClass",
			true,
			mockClusterReader,
			mockSchemaGetter,
			nil,
			mockReplicationFSM,
		)

		r, err := builder.Build()
		require.NoError(t, err)

		readReplicas, err := r.GetReadReplicasLocation("TestClass", "luke")
		require.NoError(t, err)
		require.Equal(t, []string{"node1", "node2"}, readReplicas)
	})
}

func TestRouter_BuildWriteRoutingPlan_DelegatesCorrectly(t *testing.T) {
	t.Run("single-tenant", func(t *testing.T) {
		mockSchemaGetter := schema.NewMockSchemaGetter(t)
		mockMetadataReader := schemaTypes.NewMockSchemaReader(t)
		mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
		mockClusterReader := mocks.NewMockNodeSelector("node1")

		emptyState := createShardingStateWithShards([]string{})
		mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(emptyState)

		builder := router.NewBuilder(
			"TestClass",
			false,
			mockClusterReader,
			mockSchemaGetter,
			mockMetadataReader,
			mockReplicationFSM,
		)

		r, err := builder.Build()
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

		tenantStatus := map[string]string{
			"luke": models.TenantActivityStatusHOT,
		}
		mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
			Return(tenantStatus, nil)

		mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "luke", []string{"luke"}).
			Return([]string{"node1", "node2"}, []string{"node1"})

		builder := router.NewBuilder(
			"TestClass",
			true,
			mockClusterReader,
			mockSchemaGetter,
			nil,
			mockReplicationFSM,
		)

		r, err := builder.Build()
		require.NoError(t, err)

		params := createRoutingPlanBuildOptions("TestClass", "luke")
		plan, err := r.BuildWriteRoutingPlan(params)

		require.NoError(t, err)
		require.Equal(t, "TestClass", plan.Collection)
		require.Equal(t, "luke", plan.Shard)
		require.Equal(t, []string{"node1", "node2"}, plan.Replicas)
		require.Equal(t, []string{"node1", "node2"}, plan.ReplicasHostAddrs)
	})
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
	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "shard1", []string{"node1", directCandidateNode, "node3"}).
		Return([]string{"node1", directCandidateNode, "node3"}, []string{"node1", directCandidateNode})

	// node2 should be first due to direct candidate expected to be the first node
	mockClusterReader.EXPECT().NodeHostname(directCandidateNode).Return("host2.example.com", true)
	mockClusterReader.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockClusterReader.EXPECT().NodeHostname("node3").Return("host3.example.com", true)

	builder := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	)

	r, err := builder.Build()
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

func TestMultiTenantRouter_BuildRoutingPlan_TenantNotFoundDuringBuild(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1", "node2")

	tenantStatus := map[string]string{}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "nonexistent").
		Return(tenantStatus, nil)

	builder := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	params := createRoutingPlanBuildOptions("TestClass", "nonexistent")
	plan, err := r.BuildReadRoutingPlan(params)

	require.Error(t, err)
	require.Contains(t, err.Error(), "could not get read replicas location from sharding state")
	require.Empty(t, plan.Replicas)
}

func TestSingleTenantRouter_HostnameNotFound(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := cluster.NewMockNodeSelector(t)
	mockMetadataReader := schemaTypes.NewMockSchemaReader(t)

	state := createShardingStateWithShards([]string{"shard1"})
	mockSchemaGetter.EXPECT().CopyShardingState("TestClass").Return(state)

	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard1").Return([]string{"node1", "node2"}, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "shard1", []string{"node1", "node2"}).
		Return([]string{"node1", "node2"}, []string{"node1"})

	mockClusterReader.EXPECT().LocalName().Return("node1")
	mockClusterReader.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockClusterReader.EXPECT().NodeHostname("node2").Return("", false)

	builder := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	)

	r, err := builder.Build()
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

	tenantStatus := map[string]string{"luke": models.TenantActivityStatusHOT}
	mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
		Return(tenantStatus, nil)
	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "luke", []string{"luke"}).
		Return([]string{"node1", "node2"}, []string{"node1"})

	mockClusterReader.EXPECT().LocalName().Return("node1")
	mockClusterReader.EXPECT().NodeHostname("node1").Return("host1.example.com", true)
	mockClusterReader.EXPECT().NodeHostname("node2").Return("", false)

	builder := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	params := createRoutingPlanBuildOptions("TestClass", "luke")
	plan, err := r.BuildReadRoutingPlan(params)

	require.NoError(t, err)
	require.Equal(t, []string{"node1"}, plan.Replicas)
	require.Equal(t, []string{"host1.example.com"}, plan.ReplicasHostAddrs)
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
	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "shard1", []string{"node1"}).
		Return([]string{"node1"}, []string{"node1"})

	// Second shard failure
	mockMetadataReader.EXPECT().ShardReplicas("TestClass", "shard2").Return([]string{}, errors.New("shard2 error"))

	builder := router.NewBuilder(
		"TestClass",
		false,
		mockClusterReader,
		mockSchemaGetter,
		mockMetadataReader,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "shard2 error")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
}

func TestMultiTenantRouter_TenantStatusChangeDuringOperation(t *testing.T) {
	mockSchemaGetter := schema.NewMockSchemaGetter(t)
	mockReplicationFSM := replicationTypes.NewMockReplicationFSMReader(t)
	mockClusterReader := mocks.NewMockNodeSelector("node1")

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

	mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "luke", []string{"luke"}).
		Return([]string{"node1"}, []string{"node1"}).Once()

	builder := router.NewBuilder(
		"TestClass",
		true,
		mockClusterReader,
		mockSchemaGetter,
		nil,
		mockReplicationFSM,
	)

	r, err := builder.Build()
	require.NoError(t, err)

	readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")
	require.NoError(t, err)
	require.Equal(t, []string{"node1"}, readReplicas)
	require.Equal(t, []string{"node1"}, writeReplicas)

	readReplicas, writeReplicas, err = r.GetReadWriteReplicasLocation("TestClass", "luke")
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant not active")
	require.Empty(t, readReplicas)
	require.Empty(t, writeReplicas)
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

			tenantStatus := map[string]string{
				"luke": test.status,
			}
			mockSchemaGetter.EXPECT().OptimisticTenantStatus(mock.Anything, "TestClass", "luke").
				Return(tenantStatus, nil)

			if !test.shouldErr {
				mockReplicationFSM.EXPECT().FilterOneShardReplicasReadWrite("TestClass", "luke", []string{"luke"}).
					Return([]string{"node1"}, []string{"node1"})
			}

			builder := router.NewBuilder(
				"TestClass",
				true,
				mockClusterReader,
				mockSchemaGetter,
				nil,
				mockReplicationFSM,
			)

			r, err := builder.Build()
			require.NoError(t, err)

			readReplicas, writeReplicas, err := r.GetReadWriteReplicasLocation("TestClass", "luke")

			if test.shouldErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errMsg)
				require.Empty(t, readReplicas)
				require.Empty(t, writeReplicas)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"node1"}, readReplicas)
				require.Equal(t, []string{"node1"}, writeReplicas)
			}
		})
	}
}
