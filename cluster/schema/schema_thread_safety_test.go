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

package schema

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestConcurrentSchemaAccess(t *testing.T) {
	tests := []struct {
		name string
		test func(*testing.T, *schema)
	}{
		{
			name: "concurrent read and write access to classes",
			test: testConcurrentReadWrite,
		},
		{
			name: "concurrent read-only operations",
			test: testConcurrentReadOnly,
		},
		{
			name: "concurrent class modifications",
			test: testConcurrentClassModifications,
		},
		{
			name: "concurrent schema operations",
			test: testConcurrentSchemaOperations,
		},
		{
			name: "concurrent shard operations",
			test: testConcurrentShardOperations,
		},
		{
			name: "concurrent tenant operations",
			test: testConcurrentTenantOperations,
		},
		{
			name: "concurrent meta operations",
			test: testConcurrentMetaOperations,
		},
		{
			name: "concurrent class info operations",
			test: testConcurrentClassInfoOperations,
		},
		{
			name: "concurrent read lock operations",
			test: testConcurrentReadLockOperations,
		},
		{
			name: "concurrent tenant management operations",
			test: testConcurrentTenantManagementOperations,
		},
		{
			name: "concurrent sharding state operations",
			test: testConcurrentShardingStateOperations,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSchema("testNode", &mockShardReader{}, prometheus.NewPedanticRegistry())
			tt.test(t, s)
		})
	}
}

func testConcurrentReadWrite(t *testing.T, s *schema) {
	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // readers + writers

	// Start readers
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				schema := s.ReadOnlySchema()
				_ = schema                   // Note: we just use the schema to prevent optimization
				time.Sleep(time.Microsecond) // Simulate some work
			}
		}()
	}

	// Start writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				className := fmt.Sprintf("Class%d_%d", id, j)
				class := &models.Class{
					Class: className,
					Properties: []*models.Property{
						{Name: "prop1", DataType: []string{"string"}},
					},
				}
				err := s.addClass(class, &sharding.State{}, uint64(j))
				if err != nil && !errors.Is(err, ErrClassExists) {
					t.Errorf("unexpected error adding class: %v", err)
				}
				time.Sleep(time.Microsecond) // Simulate some work
			}
		}(i)
	}

	wg.Wait()
}

func testConcurrentReadOnly(t *testing.T, s *schema) {
	// Setup some initial data
	initialClasses := []string{"Class1", "Class2", "Class3"}
	for _, className := range initialClasses {
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "prop1", DataType: []string{"string"}},
			},
		}
		require.NoError(t, s.addClass(class, &sharding.State{}, 1))
	}

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3)

	// Test concurrent ReadOnlySchema
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				schema := s.ReadOnlySchema()
				assert.NotEmpty(t, schema.Classes)
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent ReadOnlyClass
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				class, version := s.ReadOnlyClass("Class1")
				if class != nil {
					assert.Equal(t, "Class1", class.Class)
					assert.Greater(t, version, uint64(0))
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent ReadOnlyClasses
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				classes := s.ReadOnlyClasses(initialClasses...)
				assert.NotEmpty(t, classes)
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

func testConcurrentClassModifications(t *testing.T, s *schema) {
	class := &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{Name: "prop1", DataType: []string{"string"}},
		},
	}
	require.NoError(t, s.addClass(class, &sharding.State{}, 1))

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Test concurrent property additions
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				prop := &models.Property{
					Name:     fmt.Sprintf("prop_%d_%d", id, j),
					DataType: []string{"string"},
				}
				_ = s.addProperty("TestClass", uint64(j), prop)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Test concurrent reads while modifying
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				class, _ := s.ReadOnlyClass("TestClass")
				if class != nil {
					assert.Equal(t, "TestClass", class.Class)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

func testConcurrentSchemaOperations(t *testing.T, s *schema) {
	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 4)

	// Test concurrent class additions and deletions
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				className := fmt.Sprintf("Class%d_%d", id, j)
				class := &models.Class{
					Class: className,
					Properties: []*models.Property{
						{Name: "prop1", DataType: []string{"string"}},
					},
				}
				_ = s.addClass(class, &sharding.State{}, uint64(j))
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Test concurrent deletions
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				className := fmt.Sprintf("Class%d_%d", id, j)
				s.deleteClass(className)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Test concurrent class equality checks
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				className := fmt.Sprintf("Class%d_%d", id, j)
				_ = s.ClassEqual(className)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Test concurrent length checks
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = s.len()
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

func testConcurrentShardOperations(t *testing.T, s *schema) {
	// Setup initial class with shards
	class := &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{Name: "prop1", DataType: []string{"string"}},
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 2,
		},
	}
	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			"shard1": {
				Name:           "shard1",
				BelongsToNodes: []string{"node1", "node2"},
				Status:         "HOT",
			},
			"shard2": {
				Name:           "shard2",
				BelongsToNodes: []string{"node2", "node3"},
				Status:         "HOT",
			},
		},
		// Add virtual shards mapping
		Virtual: []sharding.Virtual{
			{
				Name:               "00000000-0000-0000-0000-000000000000",
				AssignedToPhysical: "shard1",
				Upper:              1000,
				OwnsPercentage:     50.0,
			},
			{
				Name:               "00000000-0000-0000-0000-000000000001",
				AssignedToPhysical: "shard2",
				Upper:              2000,
				OwnsPercentage:     50.0,
			},
		},
	}
	require.NoError(t, s.addClass(class, shardState, 1))

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3)

	// Test concurrent ShardOwner calls
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				owner, _, _ := s.ShardOwner("TestClass", "shard1")
				if owner != "" {
					assert.Contains(t, []string{"node1", "node2"}, owner)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent ShardReplicas calls
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				replicas, _, _ := s.ShardReplicas("TestClass", "shard1")
				if len(replicas) > 0 {
					assert.Subset(t, []string{"node1", "node2"}, replicas)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent ShardFromUUID calls with valid UUID
	testUUID := []byte("00000000-0000-0000-0000-000000000000")
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				shard, _ := s.ShardFromUUID("TestClass", testUUID)
				if shard != "" {
					assert.Equal(t, "shard1", shard)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

func testConcurrentTenantOperations(t *testing.T, s *schema) {
	// Setup initial class with multi-tenancy
	class := &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{Name: "prop1", DataType: []string{"string"}},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			"tenant1": {
				Name:   "tenant1",
				Status: "HOT",
			},
			"tenant2": {
				Name:   "tenant2",
				Status: "HOT",
			},
		},
	}
	require.NoError(t, s.addClass(class, shardState, 1))

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Test concurrent MultiTenancy calls
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				config := s.MultiTenancy("TestClass")
				assert.True(t, config.Enabled)
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent TenantsShards calls
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				shards, _ := s.TenantsShards("TestClass", "tenant1", "tenant2")
				if len(shards) > 0 {
					assert.Contains(t, shards, "HOT")
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

func testConcurrentMetaOperations(t *testing.T, s *schema) {
	// Setup initial data
	setupClasses := []string{"Class1", "Class2", "Class3"}
	for _, className := range setupClasses {
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: "prop1", DataType: []string{"string"}},
			},
		}
		require.NoError(t, s.addClass(class, &sharding.State{}, 1))
	}

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Test concurrent MetaClasses access
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				classes := s.MetaClasses()
				assert.NotEmpty(t, classes)
				// Verify we can safely access the data
				for _, meta := range classes {
					assert.NotEmpty(t, meta.Class.Class)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent modifications while accessing meta
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				className := fmt.Sprintf("MetaClass%d_%d", id, j)
				class := &models.Class{
					Class: className,
					Properties: []*models.Property{
						{Name: "prop1", DataType: []string{"string"}},
					},
				}
				_ = s.addClass(class, &sharding.State{}, uint64(j))
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
}

func testConcurrentClassInfoOperations(t *testing.T, s *schema) {
	// Setup initial class
	class := &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{Name: "prop1", DataType: []string{"string"}},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 2,
		},
	}
	require.NoError(t, s.addClass(class, &sharding.State{}, 1))

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Test concurrent ClassInfo calls
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				info := s.ClassInfo("TestClass")
				if info.Exists {
					assert.True(t, info.MultiTenancy.Enabled)
					assert.Equal(t, 2, info.ReplicationFactor)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent ClassEqual calls
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				name := s.ClassEqual("testclass") // Testing case-insensitive match
				if name != "" {
					assert.Equal(t, "TestClass", name)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

func testConcurrentReadLockOperations(t *testing.T, s *schema) {
	// Setup initial class
	class := &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{Name: "prop1", DataType: []string{"string"}},
		},
	}
	require.NoError(t, s.addClass(class, &sharding.State{}, 1))

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Test concurrent Read operations
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				err := s.Read("TestClass", func(cls *models.Class, state *sharding.State) error {
					assert.Equal(t, "TestClass", cls.Class)
					assert.NotNil(t, state)
					return nil
				})
				assert.NoError(t, err)
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent updateClass operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				err := s.updateClass("TestClass", func(mc *metaClass) error {
					mc.ClassVersion = uint64(j)
					return nil
				})
				assert.NoError(t, err)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
}

func testConcurrentTenantManagementOperations(t *testing.T, s *schema) {
	// Setup initial class with multi-tenancy
	class := &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{Name: "prop1", DataType: []string{"string"}},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			"tenant1": {
				Name:   "tenant1",
				Status: "READY",
			},
		},
	}
	require.NoError(t, s.addClass(class, shardState, 1))

	const numGoroutines = 10
	const iterations = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 4)

	// Test concurrent getTenants operations
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				tenants, err := s.getTenants("TestClass", []string{"tenant1"})
				if err == nil {
					assert.NotEmpty(t, tenants)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent addTenants operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				req := &command.AddTenantsRequest{
					ClusterNodes: []string{"node1"},
					Tenants: []*command.Tenant{
						{Name: fmt.Sprintf("new_tenant_%d_%d", id, j)},
					},
				}
				_ = s.addTenants("TestClass", uint64(j), req)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Test concurrent updateTenants operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				req := &command.UpdateTenantsRequest{
					Tenants: []*command.Tenant{
						{Name: "tenant1"},
					},
				}
				fsm := NewMockreplicationFSM(t)
				fsm.On("HasOngoingReplication", mock.Anything, mock.Anything, mock.Anything).Return(false).Maybe()
				_ = s.updateTenants("TestClass", uint64(j), req, fsm)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	// Test concurrent deleteTenants operations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				req := &command.DeleteTenantsRequest{
					Tenants: []string{fmt.Sprintf("new_tenant_%d_%d", id, j)},
				}
				_ = s.deleteTenants("TestClass", uint64(j), req)
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait()
}

func testConcurrentShardingStateOperations(t *testing.T, s *schema) {
	// Setup initial class
	class := &models.Class{
		Class: "TestClass",
		Properties: []*models.Property{
			{Name: "prop1", DataType: []string{"string"}},
		},
	}
	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			"shard1": {
				Name:   "shard1",
				Status: "HOT",
			},
		},
	}
	require.NoError(t, s.addClass(class, shardState, 1))

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // For CopyShardingState and GetShardsStatus operations

	// Test concurrent CopyShardingState operations
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				state, version := s.CopyShardingState("TestClass")
				if state != nil {
					assert.NotNil(t, state.Physical["shard1"])
					assert.Greater(t, version, uint64(0))
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Test concurrent GetShardsStatus operations
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				status, _ := s.GetShardsStatus("TestClass", "")
				if status != nil {
					assert.NotEmpty(t, status)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

// Additional mock for shard reader
type mockShardReader struct{}

func (m *mockShardReader) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	return models.ShardStatusList{
		{Status: "HOT", Name: "shard1"},
	}, nil
}
