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
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/sharding"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

func TestMakeTenantWithBelongsToNodes(t *testing.T) {
	const tenant = "tenant"
	const status = "status"

	physical := sharding.Physical{
		Status:         status,
		BelongsToNodes: []string{"node1"},
	}

	t.Run("Creates valid response", func(t *testing.T) {
		tenantResp := makeTenantResponse(tenant, physical)

		assert.Equal(t, tenant, tenantResp.Name)
		assert.Equal(t, status, tenantResp.ActivityStatus)
		assert.ElementsMatch(t, tenantResp.BelongsToNodes, physical.BelongsToNodes)
	})

	t.Run("BelongsToNodes is a copy", func(t *testing.T) {
		tenantResp := makeTenantResponse(tenant, physical)

		assert.NotEqual(t,
			reflect.ValueOf(tenantResp.BelongsToNodes).Pointer(),
			reflect.ValueOf(physical.BelongsToNodes).Pointer())
	})
}

func Test_schemaCollectionMetrics(t *testing.T) {
	r := prometheus.NewPedanticRegistry()

	s := NewSchema("testNode", nil, r)
	ss := &sharding.State{}

	c1 := &models.Class{
		Class: "collection1",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 1,
		},
	}
	c2 := &models.Class{
		Class: "collection2",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 1,
		},
	}

	// Collection metrics
	assert.Equal(t, float64(0), testutil.ToFloat64(s.collectionsCount))
	require.NoError(t, s.addClass(c1, ss, 0)) // adding c1 collection
	assert.Equal(t, float64(1), testutil.ToFloat64(s.collectionsCount))

	require.NoError(t, s.addClass(c2, ss, 0)) // adding c2 collection
	assert.Equal(t, float64(2), testutil.ToFloat64(s.collectionsCount))

	// delete c2
	s.deleteClass("collection2")
	assert.Equal(t, float64(1), testutil.ToFloat64(s.collectionsCount))

	// delete c1
	s.deleteClass("collection1")
	assert.Equal(t, float64(0), testutil.ToFloat64(s.collectionsCount))
}

func Test_schemaShardMetrics(t *testing.T) {
	r := prometheus.NewPedanticRegistry()

	s := NewSchema("testNode", nil, r)
	ss := &sharding.State{}

	c1 := &models.Class{
		Class: "collection1",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 1,
		},
	}
	c2 := &models.Class{
		Class: "collection2",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 1,
		},
	}

	require.NoError(t, s.addClass(c1, ss, 0)) // adding c1 collection
	require.NoError(t, s.addClass(c2, ss, 0)) // adding c2 collection

	// Shard metrics
	// no shards now.
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("")))

	// add shard to c1 collection
	err := s.addTenants(c1.Class, 0, &api.AddTenantsRequest{
		ClusterNodes: []string{"testNode"},
		Tenants: []*api.Tenant{
			{
				Name:   "tenant1",
				Status: "HOT",
			},
			nil, // nil tenant shouldn't be counted in the metrics
		},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")))

	// add shard to c2 collection
	err = s.addTenants(c2.Class, 0, &api.AddTenantsRequest{
		ClusterNodes: []string{"testNode"},
		Tenants: []*api.Tenant{
			{
				Name:   "tenant2",
				Status: "FROZEN",
			},
			nil, // nil tenant shouldn't be counted in the metrics
		},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("FROZEN")))

	// delete "existing" tenant
	err = s.deleteTenants(c1.Class, 0, &api.DeleteTenantsRequest{
		Tenants: []string{"tenant1"},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")))
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("FROZEN")))

	// delete "non-existing" tenant
	err = s.deleteTenants(c1.Class, 0, &api.DeleteTenantsRequest{
		Tenants: []string{"tenant1"},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")))
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("FROZEN")))

	// update tenant status
	err = s.updateTenants(c2.Class, 0, &api.UpdateTenantsRequest{
		Tenants:      []*api.Tenant{{Name: "tenant2", Status: "HOT"}}, // FROZEN -> HOT
		ClusterNodes: []string{"testNode"},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("UNFREEZING")))
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("FROZEN")))

	// update tenant status
	err = s.updateTenantsProcess(c2.Class, 0, &api.TenantProcessRequest{
		Node:   "testNode",
		Action: api.TenantProcessRequest_ACTION_UNFREEZING,
		TenantsProcesses: []*api.TenantsProcess{
			{
				Tenant: &api.Tenant{Name: "tenant2", Status: "HOT"},
				Op:     api.TenantsProcess_OP_DONE,
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")))
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("UNFREEZING")))

	// Deleting collection with non-zero shards should decrement the shards count as well.
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")))
	require.True(t, s.deleteClass(c2.Class))
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")))

	// Adding class with non empty shard should increase the shard count
	ss = &sharding.State{
		Physical: make(map[string]sharding.Physical),
	}
	ss.Physical["random"] = sharding.Physical{
		Name:   "random",
		Status: "",
	}
	assert.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("")))
	require.NoError(t, s.addClass(c2, ss, 0))
	assert.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("")))
}

func Test_schemaDeepCopy(t *testing.T) {
	r := prometheus.NewPedanticRegistry()
	s := NewSchema("testNode", nil, r)

	class := &models.Class{
		Class: "test",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			"shard1": {
				Name:           "shard1",
				Status:         "HOT",
				BelongsToNodes: []string{"node1"},
			},
		},
	}

	require.NoError(t, s.addClass(class, shardState, 1))

	t.Run("MetaClasses deep copy", func(t *testing.T) {
		copied := s.MetaClasses()

		original := s.classes["test"]
		copiedClass := copied["test"]

		copiedClass.Class.Class = "modified"
		physical := copiedClass.Sharding.Physical["shard1"]
		physical.Status = "COLD"
		copiedClass.Sharding.Physical["shard1"] = physical

		assert.Equal(t, "test", original.Class.Class)
		assert.Equal(t, "HOT", original.Sharding.Physical["shard1"].Status)

		assert.Equal(t, original.ClassVersion, copiedClass.ClassVersion)
		assert.Equal(t, original.ShardVersion, copiedClass.ShardVersion)
	})

	t.Run("Concurrent access", func(t *testing.T) {
		done := make(chan bool)
		go func() {
			for i := 0; i < 100; i++ {
				s.MetaClasses()
				s.States()
			}
			done <- true
		}()

		for i := 0; i < 100; i++ {
			s.addClass(&models.Class{Class: fmt.Sprintf("concurrent%d", i)}, shardState, uint64(i))
		}
		<-done
	})
}
