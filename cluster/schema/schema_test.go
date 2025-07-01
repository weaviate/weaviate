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

package schema

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/usecases/sharding"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

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
	fsm := NewMockreplicationFSM(t)
	fsm.On("HasOngoingReplication", mock.Anything, mock.Anything, mock.Anything).Return(false).Maybe()
	err = s.updateTenants(c2.Class, 0, &api.UpdateTenantsRequest{
		Tenants:      []*api.Tenant{{Name: "tenant2", Status: "HOT"}}, // FROZEN -> HOT
		ClusterNodes: []string{"testNode"},
	}, fsm)
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

func TestSchemaRestoreLegacyWithEmptyClasses(t *testing.T) {
	// Test the scenario where snapshot contains "classes":{} which should unmarshal to empty map
	t.Run("empty classes object", func(t *testing.T) {
		s := NewSchema("test-node", &MockShardReader{}, nil)

		// Create snapshot JSON with empty classes object
		snapData := `{"node_id":"test-node","snapshot_id":"test-snapshot","classes":{}}`

		// Test RestoreLegacy
		mockParser := NewMockParser(t)

		err := s.RestoreLegacy([]byte(snapData), mockParser)
		require.NoError(t, err)

		// Verify that s.classes is an empty map, not nil
		assert.NotNil(t, s.classes)
		assert.Equal(t, 0, len(s.classes))
	})
}

func TestSchemaRestoreLegacyWithNilClasses(t *testing.T) {
	// Test the scenario where snapshot JSON unmarshaling results in nil Classes
	t.Run("nil classes after unmarshal", func(t *testing.T) {
		s := NewSchema("test-node", &MockShardReader{}, nil)

		// Create a snapshot struct with nil Classes to simulate unmarshal failure
		snap := snapshot{
			NodeID:     "test-node",
			SnapshotID: "test-snapshot",
			Classes:    nil, // This simulates the problematic case
		}

		// Marshal it back to JSON
		snapData, err := json.Marshal(snap)
		require.NoError(t, err)

		// Test RestoreLegacy
		mockParser := NewMockParser(t)
		err = s.RestoreLegacy(snapData, mockParser)
		require.NoError(t, err)

		// Verify that s.classes is initialized, not nil
		assert.NotNil(t, s.classes)
		assert.Equal(t, 0, len(s.classes))
	})
}

func TestSchemaAddClassAfterRestoreWithEmptyClasses(t *testing.T) {
	// Test the scenario where addClass is called after restoring empty classes
	t.Run("add class after empty restore", func(t *testing.T) {
		s := NewSchema("test-node", &MockShardReader{}, nil)

		// First restore with empty classes
		snapData := `{"node_id":"test-node","snapshot_id":"test-snapshot","classes":{}}`
		mockParser := NewMockParser(t)
		err := s.RestoreLegacy([]byte(snapData), mockParser)
		require.NoError(t, err)

		// Verify s.classes is not nil
		assert.NotNil(t, s.classes)

		// Now try to add a class - this should not panic
		cls := &models.Class{Class: "TestClass"}
		ss := &sharding.State{Physical: map[string]sharding.Physical{}}

		err = s.addClass(cls, ss, 1)
		require.NoError(t, err)

		// Verify the class was added
		assert.Equal(t, 1, len(s.classes))
		assert.NotNil(t, s.classes["TestClass"])
	})
}

func TestSchemaAddClassAfterRestoreWithNilClasses(t *testing.T) {
	// Test the scenario where addClass is called after restoring with nil classes
	t.Run("add class after nil restore", func(t *testing.T) {
		s := NewSchema("test-node", &MockShardReader{}, nil)

		// First restore with nil classes (simulating unmarshal failure)
		snap := snapshot{
			NodeID:     "test-node",
			SnapshotID: "test-snapshot",
			Classes:    nil,
		}
		snapData, err := json.Marshal(snap)
		require.NoError(t, err)

		mockParser := NewMockParser(t)
		err = s.RestoreLegacy(snapData, mockParser)
		require.NoError(t, err)

		// Verify s.classes is not nil
		assert.NotNil(t, s.classes)

		// Now try to add a class - this should not panic
		cls := &models.Class{Class: "TestClass"}
		ss := &sharding.State{Physical: map[string]sharding.Physical{}}

		err = s.addClass(cls, ss, 1)
		require.NoError(t, err)

		// Verify the class was added
		assert.Equal(t, 1, len(s.classes))
		assert.NotNil(t, s.classes["TestClass"])
	})
}

func TestCreateAlias(t *testing.T) {
	var (
		sc = NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
		ss = &sharding.State{Physical: make(map[string]sharding.Physical)}
	)

	require.Nil(t, sc.addClass(&models.Class{Class: "C"}, ss, 1))

	t.Run("successfully create alias", func(t *testing.T) {
		err := sc.createAlias("C", "A1")
		require.Nil(t, err)
	})

	t.Run("fail on conflicting creation", func(t *testing.T) {
		err := sc.createAlias("C", "A1")
		require.EqualError(t, err, "create alias: alias A1 already exists")
	})

	t.Run("fail on non-existing class", func(t *testing.T) {
		err := sc.createAlias("D", "newAlias")
		require.EqualError(t, err, "create alias: class D does not exist")
	})

	t.Run("fail on non-existing alias", func(t *testing.T) {
		err := sc.createAlias("D", "A1")
		require.EqualError(t, err, "create alias: alias A1 already exists")
	})
}

func TestReplaceAlias(t *testing.T) {
	var (
		sc = NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
		ss = &sharding.State{Physical: make(map[string]sharding.Physical)}
	)

	require.Nil(t, sc.addClass(&models.Class{Class: "C1"}, ss, 1))
	require.Nil(t, sc.addClass(&models.Class{Class: "C2"}, ss, 1))
	require.Nil(t, sc.createAlias("C1", "A1"))

	t.Run("successfully replace alias", func(t *testing.T) {
		err := sc.replaceAlias("C2", "A1")
		require.Nil(t, err)
	})

	t.Run("fail on non-existing alias", func(t *testing.T) {
		err := sc.replaceAlias("C1", "A2")
		require.EqualError(t, err, "replace alias: alias A2 does not exist")
	})

	t.Run("fail on non-existing class", func(t *testing.T) {
		err := sc.replaceAlias("D", "A1")
		require.EqualError(t, err, "replace alias: class D does not exist")
	})
}

func TestDeleteAlias(t *testing.T) {
	var (
		sc = NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
		ss = &sharding.State{Physical: make(map[string]sharding.Physical)}
	)

	require.Nil(t, sc.addClass(&models.Class{Class: "C"}, ss, 1))
	require.Nil(t, sc.createAlias("C", "A1"))

	t.Run("successfully delete alias", func(t *testing.T) {
		err := sc.deleteAlias("A1")
		require.Nil(t, err)
	})

	t.Run("idempotent deletion with non-existent alias", func(t *testing.T) {
		err := sc.deleteAlias("A2")
		require.Nil(t, err)
	})
}

func TestResolveAlias(t *testing.T) {
	var (
		sc = NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
		ss = &sharding.State{Physical: make(map[string]sharding.Physical)}
	)

	require.Nil(t, sc.addClass(&models.Class{Class: "C1"}, ss, 1))
	require.Nil(t, sc.createAlias("C1", "A1"))

	t.Run("successfully resolve alias", func(t *testing.T) {
		alias := sc.ResolveAlias("A1")
		assert.Equal(t, alias, "C1")
	})

	t.Run("empty response for non-existent alias", func(t *testing.T) {
		alias := sc.ResolveAlias("A2")
		assert.Empty(t, alias)
	})
}

func TestGetAlias(t *testing.T) {
	var (
		sc = NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
		ss = &sharding.State{Physical: make(map[string]sharding.Physical)}
	)

	require.Nil(t, sc.addClass(&models.Class{Class: "C1"}, ss, 1))
	require.Nil(t, sc.addClass(&models.Class{Class: "C2"}, ss, 1))
	require.Nil(t, sc.addClass(&models.Class{Class: "C3"}, ss, 1))
	require.Nil(t, sc.createAlias("C1", "A1"))
	require.Nil(t, sc.createAlias("C2", "A2"))
	require.Nil(t, sc.createAlias("C2", "A3"))

	t.Run("get aliases", func(t *testing.T) {
		aliases := sc.getAliases("", "")
		expected := map[string]string{
			"A1": "C1",
			"A2": "C2",
			"A3": "C2",
		}
		assert.EqualValues(t, expected, aliases)
	})

	t.Run("get aliases for alias A1", func(t *testing.T) {
		aliases := sc.getAliases("A1", "")
		expected := map[string]string{
			"A1": "C1",
		}
		assert.EqualValues(t, expected, aliases)
	})

	t.Run("get aliases for class C2", func(t *testing.T) {
		aliases := sc.getAliases("", "C2")
		expected := map[string]string{
			"A2": "C2",
			"A3": "C2",
		}
		assert.EqualValues(t, expected, aliases)
	})

	t.Run("get updated aliases", func(t *testing.T) {
		require.Nil(t, sc.replaceAlias("C3", "A2"))

		aliases := sc.getAliases("", "")
		expected := map[string]string{
			"A1": "C1",
			"A2": "C3",
			"A3": "C2",
		}
		assert.EqualValues(t, expected, aliases)
	})
}
