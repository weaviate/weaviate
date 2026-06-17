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

package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestCollectionNameConflictWithAlias(t *testing.T) {
	var (
		sc = NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
		ss = &sharding.State{Physical: make(map[string]sharding.Physical)}
	)

	require.Nil(t, sc.addClass(&models.Class{Class: "CoolCar"}, ss, 1))

	err := sc.createAlias("CoolCar", "MyCar")
	require.NoError(t, err)

	// checking to see if class exists should consider the existing alias as well
	got, isAlias := sc.ClassEqual("MyCar")
	assert.NotEmpty(t, got)
	assert.True(t, isAlias)
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
	fsm := NewMockreplicationFSM(t)
	fsm.On("HasActiveReplicationForShard", mock.Anything, mock.Anything).Return(false).Maybe()
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

// Test_UpdateTenants_TransitionalStateRejection verifies that status changes are
// rejected when a tenant is mid-freeze (FREEZING) or mid-unfreeze (UNFREEZING).
// Without this guard a HOT request arriving while FREEZING would immediately
// trigger UNFREEZE, creating a race that can result in permanent data loss.
func Test_UpdateTenants_TransitionalStateRejection(t *testing.T) {
	const (
		nodeID     = "testNode"
		tenantName = "tenant1"
		className  = "TestClass"
	)
	newSchema := func() *schema {
		return NewSchema(nodeID, nil, prometheus.NewPedanticRegistry())
	}
	newClass := func() *models.Class {
		return &models.Class{
			Class:              className,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		}
	}
	fsm := NewMockreplicationFSM(t)
	fsm.On("HasActiveReplicationForShard", mock.Anything, mock.Anything).Return(false).Maybe()

	t.Run("FREEZING rejects HOT", func(t *testing.T) {
		s := newSchema()
		require.NoError(t, s.addClass(newClass(), &sharding.State{}, 0))
		require.NoError(t, s.addTenants(className, 0, &api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "HOT"}},
		}))
		// HOT → FROZEN puts the tenant into FREEZING
		require.NoError(t, s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "FROZEN"}},
			ClusterNodes: []string{nodeID},
		}, fsm))

		// HOT request while FREEZING must be rejected
		err := s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "HOT"}},
			ClusterNodes: []string{nodeID},
		}, fsm)
		require.ErrorIs(t, err, ErrTenantTransitionalState)
	})

	t.Run("FREEZING rejects COLD", func(t *testing.T) {
		s := newSchema()
		require.NoError(t, s.addClass(newClass(), &sharding.State{}, 0))
		require.NoError(t, s.addTenants(className, 0, &api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "HOT"}},
		}))
		require.NoError(t, s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "FROZEN"}},
			ClusterNodes: []string{nodeID},
		}, fsm))

		err := s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "COLD"}},
			ClusterNodes: []string{nodeID},
		}, fsm)
		require.ErrorIs(t, err, ErrTenantTransitionalState)
	})

	t.Run("FREEZING allows FROZEN (idempotent)", func(t *testing.T) {
		s := newSchema()
		require.NoError(t, s.addClass(newClass(), &sharding.State{}, 0))
		require.NoError(t, s.addTenants(className, 0, &api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "HOT"}},
		}))
		require.NoError(t, s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "FROZEN"}},
			ClusterNodes: []string{nodeID},
		}, fsm))

		// Second FROZEN request while already FREEZING must succeed (idempotent)
		require.NoError(t, s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "FROZEN"}},
			ClusterNodes: []string{nodeID},
		}, fsm))
	})

	t.Run("UNFREEZING rejects conflicting status", func(t *testing.T) {
		s := newSchema()
		require.NoError(t, s.addClass(newClass(), &sharding.State{}, 0))
		require.NoError(t, s.addTenants(className, 0, &api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "FROZEN"}},
		}))
		// FROZEN → HOT puts the tenant into UNFREEZING
		require.NoError(t, s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "HOT"}},
			ClusterNodes: []string{nodeID},
		}, fsm))

		// COLD request while UNFREEZING-to-HOT must be rejected
		err := s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: tenantName, Status: "COLD"}},
			ClusterNodes: []string{nodeID},
		}, fsm)
		require.ErrorIs(t, err, ErrTenantTransitionalState)
	})
}

// Test_UpdateTenants_MovementRejection verifies that tenant transitions which take the source
// shard away (HOT→COLD shutdown, freeze, unfreeze) are rejected with a partial
// ErrReplicaMovementInProgress while a movement is active, that transitions toward availability
// are allowed, and that the decision is node-independent.
func Test_UpdateTenants_MovementRejection(t *testing.T) {
	const (
		nodeID     = "testNode"
		tenantName = "tenant1"
		className  = "TestClass"
	)
	newClass := func() *models.Class {
		return &models.Class{
			Class:              className,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		}
	}
	setup := func(t *testing.T, node, startStatus string) *schema {
		t.Helper()
		s := NewSchema(node, nil, prometheus.NewPedanticRegistry())
		require.NoError(t, s.addClass(newClass(), &sharding.State{}, 0))
		require.NoError(t, s.addTenants(className, 0, &api.AddTenantsRequest{
			ClusterNodes: []string{node},
			Tenants:      []*api.Tenant{{Name: tenantName, Status: startStatus}},
		}))
		return s
	}
	movementFSM := func(t *testing.T, active bool) replicationFSM {
		t.Helper()
		fsm := NewMockreplicationFSM(t)
		fsm.On("HasActiveReplicationForShard", mock.Anything, mock.Anything).Return(active).Maybe()
		return fsm
	}
	update := func(s *schema, status string, fsm replicationFSM) error {
		return s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: tenantName, Status: status}},
			ClusterNodes: []string{nodeID},
		}, fsm)
	}

	t.Run("HOT→COLD blocked during movement", func(t *testing.T) {
		s := setup(t, nodeID, "HOT")
		err := update(s, "COLD", movementFSM(t, true))
		require.ErrorIs(t, err, ErrReplicaMovementInProgress)
		require.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")), "tenant must stay HOT")
		require.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("COLD")))
	})

	t.Run("HOT→FROZEN (freeze) blocked during movement", func(t *testing.T) {
		s := setup(t, nodeID, "HOT")
		err := update(s, "FROZEN", movementFSM(t, true))
		require.ErrorIs(t, err, ErrReplicaMovementInProgress)
		require.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")))
		require.Equal(t, float64(0), testutil.ToFloat64(s.shardsCount.WithLabelValues("FREEZING")))
	})

	t.Run("FROZEN→HOT (unfreeze) blocked during movement", func(t *testing.T) {
		s := setup(t, nodeID, "FROZEN")
		err := update(s, "HOT", movementFSM(t, true))
		require.ErrorIs(t, err, ErrReplicaMovementInProgress)
		require.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("FROZEN")))
	})

	t.Run("COLD→HOT (toward available) allowed during movement", func(t *testing.T) {
		s := setup(t, nodeID, "COLD")
		err := update(s, "HOT", movementFSM(t, true))
		require.NoError(t, err)
		require.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")))
	})

	t.Run("HOT→COLD allowed when no movement", func(t *testing.T) {
		s := setup(t, nodeID, "HOT")
		err := update(s, "COLD", movementFSM(t, false))
		require.NoError(t, err)
		require.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("COLD")))
	})

	t.Run("partial: only the moving tenant is blocked, sibling still applies", func(t *testing.T) {
		s := NewSchema(nodeID, nil, prometheus.NewPedanticRegistry())
		require.NoError(t, s.addClass(newClass(), &sharding.State{}, 0))
		require.NoError(t, s.addTenants(className, 0, &api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants:      []*api.Tenant{{Name: "tenant1", Status: "HOT"}, {Name: "tenant2", Status: "HOT"}},
		}))
		// Active movement on tenant1 only.
		fsm := NewMockreplicationFSM(t)
		fsm.On("HasActiveReplicationForShard", className, "tenant1").Return(true)
		fsm.On("HasActiveReplicationForShard", mock.Anything, mock.Anything).Return(false).Maybe()

		err := s.updateTenants(className, 0, &api.UpdateTenantsRequest{
			Tenants:      []*api.Tenant{{Name: "tenant1", Status: "COLD"}, {Name: "tenant2", Status: "COLD"}},
			ClusterNodes: []string{nodeID},
		}, fsm)
		require.ErrorIs(t, err, ErrReplicaMovementInProgress)
		// tenant1 blocked (stays HOT), tenant2 applied (COLD).
		require.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("HOT")), "blocked tenant1 stays HOT")
		require.Equal(t, float64(1), testutil.ToFloat64(s.shardsCount.WithLabelValues("COLD")), "tenant2 still transitions COLD")
	})

	t.Run("node-independent: COLD blocked on every node", func(t *testing.T) {
		for _, node := range []string{"nodeA", "nodeB", "uninvolved"} {
			s := setup(t, node, "HOT")
			err := update(s, "COLD", movementFSM(t, true))
			require.ErrorIs(t, err, ErrReplicaMovementInProgress, "must block on %s", node)
		}
	})
}

func boolPtr(b bool) *bool { return &b }

// fixedParser returns a preset parsed class from ParseClassUpdate, standing in for the
// production parser that sets concrete vector-config types (which a JSON round-trip would strip).
type fixedParser struct{ updated *models.Class }

func (p *fixedParser) ParseClass(*models.Class) error { return nil }

func (p *fixedParser) ParseClassUpdate(_, _ *models.Class) (*models.Class, error) {
	return p.updated, nil
}

// Test_UpdateClass_MovementRejection verifies that an UpdateClass which would rewrite on-disk
// vector structure is forbidden while a movement is active on the collection, that safe
// (query-time-only) changes are allowed, and that a rejected update leaves the schema untouched.
func Test_UpdateClass_MovementRejection(t *testing.T) {
	const className = "TestClass"
	legacy := func(cfg any) *models.Class { return &models.Class{Class: className, VectorIndexConfig: cfg} }
	named := func(vecs map[string]any) *models.Class {
		vc := make(map[string]models.VectorConfig, len(vecs))
		for n, c := range vecs {
			vc[n] = models.VectorConfig{VectorIndexConfig: c}
		}
		return &models.Class{Class: className, VectorConfig: vc}
	}
	// run registers oldClass, then applies an UpdateClass whose parser yields newClass.
	run := func(t *testing.T, oldClass, newClass *models.Class, movementActive bool) (*SchemaManager, error) {
		t.Helper()
		sm := NewSchemaManager("testNode", nil, &fixedParser{updated: newClass}, prometheus.NewPedanticRegistry(), logrus.New())
		fsm := NewMockreplicationFSM(t)
		fsm.On("HasActiveReplicationForCollection", mock.Anything).Return(movementActive).Maybe()
		sm.SetReplicationFSM(fsm)
		require.NoError(t, sm.schema.addClass(oldClass, &sharding.State{}, 0))
		sub, err := json.Marshal(&api.UpdateClassRequest{Class: &models.Class{Class: className}})
		require.NoError(t, err)
		// schemaOnly=true exercises the guard + schema apply without needing a real DB.
		return sm, sm.UpdateClass(&api.ApplyRequest{
			Type: api.ApplyRequest_TYPE_UPDATE_CLASS, Class: className, Version: 1, SubCommand: sub,
		}, "testNode", true, false)
	}

	t.Run("structural change forbidden during movement", func(t *testing.T) {
		cases := []struct {
			name     string
			old, new *models.Class
		}{
			// HNSW — quantizer toggles + every structural param, one field per row.
			{"hnsw distance", legacy(hnswent.UserConfig{Distance: "cosine"}), legacy(hnswent.UserConfig{Distance: "l2-squared"})},
			{"hnsw pq enabled", legacy(hnswent.UserConfig{}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}})},
			{"hnsw pq disabled", legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}}), legacy(hnswent.UserConfig{})},
			{"hnsw pq segments", legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, Segments: 8}}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, Segments: 16}})},
			{"hnsw pq centroids", legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, Centroids: 128}}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, Centroids: 256}})},
			{"hnsw pq trainingLimit", legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, TrainingLimit: 1000}}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, TrainingLimit: 2000}})},
			{"hnsw pq bitCompression", legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, BitCompression: true}})},
			{"hnsw pq encoder type", legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, Encoder: hnswent.PQEncoder{Type: "kmeans"}}}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, Encoder: hnswent.PQEncoder{Type: "tile"}}})},
			{"hnsw pq encoder distribution", legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, Encoder: hnswent.PQEncoder{Distribution: "normal"}}}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true, Encoder: hnswent.PQEncoder{Distribution: "log-normal"}}})},
			{"hnsw bq enabled", legacy(hnswent.UserConfig{}), legacy(hnswent.UserConfig{BQ: hnswent.BQConfig{Enabled: true}})},
			{"hnsw sq enabled", legacy(hnswent.UserConfig{}), legacy(hnswent.UserConfig{SQ: hnswent.SQConfig{Enabled: true}})},
			{"hnsw sq trainingLimit", legacy(hnswent.UserConfig{SQ: hnswent.SQConfig{Enabled: true, TrainingLimit: 1000}}), legacy(hnswent.UserConfig{SQ: hnswent.SQConfig{Enabled: true, TrainingLimit: 2000}})},
			{"hnsw rq enabled", legacy(hnswent.UserConfig{}), legacy(hnswent.UserConfig{RQ: hnswent.RQConfig{Enabled: true}})},
			{"hnsw rq bits", legacy(hnswent.UserConfig{RQ: hnswent.RQConfig{Enabled: true, Bits: 8}}), legacy(hnswent.UserConfig{RQ: hnswent.RQConfig{Enabled: true, Bits: 1}})},
			// Flat — quantizer toggles + bits, one field per row.
			{"flat distance", legacy(flatent.UserConfig{Distance: "cosine"}), legacy(flatent.UserConfig{Distance: "l2-squared"})},
			{"flat pq enabled", legacy(flatent.UserConfig{}), legacy(flatent.UserConfig{PQ: flatent.CompressionUserConfig{Enabled: true}})},
			{"flat bq enabled", legacy(flatent.UserConfig{}), legacy(flatent.UserConfig{BQ: flatent.CompressionUserConfig{Enabled: true}})},
			{"flat sq enabled", legacy(flatent.UserConfig{}), legacy(flatent.UserConfig{SQ: flatent.CompressionUserConfig{Enabled: true}})},
			{"flat rq enabled", legacy(flatent.UserConfig{}), legacy(flatent.UserConfig{RQ: flatent.RQUserConfig{Enabled: true}})},
			{"flat rq bits", legacy(flatent.UserConfig{RQ: flatent.RQUserConfig{Enabled: true, Bits: 8}}), legacy(flatent.UserConfig{RQ: flatent.RQUserConfig{Enabled: true, Bits: 1}})},
			// Dynamic — distance and recursion into both phases (Threshold is query-time-safe; see allowed cases).
			{"dynamic distance", legacy(dynamicent.UserConfig{Distance: "cosine"}), legacy(dynamicent.UserConfig{Distance: "l2-squared"})},
			{"dynamic hnsw rq bits", legacy(dynamicent.UserConfig{HnswUC: hnswent.UserConfig{RQ: hnswent.RQConfig{Enabled: true, Bits: 8}}}), legacy(dynamicent.UserConfig{HnswUC: hnswent.UserConfig{RQ: hnswent.RQConfig{Enabled: true, Bits: 1}}})},
			{"dynamic flat rq enabled", legacy(dynamicent.UserConfig{FlatUC: flatent.UserConfig{}}), legacy(dynamicent.UserConfig{FlatUC: flatent.UserConfig{RQ: flatent.RQUserConfig{Enabled: true}}})},
			{"dynamic compression toggled", legacy(dynamicent.UserConfig{HnswUC: hnswent.UserConfig{}}), legacy(dynamicent.UserConfig{HnswUC: hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}}})},
			// Index type, named vector add/remove, and a within-config change on a named vector.
			{"index type change", legacy(hnswent.UserConfig{}), legacy(flatent.UserConfig{})},
			{"named vector added", &models.Class{Class: className}, named(map[string]any{"v1": hnswent.UserConfig{}})},
			{"named vector removed", named(map[string]any{"v1": hnswent.UserConfig{}}), &models.Class{Class: className}},
			{"named vector pq enabled", named(map[string]any{"v1": hnswent.UserConfig{}}), named(map[string]any{"v1": hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}}})},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := run(t, tc.old, tc.new, true)
				require.ErrorIs(t, err, ErrReplicaMovementInProgress)
				require.ErrorIs(t, err, ErrBadRequest)
			})
		}
	})

	t.Run("safe change allowed during movement", func(t *testing.T) {
		cases := []struct {
			name     string
			old, new *models.Class
		}{
			{"no vector change", legacy(hnswent.UserConfig{Distance: "cosine"}), legacy(hnswent.UserConfig{Distance: "cosine"})},
			// HNSW query-time knobs.
			{"hnsw ef", legacy(hnswent.UserConfig{EF: 10}), legacy(hnswent.UserConfig{EF: 20})},
			{"hnsw cleanupIntervalSeconds", legacy(hnswent.UserConfig{CleanupIntervalSeconds: 60}), legacy(hnswent.UserConfig{CleanupIntervalSeconds: 120})},
			{"hnsw dynamicEfMin", legacy(hnswent.UserConfig{DynamicEFMin: 100}), legacy(hnswent.UserConfig{DynamicEFMin: 200})},
			{"hnsw dynamicEfMax", legacy(hnswent.UserConfig{DynamicEFMax: 500}), legacy(hnswent.UserConfig{DynamicEFMax: 600})},
			{"hnsw dynamicEfFactor", legacy(hnswent.UserConfig{DynamicEFFactor: 8}), legacy(hnswent.UserConfig{DynamicEFFactor: 16})},
			{"hnsw vectorCacheMaxObjects", legacy(hnswent.UserConfig{VectorCacheMaxObjects: 1}), legacy(hnswent.UserConfig{VectorCacheMaxObjects: 2})},
			{"hnsw flatSearchCutoff", legacy(hnswent.UserConfig{FlatSearchCutoff: 40000}), legacy(hnswent.UserConfig{FlatSearchCutoff: 50000})},
			{"hnsw filterStrategy", legacy(hnswent.UserConfig{FilterStrategy: "acorn"}), legacy(hnswent.UserConfig{FilterStrategy: "sweeping"})},
			{"hnsw rq rescoreLimit", legacy(hnswent.UserConfig{RQ: hnswent.RQConfig{Enabled: true, RescoreLimit: 10}}), legacy(hnswent.UserConfig{RQ: hnswent.RQConfig{Enabled: true, RescoreLimit: 20}})},
			{"hnsw sq rescoreLimit", legacy(hnswent.UserConfig{SQ: hnswent.SQConfig{Enabled: true, RescoreLimit: 10}}), legacy(hnswent.UserConfig{SQ: hnswent.SQConfig{Enabled: true, RescoreLimit: 20}})},
			// HNSW deferred-safe (currently non-blocking; parity must be preserved).
			{"hnsw maxConnections", legacy(hnswent.UserConfig{MaxConnections: 16}), legacy(hnswent.UserConfig{MaxConnections: 32})},
			{"hnsw efConstruction", legacy(hnswent.UserConfig{EFConstruction: 128}), legacy(hnswent.UserConfig{EFConstruction: 256})},
			{"hnsw skip", legacy(hnswent.UserConfig{Skip: false}), legacy(hnswent.UserConfig{Skip: true})},
			{"hnsw skipDefaultQuantization", legacy(hnswent.UserConfig{SkipDefaultQuantization: false}), legacy(hnswent.UserConfig{SkipDefaultQuantization: true})},
			{"hnsw trackDefaultQuantization", legacy(hnswent.UserConfig{TrackDefaultQuantization: false}), legacy(hnswent.UserConfig{TrackDefaultQuantization: true})},
			{"hnsw multivector enabled", legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{Enabled: false}}), legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{Enabled: true}})},
			{"hnsw multivector aggregation", legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{Aggregation: "maxSim"}}), legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{Aggregation: "other"}})},
			{"hnsw muvera enabled", legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{MuveraConfig: hnswent.MuveraConfig{Enabled: false}}}), legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{MuveraConfig: hnswent.MuveraConfig{Enabled: true}}})},
			{"hnsw muvera ksim", legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{MuveraConfig: hnswent.MuveraConfig{KSim: 4}}}), legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{MuveraConfig: hnswent.MuveraConfig{KSim: 8}}})},
			{"hnsw muvera dprojections", legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{MuveraConfig: hnswent.MuveraConfig{DProjections: 16}}}), legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{MuveraConfig: hnswent.MuveraConfig{DProjections: 32}}})},
			{"hnsw muvera repetitions", legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{MuveraConfig: hnswent.MuveraConfig{Repetitions: 10}}}), legacy(hnswent.UserConfig{Multivector: hnswent.MultivectorConfig{MuveraConfig: hnswent.MuveraConfig{Repetitions: 20}}})},
			// Flat query-time knobs.
			{"flat vectorCacheMaxObjects", legacy(flatent.UserConfig{VectorCacheMaxObjects: 1}), legacy(flatent.UserConfig{VectorCacheMaxObjects: 2})},
			{"flat pq rescoreLimit", legacy(flatent.UserConfig{PQ: flatent.CompressionUserConfig{RescoreLimit: 10}}), legacy(flatent.UserConfig{PQ: flatent.CompressionUserConfig{RescoreLimit: 20}})},
			{"flat pq cache", legacy(flatent.UserConfig{PQ: flatent.CompressionUserConfig{Cache: false}}), legacy(flatent.UserConfig{PQ: flatent.CompressionUserConfig{Cache: true}})},
			{"flat bq rescoreLimit", legacy(flatent.UserConfig{BQ: flatent.CompressionUserConfig{RescoreLimit: 10}}), legacy(flatent.UserConfig{BQ: flatent.CompressionUserConfig{RescoreLimit: 20}})},
			{"flat bq cache", legacy(flatent.UserConfig{BQ: flatent.CompressionUserConfig{Cache: false}}), legacy(flatent.UserConfig{BQ: flatent.CompressionUserConfig{Cache: true}})},
			{"flat sq rescoreLimit", legacy(flatent.UserConfig{SQ: flatent.CompressionUserConfig{RescoreLimit: 10}}), legacy(flatent.UserConfig{SQ: flatent.CompressionUserConfig{RescoreLimit: 20}})},
			{"flat sq cache", legacy(flatent.UserConfig{SQ: flatent.CompressionUserConfig{Cache: false}}), legacy(flatent.UserConfig{SQ: flatent.CompressionUserConfig{Cache: true}})},
			{"flat rq rescoreLimit", legacy(flatent.UserConfig{RQ: flatent.RQUserConfig{RescoreLimit: 10}}), legacy(flatent.UserConfig{RQ: flatent.RQUserConfig{RescoreLimit: 20}})},
			{"flat rq cache", legacy(flatent.UserConfig{RQ: flatent.RQUserConfig{Cache: false}}), legacy(flatent.UserConfig{RQ: flatent.RQUserConfig{Cache: true}})},
			// Dynamic — Threshold is query-time-safe (the flat→HNSW upgrade it gates is deferred during
			// a move and the schema is replicated to both replicas, so no divergence), plus recursion
			// into both phases' query-time knobs.
			{"dynamic threshold", legacy(dynamicent.UserConfig{Threshold: 1000}), legacy(dynamicent.UserConfig{Threshold: 2000})},
			{"dynamic hnsw ef", legacy(dynamicent.UserConfig{HnswUC: hnswent.UserConfig{EF: 10}}), legacy(dynamicent.UserConfig{HnswUC: hnswent.UserConfig{EF: 20}})},
			{"dynamic flat vectorCacheMaxObjects", legacy(dynamicent.UserConfig{FlatUC: flatent.UserConfig{VectorCacheMaxObjects: 1}}), legacy(dynamicent.UserConfig{FlatUC: flatent.UserConfig{VectorCacheMaxObjects: 2}})},
			// Named vector query-time change.
			{"named vector ef", named(map[string]any{"v1": hnswent.UserConfig{EF: 10}}), named(map[string]any{"v1": hnswent.UserConfig{EF: 20}})},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := run(t, tc.old, tc.new, true)
				require.NoError(t, err)
			})
		}
	})

	t.Run("structural change allowed when no movement", func(t *testing.T) {
		_, err := run(t, legacy(hnswent.UserConfig{}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}}), false)
		require.NoError(t, err)
	})

	t.Run("rejected update does not mutate the schema", func(t *testing.T) {
		sm, err := run(t, legacy(hnswent.UserConfig{}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}}), true)
		require.ErrorIs(t, err, ErrReplicaMovementInProgress)
		cls, _ := sm.schema.ReadOnlyClass(className)
		require.NotNil(t, cls)
		require.Equal(t, hnswent.UserConfig{}, cls.VectorIndexConfig, "config must stay uncompressed after a rejected update")
	})
}

// vectorConfigClassification maps every leaf field of the parsed vector-config structs (dotted
// path, rooted at the index type) to whether it is QUERY-TIME-SAFE (true) or STRUCTURAL (false).
// It is the single source of truth that Test_vectorConfigClassificationComplete cross-checks against
// both the struct shape (every leaf must appear — fail-closed) and normalize* (safe ⇔ zeroed).
var vectorConfigClassification = map[string]bool{
	// hnsw.UserConfig
	"hnsw.Skip":                                  true,
	"hnsw.CleanupIntervalSeconds":                true,
	"hnsw.MaxConnections":                        true,
	"hnsw.EFConstruction":                        true,
	"hnsw.EF":                                    true,
	"hnsw.DynamicEFMin":                          true,
	"hnsw.DynamicEFMax":                          true,
	"hnsw.DynamicEFFactor":                       true,
	"hnsw.VectorCacheMaxObjects":                 true,
	"hnsw.FlatSearchCutoff":                      true,
	"hnsw.Distance":                              false,
	"hnsw.FilterStrategy":                        true,
	"hnsw.SkipDefaultQuantization":               true,
	"hnsw.TrackDefaultQuantization":              true,
	"hnsw.PQ.Enabled":                            false,
	"hnsw.PQ.BitCompression":                     false,
	"hnsw.PQ.Segments":                           false,
	"hnsw.PQ.Centroids":                          false,
	"hnsw.PQ.TrainingLimit":                      false,
	"hnsw.PQ.Encoder.Type":                       false,
	"hnsw.PQ.Encoder.Distribution":               false,
	"hnsw.BQ.Enabled":                            false,
	"hnsw.SQ.Enabled":                            false,
	"hnsw.SQ.TrainingLimit":                      false,
	"hnsw.SQ.RescoreLimit":                       true,
	"hnsw.RQ.Enabled":                            false,
	"hnsw.RQ.Bits":                               false,
	"hnsw.RQ.RescoreLimit":                       true,
	"hnsw.Multivector.Enabled":                   true,
	"hnsw.Multivector.Aggregation":               true,
	"hnsw.Multivector.MuveraConfig.Enabled":      true,
	"hnsw.Multivector.MuveraConfig.KSim":         true,
	"hnsw.Multivector.MuveraConfig.DProjections": true,
	"hnsw.Multivector.MuveraConfig.Repetitions":  true,
	// flat.UserConfig
	"flat.Distance":                 false,
	"flat.VectorCacheMaxObjects":    true,
	"flat.SkipDefaultQuantization":  true,
	"flat.TrackDefaultQuantization": true,
	"flat.PQ.Enabled":               false,
	"flat.PQ.RescoreLimit":          true,
	"flat.PQ.Cache":                 true,
	"flat.BQ.Enabled":               false,
	"flat.BQ.RescoreLimit":          true,
	"flat.BQ.Cache":                 true,
	"flat.SQ.Enabled":               false,
	"flat.SQ.RescoreLimit":          true,
	"flat.SQ.Cache":                 true,
	"flat.RQ.Enabled":               false,
	"flat.RQ.RescoreLimit":          true,
	"flat.RQ.Cache":                 true,
	"flat.RQ.Bits":                  false,
	// dynamic.UserConfig (HnswUC/FlatUC recurse under the base hnsw/flat prefixes, not here)
	"dynamic.Distance":  false,
	"dynamic.Threshold": true,
}

// classificationNestedStructs are the config sub-structs the walk recurses into rather than
// treating as opaque leaves. Dynamic's HnswUC/FlatUC are handled specially (base-prefix remap).
var classificationNestedStructs = map[reflect.Type]bool{
	reflect.TypeOf(hnswent.PQConfig{}):              true,
	reflect.TypeOf(hnswent.PQEncoder{}):             true,
	reflect.TypeOf(hnswent.BQConfig{}):              true,
	reflect.TypeOf(hnswent.SQConfig{}):              true,
	reflect.TypeOf(hnswent.RQConfig{}):              true,
	reflect.TypeOf(hnswent.MultivectorConfig{}):     true,
	reflect.TypeOf(hnswent.MuveraConfig{}):          true,
	reflect.TypeOf(flatent.CompressionUserConfig{}): true,
	reflect.TypeOf(flatent.RQUserConfig{}):          true,
}

// Test_vectorConfigClassificationComplete is the fail-closed guarantee: it forces every leaf of
// every parsed vector-config struct to be consciously classified, and verifies normalize* zeroes
// exactly the safe leaves. A newly-added field that nobody classifies makes this test fail, which
// is the whole point — it cannot silently default to query-time-safe and slip past the move guard.
func Test_vectorConfigClassificationComplete(t *testing.T) {
	hnswType := reflect.TypeOf(hnswent.UserConfig{})
	flatType := reflect.TypeOf(flatent.UserConfig{})
	dynamicType := reflect.TypeOf(dynamicent.UserConfig{})

	t.Run("every leaf is classified", func(t *testing.T) {
		var missing []string
		var walk func(prefix string, typ reflect.Type)
		walk = func(prefix string, typ reflect.Type) {
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				switch {
				case classificationNestedStructs[f.Type]:
					walk(prefix+"."+f.Name, f.Type)
				case f.Name == "HnswUC":
					walk("hnsw", f.Type) // dynamic's HNSW phase reuses the base hnsw classification
				case f.Name == "FlatUC":
					walk("flat", f.Type)
				default:
					path := prefix + "." + f.Name
					if _, ok := vectorConfigClassification[path]; !ok {
						missing = append(missing, path)
					}
				}
			}
		}
		walk("hnsw", hnswType)
		walk("flat", flatType)
		walk("dynamic", dynamicType)
		require.Empty(t, missing,
			"unclassified vector-config field(s) — classify in vectorConfigClassification AND normalize*: %v", missing)
	})

	t.Run("normalize zeroes exactly the safe leaves", func(t *testing.T) {
		hnswNorm := normalizeHNSW(populatedHNSW())
		flatNorm := normalizeFlat(populatedFlat())
		// assertLeaves walks a normalized value and checks each leaf against the registry: safe ⇒
		// must be zero (got normalized away); structural ⇒ must be preserved (sentinel non-zero).
		var assertLeaves func(prefix string, v reflect.Value)
		assertLeaves = func(prefix string, v reflect.Value) {
			typ := v.Type()
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				fv := v.Field(i)
				path := prefix + "." + f.Name
				if classificationNestedStructs[f.Type] {
					assertLeaves(path, fv)
					continue
				}
				isSafe := vectorConfigClassification[path]
				if isSafe {
					require.True(t, fv.IsZero(), "%s is safe and must be zeroed by normalize", path)
				} else {
					require.False(t, fv.IsZero(), "%s is structural and must be preserved by normalize", path)
				}
			}
		}
		assertLeaves("hnsw", reflect.ValueOf(hnswNorm))
		assertLeaves("flat", reflect.ValueOf(flatNorm))
	})
}

// populatedHNSW returns an HNSW config with every leaf set to a non-zero sentinel, so a normalized
// copy reveals exactly which leaves normalizeHNSW zeroes.
func populatedHNSW() hnswent.UserConfig {
	return hnswent.UserConfig{
		Skip:                     true,
		CleanupIntervalSeconds:   1,
		MaxConnections:           1,
		EFConstruction:           1,
		EF:                       1,
		DynamicEFMin:             1,
		DynamicEFMax:             1,
		DynamicEFFactor:          1,
		VectorCacheMaxObjects:    1,
		FlatSearchCutoff:         1,
		Distance:                 "cosine",
		FilterStrategy:           "acorn",
		SkipDefaultQuantization:  true,
		TrackDefaultQuantization: true,
		PQ: hnswent.PQConfig{
			Enabled: true, BitCompression: true, Segments: 1, Centroids: 1, TrainingLimit: 1,
			Encoder: hnswent.PQEncoder{Type: "kmeans", Distribution: "log-normal"},
		},
		BQ: hnswent.BQConfig{Enabled: true},
		SQ: hnswent.SQConfig{Enabled: true, TrainingLimit: 1, RescoreLimit: 1},
		RQ: hnswent.RQConfig{Enabled: true, Bits: 1, RescoreLimit: 1},
		Multivector: hnswent.MultivectorConfig{
			Enabled: true, Aggregation: "maxSim",
			MuveraConfig: hnswent.MuveraConfig{Enabled: true, KSim: 1, DProjections: 1, Repetitions: 1},
		},
	}
}

// populatedFlat returns a flat config with every leaf set to a non-zero sentinel.
func populatedFlat() flatent.UserConfig {
	cuc := flatent.CompressionUserConfig{Enabled: true, RescoreLimit: 1, Cache: true}
	return flatent.UserConfig{
		Distance:                 "cosine",
		VectorCacheMaxObjects:    1,
		SkipDefaultQuantization:  true,
		TrackDefaultQuantization: true,
		PQ:                       cuc,
		BQ:                       cuc,
		SQ:                       cuc,
		RQ:                       flatent.RQUserConfig{Enabled: true, RescoreLimit: 1, Cache: true, Bits: 1},
	}
}

// Test_UpdateProperty_MovementRejection verifies that disabling a property index is forbidden
// while a movement is active, that the migration-completion path bypasses the guard, and that
// non-disabling updates are allowed.
func Test_UpdateProperty_MovementRejection(t *testing.T) {
	const className = "TestClass"
	build := func(t *testing.T, movementActive bool) *SchemaManager {
		t.Helper()
		sm := NewSchemaManager("testNode", nil, &fixedParser{}, prometheus.NewPedanticRegistry(), logrus.New())
		fsm := NewMockreplicationFSM(t)
		fsm.On("HasActiveReplicationForCollection", mock.Anything).Return(movementActive).Maybe()
		sm.SetReplicationFSM(fsm)
		// property p starts with its filterable index enabled
		cls := &models.Class{Class: className, Properties: []*models.Property{
			{Name: "p", DataType: []string{"text"}, IndexFilterable: boolPtr(true), IndexSearchable: boolPtr(false)},
		}}
		require.NoError(t, sm.schema.addClass(cls, &sharding.State{}, 0))
		return sm
	}
	mkCmd := func(t *testing.T, disable, fromMigration bool) *api.ApplyRequest {
		t.Helper()
		filterable := boolPtr(true)
		if disable {
			filterable = boolPtr(false)
		}
		sub, err := json.Marshal(&api.UpdatePropertyRequest{
			Property:              &models.Property{Name: "p", DataType: []string{"text"}, IndexFilterable: filterable, IndexSearchable: boolPtr(false)},
			FromInFlightMigration: fromMigration,
		})
		require.NoError(t, err)
		return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_UPDATE_PROPERTY, Class: className, Version: 1, SubCommand: sub}
	}

	t.Run("index disable forbidden during movement", func(t *testing.T) {
		err := build(t, true).UpdateProperty(mkCmd(t, true, false), true, false)
		require.ErrorIs(t, err, ErrReplicaMovementInProgress)
		require.ErrorIs(t, err, ErrBadRequest)
	})

	t.Run("FromInFlightMigration bypasses the guard", func(t *testing.T) {
		err := build(t, true).UpdateProperty(mkCmd(t, true, true), true, false)
		require.NotErrorIs(t, err, ErrReplicaMovementInProgress)
	})

	t.Run("non-disabling update allowed during movement", func(t *testing.T) {
		err := build(t, true).UpdateProperty(mkCmd(t, false, false), true, false)
		require.NotErrorIs(t, err, ErrReplicaMovementInProgress)
	})

	t.Run("index disable allowed when no movement", func(t *testing.T) {
		err := build(t, false).UpdateProperty(mkCmd(t, true, false), true, false)
		require.NotErrorIs(t, err, ErrReplicaMovementInProgress)
	})
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
	require.Nil(t, sc.addClass(&models.Class{Class: "AnotherClass"}, ss, 1))

	t.Run("successfully create alias", func(t *testing.T) {
		err := sc.createAlias("C", "A1")
		require.Nil(t, err)
	})

	t.Run("fail on conflicting creation", func(t *testing.T) {
		err := sc.createAlias("C", "A1")
		require.EqualError(t, err, "create alias: A1, alias already exists")
	})

	t.Run("fail on non-existing class", func(t *testing.T) {
		err := sc.createAlias("D", "newAlias")
		require.EqualError(t, err, "create alias: NewAlias, class not found, D")
	})

	t.Run("fail on non-existing alias", func(t *testing.T) {
		err := sc.createAlias("D", "A1")
		require.EqualError(t, err, "create alias: A1, alias already exists")
	})
	t.Run("fail on creating alias with existing class name", func(t *testing.T) {
		// We have two collection. "C" and "AnotherClass"
		// 1. We try to create alias with name "AnotherClass" to class "C".
		// 2. Should fail saying class with "AnotherClass" already exists.
		err := sc.createAlias("C", "AnotherClass")
		require.EqualError(t, err, "create alias: class AnotherClass already exists")
	})
}

func TestSchemaAliasCasing(t *testing.T) {
	// Alias name should be case-insensitive similar to collection.
	// Meaning, MyCar, MYCar, myCar all same.

	var (
		sc = NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
		ss = &sharding.State{Physical: make(map[string]sharding.Physical)}
	)

	require.Nil(t, sc.addClass(&models.Class{Class: "CoolCar"}, ss, 1))
	err := sc.createAlias("CoolCar", "MyCar")
	require.Nil(t, err)

	// Try creating it with different cases.
	err = sc.createAlias("CoolCar", "MYCar")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	err = sc.createAlias("CoolCar", "mYCar")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	err = sc.createAlias("CoolCar", "mycar")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	err = sc.createAlias("CoolCar", "MYCAR")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
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

// TestAliasNamespacePrefixPreserved checks that creating an alias with a
// namespace-qualified name stores it under a key whose namespace prefix is
// kept lowercase, and that ResolveAlias still finds it under that key. Only
// the class portion of the name is normalized.
func TestAliasNamespacePrefixPreserved(t *testing.T) {
	sc := NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}
	require.NoError(t, sc.addClass(&models.Class{Class: "delhappy:Movies"}, ss, 1))
	require.NoError(t, sc.createAlias("delhappy:Movies", "delhappy:Films"))

	stored := sc.getAliases("", "")
	require.Contains(t, stored, "delhappy:Films",
		"alias stored under lowercase-namespace key; got %v", stored)

	require.Equal(t, "delhappy:Movies", sc.ResolveAlias("delhappy:Films"))
}

func TestCollectionsCount_Namespaced(t *testing.T) {
	sc := NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry())
	ss := &sharding.State{Physical: make(map[string]sharding.Physical)}

	require.NoError(t, sc.addClass(&models.Class{Class: "customer1:Movies"}, ss, 1))
	require.NoError(t, sc.addClass(&models.Class{Class: "customer1:Films"}, ss, 2))
	require.NoError(t, sc.addClass(&models.Class{Class: "customer2:Movies"}, ss, 3))
	require.NoError(t, sc.addClass(&models.Class{Class: "Global"}, ss, 4))

	assert.Equal(t, 4, sc.CollectionsCount(""), "empty namespace returns total")
	assert.Equal(t, 2, sc.CollectionsCount("customer1"))
	assert.Equal(t, 1, sc.CollectionsCount("customer2"))
	assert.Equal(t, 0, sc.CollectionsCount("unknown"))
}
