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
	hfreshent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
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
			// One representative per distinct guard path. Field-by-field structural coverage lives
			// in Test_vectorConfigClassificationComplete, which proves normalize preserves exactly
			// the structural leaves — and that is what decides block-vs-allow here.
			{"hnsw quantizer toggled", legacy(hnswent.UserConfig{}), legacy(hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}})},
			{"hnsw distance change", legacy(hnswent.UserConfig{Distance: "cosine"}), legacy(hnswent.UserConfig{Distance: "l2-squared"})},
			{"index type change", legacy(hnswent.UserConfig{}), legacy(flatent.UserConfig{})},
			{"flat structural param", legacy(flatent.UserConfig{RQ: flatent.RQUserConfig{Enabled: true, Bits: 8}}), legacy(flatent.UserConfig{RQ: flatent.RQUserConfig{Enabled: true, Bits: 1}})},
			{"dynamic phase quantizer toggled", legacy(dynamicent.UserConfig{HnswUC: hnswent.UserConfig{}}), legacy(dynamicent.UserConfig{HnswUC: hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}}})},
			{"named vector added", &models.Class{Class: className}, named(map[string]any{"v1": hnswent.UserConfig{}})},
			{"named vector removed", named(map[string]any{"v1": hnswent.UserConfig{}}), &models.Class{Class: className}},
			{"named vector structural change", named(map[string]any{"v1": hnswent.UserConfig{}}), named(map[string]any{"v1": hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}}})},
			// hfresh is an unclassified (experimental) index type: any real change fails closed.
			{"hfresh field change", legacy(hfreshent.UserConfig{MaxPostingSizeKB: 48}), legacy(hfreshent.UserConfig{MaxPostingSizeKB: 64})},
			{"hfresh distance change", legacy(hfreshent.UserConfig{Distance: "cosine"}), legacy(hfreshent.UserConfig{Distance: "l2-squared"})},
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
			// One representative per distinct guard path; field-by-field safe coverage lives in
			// Test_vectorConfigClassificationComplete. dynamic-threshold and a deferred-safe field
			// are kept explicitly because both are deliberate, contestable classifications.
			{"no vector change", legacy(hnswent.UserConfig{Distance: "cosine"}), legacy(hnswent.UserConfig{Distance: "cosine"})},
			// A no-op update on an unclassified (hfresh) config must pass — nothing changed on disk.
			{"hfresh no-op", legacy(hfreshent.UserConfig{MaxPostingSizeKB: 48, Distance: "cosine"}), legacy(hfreshent.UserConfig{MaxPostingSizeKB: 48, Distance: "cosine"})},
			{"hnsw query-time knob", legacy(hnswent.UserConfig{EF: 10}), legacy(hnswent.UserConfig{EF: 20})},
			{"hnsw deferred-safe field", legacy(hnswent.UserConfig{MaxConnections: 16}), legacy(hnswent.UserConfig{MaxConnections: 32})},
			{"dynamic threshold", legacy(dynamicent.UserConfig{Threshold: 1000}), legacy(dynamicent.UserConfig{Threshold: 2000})},
			{"named vector query-time knob", named(map[string]any{"v1": hnswent.UserConfig{EF: 10}}), named(map[string]any{"v1": hnswent.UserConfig{EF: 20}})},
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

// querySafeLeaves and structuralLeaves classify every leaf of the parsed vector-config structs by
// dotted path (rooted at the index type; dynamic's HnswUC/FlatUC reuse the hnsw/flat paths). Every
// leaf must appear in exactly one — that is the fail-closed guarantee: a newly-added field fails
// Test_vectorConfigClassificationComplete until it is consciously placed here, so it cannot silently
// slip past the move guard. normalize must zero exactly the safe leaves and preserve the structural.
var (
	querySafeLeaves = []string{
		"hnsw.Skip", "hnsw.CleanupIntervalSeconds", "hnsw.MaxConnections", "hnsw.EFConstruction", "hnsw.EF",
		"hnsw.DynamicEFMin", "hnsw.DynamicEFMax", "hnsw.DynamicEFFactor", "hnsw.VectorCacheMaxObjects", "hnsw.FlatSearchCutoff",
		"hnsw.FilterStrategy", "hnsw.SkipDefaultQuantization", "hnsw.TrackDefaultQuantization", "hnsw.SQ.RescoreLimit", "hnsw.RQ.RescoreLimit",
		"hnsw.Multivector.Enabled", "hnsw.Multivector.Aggregation", "hnsw.Multivector.MuveraConfig.Enabled",
		"hnsw.Multivector.MuveraConfig.KSim", "hnsw.Multivector.MuveraConfig.DProjections", "hnsw.Multivector.MuveraConfig.Repetitions",
		"flat.VectorCacheMaxObjects", "flat.SkipDefaultQuantization", "flat.TrackDefaultQuantization",
		"flat.PQ.RescoreLimit", "flat.PQ.Cache", "flat.BQ.RescoreLimit", "flat.BQ.Cache",
		"flat.SQ.RescoreLimit", "flat.SQ.Cache", "flat.RQ.RescoreLimit", "flat.RQ.Cache", "dynamic.Threshold",
	}
	structuralLeaves = []string{
		"hnsw.Distance", "hnsw.PQ.Enabled", "hnsw.PQ.BitCompression", "hnsw.PQ.Segments", "hnsw.PQ.Centroids",
		"hnsw.PQ.TrainingLimit", "hnsw.PQ.Encoder.Type", "hnsw.PQ.Encoder.Distribution", "hnsw.BQ.Enabled",
		"hnsw.SQ.Enabled", "hnsw.SQ.TrainingLimit", "hnsw.RQ.Enabled", "hnsw.RQ.Bits",
		"flat.Distance", "flat.PQ.Enabled", "flat.BQ.Enabled", "flat.SQ.Enabled", "flat.RQ.Enabled", "flat.RQ.Bits",
		"dynamic.Distance",
	}
)

// Test_vectorConfigClassificationComplete is the fail-closed guarantee: it forces every leaf of
// every parsed vector-config struct to be classified safe or structural, and verifies normalize
// zeroes exactly the safe leaves. A newly-added field is unclassified until placed in
// querySafeLeaves or structuralLeaves, so it cannot silently default to safe and slip past the guard.
func Test_vectorConfigClassificationComplete(t *testing.T) {
	toSet := func(xs []string) map[string]struct{} {
		s := make(map[string]struct{}, len(xs))
		for _, x := range xs {
			s[x] = struct{}{}
		}
		return s
	}
	safe, structural := toSet(querySafeLeaves), toSet(structuralLeaves)

	// Walk a normalized, fully-populated config of each index type. Every leaf must be in exactly
	// one bucket, and normalize must have zeroed it iff it is safe. Any field-typed struct is
	// recursed into; dynamic's HnswUC/FlatUC reuse the hnsw/flat paths.
	var walk func(prefix string, v reflect.Value)
	walk = func(prefix string, v reflect.Value) {
		for i := 0; i < v.NumField(); i++ {
			f, fv := v.Type().Field(i), v.Field(i)
			switch {
			case f.Name == "HnswUC":
				walk("hnsw", fv)
			case f.Name == "FlatUC":
				walk("flat", fv)
			case f.Type.Kind() == reflect.Struct:
				walk(prefix+"."+f.Name, fv)
			default:
				path := prefix + "." + f.Name
				_, inSafe := safe[path]
				_, inStructural := structural[path]
				require.True(t, inSafe != inStructural,
					"vector-config leaf %q must be classified in exactly one of querySafeLeaves / structuralLeaves", path)
				if inSafe {
					require.True(t, fv.IsZero(), "%s is query-time-safe and must be zeroed by normalize", path)
				} else {
					require.False(t, fv.IsZero(), "%s is structural and must be preserved by normalize", path)
				}
			}
		}
	}
	for _, root := range []struct {
		prefix string
		typ    reflect.Type
	}{
		{"hnsw", reflect.TypeOf(hnswent.UserConfig{})},
		{"flat", reflect.TypeOf(flatent.UserConfig{})},
		{"dynamic", reflect.TypeOf(dynamicent.UserConfig{})},
	} {
		filled := reflect.New(root.typ).Elem()
		fillNonZero(filled)
		norm, ok := normalizeVectorConfig(filled.Interface())
		require.True(t, ok, "normalizeVectorConfig(%s) returned ok=false", root.prefix)
		walk(root.prefix, reflect.ValueOf(norm))
	}
}

// fillNonZero sets every scalar leaf of the addressable struct value v to a non-zero sentinel,
// recursing into nested structs. It lets the meta-test prove normalize zeroes exactly the safe
// leaves without a hand-maintained fixture, and panics on an unhandled leaf kind so a new field
// type cannot slip through unpopulated.
func fillNonZero(v reflect.Value) {
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.Struct:
			fillNonZero(f)
		case reflect.Bool:
			f.SetBool(true)
		case reflect.String:
			f.SetString("x")
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f.SetInt(1)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			f.SetUint(1)
		default:
			panic(fmt.Sprintf("fillNonZero: unhandled leaf kind %s (add a case)", f.Kind()))
		}
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
