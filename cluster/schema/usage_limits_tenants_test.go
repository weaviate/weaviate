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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// TestPreApplyFilter_TenantCap pins the pre-commit enforcement of the
// per-collection tenant cap: an over-cap AddTenants is rejected on the leader
// before it is proposed to RAFT, never in the deterministic FSM apply path.
func TestPreApplyFilter_TenantCap(t *testing.T) {
	const (
		nodeID    = "testNode"
		className = "TestClass"
	)

	tenants := func(names ...string) []*api.Tenant {
		out := make([]*api.Tenant, len(names))
		for i, n := range names {
			out[i] = &api.Tenant{Name: n, Status: "HOT"}
		}
		return out
	}

	newManager := func(t *testing.T, cap int) *SchemaManager {
		t.Helper()
		sm := NewSchemaManager(nodeID, nil, nil, prometheus.NewPedanticRegistry(), logrus.New())
		require.NoError(t, sm.schema.addClass(&models.Class{
			Class:              className,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		}, &sharding.State{Physical: make(map[string]sharding.Physical)}, 0))
		sm.SetTenantLimit(func() int { return cap }, nil)
		return sm
	}

	// seed actually creates tenants so the physical count grows.
	seed := func(t *testing.T, sm *SchemaManager, names ...string) {
		t.Helper()
		require.NoError(t, sm.schema.addTenants(className, 0, &api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants:      tenants(names...),
		}))
	}

	// preApply runs the pre-commit gate for an AddTenants of the given names.
	preApply := func(t *testing.T, sm *SchemaManager, names ...string) error {
		t.Helper()
		sub, err := gproto.Marshal(&api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants:      tenants(names...),
		})
		require.NoError(t, err)
		return sm.PreApplyFilter(&api.ApplyRequest{
			Type:       api.ApplyRequest_TYPE_ADD_TENANT,
			Class:      className,
			SubCommand: sub,
		})
	}

	physicalCount := func(sm *SchemaManager) int {
		meta := sm.schema.metaClass(className)
		meta.RLock()
		defer meta.RUnlock()
		return len(meta.Sharding.Physical)
	}

	requireRejected := func(t *testing.T, err error, wantCap int64) {
		t.Helper()
		require.Error(t, err)
		le, ok := usagelimits.AsLimitExceeded(err)
		require.True(t, ok, "expected a typed LimitExceededError, got %v", err)
		assert.Equal(t, usagelimits.LimitTenants, le.Limit)
		assert.Equal(t, wantCap, le.Value)
	}

	t.Run("under the cap is allowed", func(t *testing.T) {
		sm := newManager(t, 3)
		seed(t, sm, "t1")
		require.NoError(t, preApply(t, sm, "t2"))
	})

	t.Run("filling the cap exactly is allowed", func(t *testing.T) {
		sm := newManager(t, 3)
		seed(t, sm, "t1", "t2")
		require.NoError(t, preApply(t, sm, "t3"))
	})

	t.Run("one over the cap is rejected without mutating state", func(t *testing.T) {
		sm := newManager(t, 3)
		seed(t, sm, "t1", "t2", "t3")
		requireRejected(t, preApply(t, sm, "t4"), 3)
		assert.Equal(t, 3, physicalCount(sm), "PreApplyFilter must not mutate the schema")
	})

	t.Run("a single batch exceeding the cap is rejected", func(t *testing.T) {
		sm := newManager(t, 3)
		requireRejected(t, preApply(t, sm, "t1", "t2", "t3", "t4"), 3)
		assert.Equal(t, 0, physicalCount(sm))
	})

	t.Run("re-adding existing tenants at the cap is allowed (idempotent)", func(t *testing.T) {
		sm := newManager(t, 3)
		seed(t, sm, "t1", "t2", "t3")
		// existing tenants create no new shards, so they don't count against the cap
		require.NoError(t, preApply(t, sm, "t1", "t2"))
	})

	t.Run("mixed existing+new is counted by new shards only", func(t *testing.T) {
		sm := newManager(t, 3)
		seed(t, sm, "t1", "t2")
		require.NoError(t, preApply(t, sm, "t1", "t3")) // t1 existing, t3 new -> 3 total
		requireRejected(t, preApply(t, sm, "t1", "t3", "t4"), 3)
	})

	t.Run("negative cap means unlimited", func(t *testing.T) {
		sm := newManager(t, -1)
		require.NoError(t, preApply(t, sm, "t1", "t2", "t3", "t4", "t5"))
	})

	t.Run("cap of zero rejects the first add", func(t *testing.T) {
		sm := newManager(t, 0)
		requireRejected(t, preApply(t, sm, "t1"), 0)
	})

	t.Run("no resolver installed is unenforced", func(t *testing.T) {
		sm := newManager(t, 3)
		sm.tenantLimit = nil // USAGE_LIMITS unset
		require.NoError(t, preApply(t, sm, "t1", "t2", "t3", "t4", "t5"))
	})

	t.Run("unknown class is not falsely rejected", func(t *testing.T) {
		sm := newManager(t, 0)
		sub, err := gproto.Marshal(&api.AddTenantsRequest{Tenants: tenants("t1")})
		require.NoError(t, err)
		require.NoError(t, sm.PreApplyFilter(&api.ApplyRequest{
			Type:       api.ApplyRequest_TYPE_ADD_TENANT,
			Class:      "DoesNotExist",
			SubCommand: sub,
		}))
	})

	t.Run("operator error template renders in the pre-commit rejection", func(t *testing.T) {
		sm := newManager(t, 0)
		sm.tenantLimitErrTemplate = func() string { return "Free-tier limit of {value} {limit} reached, upgrade now" }
		le, ok := usagelimits.AsLimitExceeded(preApply(t, sm, "t1"))
		require.True(t, ok)
		assert.Equal(t, "Free-tier limit of 0 tenants reached, upgrade now", le.RenderedMessage)
	})
}
