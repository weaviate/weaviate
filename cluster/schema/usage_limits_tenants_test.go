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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// TestAddTenants_FSMTenantCapReEnforced pins the RAFT-apply re-enforcement of
// the per-collection tenant cap, closing the handler's count-then-submit TOCTOU.
func TestAddTenants_FSMTenantCapReEnforced(t *testing.T) {
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

	newSchemaWithClass := func(t *testing.T) *schema {
		t.Helper()
		s := NewSchema(nodeID, nil, prometheus.NewPedanticRegistry())
		require.NoError(t, s.addClass(&models.Class{
			Class:              className,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		}, &sharding.State{Physical: make(map[string]sharding.Physical)}, 0))
		return s
	}

	add := func(t *testing.T, s *schema, cap int, names ...string) error {
		t.Helper()
		return s.addTenants(className, 0, &api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants:      tenants(names...),
		}, cap)
	}

	physicalCount := func(s *schema) int {
		meta := s.metaClass(className)
		meta.RLock()
		defer meta.RUnlock()
		return len(meta.Sharding.Physical)
	}

	t.Run("TOCTOU: serial single-tenant adds cannot overshoot cap", func(t *testing.T) {
		s := newSchemaWithClass(t)
		require.NoError(t, add(t, s, 3, "t1", "t2")) // seed cap-1
		require.NoError(t, add(t, s, 3, "t3"))       // brings count to cap
		assert.Equal(t, 3, physicalCount(s))

		// Passed the same stale handler check (2+1=3), but applied serially it
		// overshoots and must be rejected without mutating state.
		err := add(t, s, 3, "t4")
		require.Error(t, err)
		le, ok := usagelimits.AsLimitExceeded(err)
		require.True(t, ok, "expected a typed LimitExceededError, got %v", err)
		assert.Equal(t, usagelimits.LimitTenants, le.Limit)
		assert.Equal(t, int64(3), le.Value)
		assert.Equal(t, 3, physicalCount(s))
	})

	t.Run("batch add up to the cap succeeds, one over fails", func(t *testing.T) {
		s := newSchemaWithClass(t)
		require.NoError(t, add(t, s, 3, "t1", "t2", "t3"))
		assert.Equal(t, 3, physicalCount(s))

		err := add(t, s, 3, "t4")
		require.Error(t, err)
		_, ok := usagelimits.AsLimitExceeded(err)
		require.True(t, ok)
		assert.Equal(t, 3, physicalCount(s))
	})

	t.Run("single batch exceeding the cap is rejected atomically", func(t *testing.T) {
		s := newSchemaWithClass(t)
		err := add(t, s, 3, "t1", "t2", "t3", "t4")
		require.Error(t, err)
		_, ok := usagelimits.AsLimitExceeded(err)
		require.True(t, ok)
		assert.Equal(t, 0, physicalCount(s)) // nothing partially applied
	})

	t.Run("re-adding existing tenants at the cap is idempotent, not over", func(t *testing.T) {
		s := newSchemaWithClass(t)
		require.NoError(t, add(t, s, 3, "t1", "t2", "t3"))
		// Existing tenants add zero new shards, so the count stays at the cap.
		require.NoError(t, add(t, s, 3, "t1", "t2"))
		assert.Equal(t, 3, physicalCount(s))
	})

	t.Run("negative cap means unlimited", func(t *testing.T) {
		s := newSchemaWithClass(t)
		require.NoError(t, add(t, s, -1, "t1", "t2", "t3", "t4", "t5"))
		assert.Equal(t, 5, physicalCount(s))
	})

	t.Run("cap of zero rejects the first add", func(t *testing.T) {
		s := newSchemaWithClass(t)
		err := add(t, s, 0, "t1")
		require.Error(t, err)
		_, ok := usagelimits.AsLimitExceeded(err)
		require.True(t, ok)
		assert.Equal(t, 0, physicalCount(s))
	})
}
