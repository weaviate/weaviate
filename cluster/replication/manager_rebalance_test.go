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

package replication_test

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
)

func TestManager_CreateRebalancePlan(t *testing.T) {
	createCommand := func(t *testing.T, planID string) *api.ApplyRequest {
		t.Helper()
		subCommand, err := json.Marshal(&replication.CreatePlanRequest{
			PlanID:          planID,
			SourceNodes:     []string{"node-1"},
			TargetNodes:     []string{"node-2"},
			CreatedAtUnixMs: 1000,
		})
		require.NoError(t, err)
		return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_REPLICATION_REBALANCE_CREATE_PLAN, SubCommand: subCommand}
	}
	newManager := func() (*replication.Manager, *int) {
		m := replication.NewManager(schema.SchemaReader{}, nil, prometheus.NewPedanticRegistry())
		calls := 0
		m.SetPlanCreated(func() { calls++ })
		return m, &calls
	}

	t.Run("a stored plan calls the hook", func(t *testing.T) {
		m, calls := newManager()

		require.NoError(t, m.CreateRebalancePlan(createCommand(t, "plan-1")))

		assert.Equal(t, 1, *calls)
	})

	t.Run("a create refused by an unfinished plan does not call the hook", func(t *testing.T) {
		m, calls := newManager()
		require.NoError(t, m.CreateRebalancePlan(createCommand(t, "plan-1")))

		err := m.CreateRebalancePlan(createCommand(t, "plan-2"))

		require.ErrorIs(t, err, replication.ErrPlanExists)
		assert.Equal(t, 1, *calls)
	})

	t.Run("a malformed create does not call the hook", func(t *testing.T) {
		m, calls := newManager()

		err := m.CreateRebalancePlan(&api.ApplyRequest{
			Type:       api.ApplyRequest_TYPE_REPLICATION_REBALANCE_CREATE_PLAN,
			SubCommand: []byte("not json"),
		})

		require.ErrorIs(t, err, replication.ErrBadRequest)
		assert.Zero(t, *calls)
	})
}
