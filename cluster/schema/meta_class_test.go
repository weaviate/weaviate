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

	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// A malformed tenant-process element is producible today: the migrator pre-sizes the
// slice per tenant and ErrorGroupWrapper.Go recovers panics, so a nil slot gets
// published. Dereferencing it in the FSM apply would crash-loop every node.
func TestUpdateTenantsProcessSkipsMalformedEntries(t *testing.T) {
	const (
		nodeID = "node1"
		tenant = "t1"
	)

	m := &metaClass{
		Class: models.Class{Class: "TestClass"},
		Sharding: sharding.State{
			Physical: map[string]sharding.Physical{
				tenant: {
					Name:           tenant,
					Status:         models.TenantActivityStatusHOT,
					BelongsToNodes: []string{nodeID},
				},
			},
		},
		ShardProcesses: map[string]NodeShardProcess{
			shardProcessID(tenant, command.TenantProcessRequest_ACTION_FREEZING): {
				nodeID: {
					Tenant: &command.Tenant{Name: tenant, Status: models.TenantActivityStatusHOT},
					Op:     command.TenantsProcess_OP_START,
				},
			},
		},
	}

	req := &command.TenantProcessRequest{
		Node:   nodeID,
		Action: command.TenantProcessRequest_ACTION_FREEZING,
		TenantsProcesses: []*command.TenantsProcess{
			nil,
			{Tenant: nil, Op: command.TenantsProcess_OP_DONE},
			{
				Tenant: &command.Tenant{Name: tenant, Status: models.TenantActivityStatusFROZEN},
				Op:     command.TenantsProcess_OP_DONE,
			},
		},
	}

	require.NotPanics(t, func() {
		sc, err := m.UpdateTenantsProcess(nodeID, req, 7)
		require.NoError(t, err)
		require.Equal(t, 1, sc[models.TenantActivityStatusFROZEN],
			"exactly the one well-formed element must be counted")
	})

	require.Equal(t, models.TenantActivityStatusFROZEN, m.Sharding.Physical[tenant].Status,
		"the well-formed element must still be applied")
	require.EqualValues(t, 7, m.ShardVersion)
	require.Nil(t, req.TenantsProcesses[0], "a nil element must be left untouched")
	require.Nil(t, req.TenantsProcesses[1].Tenant, "a nil-tenant element must be left untouched")
}
