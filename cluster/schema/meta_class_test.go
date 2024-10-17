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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/exp/metadata"
)

// TestUpdateTenantsProcess purpose is to verify that freezing a tenant does not throw errors or
// block regardless of the state of the classTenantDataEvents channel (eg nil or full).
// It's not ideal to test metaClass as it's not public, but our current higher level tests at the
// Raft/Store level mock the metaClass code we want to test.
// If you make chnages that break this test, you should verify/update the tests to verify that
// freezing a tenant does not throw errors or block.
// Later, we should replace this test with one that uses only public types/methods.
func TestUpdateTenantsProcess(t *testing.T) {
	metaClassesToTest := []*metaClass{
		&metaClass{},
		&metaClass{classTenantDataEvents: make(chan metadata.ClassTenant, 0)},
		&metaClass{classTenantDataEvents: make(chan metadata.ClassTenant, 1)},
	}
	for _, m := range metaClassesToTest {
		t.Run("TestUpdateTenantsProcess", func(t *testing.T) {
			err := addAndFreezeTenant(m)
			require.Nil(t, err)
		})
	}
}

func addAndFreezeTenant(m *metaClass) error {
	nodeID := "THIS"
	tenantName := "T0"
	err := m.AddTenants(
		nodeID,
		&api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants: []*api.Tenant{
				{
					Name:   tenantName,
					Status: models.TenantActivityStatusACTIVE,
				},
			},
		},
		1,
		0,
	)
	if err != nil {
		return err
	}
	err = m.UpdateTenants(
		nodeID,
		&api.UpdateTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants: []*api.Tenant{
				{
					Name:   tenantName,
					Status: models.TenantActivityStatusFROZEN,
				},
			},
		},
		0,
	)
	if err != nil {
		return err
	}
	err = m.UpdateTenantsProcess(
		nodeID,
		&api.TenantProcessRequest{
			Node:   nodeID,
			Action: api.TenantProcessRequest_ACTION_FREEZING,
			TenantsProcesses: []*api.TenantsProcess{
				{
					Op: api.TenantsProcess_OP_DONE,
					Tenant: &api.Tenant{
						Name:   tenantName,
						Status: models.TenantActivityStatusFROZEN,
					},
				},
			},
		},
		0,
	)
	if err != nil {
		return err
	}
	return nil
}
