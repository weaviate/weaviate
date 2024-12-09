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

// TestUpdateTenantsProcess purpose is to verify that un/freezing a tenant does not throw errors or
// block regardless of the state of the classTenantDataEvents channel (eg nil or full).
// It's not ideal to test metaClass as it's not public, but our current higher level tests at the
// Raft/Store level mock the metaClass code we want to test.
// If you make changes that break this test, you should verify/update the tests to verify that
// un/freezing a tenant does not throw errors or block.
// Later, we should replace this test with one that uses only public types/methods.
func TestUpdateTenantsProcess(t *testing.T) {
	classReplFactor1 := models.Class{ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
	tests := []struct {
		name                   string
		m                      *metaClass
		testFunc               func(m *metaClass) error
		finalTenantDataVersion int64
	}{
		{
			name:                   "Freeze/ClassTenantDataEventsNil",
			m:                      &metaClass{},
			testFunc:               freezeTenant,
			finalTenantDataVersion: 1,
		},
		{
			name:                   "Freeze/ClassTenantDataEventsUnbuffered",
			m:                      &metaClass{classTenantDataEvents: make(chan metadata.ClassTenant)},
			testFunc:               freezeTenant,
			finalTenantDataVersion: 1,
		},
		{
			name:                   "Freeze/ClassTenantDataEventsCapacity1",
			m:                      &metaClass{classTenantDataEvents: make(chan metadata.ClassTenant, 1)},
			testFunc:               freezeTenant,
			finalTenantDataVersion: 1,
		},
		{
			name:                   "Unfreeze/ClassTenantDataEventsNil",
			m:                      &metaClass{Class: classReplFactor1},
			testFunc:               unfreezeTenant,
			finalTenantDataVersion: 0,
		},
		{
			name:                   "Unfreeze/ClassTenantDataEventsUnbuffered",
			m:                      &metaClass{Class: classReplFactor1, classTenantDataEvents: make(chan metadata.ClassTenant)},
			testFunc:               unfreezeTenant,
			finalTenantDataVersion: 0,
		},
		{
			name:                   "Unfreeze/ClassTenantDataEventsCapacity1",
			m:                      &metaClass{Class: classReplFactor1, classTenantDataEvents: make(chan metadata.ClassTenant, 1)},
			testFunc:               unfreezeTenant,
			finalTenantDataVersion: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addTenant(tc.m, models.TenantActivityStatusACTIVE)
			require.Equal(t, int64(0), tc.m.Sharding.Physical[tenantName].DataVersion)
			err := tc.testFunc(tc.m)
			require.Nil(t, err)
			require.Equal(t, tc.finalTenantDataVersion, tc.m.Sharding.Physical[tenantName].DataVersion)
		})
	}
}

const (
	nodeID     = "THIS"
	tenantName = "T0"
)

func addTenant(m *metaClass, tenantStatus string) error {
	err := m.AddTenants(
		nodeID,
		&api.AddTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants: []*api.Tenant{
				{
					Name:   tenantName,
					Status: tenantStatus,
				},
			},
		},
		1,
		0,
	)
	return err
}

func freezeTenant(m *metaClass) error {
	return metaclassUpdateTenant(m, api.TenantProcessRequest_ACTION_FREEZING, models.TenantActivityStatusFROZEN)
}

func unfreezeTenant(m *metaClass) error {
	return metaclassUpdateTenant(m, api.TenantProcessRequest_ACTION_UNFREEZING, models.TenantActivityStatusHOT)
}

func metaclassUpdateTenant(m *metaClass, transitionStatus api.TenantProcessRequest_Action, newStatus string) error {
	err := m.UpdateTenants(
		nodeID,
		&api.UpdateTenantsRequest{
			ClusterNodes: []string{nodeID},
			Tenants: []*api.Tenant{
				{
					Name:   tenantName,
					Status: newStatus,
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
			Action: transitionStatus,
			TenantsProcesses: []*api.TenantsProcess{
				{
					Op: api.TenantsProcess_OP_DONE,
					Tenant: &api.Tenant{
						Name:   tenantName,
						Status: newStatus,
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
