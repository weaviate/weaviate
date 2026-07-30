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

package cluster

import (
	"errors"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// The replication FSM is in-memory RAFT-FSM state rebuilt from log replay, so
// DELETE_CLASS / DELETE_TENANT must flag its ops on *every* apply path — including
// the two that skip updateStore: catch-up replay below lastAppliedIndexToDB, and
// MetadataOnlyVoters. When they do not, ShouldConsumeOps() disagrees between nodes
// and the same UPDATE_CLASS / UPDATE_TENANT entry can be accepted on one node and
// rejected on another.
func TestReplicationCascade_RunsOnEverySchemaOnlyApplyPath(t *testing.T) {
	const (
		class  = "CascadeClass"
		tenant = "T1"
	)

	cases := []struct {
		name string
		// schemaOnly configures the store so the DELETE apply skips updateStore
		schemaOnly func(ms *MockStore)
		cmdType    api.ApplyRequest_Type
		expect     func(ms *MockStore)
	}{
		{
			name:       "delete class, live apply",
			schemaOnly: func(ms *MockStore) {},
			cmdType:    api.ApplyRequest_TYPE_DELETE_CLASS,
			expect: func(ms *MockStore) {
				ms.replicationFSM.EXPECT().DeleteReplicationsByCollection(class).Return(nil).Once()
			},
		},
		{
			name: "delete class, catching up on replay",
			schemaOnly: func(ms *MockStore) {
				ms.store.lastAppliedIndexToDB.Store(100)
			},
			cmdType: api.ApplyRequest_TYPE_DELETE_CLASS,
			expect: func(ms *MockStore) {
				ms.replicationFSM.EXPECT().DeleteReplicationsByCollection(class).Return(nil).Once()
			},
		},
		{
			name: "delete class, metadata-only voter",
			schemaOnly: func(ms *MockStore) {
				ms.store.cfg.MetadataOnlyVoters = true
			},
			cmdType: api.ApplyRequest_TYPE_DELETE_CLASS,
			expect: func(ms *MockStore) {
				ms.replicationFSM.EXPECT().DeleteReplicationsByCollection(class).Return(nil).Once()
			},
		},
		{
			name:       "delete tenant, live apply",
			schemaOnly: func(ms *MockStore) {},
			cmdType:    api.ApplyRequest_TYPE_DELETE_TENANT,
			expect: func(ms *MockStore) {
				ms.replicationFSM.EXPECT().DeleteReplicationsByTenants(class, []string{tenant}).Return(nil).Once()
			},
		},
		{
			name: "delete tenant, catching up on replay",
			schemaOnly: func(ms *MockStore) {
				ms.store.lastAppliedIndexToDB.Store(100)
			},
			cmdType: api.ApplyRequest_TYPE_DELETE_TENANT,
			expect: func(ms *MockStore) {
				ms.replicationFSM.EXPECT().DeleteReplicationsByTenants(class, []string{tenant}).Return(nil).Once()
			},
		},
		{
			name: "delete tenant, metadata-only voter",
			schemaOnly: func(ms *MockStore) {
				ms.store.cfg.MetadataOnlyVoters = true
			},
			cmdType: api.ApplyRequest_TYPE_DELETE_TENANT,
			expect: func(ms *MockStore) {
				ms.replicationFSM.EXPECT().DeleteReplicationsByTenants(class, []string{tenant}).Return(nil).Once()
			},
		},
		{
			name:       "a cascade error does not fail the apply",
			schemaOnly: func(ms *MockStore) {},
			cmdType:    api.ApplyRequest_TYPE_DELETE_CLASS,
			expect: func(ms *MockStore) {
				ms.replicationFSM.EXPECT().DeleteReplicationsByCollection(class).
					Return(errors.New("replication fsm is having a bad day")).Once()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := setupReplicationCascadeStore(t, class, tenant)
			tc.expect(ms)
			tc.schemaOnly(ms)

			var data []byte
			switch tc.cmdType {
			case api.ApplyRequest_TYPE_DELETE_CLASS:
				data = cmdAsBytes(class, tc.cmdType, nil, nil)
			default:
				data = cmdAsBytes(class, tc.cmdType, nil, &api.DeleteTenantsRequest{Tenants: []string{tenant}})
			}

			resp, ok := ms.store.Apply(&raft.Log{Index: 3, Type: raft.LogCommand, Data: data}).(Response)
			require.True(t, ok)
			require.NoError(t, resp.Error, "a replication-FSM hiccup must never block the deletion")

			ms.replicationFSM.AssertExpectations(t)
		})
	}
}

// setupReplicationCascadeStore returns a store with class/tenant already applied
// at indices 1 and 2, ready for a DELETE apply at index 3.
func setupReplicationCascadeStore(t *testing.T, class, tenant string) *MockStore {
	t.Helper()
	ms := NewMockStore(t, "Node-1", 0)
	ms.store.metrics = newStoreMetrics("Node-1", prometheus.NewPedanticRegistry())

	snapshotStore, err := raft.NewFileSnapshotStore(t.TempDir(), 3, nil)
	require.NoError(t, err)
	ms.store.snapshotStore = snapshotStore

	ms.indexer.On("Open", mock.Anything).Return(nil)
	ms.indexer.On("AddClass", mock.Anything).Return(nil)
	ms.indexer.On("DeleteClass", mock.Anything).Return(nil)
	ms.indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
	ms.indexer.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
	ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	ms.parser.On("ParseClass", mock.Anything).Return(nil)

	cls := &models.Class{
		Class:              class,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	state := &sharding.State{Physical: map[string]sharding.Physical{}}

	applyOrFail(t, &ms, &raft.Log{
		Index: 1,
		Type:  raft.LogCommand,
		Data:  cmdAsBytes(class, api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{Class: cls, State: state}, nil),
	}, "add-class")

	applyOrFail(t, &ms, &raft.Log{
		Index: 2,
		Type:  raft.LogCommand,
		Data: cmdAsBytes(class, api.ApplyRequest_TYPE_ADD_TENANT, nil,
			&api.AddTenantsRequest{
				ClusterNodes: []string{"Node-1"},
				Tenants: []*api.Tenant{
					{Name: tenant, Status: models.TenantActivityStatusHOT},
				},
			}),
	}, "add-tenant")

	return &ms
}
