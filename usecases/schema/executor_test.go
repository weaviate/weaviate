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
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
)

var (
	Anything = mock.Anything
	ErrAny   = errors.New("any error")
)

func newMockExecutor(m *fakeMigrator, s *fakeSchemaManager) *executor {
	logger, _ := test.NewNullLogger()
	x := NewExecutor(m, s, logger, func(string) error { return nil })
	x.RegisterSchemaUpdateCallback(func(updatedSchema schema.SchemaWithAliases) {})
	return x
}

func TestExecutor(t *testing.T) {
	ctx := context.Background()
	store := &fakeSchemaManager{}
	cls := &models.Class{
		Class:             "A",
		VectorIndexConfig: flat.NewDefaultUserConfig(),
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 1,
		},
	}
	store.On("ReadOnlySchema").Return(models.Schema{})
	store.On("ReadOnlyClass", "A", mock.Anything).Return(cls)

	t.Run("OpenClose", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("WaitForStartup", ctx).Return(nil)
		migrator.On("Shutdown", ctx).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.Open(ctx))
		assert.Nil(t, x.Close(ctx))
	})

	t.Run("AddClass", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("AddClass", Anything, Anything, Anything).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.AddClass(api.AddClassRequest{}))
	})
	t.Run("AddClassWithError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("AddClass", Anything, Anything, Anything).Return(ErrAny)
		x := newMockExecutor(migrator, store)
		assert.ErrorIs(t, x.AddClass(api.AddClassRequest{}), ErrAny)
	})

	t.Run("DropClass", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("DropClass", Anything, Anything).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.DeleteClass("A", false))
	})
	t.Run("DropClassWithError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("DropClass", Anything, Anything).Return(ErrAny)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.DeleteClass("A", false))
	})

	t.Run("UpdateIndex", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("UpdateVectorIndexConfig", Anything, "A", Anything).Return(nil)
		migrator.On("UpdateInvertedIndexConfig", Anything, "A", Anything).Return(nil)
		migrator.On("UpdateReplicationConfig", context.Background(), "A", false).Return(nil)

		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.UpdateClass(api.UpdateClassRequest{Class: cls}))
	})

	t.Run("UpdateVectorIndexConfig", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("UpdateVectorIndexConfig", Anything, "A", Anything).Return(ErrAny)
		migrator.On("UpdateReplicationConfig", context.Background(), "A", false).Return(nil)

		x := newMockExecutor(migrator, store)
		assert.ErrorIs(t, x.UpdateClass(api.UpdateClassRequest{Class: cls}), ErrAny)
	})
	t.Run("UpdateInvertedIndexConfig", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("UpdateVectorIndexConfig", Anything, "A", Anything).Return(nil)
		migrator.On("UpdateInvertedIndexConfig", Anything, "A", Anything).Return(ErrAny)
		migrator.On("UpdateReplicationConfig", context.Background(), "A", false).Return(nil)

		x := newMockExecutor(migrator, store)
		assert.ErrorIs(t, x.UpdateClass(api.UpdateClassRequest{Class: cls}), ErrAny)
	})

	t.Run("AddProperty", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := api.AddPropertyRequest{Properties: []*models.Property{}}
		migrator.On("AddProperty", Anything, "A", req.Properties).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.AddProperty("A", req))
	})

	// The two replica-add entry points differ only in which load they drive, and
	// the migrator fake panics on any call it has no expectation for, so driving
	// the other one fails the row.
	replicaAdds := []struct {
		name       string
		call       func(*executor) error
		wantLoader string
	}{
		{
			name:       "a plain replica add",
			call:       func(x *executor) error { return x.AddReplicaToShard("A", "S", "N") },
			wantLoader: "LoadShardForNewReplica",
		},
		{
			name:       "a replica movement",
			call:       func(x *executor) error { return x.AddReplicaToShardForMovement("A", "S", "N") },
			wantLoader: "LoadShardForMovement",
		},
	}

	for _, tc := range replicaAdds {
		t.Run(tc.name+" drives "+tc.wantLoader, func(t *testing.T) {
			store := &fakeSchemaManager{}
			store.On("ShardReplicas", "A", "S").Return([]string{"N"}, nil)
			migrator := &fakeMigrator{}
			migrator.On(tc.wantLoader, Anything, "A", "S").Return(nil)

			require.NoError(t, tc.call(newMockExecutor(migrator, store)))
			migrator.AssertExpectations(t)
		})

		t.Run(tc.name+" loads nothing when the schema does not list the replica", func(t *testing.T) {
			store := &fakeSchemaManager{}
			store.On("ShardReplicas", "A", "S").Return([]string{"other"}, nil)

			require.Error(t, tc.call(newMockExecutor(&fakeMigrator{}, store)))
		})

		t.Run(tc.name+" loads nothing when the replicas cannot be read", func(t *testing.T) {
			store := &fakeSchemaManager{}
			store.On("ShardReplicas", "A", "S").Return([]string(nil), ErrAny)

			require.ErrorIs(t, tc.call(newMockExecutor(&fakeMigrator{}, store)), ErrAny)
		})
	}

	tenants := []*api.Tenant{{Name: "T1"}, {Name: "T2"}}

	t.Run("DeleteTenants", func(t *testing.T) {
		migrator := &fakeMigrator{}
		tenants := []*models.Tenant{}
		migrator.On("DeleteTenants", Anything, "A", tenants).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.DeleteTenants("A", tenants))
	})
	t.Run("DeleteTenantsWithError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		tenants := []*models.Tenant{}
		migrator.On("DeleteTenants", Anything, "A", tenants).Return(ErrAny)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.DeleteTenants("A", tenants))
	})

	t.Run("UpdateTenants", func(t *testing.T) {
		cases := []struct {
			name              string
			preFreezeStatuses map[string]string
			want              map[string]string // tenant name -> expected PreFreezeStatus
		}{
			{
				name:              "freezing tenant carries its recorded status",
				preFreezeStatuses: map[string]string{"T1": models.TenantActivityStatusCOLD},
				want:              map[string]string{"T1": models.TenantActivityStatusCOLD, "T2": ""},
			},
			{
				name:              "no freeze recorded, nothing carried",
				preFreezeStatuses: map[string]string{},
				want:              map[string]string{"T1": "", "T2": ""},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				migrator := &fakeMigrator{}
				var got []*UpdateTenantPayload
				migrator.On("UpdateTenants", Anything, cls, Anything).
					Run(func(args mock.Arguments) { got = args.Get(2).([]*UpdateTenantPayload) }).
					Return(nil)
				x := newMockExecutor(migrator, store)

				require.NoError(t, x.UpdateTenants("A", &api.UpdateTenantsRequest{Tenants: tenants}, tc.preFreezeStatuses))

				require.Len(t, got, len(tc.want))
				for _, payload := range got {
					assert.Equal(t, tc.want[payload.Name], payload.PreFreezeStatus, "tenant %q", payload.Name)
				}
			})
		}
	})

	t.Run("UpdateTenantsClassNotFound", func(t *testing.T) {
		store := &fakeSchemaManager{}
		store.On("ReadOnlyClass", "A", mock.Anything).Return(nil)

		req := &api.UpdateTenantsRequest{Tenants: tenants}
		x := newMockExecutor(&fakeMigrator{}, store)
		assert.ErrorIs(t, x.UpdateTenants("A", req, map[string]string{}), ErrNotFound)
	})

	t.Run("UpdateTenantsError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &api.UpdateTenantsRequest{Tenants: tenants}
		migrator.On("UpdateTenants", Anything, cls, Anything).Return(ErrAny)
		x := newMockExecutor(migrator, store)
		assert.ErrorIs(t, x.UpdateTenants("A", req, map[string]string{}), ErrAny)
	})

	report := func(status string) *api.TenantProcessRequest {
		return &api.TenantProcessRequest{
			Action: api.TenantProcessRequest_ACTION_UNFREEZING,
			TenantsProcesses: []*api.TenantsProcess{
				{Op: api.TenantsProcess_OP_DONE, Tenant: &api.Tenant{Name: "T1", Status: status}},
			},
		}
	}

	// A finished offload or onload has to reach UpdateTenantsForProcess, the call
	// whose shard load a namespace keeping its shards closed may skip. Routing it to
	// UpdateTenants instead panics on the unexpected call.
	//
	// The status is copied through rather than branched on, so one report stands
	// for every status the schema can report — and for both the offload and the
	// onload action, which converge on the same payload here.
	t.Run("UpdateTenantsProcess", func(t *testing.T) {
		cases := []struct {
			name string
			req  *api.TenantProcessRequest
			want []*UpdateTenantPayload
		}{
			{
				name: "a reported status is carried through",
				req:  report(models.TenantActivityStatusHOT),
				want: []*UpdateTenantPayload{{Name: "T1", Status: models.TenantActivityStatusHOT}},
			},
			{
				// An unfreeze every node aborted leaves no status behind to read
				// back, and the empty one must reach the migrator as it stands:
				// the shard load behind it is what decides on the namespace.
				name: "an unfreeze with no status to report",
				req:  report(""),
				want: []*UpdateTenantPayload{{Name: "T1", Status: ""}},
			},
			{
				name: "the entries the schema dropped for this node carry nothing",
				req: &api.TenantProcessRequest{
					Action: api.TenantProcessRequest_ACTION_UNFREEZING,
					TenantsProcesses: []*api.TenantsProcess{
						nil,
						{Op: api.TenantsProcess_OP_DONE, Tenant: nil},
					},
				},
				want: []*UpdateTenantPayload{},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				migrator := &fakeMigrator{}
				migrator.On("UpdateTenantsForProcess", Anything, cls, tc.want).Return(nil)
				x := newMockExecutor(migrator, store)

				require.NoError(t, x.UpdateTenantsProcess("A", tc.req))
				migrator.AssertExpectations(t)
			})
		}
	})

	t.Run("UpdateTenantsProcessWithoutProcesses", func(t *testing.T) {
		x := newMockExecutor(&fakeMigrator{}, store)

		require.NoError(t, x.UpdateTenantsProcess("A", &api.TenantProcessRequest{}))
	})

	t.Run("UpdateTenantsProcessClassNotFound", func(t *testing.T) {
		store := &fakeSchemaManager{}
		store.On("ReadOnlyClass", "A", mock.Anything).Return(nil)
		x := newMockExecutor(&fakeMigrator{}, store)

		assert.ErrorIs(t, x.UpdateTenantsProcess("A", report(models.TenantActivityStatusHOT)), ErrNotFound)
	})

	t.Run("UpdateTenantsProcessError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		migrator.On("UpdateTenantsForProcess", Anything, cls, Anything).Return(ErrAny)
		x := newMockExecutor(migrator, store)

		assert.ErrorIs(t, x.UpdateTenantsProcess("A", report(models.TenantActivityStatusHOT)), ErrAny)
	})

	t.Run("AddTenants", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &api.AddTenantsRequest{Tenants: tenants}
		migrator.On("NewTenants", Anything, cls, Anything).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.AddTenants("A", req))
	})
	t.Run("AddTenantsEmpty", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &api.AddTenantsRequest{Tenants: nil}
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.AddTenants("A", req))
	})
	t.Run("AddTenantsError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &api.AddTenantsRequest{Tenants: tenants}
		migrator.On("NewTenants", Anything, cls, Anything).Return(ErrAny)
		x := newMockExecutor(migrator, store)
		assert.ErrorIs(t, x.AddTenants("A", req), ErrAny)
	})
	t.Run("AddTenantsClassNotFound", func(t *testing.T) {
		store := &fakeSchemaManager{}
		store.On("ReadOnlyClass", "A", mock.Anything).Return(nil)
		req := &api.AddTenantsRequest{Tenants: tenants}
		x := newMockExecutor(&fakeMigrator{}, store)
		assert.ErrorIs(t, x.AddTenants("A", req), ErrNotFound)
	})

	t.Run("GetShardsStatus", func(t *testing.T) {
		migrator := &fakeMigrator{}
		status := map[string]map[string]string{"A": {"B": "C"}}
		legacy := map[string]string{"A": "C"}
		migrator.On("GetShardsStatus", Anything, "A", "").Return(status, legacy, nil)
		x := newMockExecutor(migrator, store)
		statusList, err := x.GetShardsStatus("A", "")
		assert.NoError(t, err)
		assert.Len(t, statusList, 1, "number of shards in the status list")
		statusList0 := statusList[0]
		assert.Equal(t, statusList0.Name, "A", "shard name")
		assert.Equal(t, statusList0.Status, "C", "shard legacy status") //nolint:staticcheck
		assert.Equal(t, statusList0.PerNodeStatus, map[string]string{"B": "C"})
	})
	t.Run("GetShardsStatusError", func(t *testing.T) {
		migrator := &fakeMigrator{}
		status := map[string]map[string]string{"A": {"B": "C"}}
		legacy := map[string]string{"A": "C"}
		migrator.On("GetShardsStatus", Anything, "A", "").Return(status, legacy, ErrAny)
		x := newMockExecutor(migrator, store)
		_, err := x.GetShardsStatus("A", "")
		assert.ErrorIs(t, err, ErrAny)
	})
	t.Run("UpdateShardStatus", func(t *testing.T) {
		migrator := &fakeMigrator{}
		req := &api.UpdateShardStatusRequest{Class: "A", Shard: "S", Status: "ST", SchemaVersion: 123}
		migrator.On("UpdateShardStatus", Anything, "A", "S", "ST", uint64(123)).Return(nil)
		x := newMockExecutor(migrator, store)
		assert.Nil(t, x.UpdateShardStatus(req))
	})
}
