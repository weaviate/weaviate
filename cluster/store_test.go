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

package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var (
	errAny   = errors.New("any error")
	Anything = mock.Anything
)

func TestStoreApply(t *testing.T) {
	doFirst := func(m *MockStore) {
		m.parser.On("ParseClass", mock.Anything).Return(nil)
		m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	}

	cls := &models.Class{Class: "C1", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	ss := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
		Name:           "T1",
		BelongsToNodes: []string{"THIS"},
	}, "T2": {
		Name:           "T2",
		BelongsToNodes: []string{"THIS"},
	}}}

	tests := []struct {
		name     string
		req      raft.Log
		resp     Response
		doBefore func(*MockStore)
		doAfter  func(*MockStore) error
	}{
		{
			name: "AddClass/Unmarshal",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: schema.ErrBadRequest},
			doBefore: doFirst,
		},
		{
			name: "AddClass/StateIsNil",
			req: raft.Log{Data: cmdAsBytes("C2",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: nil},
				nil)},
			resp: Response{Error: schema.ErrBadRequest},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
			},
		},
		{
			name: "AddClass/ParseClass",
			req: raft.Log{Data: cmdAsBytes("C2",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss},
				nil)},
			resp: Response{Error: schema.ErrBadRequest},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(errAny)
			},
		},
		{
			name: "AddClass/Success",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class is missing")
				}
				return nil
			},
		},
		{
			name: "AddClass/Success/MetadataOnly",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.store.cfg.MetadataOnlyVoters = true
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class is missing")
				}
				return nil
			},
		},
		{
			name: "AddClass/Success/CatchingUp",
			req: raft.Log{
				// Fake the index to higher than 0 as we are always applying the first log entry
				Index: 2,
				Data:  cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
			},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				// Set a high enough last applied index to fake applying a log entry when catching up
				m.store.lastAppliedIndexToDB.Store(3)
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class is missing")
				}
				return nil
			},
		},
		{
			name: "AddClass/DBError",
			req: raft.Log{
				Index: 3,
				Data: cmdAsBytes("C1",
					cmd.ApplyRequest_TYPE_ADD_CLASS,
					cmd.AddClassRequest{Class: cls, State: ss},
					nil),
			},
			resp: Response{Error: errAny},
			doBefore: func(ms *MockStore) {
				doFirst(ms)
				ms.indexer.On("AddClass", mock.Anything).Return(errAny)
			},
		},
		{
			name: "AddClass/AlreadyExists",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss},
				nil)},
			resp: Response{Error: schema.ErrSchema},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
		},
		{
			name: "RestoreClass/Success",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_RESTORE_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("RestoreClassDir", cls.Class).Return(nil)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class is missing")
				}
				return nil
			},
		},
		{
			name: "UpdateClass/Unmarshal",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: schema.ErrBadRequest},
			doBefore: doFirst,
		},
		{
			name: "UpdateClass/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				cmd.UpdateClassRequest{Class: cls, State: nil},
				nil)},
			resp: Response{Error: schema.ErrSchema},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)
			},
		},
		{
			name: "UpdateClass/ParseUpdate",
			req: raft.Log{Data: cmdAsBytes("C2",
				cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				cmd.UpdateClassRequest{Class: cls, State: nil},
				nil)},
			resp: Response{Error: schema.ErrBadRequest},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				m.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(nil, errAny)
			},
		},
		{
			name: "UpdateClass/Success",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				cmd.UpdateClassRequest{Class: cls, State: nil},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)
				m.indexer.On("UpdateClass", mock.Anything).Return(nil)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
		},
		{
			name: "DeleteClass/Success/NoErrorDeletingReplications",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_DELETE_CLASS, nil,
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.indexer.On("DeleteClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.replicationFSM.On("DeleteReplicationsByCollection", mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class != nil {
					return fmt.Errorf("class still exists")
				}
				return nil
			},
		},
		{
			name: "DeleteClass/Success/ErrorDeletingReplications",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_DELETE_CLASS, nil,
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.indexer.On("DeleteClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.replicationFSM.On("DeleteReplicationsByCollection", mock.Anything).Return(fmt.Errorf("any error"))
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class != nil {
					return fmt.Errorf("class still exists")
				}
				return nil
			},
		},
		{
			name: "AddProperty/Unmarshal",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: schema.ErrBadRequest},
			doBefore: doFirst,
		},
		{
			name: "AddProperty/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
				cmd.AddPropertyRequest{Properties: []*models.Property{{Name: "P1"}}}, nil)},
			resp:     Response{Error: schema.ErrSchema},
			doBefore: doFirst,
		},
		{
			name: "AddProperty/Nil",
			req: raft.Log{
				Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
					cmd.AddPropertyRequest{Properties: nil}, nil),
			},
			resp: Response{Error: schema.ErrBadRequest},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
		},
		{
			name: "AddProperty/Success",
			req: raft.Log{
				Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
					cmd.AddPropertyRequest{Properties: []*models.Property{{Name: "P1"}}}, nil),
			},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				m.indexer.On("AddProperty", mock.Anything, mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class not found")
				}

				ok := false
				for _, p := range class.Properties {
					if p.Name == "P1" {
						ok = true
						break
					}
				}
				if !ok {
					return fmt.Errorf("property is missing")
				}
				return nil
			},
		},
		{
			name: "UpdateShard/Unmarshal",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: schema.ErrBadRequest},
			doBefore: doFirst,
		},
		{
			name: "UpdateShard/Success",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
				cmd.UpdateShardStatusRequest{Class: "C1"}, nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("UpdateShardStatus", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
		},
		{
			name:     "AddTenant/Unmarshal",
			req:      raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, cmd.AddClassRequest{}, nil)},
			resp:     Response{Error: schema.ErrBadRequest},
			doBefore: doFirst,
		},
		{
			name: "AddTenant/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, nil, &cmd.AddTenantsRequest{
				Tenants: []*cmd.Tenant{nil, {Name: "T1"}, nil},
			})},
			resp:     Response{Error: schema.ErrSchema},
			doBefore: doFirst,
		},
		{
			name: "AddTenant/Success",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, nil, &cmd.AddTenantsRequest{
				ClusterNodes: []string{"THIS"},
				Tenants:      []*cmd.Tenant{nil, {Name: "T1"}, nil},
			})},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{
						Class: cls, State: &sharding.State{
							Physical: map[string]sharding.Physical{"T1": {}},
						},
					}, nil),
				})
				m.indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState == nil {
					return fmt.Errorf("sharding state not found")
				}
				if _, ok := shardingState.Physical["T1"]; !ok {
					return fmt.Errorf("tenant is missing")
				}
				return nil
			},
		},
		{
			name:     "UpdateTenant/Unmarshal",
			req:      raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT, cmd.AddClassRequest{}, nil)},
			resp:     Response{Error: schema.ErrBadRequest},
			doBefore: doFirst,
		},
		{
			name: "UpdateTenant/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*cmd.Tenant{nil, {Name: "T1"}, nil}})},
			resp:     Response{Error: schema.ErrSchema},
			doBefore: doFirst,
		},
		{
			name: "UpdateTenant/NoFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*cmd.Tenant{
					{Name: "T1", Status: models.TenantActivityStatusCOLD},
				}})},
			resp: Response{Error: schema.ErrSchema},
			doBefore: func(m *MockStore) {
				ss := &sharding.State{Physical: map[string]sharding.Physical{}}
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
		},
		{
			name: "UpdateTenant/HasOngoingReplication/true",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*cmd.Tenant{
					{Name: "T1", Status: models.TenantActivityStatusCOLD},
				}})},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				ss := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-1"},
					Status:         models.TenantActivityStatusHOT,
				}}}
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				m.replicationFSM.EXPECT().HasOngoingReplication("C1", "T1", "Node-1").Return(true)
				m.indexer.On("UpdateTenants", mock.Anything, mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				want := map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-1"},
					Status:         models.TenantActivityStatusHOT,
				}}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if got := shardingState.Physical; !reflect.DeepEqual(got, want) {
					return fmt.Errorf("physical state want: %v got: %v", want, got)
				}
				return nil
			},
		},
		{
			name: "UpdateTenant/HasOngoingReplication/false",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*cmd.Tenant{
					{Name: "T1", Status: models.TenantActivityStatusCOLD},
				}})},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				ss := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-1"},
					Status:         models.TenantActivityStatusHOT,
				}}}
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				m.replicationFSM.EXPECT().HasOngoingReplication("C1", "T1", "Node-1").Return(false)
				m.indexer.On("UpdateTenants", mock.Anything, mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				want := map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-1"},
					Status:         models.TenantActivityStatusCOLD,
				}}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if got := shardingState.Physical; !reflect.DeepEqual(got, want) {
					return fmt.Errorf("physical state want: %v got: %v", want, got)
				}
				return nil
			},
		},
		{
			name: "UpdateTenant/Success",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*cmd.Tenant{
					{Name: "T1", Status: models.TenantActivityStatusCOLD},
					{Name: "T2", Status: models.TenantActivityStatusCOLD},
					{Name: "T3", Status: models.TenantActivityStatusCOLD},
				}})},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				ss := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"THIS"},
					Status:         models.TenantActivityStatusHOT,
				}, "T2": {
					Name:           "T2",
					BelongsToNodes: []string{"THIS"},
					Status:         models.TenantActivityStatusCOLD,
				}, "T3": {
					Name:           "T3",
					BelongsToNodes: []string{"NODE-2"},
					Status:         models.TenantActivityStatusHOT,
				}}}
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				m.indexer.On("UpdateTenants", mock.Anything, mock.Anything).Return(nil)
				m.replicationFSM.EXPECT().HasOngoingReplication(Anything, Anything, Anything).Return(false)
			},
			doAfter: func(ms *MockStore) error {
				want := map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"THIS"},
					Status:         models.TenantActivityStatusCOLD,
				}, "T2": {
					Name:           "T2",
					BelongsToNodes: []string{"THIS"},
					Status:         models.TenantActivityStatusCOLD,
				}, "T3": {
					Name:           "T3",
					BelongsToNodes: []string{"NODE-2"},
					Status:         models.TenantActivityStatusCOLD,
				}}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if got := shardingState.Physical; !reflect.DeepEqual(got, want) {
					return fmt.Errorf("physical state want: %v got: %v", want, got)
				}
				return nil
			},
		},
		{
			name:     "DeleteTenant/Unmarshal",
			req:      raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT, cmd.AddClassRequest{}, nil)},
			resp:     Response{Error: schema.ErrBadRequest},
			doBefore: doFirst,
		},
		{
			name: "DeleteTenant/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT,
				nil, &cmd.DeleteTenantsRequest{Tenants: []string{"T1", "T2"}})},
			resp: Response{Error: schema.ErrSchema},
			doBefore: func(m *MockStore) {
				doFirst(m)
			},
		},
		{
			name: "DeleteTenant/Success/NoErrorDeletingReplications",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT,
				nil, &cmd.DeleteTenantsRequest{Tenants: []string{"T1", "T2"}})},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{
						Class: cls, State: &sharding.State{
							Physical: map[string]sharding.Physical{"T1": {}},
						},
					}, nil),
				})
				m.indexer.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
				m.replicationFSM.On("DeleteReplicationsByTenants", mock.Anything, mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if len(shardingState.Physical) != 0 {
					return fmt.Errorf("sharding state mus be empty after deletion")
				}
				return nil
			},
		},
		{
			name: "DeleteTenant/Success/ErrorDeletingReplications",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT,
				nil, &cmd.DeleteTenantsRequest{Tenants: []string{"T1", "T2"}})},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{
						Class: cls, State: &sharding.State{
							Physical: map[string]sharding.Physical{"T1": {}},
						},
					}, nil),
				})
				m.indexer.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
				m.replicationFSM.On("DeleteReplicationsByTenants", mock.Anything, mock.Anything).Return(fmt.Errorf("any error"))
			},
			doAfter: func(ms *MockStore) error {
				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if len(shardingState.Physical) != 0 {
					return fmt.Errorf("sharding state mus be empty after deletion")
				}
				return nil
			},
		},
		{
			name: "DeleteReplicaFromShard/Success/UpdateDB",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD, cmd.DeleteReplicaFromShard{Class: "C1", Shard: "T1", TargetNode: "Node-1"}, nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				ss := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-1", "Node-2"},
				}}, ReplicationFactor: 1}
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("DeleteReplicaFromShard", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
			doAfter: func(ms *MockStore) error {
				replicas, err := ms.store.SchemaReader().ShardReplicas("C1", "T1")
				if err != nil {
					return err
				}
				if len(replicas) != 1 {
					return fmt.Errorf("sharding state should have 1 shard for class C1 after deleting a shard")
				}

				return nil
			},
		},
		{
			name: "DeleteReplicaFromShard/Success/NotUpdateDB",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD, cmd.DeleteReplicaFromShard{Class: "C1", Shard: "T1", TargetNode: "Node-2"}, nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				ss := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-2", "Node-3"},
				}}, ReplicationFactor: 1}
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
			doAfter: func(ms *MockStore) error {
				replicas, err := ms.store.SchemaReader().ShardReplicas("C1", "T1")
				if err != nil {
					return err
				}
				if len(replicas) != 1 {
					return fmt.Errorf("sharding state should have 1 shard for class C1 after deleting a shard")
				}

				return nil
			},
		},
		{
			name: "DeleteReplicaFromShard/Fail/ClassNotFound",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD, cmd.DeleteReplicaFromShard{Class: "C1", Shard: "T1", TargetNode: "Node-2"}, nil)},
			resp: Response{Error: schema.ErrSchema},
		},
		{
			name: "DeleteReplicaFromShard/Fail/ShardNotFound",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD, cmd.DeleteReplicaFromShard{Class: "C1", Shard: "T1", TargetNode: "Node-2"}, nil)},
			resp: Response{Error: schema.ErrSchema},
			doBefore: func(m *MockStore) {
				ss := &sharding.State{Physical: map[string]sharding.Physical{"T2": {
					Name:           "T2",
					BelongsToNodes: []string{"Node-2"},
				}}}
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
			doAfter: func(ms *MockStore) error {
				replicas, err := ms.store.SchemaReader().ShardReplicas("C1", "T2")
				if err != nil {
					return err
				}
				if len(replicas) != 1 {
					return fmt.Errorf("sharding state should have 1 shard for class C1")
				}

				return nil
			},
		},
		{
			name: "DeleteReplicaFromShard/Fail/BelowMinimumReplicationFactor/SingleReplica",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD, cmd.DeleteReplicaFromShard{Class: "C1", Shard: "T2", TargetNode: "Node-1"}, nil)},
			resp: Response{Error: schema.ErrSchema}, // Expect an error
			doBefore: func(m *MockStore) {
				ss := &sharding.State{
					Physical: map[string]sharding.Physical{"T2": {
						Name:           "T2",
						BelongsToNodes: []string{"Node-1"},
					}},
					// ReplicationFactor will be migrated to 1 as the default minimum
				}
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
			doAfter: func(ms *MockStore) error {
				replicas, err := ms.store.SchemaReader().ShardReplicas("C1", "T2")
				if err != nil {
					return err
				}
				if len(replicas) != 1 {
					return fmt.Errorf("sharding state should still have 1 replica for class C1, shard T2")
				}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState.ReplicationFactor != 1 {
					return fmt.Errorf("replication factor should be 1, got %d", shardingState.ReplicationFactor)
				}

				return nil
			},
		},
		{
			name: "DeleteReplicaFromShard/Success/AboveMinimumReplicationFactor/DefaultReplicationFactor",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD, cmd.DeleteReplicaFromShard{Class: "C1", Shard: "T2", TargetNode: "Node-2"}, nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				ss := &sharding.State{
					Physical: map[string]sharding.Physical{"T2": {
						Name:           "T2",
						BelongsToNodes: []string{"Node-1", "Node-2", "Node-3"},
					}},
					// ReplicationFactor will be migrated to 1 as the default minimum
				}
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
			doAfter: func(ms *MockStore) error {
				replicas, err := ms.store.SchemaReader().ShardReplicas("C1", "T2")
				if err != nil {
					return err
				}
				if len(replicas) != 2 {
					return fmt.Errorf("sharding state should have 2 replicas after deletion, got %d", len(replicas))
				}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState.ReplicationFactor != 1 {
					return fmt.Errorf("replication factor should be 1, got %d", shardingState.ReplicationFactor)
				}

				return nil
			},
		},
		{
			name: "DeleteReplicaFromShard/Fail/BelowCustomReplicationFactor",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD, cmd.DeleteReplicaFromShard{Class: "C1", Shard: "T2", TargetNode: "Node-2"}, nil)},
			resp: Response{Error: schema.ErrSchema},
			doBefore: func(m *MockStore) {
				ss := &sharding.State{
					Physical: map[string]sharding.Physical{"T2": {
						Name:           "T2",
						BelongsToNodes: []string{"Node-1", "Node-2"},
					}},
					ReplicationFactor: 2,
				}
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
			doAfter: func(ms *MockStore) error {
				replicas, err := ms.store.SchemaReader().ShardReplicas("C1", "T2")
				if err != nil {
					return err
				}
				if len(replicas) != 2 {
					return fmt.Errorf("sharding state should still have 2 replicas for class C1, shard T2")
				}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState.ReplicationFactor != 2 {
					return fmt.Errorf("replication factor should be 2, got %d", shardingState.ReplicationFactor)
				}

				return nil
			},
		},
		{
			name: "DeleteReplicaFromShard/Success/AboveCustomReplicationFactor",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD, cmd.DeleteReplicaFromShard{Class: "C1", Shard: "T2", TargetNode: "Node-3"}, nil)},
			resp: Response{Error: nil}, // Should succeed
			doBefore: func(m *MockStore) {
				ss := &sharding.State{
					Physical: map[string]sharding.Physical{"T2": {
						Name:           "T2",
						BelongsToNodes: []string{"Node-1", "Node-2", "Node-3", "Node-4"},
					}},
					ReplicationFactor: 3,
				}
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
			doAfter: func(ms *MockStore) error {
				replicas, err := ms.store.SchemaReader().ShardReplicas("C1", "T2")
				if err != nil {
					return err
				}
				if len(replicas) != 3 {
					return fmt.Errorf("sharding state should have 3 replicas after deletion, got %d", len(replicas))
				}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState.ReplicationFactor != 3 {
					return fmt.Errorf("replication factor should be 3, got %d", shardingState.ReplicationFactor)
				}

				return nil
			},
		},
		{
			name: "AddReplicaToShard/Success/UpdateDB",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD, cmd.AddReplicaToShard{Class: "C1", Shard: "T1", TargetNode: "Node-1"}, nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("AddReplicaToShard", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
			doAfter: func(ms *MockStore) error {
				replicas, err := ms.store.SchemaReader().ShardReplicas("C1", "T1")
				if err != nil {
					return err
				}
				if len(replicas) != 2 {
					return fmt.Errorf("sharding state should have 2 shards for class C1")
				}
				if !slices.Contains(replicas, "THIS") || !slices.Contains(replicas, "Node-1") {
					return fmt.Errorf("replias for coll C1 shard T1 is missing the correct replicas got=%v want=[\"THIS\", \"Node-1\"]", replicas)
				}

				return nil
			},
		},
		{
			name: "AddReplicaToShard/Success/NotUpdateDB",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD, cmd.AddReplicaToShard{Class: "C1", Shard: "T1", TargetNode: "Node-3"}, nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
			doAfter: func(ms *MockStore) error {
				replicas, err := ms.store.SchemaReader().ShardReplicas("C1", "T1")
				if err != nil {
					return err
				}
				if len(replicas) != 2 {
					return fmt.Errorf("sharding state should have 2 shards for class C1")
				}
				if !slices.Contains(replicas, "THIS") || !slices.Contains(replicas, "Node-3") {
					return fmt.Errorf("replias for coll C1 shard T1 is missing the correct replicas got=%v want=[\"THIS\", \"Node-3\"]", replicas)
				}

				return nil
			},
		},
		{
			name: "AddReplicaToShard/FailClassNotFound",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD, cmd.AddReplicaToShard{Class: "C1", Shard: "T1", TargetNode: "Node-3"}, nil)},
			resp: Response{Error: schema.ErrSchema},
		},
		{
			name: "AddReplicaToShard/FailShardNotFound",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD, cmd.AddReplicaToShard{Class: "C1", Shard: "T1000", TargetNode: "Node-3"}, nil)},
			resp: Response{Error: schema.ErrSchema},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
		},
		{
			name: "AddReplicaToShard/FailReplicaAlreadyExists",
			req:  raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD, cmd.AddReplicaToShard{Class: "C1", Shard: "T1", TargetNode: "THIS"}, nil)},
			resp: Response{Error: schema.ErrSchema},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
			},
		},
		{
			name: "AddClass/MigrateReplicationFactor/Uninitialized",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{
					Class: cls,
					State: &sharding.State{
						IndexID: "C1",
						Physical: map[string]sharding.Physical{
							"T1": {
								Name:           "T1",
								BelongsToNodes: []string{"THIS", "THAT"},
								Status:         models.TenantActivityStatusHOT,
							},
						},
						// ReplicationFactor intentionally not set (uninitialized)
					},
				},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class is missing")
				}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState == nil {
					return fmt.Errorf("sharding state is missing")
				}

				if shardingState.ReplicationFactor != 1 {
					return fmt.Errorf("replication factor not properly migrated, expected 1, got %d",
						shardingState.ReplicationFactor)
				}

				for tenantName, tenant := range shardingState.Physical {
					if len(tenant.BelongsToNodes) != 2 {
						return fmt.Errorf("tenant %s should have 2 replicas, got %d",
							tenantName, len(tenant.BelongsToNodes))
					}
				}

				return nil
			},
		},
		{
			name: "AddClass/MigrateReplicationFactor/ExplicitZero",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{
					Class: cls,
					State: &sharding.State{
						IndexID: "C1",
						Physical: map[string]sharding.Physical{
							"T1": {
								Name:           "T1",
								BelongsToNodes: []string{"THIS", "THAT", "ANOTHER"},
								Status:         models.TenantActivityStatusHOT,
							},
						},
						ReplicationFactor: 0,
					},
				},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class is missing")
				}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState == nil {
					return fmt.Errorf("sharding state is missing")
				}

				if shardingState.ReplicationFactor != 1 {
					return fmt.Errorf("replication factor not properly migrated, expected 1, got %d",
						shardingState.ReplicationFactor)
				}

				for tenantName, tenant := range shardingState.Physical {
					if len(tenant.BelongsToNodes) != 3 {
						return fmt.Errorf("tenant %s should have 3 replicas, got %d",
							tenantName, len(tenant.BelongsToNodes))
					}
				}

				return nil
			},
		},
		{
			name: "AddClass/MigrateReplicationFactor/Partitioned",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{
					Class: cls,
					State: &sharding.State{
						IndexID:             "C1",
						Physical:            map[string]sharding.Physical{},
						PartitioningEnabled: true,
						// ReplicationFactor intentionally not set (uninitialized)
					},
				},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class is missing")
				}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState == nil {
					return fmt.Errorf("sharding state is missing")
				}

				if shardingState.ReplicationFactor != 1 {
					return fmt.Errorf("replication factor for partitioned state not properly migrated, expected 1, got %d",
						shardingState.ReplicationFactor)
				}

				if !shardingState.PartitioningEnabled {
					return fmt.Errorf("partitioning should still be enabled")
				}

				return nil
			},
		},
		{
			name: "AddClass/PreserveReplicationFactor/NonDefault",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{
					Class: cls,
					State: &sharding.State{
						IndexID: "C1",
						Physical: map[string]sharding.Physical{
							"T1": {
								Name:           "T1",
								BelongsToNodes: []string{"THIS", "THAT"},
								Status:         models.TenantActivityStatusHOT,
							},
						},
						ReplicationFactor: 5,
					},
				},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class is missing")
				}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState == nil {
					return fmt.Errorf("sharding state is missing")
				}

				if shardingState.ReplicationFactor != 5 {
					return fmt.Errorf("non-default replication factor not preserved, expected 5, got %d",
						shardingState.ReplicationFactor)
				}

				return nil
			},
		},
		{
			name: "RestoreClass/MigrateReplicationFactor/Uninitialized",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_RESTORE_CLASS,
				cmd.AddClassRequest{
					Class: cls,
					State: &sharding.State{
						IndexID: "C1",
						Physical: map[string]sharding.Physical{
							"T1": {
								Name:           "T1",
								BelongsToNodes: []string{"THIS", "THAT"},
								Status:         models.TenantActivityStatusHOT,
							},
						},
						// ReplicationFactor intentionally not set (uninitialized)
					},
				},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.indexer.On("RestoreClassDir", cls.Class).Return(nil)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			doAfter: func(ms *MockStore) error {
				class := ms.store.SchemaReader().ReadOnlyClass("C1")
				if class == nil {
					return fmt.Errorf("class is missing")
				}

				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if shardingState == nil {
					return fmt.Errorf("sharding state is missing")
				}

				if shardingState.ReplicationFactor != 1 {
					return fmt.Errorf("replication factor not properly migrated during restore, expected 1, got %d",
						shardingState.ReplicationFactor)
				}

				return nil
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMockStore(t, "Node-1", 9091)
			store := m.Store(tc.doBefore)
			ret := store.Apply(&tc.req)
			resp, ok := ret.(Response)
			if !ok {
				t.Errorf("%s: response has wrong type", tc.name)
			}
			if got, want := resp.Error, tc.resp.Error; want != nil {
				if !errors.Is(resp.Error, tc.resp.Error) {
					t.Errorf("%s: error want: %v got: %v", tc.name, want, got)
				}
			} else if got != nil {
				t.Errorf("%s: error want: nil got: %v", tc.name, got)
			}
			if tc.doAfter != nil {
				if err := tc.doAfter(&m); err != nil {
					t.Errorf("%s check updates: %v", tc.name, err)
				}
				m.indexer.AssertExpectations(t)
				m.parser.AssertExpectations(t)
				m.replicationFSM.AssertExpectations(t)
			}
		})
	}
}

func TestStoreMetrics(t *testing.T) {
	t.Run("store_apply_duration", func(t *testing.T) {
		doBefore := func(m *MockStore) {
			m.indexer.On("AddClass", mock.Anything).Return(nil)
			m.parser.On("ParseClass", mock.Anything).Return(nil)
			m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
		}
		nodeID := t.Name()
		cls := &models.Class{Class: "C1", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
		ss := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
			Name:           "T1",
			BelongsToNodes: []string{"THIS"},
		}, "T2": {
			Name:           "T2",
			BelongsToNodes: []string{"THIS"},
		}}}
		ms := NewMockStore(t, nodeID, 9092)
		store := ms.Store(doBefore)
		m := dto.Metric{}
		require.NoError(t, store.metrics.applyDuration.Write(&m))
		// before
		assert.Equal(t, 0, int(*m.Histogram.SampleCount))
		store.Apply(
			&raft.Log{
				Data: cmdAsBytes("CI",
					cmd.ApplyRequest_TYPE_ADD_CLASS,
					cmd.AddClassRequest{Class: cls, State: ss}, nil),
			},
		)
		// after
		require.NoError(t, store.metrics.applyDuration.Write(&m))
		assert.Equal(t, 1, int(*m.Histogram.SampleCount))
		assert.Equal(t, 0, int(testutil.ToFloat64(store.metrics.applyFailures)))
	})
	t.Run("fsm_last_applied_index", func(t *testing.T) {
		appliedIndex := 34 // after successful apply, this node should have 34 as last applied index metric

		doBefore := func(m *MockStore) {
			m.indexer.On("AddClass", mock.Anything).Return(nil)
			m.parser.On("ParseClass", mock.Anything).Return(nil)
			m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
		}
		nodeID := t.Name()
		cls := &models.Class{Class: "C1", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
		ss := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
			Name:           "T1",
			BelongsToNodes: []string{"THIS"},
		}, "T2": {
			Name:           "T2",
			BelongsToNodes: []string{"THIS"},
		}}}
		ms := NewMockStore(t, nodeID, 9092)
		store := ms.Store(doBefore)

		// before
		require.Equal(t, 0, int(testutil.ToFloat64(store.metrics.fsmLastAppliedIndex)))
		require.Equal(t, 0, int(testutil.ToFloat64(store.metrics.raftLastAppliedIndex)))

		store.Apply(
			&raft.Log{
				Index: uint64(appliedIndex),
				Data: cmdAsBytes("CI",
					cmd.ApplyRequest_TYPE_ADD_CLASS,
					cmd.AddClassRequest{Class: cls, State: ss}, nil),
			},
		)
		// after
		require.Equal(t, appliedIndex, int(testutil.ToFloat64(store.metrics.fsmLastAppliedIndex)))
		require.Equal(t, appliedIndex, int(testutil.ToFloat64(store.metrics.raftLastAppliedIndex)))
	})

	t.Run("last_applied_index on Configuration LogType", func(t *testing.T) {
		appliedIndex := 34 // after successful apply, this node should have 34 as last applied index metric

		doBefore := func(m *MockStore) {
			m.indexer.On("AddClass", mock.Anything).Return(nil)
			m.parser.On("ParseClass", mock.Anything).Return(nil)
			m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
		}
		nodeID := t.Name()

		ms := NewMockStore(t, nodeID, 9092)
		store := ms.Store(doBefore)

		// before
		require.Equal(t, 0, int(testutil.ToFloat64(store.metrics.fsmLastAppliedIndex)))
		require.Equal(t, 0, int(testutil.ToFloat64(store.metrics.raftLastAppliedIndex)))

		store.StoreConfiguration(uint64(appliedIndex), raft.Configuration{})

		// after
		require.Equal(t, 0, int(testutil.ToFloat64(store.metrics.fsmLastAppliedIndex))) // fsm index should staty the same because it counts non-config commands.
		require.Equal(t, appliedIndex, int(testutil.ToFloat64(store.metrics.raftLastAppliedIndex)))
	})

	t.Run("apply_failures", func(t *testing.T) {
		doBefore := func(m *MockStore) {
			m.indexer.On("AddClass", mock.Anything).Return(nil)
			m.parser.On("ParseClass", mock.Anything).Return(nil)
			m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
		}

		nodeID := t.Name()
		ms := NewMockStore(t, nodeID, 9092)
		store := ms.Store(doBefore)

		// before
		require.Equal(t, 0, int(testutil.ToFloat64(store.metrics.applyFailures)))

		// this apply will trigger failure with BadRequest as we pass empty (nil) AddClassRequest.
		store.Apply(
			&raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS,
				nil, &cmd.AddTenantsRequest{})},
		)
		// after
		require.Equal(t, 1, int(testutil.ToFloat64(store.metrics.applyFailures)))
	})
}

type MockStore struct {
	indexer        *fakes.MockSchemaExecutor
	parser         *fakes.MockParser
	logger         *logrus.Logger
	cfg            Config
	store          *Store
	replicationFSM *schema.MockreplicationFSM
}

func NewMockStore(t *testing.T, nodeID string, raftPort int) MockStore {
	indexer := fakes.NewMockSchemaExecutor()
	parser := fakes.NewMockParser()
	logger, _ := logrustest.NewNullLogger()
	ms := MockStore{
		indexer: indexer,
		parser:  parser,
		logger:  logger,
		cfg: Config{
			WorkDir:                t.TempDir(),
			NodeID:                 nodeID,
			Host:                   "localhost",
			RaftPort:               raftPort,
			Voter:                  true,
			BootstrapExpect:        1,
			HeartbeatTimeout:       1 * time.Second,
			ElectionTimeout:        1 * time.Second,
			SnapshotInterval:       2 * time.Second,
			SnapshotThreshold:      125,
			DB:                     indexer,
			Parser:                 parser,
			NodeSelector:           mocks.NewMockNodeSelector("localhost"),
			Logger:                 logger,
			ConsistencyWaitTimeout: time.Millisecond * 50,
		},
		replicationFSM: schema.NewMockreplicationFSM(t),
	}

	s := NewFSM(ms.cfg, nil, nil, prometheus.NewPedanticRegistry())
	s.schemaManager.SetReplicationFSM(ms.replicationFSM)
	ms.store = &s
	return ms
}

func (m *MockStore) Store(doBefore func(*MockStore)) *Store {
	if doBefore != nil {
		doBefore(m)
	}
	return m.store
}

// Runs the provided function `predicate` up to `n` times, sleeping `sleepDuration` between each
// function call until `f` returns true or returns false if all `n` calls return false.
// Useful in tests which require an unknown but bounded delay where the component under test has
// a way to indicate when it's ready to proceed.
func tryNTimesWithWait(n int, sleepDuration time.Duration, predicate func() bool) bool {
	for i := 0; i < n; i++ {
		if predicate() {
			return true
		}
		time.Sleep(sleepDuration)
	}
	return false
}

func cmdAsBytes(class string,
	cmdType cmd.ApplyRequest_Type,
	jsonSubCmd interface{},
	rpcSubCmd protoreflect.ProtoMessage,
) []byte {
	var (
		subData []byte
		err     error
	)
	if rpcSubCmd != nil {
		subData, err = gproto.Marshal(rpcSubCmd)
		if err != nil {
			panic("proto.Marshal: " + err.Error())
		}
	} else if jsonSubCmd != nil {
		subData, err = json.Marshal(jsonSubCmd)
		if err != nil {
			panic("json.Marshal( " + err.Error())
		}
	}

	cmd := cmd.ApplyRequest{
		Type:       cmdType,
		Class:      class,
		SubCommand: subData,
	}
	data, err := gproto.Marshal(&cmd)
	if err != nil {
		panic(err)
	}

	return data
}
