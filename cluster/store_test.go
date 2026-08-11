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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
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

	"github.com/weaviate/weaviate/adapters/repos/db"
	clustermocks "github.com/weaviate/weaviate/cluster/mocks"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
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
	ss := &sharding.State{
		Physical: map[string]sharding.Physical{"T1": {
			Name:           "T1",
			BelongsToNodes: []string{"THIS"},
		}, "T2": {
			Name:           "T2",
			BelongsToNodes: []string{"THIS"},
		}},
		PartitioningEnabled: true, // multi-tenant collection
	}

	// captured by UpdateTenant/FreezeHandsPreFreezeStatusToTheDB
	var preFreezeStatusesSeenByDB map[string]string

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
				m.replicationFSM.EXPECT().HasActiveReplicationForCollection(mock.Anything).Return(false)
			},
		},
		{
			name: "DeleteClass/Success/NoErrorDeletingReplications",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_DELETE_CLASS, nil,
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.indexer.On("DeleteClass", mock.Anything, mock.Anything).Return(nil)
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
				m.indexer.On("DeleteClass", mock.Anything, mock.Anything).Return(nil)
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
							Physical: map[string]sharding.Physical{"T1": {}}, PartitioningEnabled: true,
						},
					}, nil),
				})
				m.indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
				// ErrShardNotFound (partial success), so the DB layer is invoked with the
				// filtered (empty) tenant list before the schema error is propagated.
				m.indexer.On("UpdateTenants", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error { return nil },
		},
		{
			name: "UpdateTenant/HasActiveReplicationForShard/true",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*cmd.Tenant{
					{Name: "T1", Status: models.TenantActivityStatusCOLD},
				}})},
			resp: Response{Error: schema.ErrReplicaMovementInProgress},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				ss := &sharding.State{Physical: map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-1"},
					Status:         models.TenantActivityStatusHOT,
				}}, PartitioningEnabled: true}
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				m.replicationFSM.EXPECT().HasActiveReplicationForShard("C1", "T1").Return(true)
				m.indexer.On("UpdateTenants", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				want := map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-1"},
					Status:         models.TenantActivityStatusHOT,
				}}

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
				if got := shardingState.Physical; !reflect.DeepEqual(got, want) {
					return fmt.Errorf("physical state want: %v got: %v", want, got)
				}
				return nil
			},
		},
		{
			name: "UpdateTenant/HasActiveReplicationForShard/false",
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
				}}, PartitioningEnabled: true}
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				m.replicationFSM.EXPECT().HasActiveReplicationForShard("C1", "T1").Return(false)
				m.indexer.On("UpdateTenants", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				want := map[string]sharding.Physical{"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-1"},
					Status:         models.TenantActivityStatusCOLD,
				}}

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
				}}, PartitioningEnabled: true}
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				m.indexer.On("UpdateTenants", mock.Anything, mock.Anything, mock.Anything).Return(nil)
				m.replicationFSM.EXPECT().HasActiveReplicationForShard(Anything, Anything).Return(false)
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
				if got := shardingState.Physical; !reflect.DeepEqual(got, want) {
					return fmt.Errorf("physical state want: %v got: %v", want, got)
				}
				return nil
			},
		},
		{
			// The DB reports the pre-freeze status back if the freeze aborts, so the
			// schema update must hand the DB update what it recorded.
			name: "UpdateTenant/FreezeHandsPreFreezeStatusToTheDB",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*cmd.Tenant{
					{Name: "T1", Status: models.TenantActivityStatusFROZEN},
					{Name: "T2", Status: models.TenantActivityStatusFROZEN},
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
				}}, PartitioningEnabled: true}
				m.indexer.On("AddClass", mock.Anything).Return(nil)
				m.store.Apply(&raft.Log{
					Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil),
				})
				preFreezeStatusesSeenByDB = nil
				m.indexer.On("UpdateTenants", mock.Anything, mock.Anything, mock.Anything).
					Run(func(args mock.Arguments) {
						preFreezeStatusesSeenByDB = args.Get(2).(map[string]string)
					}).Return(nil)
				m.replicationFSM.EXPECT().HasActiveReplicationForShard(Anything, Anything).Return(false)
			},
			doAfter: func(ms *MockStore) error {
				want := map[string]string{
					"T1": models.TenantActivityStatusHOT,
					"T2": models.TenantActivityStatusCOLD,
				}
				if !reflect.DeepEqual(preFreezeStatusesSeenByDB, want) {
					return fmt.Errorf("pre-freeze statuses want: %v got: %v", want, preFreezeStatusesSeenByDB)
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
							Physical: map[string]sharding.Physical{"T1": {}}, PartitioningEnabled: true,
						},
					}, nil),
				})
				m.indexer.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
				m.replicationFSM.On("DeleteReplicationsByTenants", mock.Anything, mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
							Physical: map[string]sharding.Physical{"T1": {}}, PartitioningEnabled: true,
						},
					}, nil),
				})
				m.indexer.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
				m.replicationFSM.On("DeleteReplicationsByTenants", mock.Anything, mock.Anything).Return(fmt.Errorf("any error"))
			},
			doAfter: func(ms *MockStore) error {
				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
					PartitioningEnabled: true,
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
					PartitioningEnabled: true,
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
					PartitioningEnabled: true,
					ReplicationFactor:   2,
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
					PartitioningEnabled: true,
					ReplicationFactor:   3,
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
						PartitioningEnabled: true,
						ReplicationFactor:   0,
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
						PartitioningEnabled: true,
						ReplicationFactor:   5,
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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
						PartitioningEnabled: true,
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

				shardingState, err := readShardingState(ms.store.SchemaReader(), "C1")
				require.Nil(t, err)
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

func TestStoreDBLoadProgressFields(t *testing.T) {
	tests := []struct {
		name     string
		progress func() *db.StartupProgressSnapshot
		want     logrus.Fields
	}{
		{
			name:     "no progress source returns nil",
			progress: nil,
			want:     nil,
		},
		{
			name:     "no shards to load returns nil",
			progress: func() *db.StartupProgressSnapshot { return &db.StartupProgressSnapshot{Loaded: 0, Total: 0} },
			want:     nil,
		},
		{
			name:     "negative total returns nil",
			progress: func() *db.StartupProgressSnapshot { return &db.StartupProgressSnapshot{Loaded: 0, Total: -1} },
			want:     nil,
		},
		{
			name:     "nothing loaded yet",
			progress: func() *db.StartupProgressSnapshot { return &db.StartupProgressSnapshot{Loaded: 0, Total: 10} },
			want:     logrus.Fields{"shards_loaded": int64(0), "shards_total": int64(10), "progress": "0%"},
		},
		{
			name:     "partial progress rounds to whole percent",
			progress: func() *db.StartupProgressSnapshot { return &db.StartupProgressSnapshot{Loaded: 1, Total: 3} },
			want:     logrus.Fields{"shards_loaded": int64(1), "shards_total": int64(3), "progress": "33%"},
		},
		{
			name:     "partial progress",
			progress: func() *db.StartupProgressSnapshot { return &db.StartupProgressSnapshot{Loaded: 3, Total: 10} },
			want:     logrus.Fields{"shards_loaded": int64(3), "shards_total": int64(10), "progress": "30%"},
		},
		{
			name:     "fully loaded",
			progress: func() *db.StartupProgressSnapshot { return &db.StartupProgressSnapshot{Loaded: 10, Total: 10} },
			want:     logrus.Fields{"shards_loaded": int64(10), "shards_total": int64(10), "progress": "100%"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &Store{cfg: Config{DBLoadProgress: tt.progress}}
			assert.Equal(t, tt.want, st.dbLoadProgressFields())
		})
	}
}

// TestStoreDBLoadProgressFieldsRecomputesPerCall pins that each call re-reads
// the progress source. The schema arrives from RAFT while the shards are still
// loading, so a value read once would describe the pre-schema state for the
// whole load.
func TestStoreDBLoadProgressFieldsRecomputesPerCall(t *testing.T) {
	var calls int64
	st := &Store{cfg: Config{DBLoadProgress: func() *db.StartupProgressSnapshot {
		calls++
		return &db.StartupProgressSnapshot{Loaded: calls, Total: 10}
	}}}

	assert.Equal(t, "10%", st.dbLoadProgressFields()["progress"])
	assert.Equal(t, "20%", st.dbLoadProgressFields()["progress"])
	assert.Equal(t, int64(2), calls)
}

// TestStoreReloadDBFromSchemaReportsProgressDuringReload pins that the reload is
// bracketed by the progress tracker. The snapshot-path reload runs inside
// raft.NewRaft, before WaitToRestoreDB starts its heartbeat, so a tracker
// started any later reports nothing for that whole phase.
func TestStoreReloadDBFromSchemaReportsProgressDuringReload(t *testing.T) {
	t.Parallel()

	ms := NewMockStore(t, t.Name(), 9092)
	logHook := logrustest.NewLocal(ms.logger)
	logged := func(msg string) bool {
		for _, e := range logHook.AllEntries() {
			if e.Message == msg {
				return true
			}
		}
		return false
	}

	ms.store.cfg.DBLoadProgress = func() *db.StartupProgressSnapshot {
		return &db.StartupProgressSnapshot{Loaded: 3, Total: 10}
	}

	st := ms.Store(func(m *MockStore) {
		// TriggerSchemaUpdateCallbacks runs inside the reload. Holding it open
		// until the tracker has logged proves the sampling happens while the
		// reload is in flight, not after.
		m.indexer.On("TriggerSchemaUpdateCallbacks").Run(func(mock.Arguments) {
			if !tryNTimesWithWait(3000, 10*time.Millisecond, func() bool {
				return logged("loading local DB from schema")
			}) {
				t.Error("no progress logged while the reload was in flight")
			}
		}).Return()
	})

	st.reloadDBFromSchema(loadNow)

	require.True(t, st.dbLoaded.Load())
	require.True(t, logged("local DB loaded from schema"))
	for _, e := range logHook.AllEntries() {
		if e.Message == "local DB loaded from schema" {
			assert.Equal(t, "30%", e.Data["progress"])
		}
	}
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
			NamespacesController:   usecasesNamespaces.NewController(logger),
			TelemetryEnabled:       true,
		},
		replicationFSM: schema.NewMockreplicationFSM(t),
	}

	s := NewFSM(ms.cfg, nil, prometheus.NewPedanticRegistry())
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

// TestStoreWaitToRestoreDBAnnouncesOnce pins that the wait is reported as a
// phase, not a heartbeat. Progress belongs to trackDBLoadProgress: on a restart
// into a live cluster the waiter is still waiting while the load runs, so a
// per-tick line here would report the same startup twice.
func TestStoreWaitToRestoreDBAnnouncesOnce(t *testing.T) {
	t.Parallel()

	ms := NewMockStore(t, t.Name(), utils.MustGetFreeTCPPort())
	logHook := logrustest.NewLocal(ms.logger)
	st := ms.store

	countMsg := func(msg string) int {
		n := 0
		for _, e := range logHook.AllEntries() {
			if e.Message == msg {
				n++
			}
		}
		return n
	}

	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(done)
		_ = st.WaitToRestoreDB(context.Background(), 10*time.Millisecond, make(chan struct{}))
	}, ms.logger)

	require.True(t, tryNTimesWithWait(200, 10*time.Millisecond, func() bool {
		return countMsg("waiting for database to be restored") > 0
	}), "the waiter must announce itself while the DB is not loaded")

	// Many further ticks pass at 10ms; none of them may add another line.
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 1, countMsg("waiting for database to be restored"),
		"announced once, not once per tick")

	st.dbLoaded.Store(true)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitToRestoreDB did not return once dbLoaded flipped")
	}
}

// TestStoreApplyDoesNotBlockOnShardLoad pins that the shard load no longer runs
// on raft's FSM goroutine. Inline, it applies nothing else for minutes to
// hours, configuration entries included, so on a cold start every bootstrap
// join queues behind the leader's own load until RAFT_BOOTSTRAP_TIMEOUT.
func TestStoreApplyDoesNotBlockOnShardLoad(t *testing.T) {
	t.Parallel()

	ms := NewMockStore(t, t.Name(), utils.MustGetFreeTCPPort())
	st := ms.store
	st.raft = &raft.Raft{} // non-nil: this is the Apply path, not Restore

	release := make(chan struct{})
	loading := make(chan struct{})
	var once sync.Once
	ms.indexer.On("TriggerSchemaUpdateCallbacks").Run(func(mock.Arguments) {
		once.Do(func() { close(loading) })
		<-release
	}).Return()

	returned := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(returned)
		st.dbLoad.start(st.reloadDBFromSchema, st.log)
	}, ms.logger)

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("reloadDBFromSchema blocked on the shard load; the FSM goroutine is still occupied")
	}

	// The load is genuinely still running, so this was a handover, not a no-op.
	select {
	case <-loading:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("the background load never started")
	}
	require.False(t, st.dbLoaded.Load(), "dbLoaded must not be set until the load finishes")

	close(release)
	require.True(t, tryNTimesWithWait(200, 10*time.Millisecond, st.dbLoaded.Load),
		"dbLoaded must be set once the load finishes")
}

// TestStoreApplyIsSchemaOnlyWhileLoading pins the barrier that replaces the
// serialisation the FSM goroutine used to give for free.
func TestStoreApplyIsSchemaOnlyWhileLoading(t *testing.T) {
	t.Parallel()

	cls := &models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	ss := &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{"T0": {Name: "T0"}}}

	addClass := func(index uint64) *raft.Log {
		return &raft.Log{
			Index: index,
			Type:  raft.LogCommand,
			Data: cmdAsBytes("C", cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss}, nil),
		}
	}

	newStore := func(t *testing.T) *MockStore {
		ms := NewMockStore(t, t.Name(), utils.MustGetFreeTCPPort())
		ms.parser.On("ParseClass", mock.Anything).Return(nil)
		ms.indexer.On("AddClass", mock.Anything).Return(nil)
		ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
		ms.indexer.On("Open", mock.Anything).Return(nil)
		return &ms
	}

	t.Run("nothing loading: the command reaches the DB", func(t *testing.T) {
		ms := newStore(t)
		ms.store.Apply(addClass(1))
		ms.indexer.AssertCalled(t, "AddClass", mock.Anything)
	})

	t.Run("loading: the command is applied schema-only", func(t *testing.T) {
		ms := newStore(t)
		require.True(t, ms.store.dbLoad.begin())

		ms.store.Apply(addClass(1))

		ms.indexer.AssertNotCalled(t, "AddClass", mock.Anything)

		// And the loader is told its snapshot is stale, so it reconciles the
		// command it just caused to be skipped.
		_, done := ms.store.dbLoad.finish()
		require.False(t, done, "a deferred write must leave the loader another pass")
	})
}

// TestStoreRestorePathStaysSynchronous pins that only the Apply call site moved
// off-thread. Restore runs inside raft.NewRaft, where the lastAppliedIndexToDB
// bookkeeping must be in place before it returns, and raft requires Restore not
// to overlap other commands.
func TestStoreRestorePathStaysSynchronous(t *testing.T) {
	source := NewMockStore(t, "restore-sync-source", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)
	snapshot, err := source.store.Snapshot()
	require.NoError(t, err)
	sink := &clustermocks.SnapshotSink{Buffer: bytes.NewBuffer(nil)}
	require.NoError(t, snapshot.Persist(sink))

	target := NewMockStore(t, "restore-sync-target", utils.MustGetFreeTCPPort())
	target.store.init()
	require.Nil(t, target.store.raft, "precondition: Restore runs before raft is constructed")
	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	target.indexer.On("RestoreClassDir", mock.Anything).Return(nil)
	target.indexer.On("UpdateShardStatus", mock.Anything).Return(nil)
	target.indexer.On("AddClass", mock.Anything).Return(nil)

	// Hold the load open at the point the real one is slow.
	loading, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	target.indexer.ReloadLocalDBHook = func() {
		once.Do(func() { close(loading) })
		<-release
	}

	restored := make(chan error, 1)
	enterrors.GoWrapper(func() {
		restored <- target.store.Restore(io.NopCloser(bytes.NewReader(sink.Buffer.Bytes())))
	}, target.logger)

	<-loading
	select {
	case <-restored:
		t.Fatal("Restore returned while the local DB was still loading; raft requires the restore not to overlap other commands, and the applied-index bookkeeping must land before raft.NewRaft returns")
	case <-time.After(250 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-restored)
	require.True(t, target.store.dbLoaded.Load(), "dbLoaded must be set before Restore returns")
}

// TestStoreDBLoadHandoverUnderStress hammers the handover to catch interleavings
// the targeted tests cannot reach. The invariant: a command applied schema-only
// is always followed by a pass.
func TestStoreDBLoadHandoverUnderStress(t *testing.T) {
	t.Parallel()

	const rounds = 2000

	for i := 0; i < rounds; i++ {
		var (
			st        = &Store{}
			deferred  atomic.Int64
			drained   atomic.Int64
			reconcile atomic.Int64
			start     = make(chan struct{})
			wg        sync.WaitGroup
		)
		require.True(t, st.dbLoad.begin(), "round %d: the load must start idle", i)

		// A pass, then the handover, repeating while commands keep arriving.
		// Mirrors the loop in reloadDBFromSchema.
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				deletes, done := st.dbLoad.finish()
				if done {
					return
				}
				drained.Add(int64(len(deletes)))
				reconcile.Add(1)
			}
		}()

		// Concurrent commands, each deleting a distinct class so a dropped
		// record is countable.
		for j := 0; j < 4; j++ {
			class := fmt.Sprintf("C%d", j)
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if st.dbLoad.deferWrite(class, false) {
					deferred.Add(1)
				}
			}()
		}

		close(start)
		wg.Wait()

		require.False(t, st.dbLoad.inFlight.Load(),
			"round %d: the loader must leave the load idle", i)
		if deferred.Load() > 0 {
			require.Positive(t, reconcile.Load(),
				"round %d: %d command(s) deferred a DB write with no reconcile pass; their effect would be lost",
				i, deferred.Load())
		}
		require.Equal(t, deferred.Load(), drained.Load(),
			"round %d: %d deferred delete(s) but %d reached a pass; the rest keep their shards on disk for good",
			i, deferred.Load(), drained.Load())
	}
}

// TestStoreDeferredDeleteClassReachesTheDB pins that a class deleted while the
// background load runs is removed from the DB, with everything the normal
// delete carries.
func TestStoreDeferredDeleteClassReachesTheDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tenant     sharding.Physical
		wantFrozen bool
	}{
		{name: "hot tenant", tenant: sharding.Physical{Name: "T0", Status: models.TenantActivityStatusHOT}},
		{name: "frozen tenant", tenant: sharding.Physical{Name: "T0", Status: models.TenantActivityStatusFROZEN}, wantFrozen: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ms := NewMockStore(t, t.Name(), utils.MustGetFreeTCPPort())
			st := ms.store
			st.raft = &raft.Raft{} // Apply path

			cls := &models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
			ss := &sharding.State{
				PartitioningEnabled: true,
				Physical:            map[string]sharding.Physical{test.tenant.Name: test.tenant},
			}

			ms.parser.On("ParseClass", mock.Anything).Return(nil)
			ms.indexer.On("Open", mock.Anything).Return(nil)
			ms.indexer.On("AddClass", mock.Anything).Return(nil)
			ms.indexer.On("DeleteClass", mock.Anything, mock.Anything).Return(nil)
			ms.replicationFSM.On("DeleteReplicationsByCollection", mock.Anything).Return(nil)

			// The class exists before the load starts.
			st.Apply(&raft.Log{Index: 1, Type: raft.LogCommand, Data: cmdAsBytes("C",
				cmd.ApplyRequest_TYPE_ADD_CLASS, cmd.AddClassRequest{Class: cls, State: ss}, nil)})

			// Only the first pass blocks: commands applied meanwhile trigger
			// this callback too, and blocking those would deadlock.
			release := make(chan struct{})
			loading := make(chan struct{})
			var first atomic.Bool
			ms.indexer.On("TriggerSchemaUpdateCallbacks").Run(func(mock.Arguments) {
				if first.CompareAndSwap(false, true) {
					close(loading)
					<-release
				}
			}).Return()

			st.dbLoad.start(st.reloadDBFromSchema, st.log)
			<-loading

			// Deleted mid-load: the schema drops it, the DB write is deferred.
			st.Apply(&raft.Log{Index: 2, Type: raft.LogCommand, Data: cmdAsBytes("C",
				cmd.ApplyRequest_TYPE_DELETE_CLASS, cmd.DeleteClassRequest{Name: "C"}, nil)})
			ms.indexer.AssertNotCalled(t, "DeleteClass", mock.Anything, mock.Anything)

			close(release)
			require.True(t, tryNTimesWithWait(200, 10*time.Millisecond, st.dbLoaded.Load),
				"the load must finish")

			ms.indexer.AssertCalled(t, "DeleteClass", "C", test.wantFrozen)
			ms.replicationFSM.AssertCalled(t, "DeleteReplicationsByCollection", "C")
		})
	}
}

// TestStoreIncompleteLoadStillGoesReady pins the deliberate choice: a load that
// could not open everything the schema names still reports ready, and says so
// on a counter.
func TestStoreIncompleteLoadStillGoesReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(st *Store)
	}{
		{name: "called directly", run: func(st *Store) { st.reloadDBFromSchema() }},
		{name: "started in the background", run: func(st *Store) {
			st.dbLoad.start(st.reloadDBFromSchema, st.log)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ms := NewMockStore(t, t.Name(), utils.MustGetFreeTCPPort())
			st := ms.store
			ms.indexer.ReloadLocalDBErr = fmt.Errorf("shard did not open")
			ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()

			test.run(st)

			require.True(t, tryNTimesWithWait(200, 10*time.Millisecond, st.dbLoaded.Load),
				"a partial load must still report ready")
			require.Equal(t, float64(1), testutil.ToFloat64(st.metrics.localDBLoadFailures),
				"a partial load must be counted; it is the only signal that the node's data is incomplete")
		})
	}
}
