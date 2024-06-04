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
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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
			name: "DeleteClass/Success",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_DELETE_CLASS, nil,
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.indexer.On("DeleteClass", mock.Anything).Return(nil)
				m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
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
				Tenants: []*command.Tenant{nil, {Name: "T1"}, nil},
			})},
			resp:     Response{Error: schema.ErrSchema},
			doBefore: doFirst,
		},
		{
			name: "AddTenant/Success",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, nil, &cmd.AddTenantsRequest{
				ClusterNodes: []string{"THIS"},
				Tenants:      []*command.Tenant{nil, {Name: "T1"}, nil},
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
				nil, &cmd.UpdateTenantsRequest{Tenants: []*command.Tenant{nil, {Name: "T1"}, nil}})},
			resp:     Response{Error: schema.ErrSchema},
			doBefore: doFirst,
		},
		{
			name: "UpdateTenant/NoFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*command.Tenant{
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
			name: "UpdateTenant/Success",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*command.Tenant{
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
			resp:     Response{Error: schema.ErrSchema},
			doBefore: doFirst,
		},
		{
			name: "DeleteTenant/Success",
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
			},
			doAfter: func(ms *MockStore) error {
				shardingState := ms.store.SchemaReader().CopyShardingState("C1")
				if len(shardingState.Physical) != 0 {
					return fmt.Errorf("sharding state mus be empty after deletion")
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
			}
		})
	}
}

type MockStore struct {
	indexer *fakes.MockSchemaExecutor
	parser  *fakes.MockParser
	logger  *logrus.Logger
	cfg     Config
	store   *Store
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
			NodeToAddressResolver:  fakes.NewMockAddressResolver(nil),
			Logger:                 logger,
			ConsistencyWaitTimeout: time.Millisecond * 50,
		},
	}
	s := NewFSM(ms.cfg)
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

	cmd := command.ApplyRequest{
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
