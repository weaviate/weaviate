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

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var errAny = errors.New("any error")

func TestStoreApplyPanics(t *testing.T) {
	var (
		mIDX    = &MockIndexer{}
		mParser = &MockParser{}
		mLogger = NewMockSLog(t)
	)
	cfg := Config{
		DB:     mIDX,
		Parser: mParser,
		Logger: mLogger.Logger,
		NodeID: "THIS",
	}
	store := New(cfg)
	ret := store.Apply(&raft.Log{Type: raft.LogNoop})
	resp, ok := ret.(Response)
	assert.True(t, ok)
	assert.Equal(t, resp, Response{})

	assert.Panics(t, func() { store.Apply(&raft.Log{}) }, "unknown command")
	assert.Panics(t, func() { store.Apply(&raft.Log{Data: []byte("a")}) }, "command payload")
}

func TestStoreApply(t *testing.T) {
	doFirst := func(m *MockStore) {
		m.indexer.On("Open", mock.Anything).Return(nil)
		m.parser.On("ParseClass", mock.Anything).Return(nil)
	}

	cls := &models.Class{Class: "C1"}
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
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_ADD_CLASS,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "AddClass/StateIsNil",
			req: raft.Log{Data: mustCommand("C2",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: nil},
				nil)},
			resp: Response{Error: errBadRequest},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
			},
		},
		{
			name: "AddClass/ParseClass",
			req: raft.Log{Data: mustCommand("C2",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss},
				nil)},
			resp: Response{Error: errBadRequest},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(errAny)
			},
		},
		{
			name: "AddClass/Success",
			req: raft.Log{Data: mustCommand("C1",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss},
				nil)},
			resp:     Response{Error: nil},
			doBefore: doFirst,
			doAfter: func(ms *MockStore) error {
				_, ok := ms.store.db.Schema.Classes["C1"]
				if !ok {
					return fmt.Errorf("class is missing")
				}
				return nil
			},
		},
		{
			name: "AddClass/AlreadyExists",
			req: raft.Log{Data: mustCommand("C1",
				cmd.ApplyRequest_TYPE_ADD_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss},
				nil)},
			resp: Response{Error: errSchema},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.store.db.Schema.addClass(cls, ss)
			},
		},
		{
			name: "RestoreClass/Success",
			req: raft.Log{Data: mustCommand("C1",
				cmd.ApplyRequest_TYPE_RESTORE_CLASS,
				cmd.AddClassRequest{Class: cls, State: ss},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.store.db.Schema.addClass(cls, ss)
			},
			doAfter: func(ms *MockStore) error {
				_, ok := ms.store.db.Schema.Classes["C1"]
				if !ok {
					return fmt.Errorf("class is missing")
				}
				return nil
			},
		},
		{
			name: "UpdateClass/Unmarshal",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "UpdateClass/ParseClass",
			req: raft.Log{Data: mustCommand("C2",
				cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				cmd.UpdateClassRequest{Class: cls, State: ss},
				nil)},
			resp: Response{Error: errBadRequest},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(errAny)
			},
		},
		{
			name: "UpdateClass/ClassNotFound",
			req: raft.Log{Data: mustCommand("C1",
				cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				cmd.UpdateClassRequest{Class: cls, State: nil},
				nil)},
			resp: Response{Error: errSchema},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(nil)
			},
		},
		{
			name: "UpdateClass/Success",
			req: raft.Log{Data: mustCommand("C1",
				cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				cmd.UpdateClassRequest{Class: cls, State: nil},
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.store.db.Schema.addClass(cls, ss)
			},
		},
		{
			name: "DeleteClass/Success",
			req: raft.Log{Data: mustCommand("C1",
				cmd.ApplyRequest_TYPE_DELETE_CLASS, nil,
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(nil)
			},
			doAfter: func(ms *MockStore) error {
				if _, ok := ms.store.db.Schema.Classes["C1"]; ok {
					return fmt.Errorf("class still exits")
				}
				return nil
			},
		},
		{
			name: "AddProperty/Unmarshal",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "AddProperty/ClassNotFound",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
				cmd.AddPropertyRequest{Property: &models.Property{Name: "P1"}}, nil)},
			resp:     Response{Error: errSchema},
			doBefore: doFirst,
		},
		{
			name: "AddProperty/Success",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
				cmd.AddPropertyRequest{Property: &models.Property{Name: "P1"}}, nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.store.db.Schema.addClass(cls, ss)
			},
			doAfter: func(ms *MockStore) error {
				ok := false
				for _, p := range ms.store.db.Schema.Classes["C1"].Class.Properties {
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
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "UpdateShard/Success",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
				cmd.UpdateShardStatusRequest{Class: "C1"}, nil)},
			resp:     Response{Error: nil},
			doBefore: doFirst,
		},
		{
			name:     "AddTenant/Unmarshal",
			req:      raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, cmd.AddClassRequest{}, nil)},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "AddTenant/ClassNotFound",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, nil, &cmd.AddTenantsRequest{
				Tenants: []*command.Tenant{nil, {Name: "T1"}, nil},
			})},
			resp:     Response{Error: errSchema},
			doBefore: doFirst,
		},
		{
			name: "AddTenant/Success",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, nil, &cmd.AddTenantsRequest{
				Tenants: []*command.Tenant{nil, {Name: "T1"}, nil},
			})},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.store.db.Schema.addClass(cls, &sharding.State{
					Physical: map[string]sharding.Physical{"T1": {}},
				})
			},
			doAfter: func(ms *MockStore) error {
				if _, ok := ms.store.db.Schema.Classes["C1"].Sharding.Physical["T1"]; !ok {
					return fmt.Errorf("tenant is missing")
				}
				return nil
			},
		},
		{
			name:     "UpdateTenant/Unmarshal",
			req:      raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT, cmd.AddClassRequest{}, nil)},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "UpdateTenant/ClassNotFound",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*command.Tenant{nil, {Name: "T1"}, nil}})},
			resp:     Response{Error: errSchema},
			doBefore: doFirst,
		},
		{
			name: "UpdateTenant/NoFound",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*command.Tenant{
					{Name: "T1", Status: models.TenantActivityStatusCOLD, Nodes: []string{"THIS"}},
				}})},
			resp: Response{Error: errSchema},
			doBefore: func(m *MockStore) {
				ss := &sharding.State{Physical: map[string]sharding.Physical{}}
				doFirst(m)
				m.store.db.Schema.addClass(cls, ss)
			},
		},
		{
			name: "UpdateTenant/Success",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*command.Tenant{
					{Name: "T1", Status: models.TenantActivityStatusCOLD, Nodes: []string{"THIS"}},
					{Name: "T2", Status: models.TenantActivityStatusCOLD, Nodes: []string{"THIS"}},
					{Name: "T3", Status: models.TenantActivityStatusCOLD, Nodes: []string{"NODE-2"}},
				}})},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
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
				doFirst(m)
				m.store.db.Schema.addClass(cls, ss)
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
				cls := ms.store.db.Schema.Classes["C1"]
				if got := cls.Sharding.Physical; !reflect.DeepEqual(got, want) {
					return fmt.Errorf("physical state want: %v got: %v", want, got)
				}
				return nil
			},
		},
		{
			name:     "DeleteTenant/Unmarshal",
			req:      raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT, cmd.AddClassRequest{}, nil)},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "DeleteTenant/ClassNotFound",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT,
				nil, &cmd.DeleteTenantsRequest{Tenants: []string{"T1", "T2"}})},
			resp:     Response{Error: errSchema},
			doBefore: doFirst,
		},
		{
			name: "DeleteTenant/Success",
			req: raft.Log{Data: mustCommand("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT,
				nil, &cmd.DeleteTenantsRequest{Tenants: []string{"T1", "T2"}})},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.store.db.Schema.addClass(cls, &sharding.State{Physical: map[string]sharding.Physical{"T1": {}}})
			},
			doAfter: func(ms *MockStore) error {
				if len(ms.store.db.Schema.Classes["C1"].Sharding.Physical) != 0 {
					return fmt.Errorf("sharding state mus be empty after deletion")
				}
				return nil
			},
		},
	}

	for _, tc := range tests {
		m := NewMockStore(t)
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
		}
	}
}

func mustCommand(class string,
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

type MockStore struct {
	indexer *MockIndexer
	parser  *MockParser
	logger  MockSLog
	cfg     Config
	store   *Store
}

func NewMockStore(t *testing.T) MockStore {
	indexer := &MockIndexer{}
	parser := &MockParser{}
	logger := NewMockSLog(t)
	ms := MockStore{
		indexer: indexer,
		parser:  parser,
		logger:  logger,
		cfg: Config{
			DB:     indexer,
			Parser: parser,
			Logger: logger.Logger,
			NodeID: "THIS",
		},
	}
	s := New(ms.cfg)
	ms.store = &s
	return ms
}

func (m *MockStore) Store(doBefore func(*MockStore)) *Store {
	if doBefore != nil {
		doBefore(m)
	}
	return m.store
}

type MockSLog struct {
	buf    *bytes.Buffer
	Logger *slog.Logger
}

func NewMockSLog(t *testing.T) MockSLog {
	buf := new(bytes.Buffer)
	m := MockSLog{
		buf: buf,
	}
	m.Logger = slog.New(slog.NewJSONHandler(buf, nil))
	return m
}

type MockIndexer struct {
	mock.Mock
}

func (m *MockIndexer) AddClass(req cmd.AddClassRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *MockIndexer) UpdateClass(req cmd.UpdateClassRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *MockIndexer) DeleteClass(name string) error {
	args := m.Called(name)
	return args.Error(0)
}

func (m *MockIndexer) AddProperty(class string, req cmd.AddPropertyRequest) error {
	args := m.Called(class, req)
	return args.Error(0)
}

func (m *MockIndexer) AddTenants(class string, req *cmd.AddTenantsRequest) error {
	args := m.Called(class, req)
	return args.Error(0)
}

func (m *MockIndexer) UpdateTenants(class string, req *cmd.UpdateTenantsRequest) error {
	args := m.Called(class, req)
	return args.Error(0)
}

func (m *MockIndexer) DeleteTenants(class string, req *cmd.DeleteTenantsRequest) error {
	args := m.Called(class, req)
	return args.Error(0)
}

func (m *MockIndexer) UpdateShardStatus(req *cmd.UpdateShardStatusRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *MockIndexer) GetShardsStatus(class string) (models.ShardStatusList, error) {
	args := m.Called(class)
	return models.ShardStatusList{}, args.Error(1)
}

func (m *MockIndexer) Open(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockIndexer) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

type MockParser struct {
	mock.Mock
}

func (m *MockParser) ParseClass(class *models.Class) error {
	args := m.Called(class)
	return args.Error(0)
}
