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
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/cloud/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	errAny   = errors.New("any error")
	Anything = mock.Anything
)

func TestServiceEndpoints(t *testing.T) {
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	m.indexer.On("Open", Anything).Return(nil)
	m.indexer.On("Close", Anything).Return(nil)
	m.indexer.On("AddClass", Anything).Return(nil)
	m.indexer.On("UpdateClass", Anything).Return(nil)
	m.indexer.On("DeleteClass", Anything).Return(nil)
	m.indexer.On("AddProperty", Anything, Anything).Return(nil)
	m.indexer.On("UpdateShardStatus", Anything).Return(nil)
	m.indexer.On("AddTenants", Anything, Anything).Return(nil)
	m.indexer.On("UpdateTenants", Anything, Anything).Return(nil)
	m.indexer.On("DeleteTenants", Anything, Anything).Return(nil)

	m.parser.On("ParseClass", mock.Anything).Return(nil)
	m.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)

	srv := NewService(m.store, nil)

	// LeaderNotFound
	assert.ErrorIs(t, srv.Execute(&command.ApplyRequest{}), ErrLeaderNotFound)
	assert.ErrorIs(t, srv.Join(ctx, m.store.nodeID, addr, true), ErrLeaderNotFound)
	assert.ErrorIs(t, srv.Remove(ctx, m.store.nodeID), ErrLeaderNotFound)

	// Deadline exceeded while waiting for DB to be restored
	func() {
		ctx, cancel := context.WithTimeout(ctx, time.Millisecond*30)
		defer cancel()
		assert.ErrorIs(t, srv.WaitUntilDBRestored(ctx, 5*time.Millisecond), context.DeadlineExceeded)
	}()

	// Open
	defer srv.Close(ctx)
	assert.Nil(t, srv.Open(ctx, m.indexer))

	// node lose leadership after service call
	assert.ErrorIs(t, srv.store.Join(m.store.nodeID, addr, true), ErrNotLeader)
	assert.ErrorIs(t, srv.store.Remove(m.store.nodeID), ErrNotLeader)

	// Connect
	assert.Nil(t, srv.store.Notify(m.cfg.NodeID, addr))

	assert.Nil(t, srv.WaitUntilDBRestored(ctx, time.Second*1))
	assert.True(t, srv.Ready())
	for i := 0; i < 20; i++ {
		if srv.store.IsLeader() {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	assert.True(t, srv.store.IsLeader())
	schema := srv.SchemaReader()
	assert.Equal(t, schema.Len(), 0)

	// AddClass
	assert.ErrorIs(t, srv.AddClass(nil, nil), errBadRequest)
	assert.Equal(t, schema.ClassEqual("C"), "")

	cls := &models.Class{
		Class:              "C",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}
	ss := &sharding.State{Physical: map[string]sharding.Physical{"T0": {Name: "T0"}}}
	assert.Nil(t, srv.AddClass(cls, ss))
	assert.Equal(t, schema.ClassEqual("C"), "C")

	// UpdateClass
	info := ClassInfo{
		Exists:            true,
		MultiTenancy:      models.MultiTenancyConfig{Enabled: true},
		ReplicationFactor: 1,
		Tenants:           1,
	}
	assert.ErrorIs(t, srv.UpdateClass(nil, nil), errBadRequest)
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	cls.ReplicationConfig = &models.ReplicationConfig{Factor: 1}
	ss.Physical = map[string]sharding.Physical{"T0": {Name: "T0"}}
	assert.Nil(t, srv.UpdateClass(cls, nil))
	assert.Equal(t, info, schema.ClassInfo("C"))

	// DeleteClass
	assert.Nil(t, srv.DeleteClass("X"))
	assert.Nil(t, srv.DeleteClass("C"))
	assert.Equal(t, ClassInfo{}, schema.ClassInfo("C"))

	// RestoreClass
	assert.ErrorIs(t, srv.RestoreClass(nil, nil), errBadRequest)
	assert.Nil(t, srv.RestoreClass(cls, ss))
	assert.Equal(t, info, schema.ClassInfo("C"))

	// AddProperty
	assert.ErrorIs(t, srv.AddProperty("C", nil), errBadRequest)
	assert.ErrorIs(t, srv.AddProperty("", &models.Property{Name: "P1"}), errBadRequest)
	assert.Nil(t, srv.AddProperty("C", &models.Property{Name: "P1"}))
	info.Properties = 1
	assert.Equal(t, info, schema.ClassInfo("C"))

	// UpdateStatus
	assert.ErrorIs(t, srv.UpdateShardStatus("", "A", "ACTIVE"), errBadRequest)
	assert.ErrorIs(t, srv.UpdateShardStatus("C", "", "ACTIVE"), errBadRequest)
	assert.Nil(t, srv.UpdateShardStatus("C", "A", "ACTIVE"))

	// AddTenants
	assert.ErrorIs(t, srv.AddTenants("", &command.AddTenantsRequest{}), errBadRequest)
	assert.Nil(t, srv.AddTenants("C", &command.AddTenantsRequest{
		Tenants: []*command.Tenant{nil, {Name: "T2", Status: "S1"}, nil},
	}))
	info.Tenants += 1
	assert.Equal(t, schema.ClassInfo("C"), info)

	// UpdateTenants
	assert.ErrorIs(t, srv.UpdateTenants("", &command.UpdateTenantsRequest{}), errBadRequest)
	assert.Nil(t, srv.UpdateTenants("C", &command.UpdateTenantsRequest{Tenants: []*command.Tenant{{Name: "T2", Status: "S2"}}}))

	// DeleteTenants
	assert.ErrorIs(t, srv.DeleteTenants("", &command.DeleteTenantsRequest{}), errBadRequest)
	assert.Nil(t, srv.DeleteTenants("C", &command.DeleteTenantsRequest{Tenants: []string{"T0", "Tn"}}))
	info.Tenants -= 1
	assert.Equal(t, info, schema.ClassInfo("C"))
	assert.Equal(t, "S2", schema.CopyShardingState("C").Physical["T2"].Status)

	// Self Join
	assert.Nil(t, srv.Join(ctx, m.store.nodeID, addr, true))
	assert.True(t, srv.store.IsLeader())
	assert.Nil(t, srv.Join(ctx, m.store.nodeID, addr, false))
	assert.True(t, srv.store.IsLeader())
	assert.ErrorContains(t, srv.Remove(ctx, m.store.nodeID), "configuration")
	assert.True(t, srv.store.IsLeader())

	// Stats
	stats := srv.Stats()
	assert.Equal(t, "Leader", stats["state"])

	// create snapshot
	assert.Nil(t, srv.store.raft.Barrier(2*time.Second).Error())
	assert.Nil(t, srv.store.raft.Snapshot().Error())

	// restore from snapshot
	assert.Nil(t, srv.Close(ctx))
	srv.store.db.Schema.clear()
	assert.Nil(t, srv.Open(ctx, m.indexer))
	assert.Nil(t, srv.store.Notify(m.cfg.NodeID, addr))
	assert.Nil(t, srv.WaitUntilDBRestored(ctx, time.Second*1))
	assert.True(t, srv.Ready())
	for i := 0; i < 20; i++ {
		if srv.store.IsLeader() {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	assert.Equal(t, info, srv.store.db.Schema.ClassInfo("C"))
}

func TestServiceStoreInit(t *testing.T) {
	var (
		ctx   = context.Background()
		m     = NewMockStore(t, "Node-1", 9093)
		store = m.store
		addr  = fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	)

	// NotOpen
	assert.ErrorIs(t, store.Join(m.store.nodeID, addr, true), ErrNotOpen)
	assert.ErrorIs(t, store.Remove(m.store.nodeID), ErrNotOpen)
	assert.ErrorIs(t, store.Notify(m.store.nodeID, addr), ErrNotOpen)

	// Already Open
	store.open.Store(true)
	assert.Nil(t, store.Open(ctx))

	// notify non voter
	store.bootstrapExpect = 0
	assert.Nil(t, store.Notify("A", "localhost:123"))

	// not enough voter
	store.bootstrapExpect = 2
	assert.Nil(t, store.Notify("A", "localhost:123"))
}

func TestServicePanics(t *testing.T) {
	m := NewMockStore(t, "Node-1", 9091)

	// Assert Correct Response Type
	ret := m.store.Apply(&raft.Log{Type: raft.LogNoop})
	resp, ok := ret.(Response)
	assert.True(t, ok)
	assert.Equal(t, resp, Response{})

	// Unknown Command
	assert.Panics(t, func() { m.store.Apply(&raft.Log{}) })

	// Not a Valid Payload
	assert.Panics(t, func() { m.store.Apply(&raft.Log{Data: []byte("a")}) })

	// Cannot Open File Store
	m.indexer.On("Open", mock.Anything).Return(errAny)
	assert.Panics(t, func() { m.store.loadDatabase(context.TODO()) })
}

func TestStoreApply(t *testing.T) {
	doFirst := func(m *MockStore) {
		m.indexer.On("Open", mock.Anything).Return(nil)
		m.parser.On("ParseClass", mock.Anything).Return(nil)
		m.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)
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
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_CLASS,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "AddClass/StateIsNil",
			req: raft.Log{Data: cmdAsBytes("C2",
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
			req: raft.Log{Data: cmdAsBytes("C2",
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
			req: raft.Log{Data: cmdAsBytes("C1",
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
			resp: Response{Error: errSchema},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClass", mock.Anything).Return(nil)
				m.store.db.Schema.addClass(cls, ss)
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
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "UpdateClass/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_UPDATE_CLASS,
				cmd.UpdateClassRequest{Class: cls, State: nil},
				nil)},
			resp: Response{Error: errSchema},
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
			resp: Response{Error: errBadRequest},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.store.db.Schema.addClass(cls, ss)
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
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)
				m.store.db.Schema.addClass(cls, ss)
			},
		},
		{
			name: "DeleteClass/Success",
			req: raft.Log{Data: cmdAsBytes("C1",
				cmd.ApplyRequest_TYPE_DELETE_CLASS, nil,
				nil)},
			resp: Response{Error: nil},
			doBefore: func(m *MockStore) {
				m.indexer.On("Open", mock.Anything).Return(nil)
				m.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(mock.Anything, nil)
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
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "AddProperty/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
				cmd.AddPropertyRequest{Property: &models.Property{Name: "P1"}}, nil)},
			resp:     Response{Error: errSchema},
			doBefore: doFirst,
		},
		{
			name: "AddProperty/Nil",
			req: raft.Log{
				Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
					cmd.AddPropertyRequest{Property: nil}, nil),
			},
			resp: Response{Error: errBadRequest},
			doBefore: func(m *MockStore) {
				doFirst(m)
				m.store.db.Schema.addClass(cls, ss)
			},
		},
		{
			name: "AddProperty/Success",
			req: raft.Log{
				Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_PROPERTY,
					cmd.AddPropertyRequest{Property: &models.Property{Name: "P1"}}, nil),
			},
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
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
				nil, &cmd.AddTenantsRequest{})},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "UpdateShard/Success",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
				cmd.UpdateShardStatusRequest{Class: "C1"}, nil)},
			resp:     Response{Error: nil},
			doBefore: doFirst,
		},
		{
			name:     "AddTenant/Unmarshal",
			req:      raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, cmd.AddClassRequest{}, nil)},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "AddTenant/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, nil, &cmd.AddTenantsRequest{
				Tenants: []*command.Tenant{nil, {Name: "T1"}, nil},
			})},
			resp:     Response{Error: errSchema},
			doBefore: doFirst,
		},
		{
			name: "AddTenant/Success",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_ADD_TENANT, nil, &cmd.AddTenantsRequest{
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
			req:      raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT, cmd.AddClassRequest{}, nil)},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "UpdateTenant/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
				nil, &cmd.UpdateTenantsRequest{Tenants: []*command.Tenant{nil, {Name: "T1"}, nil}})},
			resp:     Response{Error: errSchema},
			doBefore: doFirst,
		},
		{
			name: "UpdateTenant/NoFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
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
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_UPDATE_TENANT,
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
			req:      raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT, cmd.AddClassRequest{}, nil)},
			resp:     Response{Error: errBadRequest},
			doBefore: doFirst,
		},
		{
			name: "DeleteTenant/ClassNotFound",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT,
				nil, &cmd.DeleteTenantsRequest{Tenants: []string{"T1", "T2"}})},
			resp:     Response{Error: errSchema},
			doBefore: doFirst,
		},
		{
			name: "DeleteTenant/Success",
			req: raft.Log{Data: cmdAsBytes("C1", cmd.ApplyRequest_TYPE_DELETE_TENANT,
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
		}
	}
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

type MockStore struct {
	indexer *MockIndexer
	parser  *MockParser
	logger  MockSLog
	cfg     Config
	store   *Store
}

func NewMockStore(t *testing.T, nodeID string, raftPort int) MockStore {
	indexer := &MockIndexer{}
	parser := &MockParser{}
	logger := NewMockSLog(t)
	ms := MockStore{
		indexer: indexer,
		parser:  parser,
		logger:  logger,

		cfg: Config{
			WorkDir:  t.TempDir(),
			NodeID:   nodeID,
			Host:     "localhost",
			RaftPort: raftPort,
			// RPCPort:           9092,
			BootstrapExpect:   1,
			HeartbeatTimeout:  1 * time.Second,
			ElectionTimeout:   1 * time.Second,
			RecoveryTimeout:   500 * time.Millisecond,
			SnapshotInterval:  2 * time.Second,
			SnapshotThreshold: 125,
			DB:                indexer,
			Parser:            parser,
			Logger:            logger.Logger,
		},
	}
	s := New(ms.cfg, NewMockCluster(nil))
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

func (m *MockParser) ParseClassUpdate(class, update *models.Class) (*models.Class, error) {
	args := m.Called(class)
	return update, args.Error(1)
}
