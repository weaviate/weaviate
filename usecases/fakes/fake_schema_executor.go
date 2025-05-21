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

package fakes

import (
	"context"

	"github.com/stretchr/testify/mock"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
)

type MockSchemaExecutor struct {
	mock.Mock
}

func NewMockSchemaExecutor() *MockSchemaExecutor {
	return &MockSchemaExecutor{}
}

func (m *MockSchemaExecutor) AddClass(req cmd.AddClassRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *MockSchemaExecutor) RestoreClassDir(class string) error {
	args := m.Called(class)
	return args.Error(0)
}

func (m *MockSchemaExecutor) UpdateClass(req cmd.UpdateClassRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *MockSchemaExecutor) AddReplicaToShard(class string, shard string, targetNode string) error {
	args := m.Called(class, shard, targetNode)
	return args.Error(0)
}

func (m *MockSchemaExecutor) DeleteReplicaFromShard(class string, shard string, targetNode string) error {
	args := m.Called(class, shard, targetNode)
	return args.Error(0)
}

func (m *MockSchemaExecutor) LoadShard(class string, shard string) {
	m.Called(class, shard)
}

func (m *MockSchemaExecutor) DropShard(class string, shard string) {
	m.Called(class, shard)
}

func (m *MockSchemaExecutor) ShutdownShard(class string, shard string) {
	m.Called(class, shard)
}

func (m *MockSchemaExecutor) UpdateIndex(req cmd.UpdateClassRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *MockSchemaExecutor) ReloadLocalDB(ctx context.Context, all []cmd.UpdateClassRequest) error {
	return nil
}

func (m *MockSchemaExecutor) DeleteClass(name string, hasFrozen bool) error {
	args := m.Called(name)
	return args.Error(0)
}

func (m *MockSchemaExecutor) AddProperty(class string, req cmd.AddPropertyRequest) error {
	args := m.Called(class, req)
	return args.Error(0)
}

func (m *MockSchemaExecutor) AddTenants(class string, req *cmd.AddTenantsRequest) error {
	args := m.Called(class, req)
	return args.Error(0)
}

func (m *MockSchemaExecutor) UpdateTenants(class string, req *cmd.UpdateTenantsRequest) error {
	args := m.Called(class, req)
	return args.Error(0)
}

func (m *MockSchemaExecutor) UpdateTenantsProcess(class string, req *cmd.TenantProcessRequest) error {
	args := m.Called(class, req)
	return args.Error(0)
}

func (m *MockSchemaExecutor) DeleteTenants(class string, tenants []*models.Tenant) error {
	args := m.Called(class, tenants)
	return args.Error(0)
}

func (m *MockSchemaExecutor) UpdateShardStatus(req *cmd.UpdateShardStatusRequest) error {
	args := m.Called(req)
	return args.Error(0)
}

func (m *MockSchemaExecutor) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	args := m.Called(class, tenant)
	return models.ShardStatusList{}, args.Error(1)
}

func (m *MockSchemaExecutor) Open(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockSchemaExecutor) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockSchemaExecutor) TriggerSchemaUpdateCallbacks() {
	m.Called()
}
