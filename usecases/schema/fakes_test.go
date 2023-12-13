//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"

	"github.com/stretchr/testify/mock"
	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/cloud/store"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type fakeMetaHandler struct {
	mock.Mock
	countClassEqual bool
}

func (f *fakeMetaHandler) AddClass(cls *models.Class, ss *sharding.State) error {
	args := f.Called(cls, ss)
	return args.Error(0)
}

func (f *fakeMetaHandler) RestoreClass(cls *models.Class, ss *sharding.State) error {
	args := f.Called(cls, ss)
	return args.Error(0)
}

func (f *fakeMetaHandler) UpdateClass(cls *models.Class, ss *sharding.State) error {
	args := f.Called(cls, ss)
	return args.Error(0)
}

func (f *fakeMetaHandler) DeleteClass(name string) error {
	args := f.Called(name)
	return args.Error(0)
}

func (f *fakeMetaHandler) AddProperty(class string, p *models.Property) error {
	args := f.Called(class, p)
	return args.Error(0)
}

func (f *fakeMetaHandler) UpdateShardStatus(class, shard, status string) error {
	args := f.Called(class, shard, status)
	return args.Error(0)
}

func (f *fakeMetaHandler) AddTenants(class string, req *command.AddTenantsRequest) error {
	args := f.Called(class, req)
	return args.Error(0)
}

func (f *fakeMetaHandler) UpdateTenants(class string, req *command.UpdateTenantsRequest) error {
	args := f.Called(class, req)
	return args.Error(0)
}

func (f *fakeMetaHandler) DeleteTenants(class string, req *command.DeleteTenantsRequest) error {
	args := f.Called(class, req)
	return args.Error(0)
}

func (f *fakeMetaHandler) Join(ctx context.Context, nodeID, raftAddr string, voter bool) error {
	args := f.Called(ctx, nodeID, raftAddr, voter)
	return args.Error(0)
}

func (f *fakeMetaHandler) Remove(ctx context.Context, nodeID string) error {
	args := f.Called(ctx, nodeID)
	return args.Error(0)
}

func (f *fakeMetaHandler) Stats() map[string]string {
	return map[string]string{}
}

func (f *fakeMetaHandler) ClassEqual(name string) string {
	if f.countClassEqual {
		args := f.Called(name)
		return args.String(0)
	}
	return ""
}

func (f *fakeMetaHandler) MultiTenancy(class string) bool {
	args := f.Called(class)
	return args.Bool(0)
}

func (f *fakeMetaHandler) ClassInfo(class string) (ci store.ClassInfo) {
	args := f.Called(class)
	return args.Get(0).(store.ClassInfo)
}

func (f *fakeMetaHandler) ReadOnlyClass(class string) *models.Class {
	args := f.Called(class)
	model := args.Get(0)
	if model == nil {
		return nil
	}
	return model.(*models.Class)
}

func (f *fakeMetaHandler) ReadOnlySchema() models.Schema {
	args := f.Called()
	return args.Get(0).(models.Schema)
}

func (f *fakeMetaHandler) CopyShardingState(class string) *sharding.State {
	args := f.Called(class)
	return args.Get(0).(*sharding.State)
}

func (f *fakeMetaHandler) ShardReplicas(class, shard string) ([]string, error) {
	args := f.Called(class, shard)
	return args.Get(0).([]string), args.Error(1)
}

func (f *fakeMetaHandler) ShardFromUUID(class string, uuid []byte) string {
	args := f.Called(class, uuid)
	return args.String(0)
}

func (f *fakeMetaHandler) ShardOwner(class, shard string) (string, error) {
	args := f.Called(class, shard)
	return args.String(0), args.Error(1)
}

func (f *fakeMetaHandler) TenantShard(class, tenant string) (string, string) {
	args := f.Called(class, tenant)
	return args.String(0), args.String(1)
}

func (f *fakeMetaHandler) Read(class string, reader func(*models.Class, *sharding.State) error) error {
	args := f.Called(class, reader)
	return args.Error(0)
}

func (f *fakeMetaHandler) GetShardsStatus(class string) (models.ShardStatusList, error) {
	args := f.Called(class)
	return args.Get(0).(models.ShardStatusList), args.Error(1)
}
