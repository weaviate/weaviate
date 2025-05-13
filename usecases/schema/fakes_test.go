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
	"context"
	"encoding/json"
	"fmt"

	"github.com/stretchr/testify/mock"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type fakeSchemaManager struct {
	mock.Mock
	countClassEqual bool
}

func (f *fakeSchemaManager) AddClass(_ context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	args := f.Called(cls, ss)
	return 0, args.Error(0)
}

func (f *fakeSchemaManager) RestoreClass(_ context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	args := f.Called(cls, ss)
	return 0, args.Error(0)
}

func (f *fakeSchemaManager) UpdateClass(_ context.Context, cls *models.Class, ss *sharding.State) (uint64, error) {
	args := f.Called(cls, ss)
	return 0, args.Error(0)
}

func (f *fakeSchemaManager) DeleteClass(_ context.Context, name string) (uint64, error) {
	args := f.Called(name)
	return 0, args.Error(0)
}

func (f *fakeSchemaManager) AddProperty(_ context.Context, class string, p ...*models.Property) (uint64, error) {
	args := f.Called(class, p)
	return 0, args.Error(0)
}

func (f *fakeSchemaManager) UpdateShardStatus(c_ context.Context, class, shard, status string) (uint64, error) {
	args := f.Called(class, shard, status)
	return 0, args.Error(0)
}

func (f *fakeSchemaManager) AddTenants(_ context.Context, class string, req *command.AddTenantsRequest) (uint64, error) {
	args := f.Called(class, req)
	return 0, args.Error(0)
}

func (f *fakeSchemaManager) UpdateTenants(_ context.Context, class string, req *command.UpdateTenantsRequest) (uint64, error) {
	args := f.Called(class, req)
	return 0, args.Error(0)
}

func (f *fakeSchemaManager) DeleteTenants(_ context.Context, class string, req *command.DeleteTenantsRequest) (uint64, error) {
	args := f.Called(class, req)
	return 0, args.Error(0)
}

func (f *fakeSchemaManager) Join(ctx context.Context, nodeID, raftAddr string, voter bool) error {
	args := f.Called(ctx, nodeID, raftAddr, voter)
	return args.Error(0)
}

func (f *fakeSchemaManager) Remove(ctx context.Context, nodeID string) error {
	args := f.Called(ctx, nodeID)
	return args.Error(0)
}

func (f *fakeSchemaManager) Stats() map[string]any {
	return map[string]any{}
}

func (f *fakeSchemaManager) StoreSchemaV1() error {
	return nil
}

func (f *fakeSchemaManager) ClassEqual(name string) string {
	if f.countClassEqual {
		args := f.Called(name)
		return args.String(0)
	}
	return ""
}

func (f *fakeSchemaManager) MultiTenancy(class string) models.MultiTenancyConfig {
	args := f.Called(class)
	return args.Get(0).(models.MultiTenancyConfig)
}

func (f *fakeSchemaManager) MultiTenancyWithVersion(ctx context.Context, class string, version uint64) (models.MultiTenancyConfig, error) {
	args := f.Called(ctx, class, version)
	return args.Get(0).(models.MultiTenancyConfig), args.Error(1)
}

func (f *fakeSchemaManager) ClassInfo(class string) (ci clusterSchema.ClassInfo) {
	args := f.Called(class)
	return args.Get(0).(clusterSchema.ClassInfo)
}

func (f *fakeSchemaManager) StorageCandidates() []string {
	return []string{"node-1"}
}

func (f *fakeSchemaManager) ClassInfoWithVersion(ctx context.Context, class string, version uint64) (clusterSchema.ClassInfo, error) {
	args := f.Called(ctx, class, version)
	return args.Get(0).(clusterSchema.ClassInfo), args.Error(1)
}

func (f *fakeSchemaManager) QuerySchema() (models.Schema, error) {
	args := f.Called()
	return args.Get(0).(models.Schema), args.Error(1)
}

func (f *fakeSchemaManager) QueryCollectionsCount() (int, error) {
	args := f.Called()
	return args.Get(0).(int), args.Error(1)
}

func (f *fakeSchemaManager) QueryReadOnlyClasses(classes ...string) (map[string]versioned.Class, error) {
	args := f.Called(classes)

	models := args.Get(0)
	if models == nil {
		return nil, args.Error(1)
	}

	return models.(map[string]versioned.Class), nil
}

func (f *fakeSchemaManager) QueryClassVersions(classes ...string) (map[string]uint64, error) {
	args := f.Called(classes)

	models := args.Get(0)
	if models == nil {
		return nil, args.Error(1)
	}

	return models.(map[string]uint64), nil
}

func (f *fakeSchemaManager) QueryTenants(class string, tenants []string) ([]*models.Tenant, uint64, error) {
	args := f.Called(class, tenants)
	return args.Get(0).([]*models.Tenant), 0, args.Error(2)
}

func (f *fakeSchemaManager) QueryShardOwner(class, shard string) (string, uint64, error) {
	args := f.Called(class, shard)
	return args.Get(0).(string), 0, args.Error(0)
}

func (f *fakeSchemaManager) QueryTenantsShards(class string, tenants ...string) (map[string]string, uint64, error) {
	args := f.Called(class, tenants)
	res := map[string]string{}
	for idx := range tenants {
		res[args.String(idx+1)] = ""
	}
	return res, 0, nil
}

func (f *fakeSchemaManager) QueryShardingState(class string) (*sharding.State, uint64, error) {
	args := f.Called(class)
	return args.Get(0).(*sharding.State), 0, args.Error(0)
}

func (f *fakeSchemaManager) ReadOnlyClass(class string) *models.Class {
	args := f.Called(class)
	model := args.Get(0)
	if model == nil {
		return nil
	}
	return model.(*models.Class)
}

func (f *fakeSchemaManager) ReadOnlyVersionedClass(class string) versioned.Class {
	args := f.Called(class)
	model := args.Get(0)
	return model.(versioned.Class)
}

func (f *fakeSchemaManager) ReadOnlyClassWithVersion(ctx context.Context, class string, version uint64) (*models.Class, error) {
	args := f.Called(ctx, class, version)
	model := args.Get(0)
	if model == nil {
		return nil, args.Error(1)
	}
	return model.(*models.Class), args.Error(1)
}

func (f *fakeSchemaManager) ReadOnlySchema() models.Schema {
	args := f.Called()
	return args.Get(0).(models.Schema)
}

func (f *fakeSchemaManager) CopyShardingState(class string) *sharding.State {
	args := f.Called(class)
	return args.Get(0).(*sharding.State)
}

func (f *fakeSchemaManager) CopyShardingStateWithVersion(ctx context.Context, class string, version uint64) (*sharding.State, error) {
	args := f.Called(ctx, class, version)
	return args.Get(0).(*sharding.State), args.Error(1)
}

func (f *fakeSchemaManager) ShardReplicas(class, shard string) ([]string, error) {
	args := f.Called(class, shard)
	return args.Get(0).([]string), args.Error(1)
}

func (f *fakeSchemaManager) ShardReplicasWithVersion(ctx context.Context, class, shard string, version uint64) ([]string, error) {
	args := f.Called(ctx, class, shard, version)
	return args.Get(0).([]string), args.Error(1)
}

func (f *fakeSchemaManager) ShardFromUUID(class string, uuid []byte) string {
	args := f.Called(class, uuid)
	return args.String(0)
}

func (f *fakeSchemaManager) ShardFromUUIDWithVersion(ctx context.Context, class string, uuid []byte, version uint64) (string, error) {
	args := f.Called(ctx, class, uuid, version)
	return args.String(0), args.Error(1)
}

func (f *fakeSchemaManager) ShardOwner(class, shard string) (string, error) {
	args := f.Called(class, shard)
	return args.String(0), args.Error(1)
}

func (f *fakeSchemaManager) ShardOwnerWithVersion(ctx context.Context, class, shard string, version uint64) (string, error) {
	args := f.Called(ctx, class, shard, version)
	return args.String(0), args.Error(1)
}

func (f *fakeSchemaManager) TenantsShardsWithVersion(ctx context.Context, version uint64, class string, tenants ...string) (tenantShards map[string]string, err error) {
	args := f.Called(ctx, version, class, tenants)
	return map[string]string{args.String(0): args.String(1)}, args.Error(2)
}

func (f *fakeSchemaManager) Read(class string, reader func(*models.Class, *sharding.State) error) error {
	args := f.Called(class, reader)
	return args.Error(0)
}

func (f *fakeSchemaManager) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	args := f.Called(class, tenant)
	return args.Get(0).(models.ShardStatusList), args.Error(1)
}

func (f *fakeSchemaManager) WaitForUpdate(ctx context.Context, schemaVersion uint64) error {
	return nil
}

type fakeStore struct {
	collections map[string]*models.Class
	parser      Parser
}

func NewFakeStore() *fakeStore {
	return &fakeStore{
		collections: make(map[string]*models.Class),
		parser:      *NewParser(fakes.NewFakeClusterState(), dummyParseVectorConfig, &fakeValidator{}, fakeModulesProvider{}),
	}
}

func (f *fakeStore) AddClass(cls *models.Class) {
	f.collections[cls.Class] = cls
}

func (f *fakeStore) UpdateClass(cls *models.Class) error {
	bytes, err := json.Marshal(cls)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	cls = f.collections[cls.Class]
	if cls == nil {
		return ErrNotFound
	}

	cls2 := &models.Class{}
	if err := json.Unmarshal(bytes, cls2); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	u, err := f.parser.ParseClassUpdate(cls, cls2)
	if err != nil {
		return fmt.Errorf("parse class update: %w", err)
	}

	cls.VectorIndexConfig = u.VectorIndexConfig
	cls.InvertedIndexConfig = u.InvertedIndexConfig
	return nil
}
