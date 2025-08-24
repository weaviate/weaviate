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
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/scaler"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

func newTestHandler(t *testing.T, db clusterSchema.Indexer) (*Handler, *fakeSchemaManager) {
	schemaManager := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{"text2vec-contextionary", "model1", "model2"},
	}
	cfg := config.Config{
		DefaultVectorizerModule:     config.VectorizerModuleNone,
		DefaultVectorDistanceMetric: "cosine",
	}
	fakeClusterState := fakes.NewFakeClusterState()
	fakeValidator := &fakeValidator{}
	schemaParser := NewParser(fakeClusterState, dummyParseVectorConfig, fakeValidator, fakeModulesProvider{})
	handler, err := NewHandler(
		schemaManager, schemaManager, fakeValidator, logger, mocks.NewMockAuthorizer(),
		&cfg.SchemaHandlerConfig, cfg, dummyParseVectorConfig, vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, fakeClusterState, &fakeScaleOutManager{}, nil, *schemaParser, nil)
	require.NoError(t, err)
	handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
	return &handler, schemaManager
}

func newTestHandlerWithCustomAuthorizer(t *testing.T, db clusterSchema.Indexer, authorizer authorization.Authorizer) (*Handler, *fakeSchemaManager) {
	cfg := config.Config{}
	metaHandler := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{
			"model1", "model2",
		},
	}
	fakeClusterState := fakes.NewFakeClusterState()
	fakeValidator := &fakeValidator{}
	schemaParser := NewParser(fakeClusterState, dummyParseVectorConfig, fakeValidator, nil)
	handler, err := NewHandler(
		metaHandler, metaHandler, fakeValidator, logger, authorizer,
		&cfg.SchemaHandlerConfig, cfg, dummyParseVectorConfig, vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, fakeClusterState, &fakeScaleOutManager{}, nil, *schemaParser, nil)
	require.Nil(t, err)
	return &handler, metaHandler
}

type fakeDB struct {
	mock.Mock
}

func (f *fakeDB) Open(context.Context) error {
	return nil
}

func (f *fakeDB) Close(context.Context) error {
	return nil
}

func (f *fakeDB) AddClass(cmd command.AddClassRequest) error {
	return nil
}

func (f *fakeDB) RestoreClassDir(class string) error {
	return nil
}

func (f *fakeDB) AddReplicaToShard(class string, shard string, targetNode string) error {
	return nil
}

func (f *fakeDB) DeleteReplicaFromShard(class string, shard string, targetNode string) error {
	return nil
}

func (f *fakeDB) LoadShard(class string, shard string) {
}

func (f *fakeDB) DropShard(class string, shard string) {
}

func (f *fakeDB) ShutdownShard(class string, shard string) {
}

func (f *fakeDB) UpdateClass(cmd command.UpdateClassRequest) error {
	return nil
}

func (f *fakeDB) UpdateIndex(cmd command.UpdateClassRequest) error {
	return nil
}

func (f *fakeDB) ReloadLocalDB(ctx context.Context, all []command.UpdateClassRequest) error {
	return nil
}

func (f *fakeDB) DeleteClass(class string, hasFrozen bool) error {
	return nil
}

func (f *fakeDB) AddProperty(prop string, cmd command.AddPropertyRequest) error {
	return nil
}

func (f *fakeDB) AddTenants(class string, cmd *command.AddTenantsRequest) error {
	return nil
}

func (f *fakeDB) UpdateTenants(class string, cmd *command.UpdateTenantsRequest) error {
	return nil
}

func (f *fakeDB) UpdateTenantsProcess(class string, req *command.TenantProcessRequest) error {
	return nil
}

func (f *fakeDB) DeleteTenants(class string, tenants []*models.Tenant) error {
	return nil
}

func (f *fakeDB) UpdateShardStatus(cmd *command.UpdateShardStatusRequest) error {
	return nil
}

func (f *fakeDB) GetShardsStatus(class, tenant string) (models.ShardStatusList, error) {
	args := f.Called(class, tenant)
	return args.Get(0).(models.ShardStatusList), nil
}

func (f *fakeDB) TriggerSchemaUpdateCallbacks() {
	f.Called()
}

type fakeScaleOutManager struct{}

func (f *fakeScaleOutManager) Scale(ctx context.Context,
	className string, updated shardingConfig.Config, _, _ int64,
) (*sharding.State, error) {
	return nil, nil
}

func (f *fakeScaleOutManager) SetSchemaReader(sr scaler.SchemaReader) {
}

type fakeValidator struct{}

func (f fakeValidator) ValidateVectorIndexConfigUpdate(
	old, updated schemaConfig.VectorIndexConfig,
) error {
	return nil
}

func (f fakeValidator) ValidateInvertedIndexConfigUpdate(
	old, updated *models.InvertedIndexConfig,
) error {
	return nil
}

func (fakeValidator) ValidateVectorIndexConfigsUpdate(old, updated map[string]schemaConfig.VectorIndexConfig,
) error {
	return nil
}

type fakeModuleConfig struct{}

func (f *fakeModuleConfig) SetClassDefaults(class *models.Class) {
	defaultConfig := map[string]interface{}{
		"my-module1": map[string]interface{}{
			"my-setting": "default-value",
		},
	}

	asMap, ok := class.ModuleConfig.(map[string]interface{})
	if !ok {
		class.ModuleConfig = defaultConfig
		return
	}

	module, ok := asMap["my-module1"]
	if !ok {
		class.ModuleConfig = defaultConfig
		return
	}

	asMap, ok = module.(map[string]interface{})
	if !ok {
		class.ModuleConfig = defaultConfig
		return
	}

	if _, ok := asMap["my-setting"]; !ok {
		asMap["my-setting"] = "default-value"
		defaultConfig["my-module1"] = asMap
		class.ModuleConfig = defaultConfig
	}
}

func (f *fakeModuleConfig) SetSinglePropertyDefaults(class *models.Class,
	prop ...*models.Property,
) {
}

func (f *fakeModuleConfig) ValidateClass(ctx context.Context, class *models.Class) error {
	return nil
}

func (f *fakeModuleConfig) GetByName(name string) modulecapabilities.Module {
	return nil
}

func (f *fakeModuleConfig) IsGenerative(moduleName string) bool {
	return strings.Contains(moduleName, "generative")
}

func (f *fakeModuleConfig) IsReranker(moduleName string) bool {
	return strings.Contains(moduleName, "reranker")
}

func (f *fakeModuleConfig) IsMultiVector(moduleName string) bool {
	return strings.Contains(moduleName, "colbert")
}

type fakeVectorizerValidator struct {
	valid []string
}

func (f *fakeVectorizerValidator) ValidateVectorizer(moduleName string) error {
	for _, valid := range f.valid {
		if moduleName == valid {
			return nil
		}
	}

	return fmt.Errorf("invalid vectorizer %q", moduleName)
}

type fakeVectorConfig struct {
	raw interface{}
}

func (f fakeVectorConfig) IndexType() string {
	return "fake"
}

func (f fakeVectorConfig) DistanceName() string {
	return common.DistanceCosine
}

func (f fakeVectorConfig) IsMultiVector() bool {
	return false
}

func dummyParseVectorConfig(in interface{}, vectorIndexType string, isMultiVector bool) (schemaConfig.VectorIndexConfig, error) {
	return fakeVectorConfig{raw: in}, nil
}

func dummyValidateInvertedConfig(in *models.InvertedIndexConfig) error {
	return nil
}

type fakeMigrator struct {
	mock.Mock
}

func (f *fakeMigrator) GetShardsQueueSize(ctx context.Context, className, tenant string) (map[string]int64, error) {
	return nil, nil
}

func (f *fakeMigrator) AddClass(ctx context.Context, cls *models.Class, ss *sharding.State) error {
	args := f.Called(ctx, cls, ss)
	return args.Error(0)
}

func (f *fakeMigrator) DropClass(ctx context.Context, className string, hasFrozen bool) error {
	args := f.Called(ctx, className)
	return args.Error(0)
}

func (f *fakeMigrator) AddProperty(ctx context.Context, className string, prop ...*models.Property) error {
	args := f.Called(ctx, className, prop)
	return args.Error(0)
}

func (f *fakeMigrator) LoadShard(ctx context.Context, class string, shard string) error {
	args := f.Called(ctx, class, shard)
	return args.Error(0)
}

func (f *fakeMigrator) DropShard(ctx context.Context, class string, shard string) error {
	args := f.Called(ctx, class, shard)
	return args.Error(0)
}

func (f *fakeMigrator) ShutdownShard(ctx context.Context, class string, shard string) error {
	args := f.Called(ctx, class, shard)
	return args.Error(0)
}

func (f *fakeMigrator) UpdateProperty(ctx context.Context, className string, propName string, newName *string) error {
	return nil
}

func (f *fakeMigrator) NewTenants(ctx context.Context, class *models.Class, creates []*CreateTenantPayload) error {
	args := f.Called(ctx, class, creates)
	return args.Error(0)
}

func (f *fakeMigrator) UpdateTenants(ctx context.Context, class *models.Class, updates []*UpdateTenantPayload, implicitUpdate bool) error {
	args := f.Called(ctx, class, updates)
	return args.Error(0)
}

func (f *fakeMigrator) DeleteTenants(ctx context.Context, class string, tenants []*models.Tenant) error {
	args := f.Called(ctx, class, tenants)
	return args.Error(0)
}

func (f *fakeMigrator) GetShardsStatus(ctx context.Context, className, tenant string) (map[string]string, error) {
	args := f.Called(ctx, className, tenant)
	return args.Get(0).(map[string]string), args.Error(1)
}

func (f *fakeMigrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string, schemaVersion uint64) error {
	args := f.Called(ctx, className, shardName, targetStatus, schemaVersion)
	return args.Error(0)
}

func (f *fakeMigrator) UpdateVectorIndexConfig(ctx context.Context, className string, updated schemaConfig.VectorIndexConfig) error {
	args := f.Called(ctx, className, updated)
	return args.Error(0)
}

func (*fakeMigrator) ValidateVectorIndexConfigsUpdate(old, updated map[string]schemaConfig.VectorIndexConfig,
) error {
	return nil
}

func (*fakeMigrator) UpdateVectorIndexConfigs(ctx context.Context, className string,
	updated map[string]schemaConfig.VectorIndexConfig,
) error {
	return nil
}

func (*fakeMigrator) ValidateInvertedIndexConfigUpdate(old, updated *models.InvertedIndexConfig) error {
	return nil
}

func (f *fakeMigrator) UpdateInvertedIndexConfig(ctx context.Context, className string, updated *models.InvertedIndexConfig) error {
	args := f.Called(ctx, className, updated)
	return args.Error(0)
}

func (f *fakeMigrator) UpdateReplicationConfig(ctx context.Context, className string, cfg *models.ReplicationConfig) error {
	return nil
}

func (f *fakeMigrator) WaitForStartup(ctx context.Context) error {
	args := f.Called(ctx)
	return args.Error(0)
}

func (f *fakeMigrator) Shutdown(ctx context.Context) error {
	args := f.Called(ctx)
	return args.Error(0)
}

func (f *fakeMigrator) UpdateIndex(ctx context.Context, class *models.Class, shardingState *sharding.State) error {
	args := f.Called(class, shardingState)
	return args.Error(0)
}
