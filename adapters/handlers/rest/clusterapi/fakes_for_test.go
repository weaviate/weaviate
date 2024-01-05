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

package clusterapi_test

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	schemaent "github.com/weaviate/weaviate/entities/schema"
	ucs "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/migrate"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type fakeRepo struct {
	schema ucs.State
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		schema: ucs.NewState(1),
	}
}

func (f *fakeRepo) Save(ctx context.Context, schema ucs.State) error {
	f.schema = schema
	return nil
}

func (f *fakeRepo) Load(context.Context) (ucs.State, error) {
	return f.schema, nil
}

func (f *fakeRepo) NewClass(context.Context, ucs.ClassPayload) error {
	return nil
}

func (f *fakeRepo) UpdateClass(context.Context, ucs.ClassPayload) error {
	return nil
}

func (f *fakeRepo) DeleteClass(ctx context.Context, class string) error {
	return nil
}

func (f *fakeRepo) NewShards(ctx context.Context, class string, shards []ucs.KeyValuePair) error {
	return nil
}

func (f *fakeRepo) UpdateShards(ctx context.Context, class string, shards []ucs.KeyValuePair) error {
	return nil
}

func (f *fakeRepo) DeleteShards(ctx context.Context, class string, shards []string) error {
	return nil
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}

type fakeVectorConfig map[string]interface{}

func (f fakeVectorConfig) IndexType() string {
	return "fake"
}

func (f fakeVectorConfig) DistanceName() string {
	return "fake"
}

func dummyParseVectorConfig(in interface{}, indexType string) (schemaent.VectorIndexConfig, error) {
	return fakeVectorConfig(in.(map[string]interface{})), nil
}

func dummyValidateInvertedConfig(in *models.InvertedIndexConfig) error {
	return nil
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

	return errors.Errorf("invalid vectorizer %q", moduleName)
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
	prop *models.Property) {
}

func (f *fakeModuleConfig) ValidateClass(ctx context.Context, class *models.Class) error {
	return nil
}

type fakeClusterState struct {
	hosts []string
}

func (f *fakeClusterState) SchemaSyncIgnored() bool {
	return false
}

func (f *fakeClusterState) SkipSchemaRepair() bool {
	return false
}

func (f *fakeClusterState) Hostnames() []string {
	return f.hosts
}

func (f *fakeClusterState) AllNames() []string {
	return f.hosts
}

func (f *fakeClusterState) Candidates() []string {
	return f.hosts
}

func (f *fakeClusterState) LocalName() string {
	return f.hosts[0]
}

func (f *fakeClusterState) NodeCount() int {
	return len(f.hosts)
}

func (f *fakeClusterState) ClusterHealthScore() int {
	// 0 - healthy, >0 - unhealthy
	return 0
}

func (f *fakeClusterState) ResolveParentNodes(string, string) (map[string]string, error) {
	return nil, nil
}

func (f *fakeClusterState) NodeHostname(nodeName string) (string, bool) {
	return "", false
}

type NilMigrator struct{}

func (n *NilMigrator) AddClass(ctx context.Context, class *models.Class,
	shardingState *sharding.State,
) error {
	return nil
}

func (n *NilMigrator) DropClass(ctx context.Context, className string) error {
	return nil
}

func (n *NilMigrator) UpdateClass(ctx context.Context, className string, newClassName *string) error {
	return nil
}

func (n *NilMigrator) GetShardsQueueSize(ctx context.Context, className, tenant string) (map[string]int64, error) {
	return nil, nil
}

func (n *NilMigrator) GetShardsStatus(ctx context.Context, className, tenant string) (map[string]string, error) {
	return nil, nil
}

func (n *NilMigrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error {
	return nil
}

func (n *NilMigrator) AddProperty(ctx context.Context, className string, prop *models.Property) error {
	return nil
}

func (n *NilMigrator) NewTenants(ctx context.Context, class *models.Class, creates []*migrate.CreateTenantPayload) (commit func(success bool), err error) {
	return nil, nil
}

func (n *NilMigrator) UpdateTenants(ctx context.Context, class *models.Class, updates []*migrate.UpdateTenantPayload) (commit func(success bool), err error) {
	return nil, nil
}

func (n *NilMigrator) DeleteTenants(ctx context.Context, class *models.Class, partitions []string) (commit func(success bool), err error) {
	return nil, nil
}

func (n *NilMigrator) UpdateProperty(ctx context.Context, className string, propName string, newName *string) error {
	return nil
}

func (n *NilMigrator) UpdatePropertyAddDataType(ctx context.Context, className string, propName string, newDataType string) error {
	return nil
}

func (n *NilMigrator) DropProperty(ctx context.Context, className string, propName string) error {
	return nil
}

func (n *NilMigrator) ValidateVectorIndexConfigUpdate(ctx context.Context, old, updated schemaent.VectorIndexConfig) error {
	return nil
}

func (n *NilMigrator) UpdateVectorIndexConfig(ctx context.Context, className string, updated schemaent.VectorIndexConfig) error {
	return nil
}

func (n *NilMigrator) ValidateInvertedIndexConfigUpdate(ctx context.Context, old, updated *models.InvertedIndexConfig) error {
	return nil
}

func (n *NilMigrator) UpdateInvertedIndexConfig(ctx context.Context, className string, updated *models.InvertedIndexConfig) error {
	return nil
}

func (n *NilMigrator) RecalculateVectorDimensions(ctx context.Context) error {
	return nil
}

func (n *NilMigrator) RecountProperties(ctx context.Context) error {
	return nil
}

func (n *NilMigrator) InvertedReindex(ctx context.Context, taskNames ...string) error {
	return nil
}

func (n *NilMigrator) AdjustFilterablePropSettings(ctx context.Context) error {
	return nil
}
