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

package clusterapi_test

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	schemaent "github.com/weaviate/weaviate/entities/schema"
	schemauc "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type fakeRepo struct {
	schema *schemauc.State
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{}
}

func (f *fakeRepo) LoadSchema(context.Context) (*schemauc.State, error) {
	return f.schema, nil
}

func (f *fakeRepo) SaveSchema(ctx context.Context, schema schemauc.State) error {
	f.schema = &schema
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

func dummyParseVectorConfig(in interface{}) (schemaent.VectorIndexConfig, error) {
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

func (f *fakeClusterState) Hostnames() []string {
	return f.hosts
}

func (f *fakeClusterState) AllNames() []string {
	return f.hosts
}

func (f *fakeClusterState) LocalName() string {
	return f.hosts[0]
}

func (f *fakeClusterState) NodeCount() int {
	return len(f.hosts)
}

func (f *fakeClusterState) ClusterHealthScore() int {
	// 0 - healty, >0 - unhealthy
	return 0
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

func (n *NilMigrator) GetShardsStatus(ctx context.Context, className string) (map[string]string, error) {
	return nil, nil
}

func (n *NilMigrator) UpdateShardStatus(ctx context.Context, className, shardName, targetStatus string) error {
	return nil
}

func (n *NilMigrator) AddProperty(ctx context.Context, className string, prop *models.Property) error {
	return nil
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
