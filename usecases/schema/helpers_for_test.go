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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/usecases/cluster"
)

type fakeRepo struct {
	schema State
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{schema: NewState(2)}
}

func (f *fakeRepo) Save(ctx context.Context, schema State) error {
	f.schema = schema
	return nil
}

func (f *fakeRepo) Load(context.Context) (State, error) {
	return f.schema, nil
}

func (f *fakeRepo) NewClass(context.Context, ClassPayload) error {
	return nil
}

func (f *fakeRepo) UpdateClass(context.Context, ClassPayload) error {
	return nil
}

func (f *fakeRepo) DeleteClass(ctx context.Context, class string) error {
	return nil
}

func (f *fakeRepo) NewShards(ctx context.Context, class string, shards []KeyValuePair) error {
	return nil
}

func (f *fakeRepo) UpdateShards(ctx context.Context, class string, shards []KeyValuePair) error {
	return nil
}

func (f *fakeRepo) DeleteShards(_ context.Context, class string, _ []string) error {
	return nil
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
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

func dummyParseVectorConfig(in interface{}, vectorIndexType string) (schema.VectorIndexConfig, error) {
	return fakeVectorConfig{raw: in}, nil
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

	return fmt.Errorf("invalid vectorizer %q", moduleName)
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
	hosts       []string
	syncIgnored bool
	skipRepair  bool
}

func (f *fakeClusterState) SchemaSyncIgnored() bool {
	return f.syncIgnored
}

func (f *fakeClusterState) SkipSchemaRepair() bool {
	return f.skipRepair
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
	return "node1"
}

func (f *fakeClusterState) NodeCount() int {
	return 1
}

func (f *fakeClusterState) ClusterHealthScore() int {
	return 0
}

func (f *fakeClusterState) ResolveParentNodes(string, string,
) (map[string]string, error) {
	return nil, nil
}

func (f *fakeClusterState) NodeHostname(string) (string, bool) {
	return "", false
}

type fakeTxClient struct {
	openInjectPayload interface{}
	openErr           error
	abortErr          error
	commitErr         error
}

func (f *fakeTxClient) OpenTransaction(ctx context.Context, host string, tx *cluster.Transaction) error {
	if f.openInjectPayload != nil {
		tx.Payload = f.openInjectPayload
	}

	return f.openErr
}

func (f *fakeTxClient) AbortTransaction(ctx context.Context, host string, tx *cluster.Transaction) error {
	return f.abortErr
}

func (f *fakeTxClient) CommitTransaction(ctx context.Context, host string, tx *cluster.Transaction) error {
	return f.commitErr
}
