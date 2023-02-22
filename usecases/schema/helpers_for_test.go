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
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
)

type fakeRepo struct {
	schema *State
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{}
}

func (f *fakeRepo) LoadSchema(context.Context) (*State, error) {
	return f.schema, nil
}

func (f *fakeRepo) SaveSchema(ctx context.Context, schema State) error {
	f.schema = &schema
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

func dummyParseVectorConfig(in interface{}) (schema.VectorIndexConfig, error) {
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

	return errors.Errorf("invalid vectorizer %q", moduleName)
}

type fakeModuleConfig struct{}

func (f *fakeModuleConfig) SetClassDefaults(class *models.Class) error {
	defaultConfig := map[string]interface{}{
		"my-module1": map[string]interface{}{
			"my-setting": "default-value",
		},
	}

	asMap, ok := class.ModuleConfig.(map[string]interface{})
	if !ok {
		class.ModuleConfig = defaultConfig
		return fmt.Errorf("invalid class config")
	}

	module, ok := asMap["my-module1"]
	if !ok {
		class.ModuleConfig = defaultConfig
		return fmt.Errorf("my-module1 vectorizer module not part of the class")
	}

	asMap, ok = module.(map[string]interface{})
	if !ok {
		class.ModuleConfig = defaultConfig
		return errors.New("invalid module config")
	}

	if _, ok := asMap["my-setting"]; !ok {
		asMap["my-setting"] = "default-value"
		defaultConfig["my-module1"] = asMap
		class.ModuleConfig = defaultConfig
	}
	return nil
}

func (f *fakeModuleConfig) SetSinglePropertyDefaults(class *models.Class,
	prop *models.Property,
) error {
	return nil
}

func (f *fakeModuleConfig) ValidateClass(ctx context.Context, class *models.Class) error {
	return nil
}

type fakeClusterState struct {
	hosts       []string
	syncIgnored bool
}

func (f *fakeClusterState) SchemaSyncIgnored() bool {
	return f.syncIgnored
}

func (f *fakeClusterState) Hostnames() []string {
	return f.hosts
}

func (f *fakeClusterState) AllNames() []string {
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
