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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	shardingCfg "github.com/weaviate/weaviate/usecases/sharding/config"
)

func TestClassGetterFromSchema(t *testing.T) {
	testCases := []struct {
		name          string
		getFromSchema []string
		strategy      configRuntime.CollectionRetrievalStrategy
		schemaExpect  func(*fakeSchemaManager)
	}{
		{
			name:          "Read only from leader",
			getFromSchema: []string{"class1", "class2", "class3"},
			strategy:      configRuntime.LeaderOnly,
			schemaExpect: func(f *fakeSchemaManager) {
				f.On("QueryReadOnlyClasses", []string{"class1", "class2", "class3"}).Return(map[string]versioned.Class{
					"class1": {Version: 1, Class: &models.Class{Class: "class1", VectorIndexType: "hnsw", ShardingConfig: make(map[string]interface{})}},
					"class2": {Version: 2, Class: &models.Class{Class: "class2", VectorIndexType: "hnsw", ShardingConfig: make(map[string]interface{})}},
					"class3": {Version: 3, Class: &models.Class{Class: "class3", VectorIndexType: "hnsw", ShardingConfig: make(map[string]interface{})}},
				}, nil)
			},
		},
		{
			name:          "Read only from local",
			getFromSchema: []string{"class1", "class2", "class3"},
			strategy:      configRuntime.LocalOnly,
			schemaExpect: func(f *fakeSchemaManager) {
				f.On("ReadOnlyVersionedClass", "class1").Return(versioned.Class{Version: 1, Class: &models.Class{Class: "class1", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class2").Return(versioned.Class{Version: 2, Class: &models.Class{Class: "class2", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class3").Return(versioned.Class{Version: 3, Class: &models.Class{Class: "class3", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
			},
		},
		{
			name:          "Read all from leader if mismatch",
			getFromSchema: []string{"class1", "class2", "class3"},
			strategy:      configRuntime.LeaderOnMismatch,
			schemaExpect: func(f *fakeSchemaManager) {
				// First we will query the versions from the leader
				f.On("QueryClassVersions", []string{"class1", "class2", "class3"}).Return(map[string]uint64{
					"class1": 4,
					"class2": 5,
					"class3": 6,
				}, nil)
				// Then we check the local version
				f.On("ReadOnlyVersionedClass", "class1").Return(versioned.Class{Version: 1, Class: &models.Class{Class: "class1", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class2").Return(versioned.Class{Version: 2, Class: &models.Class{Class: "class2", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class3").Return(versioned.Class{Version: 3, Class: &models.Class{Class: "class3", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				// Then we fetch what we need to update
				f.On("QueryReadOnlyClasses", []string{"class1", "class2", "class3"}).Return(map[string]versioned.Class{
					"class1": {Version: 1, Class: &models.Class{Class: "class1", VectorIndexType: "hnsw"}},
					"class2": {Version: 2, Class: &models.Class{Class: "class2", VectorIndexType: "hnsw"}},
					"class3": {Version: 3, Class: &models.Class{Class: "class3", VectorIndexType: "hnsw"}},
				}, nil)
			},
		},
		{
			name:          "Read subset from leader if mismatch",
			getFromSchema: []string{"class1", "class2", "class3"},
			strategy:      configRuntime.LeaderOnMismatch,
			schemaExpect: func(f *fakeSchemaManager) {
				// First we will query the versions from the leader
				f.On("QueryClassVersions", []string{"class1", "class2", "class3"}).Return(map[string]uint64{
					"class1": 1,
					"class2": 2,
					"class3": 6,
				}, nil)
				// Then we check the local version
				f.On("ReadOnlyVersionedClass", "class1").Return(versioned.Class{Version: 1, Class: &models.Class{Class: "class1", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class2").Return(versioned.Class{Version: 2, Class: &models.Class{Class: "class2", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class3").Return(versioned.Class{Version: 3, Class: &models.Class{Class: "class3", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				// Then we fetch what we need to update
				f.On("QueryReadOnlyClasses", []string{"class3"}).Return(map[string]versioned.Class{
					"class3": {Version: 6, Class: &models.Class{Class: "class3", VectorIndexType: "hnsw"}},
				}, nil)
			},
		},
		{
			name:          "Read from leader local equal",
			getFromSchema: []string{"class1", "class2", "class3"},
			strategy:      configRuntime.LeaderOnMismatch,
			schemaExpect: func(f *fakeSchemaManager) {
				// First we will query the versions from the leader
				f.On("QueryClassVersions", []string{"class1", "class2", "class3"}).Return(map[string]uint64{
					"class1": 1,
					"class2": 2,
					"class3": 3,
				}, nil)
				f.On("ReadOnlyVersionedClass", "class1").Return(versioned.Class{Version: 1, Class: &models.Class{Class: "class1", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class2").Return(versioned.Class{Version: 2, Class: &models.Class{Class: "class2", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class3").Return(versioned.Class{Version: 3, Class: &models.Class{Class: "class3", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
			},
		},
		{
			name:          "Read from leader local ahead",
			getFromSchema: []string{"class1", "class2", "class3"},
			strategy:      configRuntime.LeaderOnMismatch,
			schemaExpect: func(f *fakeSchemaManager) {
				// First we will query the versions from the leader
				f.On("QueryClassVersions", []string{"class1", "class2", "class3"}).Return(map[string]uint64{
					"class1": 1,
					"class2": 2,
					"class3": 3,
				}, nil)
				// Here we assume a delay between the leader returning the version and a change, so local > leader
				f.On("ReadOnlyVersionedClass", "class1").Return(versioned.Class{Version: 4, Class: &models.Class{Class: "class1", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class2").Return(versioned.Class{Version: 5, Class: &models.Class{Class: "class2", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
				f.On("ReadOnlyVersionedClass", "class3").Return(versioned.Class{Version: 6, Class: &models.Class{Class: "class3", VectorIndexType: "hnsw", ShardingConfig: shardingCfg.Config{}}})
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			// Configure test setup
			handler, fakeSchema := newTestHandler(t, &fakeDB{})
			log, _ := test.NewNullLogger()
			classGetter := NewClassGetter(
				&handler.parser,
				fakeSchema,
				fakeSchema,
				configRuntime.NewFeatureFlag(
					"fake-key",
					string(testCase.strategy),
					nil,
					"",
					log,
				),
				log,
			)
			require.NotNil(t, classGetter)

			// Configure expectation
			testCase.schemaExpect(fakeSchema)

			// Get class and ensure we receive all classes as expected
			classes, err := classGetter.getClasses(testCase.getFromSchema)
			require.NoError(t, err)
			require.Equal(t, len(testCase.getFromSchema), len(classes))
			for _, c := range classes {
				require.Contains(t, testCase.getFromSchema, c.Class.Class)
			}

			// Assert all the mock happened as expected
			fakeSchema.AssertExpectations(t)
		})
	}
}
