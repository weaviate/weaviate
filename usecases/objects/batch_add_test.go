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

package objects

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func Test_BatchManager_AddObjects_WithNoVectorizerModule(t *testing.T) {
	var (
		vectorRepo      *fakeVectorRepo
		modulesProvider *fakeModulesProvider
		manager         *BatchManager
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Vectorizer:        config.VectorizerModuleNone,
					Class:             "Foo",
					VectorIndexConfig: hnsw.UserConfig{},
				},
				{
					Vectorizer: config.VectorizerModuleNone,
					Class:      "FooSkipped",
					VectorIndexConfig: hnsw.UserConfig{
						Skip: true,
					},
				},
			},
		},
	}

	resetAutoSchema := func(autoSchema bool) {
		vectorRepo = &fakeVectorRepo{}
		config := &config.WeaviateConfig{
			Config: config.Config{
				AutoSchema: config.AutoSchema{
					Enabled: runtime.NewDynamicValue(autoSchema),
				},
				TrackVectorDimensions: true,
			},
		}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		logger, _ := test.NewNullLogger()
		authorizer := mocks.NewMockAuthorizer()
		modulesProvider = getFakeModulesProvider()
		manager = NewBatchManager(vectorRepo, modulesProvider, schemaManager, config, logger, authorizer, nil,
			NewAutoSchemaManager(schemaManager, vectorRepo, config, authorizer, logger, prometheus.NewPedanticRegistry()))
	}

	reset := func() {
		resetAutoSchema(false)
	}
	ctx := context.Background()

	t.Run("without any objects", func(t *testing.T) {
		reset()
		expectedErr := NewErrInvalidUserInput("invalid param 'objects': cannot be empty, need at least" +
			" one object for batching")

		_, err := manager.AddObjects(ctx, nil, []*models.Object{}, []*string{}, nil)

		assert.Equal(t, expectedErr, err)
	})

	t.Run("with objects without IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		objects := []*models.Object{
			{
				Class:  "Foo",
				Vector: []float32{0.1, 0.1, 0.1111},
			},
			{
				Class:  "Foo",
				Vector: []float32{0.2, 0.2, 0.2222},
			},
		}

		for range objects {
			modulesProvider.On("BatchUpdateVector").
				Return(nil, nil)
		}

		_, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
		repoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithObjects, 2)
		assert.Len(t, repoCalledWithObjects[0].UUID, 36,
			"a uuid was set for the first object")
		assert.Len(t, repoCalledWithObjects[1].UUID, 36,
			"a uuid was set for the second object")
		assert.Nil(t, repoCalledWithObjects[0].Err)
		assert.Nil(t, repoCalledWithObjects[1].Err)
		assert.Equal(t, models.C11yVector{0.1, 0.1, 0.1111}, repoCalledWithObjects[0].Object.Vector,
			"the correct vector was used")
		assert.Equal(t, models.C11yVector{0.2, 0.2, 0.2222}, repoCalledWithObjects[1].Object.Vector,
			"the correct vector was used")
	})

	t.Run("object without class", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		objects := []*models.Object{
			{
				Class:  "",
				Vector: []float32{0.1, 0.1, 0.1111},
			},
			{
				Class:  "Foo",
				Vector: []float32{0.2, 0.2, 0.2222},
			},
		}

		for range objects {
			modulesProvider.On("BatchUpdateVector").
				Return(nil, nil)
		}

		resp, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
		repoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)
		assert.Nil(t, err)
		assert.NotNil(t, resp)
		require.Len(t, repoCalledWithObjects, 2)

		require.NotNil(t, resp[0].Err)
		require.Equal(t, resp[0].Err.Error(), "object has an empty class")
		require.Nil(t, resp[1].Err)
	})

	t.Run("with objects without IDs and nonexistent class and auto schema enabled", func(t *testing.T) {
		resetAutoSchema(true)
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		objects := []*models.Object{
			{
				Class:  "NonExistentFoo",
				Vector: []float32{0.1, 0.1, 0.1111},
			},
			{
				Class:  "NonExistentFoo",
				Vector: []float32{0.2, 0.2, 0.2222},
			},
		}

		for range objects {
			modulesProvider.On("BatchUpdateVector").
				Return(nil, nil)
		}

		_, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
		repoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithObjects, 2)
		assert.Len(t, repoCalledWithObjects[0].UUID, 36,
			"a uuid was set for the first object")
		assert.Len(t, repoCalledWithObjects[1].UUID, 36,
			"a uuid was set for the second object")
		assert.Nil(t, repoCalledWithObjects[0].Err)
		assert.Nil(t, repoCalledWithObjects[1].Err)
		assert.Equal(t, models.C11yVector{0.1, 0.1, 0.1111}, repoCalledWithObjects[0].Object.Vector,
			"the correct vector was used")
		assert.Equal(t, models.C11yVector{0.2, 0.2, 0.2222}, repoCalledWithObjects[1].Object.Vector,
			"the correct vector was used")
	})

	t.Run("with user-specified IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("2d3942c3-b412-4d80-9dfa-99a646629cd2")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		objects := []*models.Object{
			{
				ID:     id1,
				Class:  "Foo",
				Vector: []float32{0.1, 0.1, 0.1111},
			},
			{
				ID:     id2,
				Class:  "Foo",
				Vector: []float32{0.2, 0.2, 0.2222},
			},
		}

		for range objects {
			modulesProvider.On("BatchUpdateVector").
				Return(nil, nil)
		}

		_, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
		repoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithObjects, 2)
		assert.Equal(t, id1, repoCalledWithObjects[0].UUID, "the user-specified uuid was used")
		assert.Equal(t, id2, repoCalledWithObjects[1].UUID, "the user-specified uuid was used")
		assert.Nil(t, repoCalledWithObjects[0].Err)
		assert.Nil(t, repoCalledWithObjects[1].Err)
		assert.Equal(t, models.C11yVector{0.1, 0.1, 0.1111}, repoCalledWithObjects[0].Object.Vector,
			"the correct vector was used")
		assert.Equal(t, models.C11yVector{0.2, 0.2, 0.2222}, repoCalledWithObjects[1].Object.Vector,
			"the correct vector was used")
	})

	t.Run("with an invalid user-specified IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("invalid")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		objects := []*models.Object{
			{
				ID:     id1,
				Class:  "Foo",
				Vector: []float32{0.1, 0.1, 0.1111},
			},
			{
				ID:     id2,
				Class:  "Foo",
				Vector: []float32{0.2, 0.2, 0.2222},
			},
		}

		for range objects {
			modulesProvider.On("BatchUpdateVector").
				Return(nil, nil)
		}

		_, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
		repoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithObjects, 2)
		assert.Equal(t, repoCalledWithObjects[0].Err.Error(), fmt.Sprintf("invalid UUID length: %d", len(id1)))
		assert.Equal(t, id2, repoCalledWithObjects[1].UUID, "the user-specified uuid was used")
	})

	t.Run("without any vectors", func(t *testing.T) {
		// prior to v1.10 this was the desired behavior:
		// note that this should fail on class Foo, but be accepted on class
		// FooSkipped
		//
		// However, since v1.10, it is acceptable to exclude a vector, even if
		// indexing is not skipped. In this case only the individual element is
		// skipped. See https://github.com/weaviate/weaviate/issues/1800
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		objects := []*models.Object{
			{
				Class: "Foo",
			},
			{
				Class: "FooSkipped",
			},
		}

		for range objects {
			modulesProvider.On("BatchUpdateVector").
				Return(nil, nil)
		}

		_, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
		repoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithObjects, 2)
		assert.Nil(t, repoCalledWithObjects[0].Err)
		assert.Nil(t, repoCalledWithObjects[1].Err)
	})
}

func Test_BatchManager_AddObjects_WithExternalVectorizerModule(t *testing.T) {
	var (
		vectorRepo      *fakeVectorRepo
		modulesProvider *fakeModulesProvider
		manager         *BatchManager
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Vectorizer:        config.VectorizerModuleText2VecContextionary,
					VectorIndexConfig: hnsw.UserConfig{},
					Class:             "Foo",
				},
			},
		},
	}

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		config := &config.WeaviateConfig{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		logger, _ := test.NewNullLogger()
		authorizer := mocks.NewMockAuthorizer()
		modulesProvider = getFakeModulesProvider()
		manager = NewBatchManager(vectorRepo, modulesProvider, schemaManager, config, logger, authorizer, nil,
			NewAutoSchemaManager(schemaManager, vectorRepo, config, authorizer, logger, prometheus.NewPedanticRegistry()))
	}

	ctx := context.Background()

	t.Run("without any objects", func(t *testing.T) {
		reset()
		expectedErr := NewErrInvalidUserInput("invalid param 'objects': cannot be empty, need at least" +
			" one object for batching")

		_, err := manager.AddObjects(ctx, nil, []*models.Object{}, []*string{}, nil)

		assert.Equal(t, expectedErr, err)
	})

	t.Run("with objects without IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		expectedVector := models.C11yVector{0, 1, 2}
		objects := []*models.Object{
			{
				Class: "Foo",
			},
			{
				Class: "Foo",
			},
		}

		for range objects {
			modulesProvider.On("BatchUpdateVector").
				Return(expectedVector, nil)
		}

		_, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
		repoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithObjects, 2)
		assert.Len(t, repoCalledWithObjects[0].UUID, 36, "a uuid was set for the first object")
		assert.Len(t, repoCalledWithObjects[1].UUID, 36, "a uuid was set for the second object")
		assert.Nil(t, repoCalledWithObjects[0].Err)
		assert.Nil(t, repoCalledWithObjects[1].Err)
		assert.Equal(t, expectedVector, repoCalledWithObjects[0].Object.Vector,
			"the correct vector was used")
		assert.Equal(t, expectedVector, repoCalledWithObjects[1].Object.Vector,
			"the correct vector was used")
	})

	t.Run("with user-specified IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("2d3942c3-b412-4d80-9dfa-99a646629cd2")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		objects := []*models.Object{
			{
				ID:    id1,
				Class: "Foo",
			},
			{
				ID:    id2,
				Class: "Foo",
			},
		}

		for range objects {
			modulesProvider.On("BatchUpdateVector").
				Return(nil, nil)
		}

		_, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
		repoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithObjects, 2)
		assert.Equal(t, id1, repoCalledWithObjects[0].UUID, "the user-specified uuid was used")
		assert.Equal(t, id2, repoCalledWithObjects[1].UUID, "the user-specified uuid was used")
	})

	t.Run("with an invalid user-specified IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("invalid")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		objects := []*models.Object{
			{
				ID:    id1,
				Class: "Foo",
			},
			{
				ID:    id2,
				Class: "Foo",
			},
		}

		for range objects {
			modulesProvider.On("BatchUpdateVector").
				Return(nil, nil)
		}

		_, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
		repoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithObjects, 2)
		assert.Equal(t, repoCalledWithObjects[0].Err.Error(), fmt.Sprintf("invalid UUID length: %d", len(id1)))
		assert.Equal(t, id2, repoCalledWithObjects[1].UUID, "the user-specified uuid was used")
	})
}

func Test_BatchManager_AddObjectsEmptyProperties(t *testing.T) {
	var (
		vectorRepo      *fakeVectorRepo
		modulesProvider *fakeModulesProvider
		manager         *BatchManager
	)
	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "TestClass",
					VectorIndexConfig: hnsw.UserConfig{},

					Properties: []*models.Property{
						{
							Name:         "strings",
							DataType:     schema.DataTypeTextArray.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
					},
				},
			},
		},
	}
	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		config := &config.WeaviateConfig{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		logger, _ := test.NewNullLogger()
		authorizer := mocks.NewMockAuthorizer()
		modulesProvider = getFakeModulesProvider()
		manager = NewBatchManager(vectorRepo, modulesProvider, schemaManager, config, logger, authorizer, nil,
			NewAutoSchemaManager(schemaManager, vectorRepo, config, authorizer, logger, prometheus.NewPedanticRegistry()))
	}
	reset()
	objects := []*models.Object{
		{
			ID:    strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6"),
			Class: "TestClass",
		},
		{
			ID:    strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff3"),
			Class: "TestClass",
			Properties: map[string]interface{}{
				"name": "testName",
			},
		},
	}
	require.Nil(t, objects[0].Properties)
	require.NotNil(t, objects[1].Properties)

	ctx := context.Background()
	for range objects {
		modulesProvider.On("BatchUpdateVector").
			Return(nil, nil)
	}
	addedObjects, err := manager.AddObjects(ctx, nil, objects, []*string{}, nil)
	assert.Nil(t, err)
	require.Len(t, addedObjects, 2)
	require.NotNil(t, addedObjects[0].Object.Properties)
	require.NotNil(t, addedObjects[1].Object.Properties)
}
