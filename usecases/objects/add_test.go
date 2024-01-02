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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_Add_Object_WithNoVectorizerModule(t *testing.T) {
	var (
		vectorRepo      *fakeVectorRepo
		modulesProvider *fakeModulesProvider
		manager         *Manager
	)

	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "Foo",
					Vectorizer:        config.VectorizerModuleNone,
					VectorIndexConfig: hnsw.UserConfig{},
				},
				{
					Class:      "FooSkipped",
					Vectorizer: config.VectorizerModuleNone,
					VectorIndexConfig: hnsw.UserConfig{
						Skip: true,
					},
				},
			},
		},
	}

	resetAutoSchema := func(autoSchemaEnabled bool) {
		vectorRepo = &fakeVectorRepo{}
		vectorRepo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: sch,
		}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{
			Config: config.Config{
				AutoSchema: config.AutoSchema{
					Enabled:       autoSchemaEnabled,
					DefaultString: schema.DataTypeText.String(),
				},
			},
		}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		modulesProvider = getFakeModulesProvider()
		metrics := &fakeMetrics{}
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer,
			vectorRepo, modulesProvider, metrics)
	}

	reset := func() {
		resetAutoSchema(false)
	}

	t.Run("without an id set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		class := &models.Object{
			Vector: []float32{0.1, 0.2, 0.3},
			Class:  "Foo",
		}

		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		res, err := manager.AddObject(ctx, nil, class, nil)
		require.Nil(t, err)
		uuidDuringCreation := vectorRepo.Mock.Calls[0].Arguments.Get(0).(*models.Object).ID

		assert.Len(t, uuidDuringCreation, 36, "check that a uuid was assigned")
		assert.Equal(t, uuidDuringCreation, res.ID, "check that connector add ID and user response match")
	})

	t.Run("with an explicit (correct) ID set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		object := &models.Object{
			Vector: []float32{0.1, 0.2, 0.3},
			ID:     id,
			Class:  "Foo",
		}
		vectorRepo.On("Exists", "Foo", id).Return(false, nil).Once()
		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		res, err := manager.AddObject(ctx, nil, object, nil)
		require.Nil(t, err)
		uuidDuringCreation := vectorRepo.Mock.Calls[1].Arguments.Get(0).(*models.Object).ID

		assert.Equal(t, id, uuidDuringCreation, "check that a uuid is the user specified one")
		assert.Equal(t, res.ID, uuidDuringCreation, "check that connector add ID and user response match")
	})

	t.Run("with an explicit (correct) uppercase id set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("4A334D0B-6347-40A0-A5AE-339677B20EDE")
		lowered := strfmt.UUID(strings.ToLower(id.String()))
		object := &models.Object{
			ID:     id,
			Class:  "Foo",
			Vector: []float32{0.1, 0.2, 0.3},
		}
		vectorRepo.On("Exists", "Foo", lowered).Return(false, nil).Once()
		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		res, err := manager.AddObject(ctx, nil, object, nil)
		require.Nil(t, err)
		assert.Equal(t, res.ID, lowered, "check that id was lowered and added")
	})

	t.Run("with an explicit (correct) ID set and a property that doesn't exist", func(t *testing.T) {
		resetAutoSchema(true)

		ctx := context.Background()
		id := strfmt.UUID("5aaad361-1e0d-42ae-bb52-ee09cb5f31cc")
		object := &models.Object{
			Vector: []float32{0.1, 0.2, 0.3},
			ID:     id,
			Class:  "Foo",
			Properties: map[string]interface{}{
				"newProperty": "string value",
			},
		}
		vectorRepo.On("Exists", "Foo", id).Return(false, nil).Once()
		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		res, err := manager.AddObject(ctx, nil, object, nil)
		require.Nil(t, err)
		uuidDuringCreation := vectorRepo.Mock.Calls[1].Arguments.Get(0).(*models.Object).ID

		assert.Equal(t, id, uuidDuringCreation, "check that a uuid is the user specified one")
		assert.Equal(t, res.ID, uuidDuringCreation, "check that connector add ID and user response match")
	})

	t.Run("with a uuid that's already taken", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		class := &models.Object{
			ID:    id,
			Class: "Foo",
		}

		vectorRepo.On("Exists", "Foo", id).Return(true, nil).Once()
		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		_, err := manager.AddObject(ctx, nil, class, nil)
		assert.Equal(t, NewErrInvalidUserInput("id '%s' already exists", id), err)
	})

	t.Run("with a uuid that's malformed", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-4fooooooo2ae-bd52-ee09cb5f31cc")
		class := &models.Object{
			ID:    id,
			Class: "Foo",
		}

		vectorRepo.On("Exists", "Foo", id).Return(false, nil).Once()

		_, err := manager.AddObject(ctx, nil, class, nil)
		assert.Equal(t, NewErrInvalidUserInput("invalid object: invalid UUID length: %d", len(id)), err)
	})

	t.Run("without a vector", func(t *testing.T) {
		// Note that this was an invalid case before v1.10 which added this
		// functionality, as part of
		// https://github.com/weaviate/weaviate/issues/1800
		reset()

		ctx := context.Background()
		class := &models.Object{
			Class: "Foo",
		}

		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		_, err := manager.AddObject(ctx, nil, class, nil)
		assert.Nil(t, err)
	})

	t.Run("without a vector, but indexing skipped", func(t *testing.T) {
		reset()

		ctx := context.Background()
		class := &models.Object{
			Class: "FooSkipped",
		}

		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		_, err := manager.AddObject(ctx, nil, class, nil)
		assert.Nil(t, err)
	})
}

func Test_Add_Object_WithExternalVectorizerModule(t *testing.T) {
	var (
		vectorRepo      *fakeVectorRepo
		modulesProvider *fakeModulesProvider
		manager         *Manager
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "Foo",
					Vectorizer:        config.VectorizerModuleText2VecContextionary,
					VectorIndexConfig: hnsw.UserConfig{},
				},
			},
		},
	}

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		vectorRepo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		modulesProvider = getFakeModulesProvider()
		modulesProvider.On("UsingRef2Vec", mock.Anything).Return(false)
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer,
			vectorRepo, modulesProvider, metrics)
	}

	t.Run("without an id set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		object := &models.Object{
			Class: "Foo",
		}

		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		res, err := manager.AddObject(ctx, nil, object, nil)
		require.Nil(t, err)

		uuidDuringCreation := vectorRepo.Mock.Calls[0].Arguments.Get(0).(*models.Object).ID

		assert.Len(t, uuidDuringCreation, 36, "check that a uuid was assigned")
		assert.Equal(t, uuidDuringCreation, res.ID, "check that connector add ID and user response match")
	})

	t.Run("with an explicit (correct) ID set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		object := &models.Object{
			ID:    id,
			Class: "Foo",
		}
		vectorRepo.On("Exists", "Foo", id).Return(false, nil).Once()
		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		res, err := manager.AddObject(ctx, nil, object, nil)
		uuidDuringCreation := vectorRepo.Mock.Calls[1].Arguments.Get(0).(*models.Object).ID

		assert.Nil(t, err)
		assert.Equal(t, id, uuidDuringCreation, "check that a uuid is the user specified one")
		assert.Equal(t, res.ID, uuidDuringCreation, "check that connector add ID and user response match")
	})

	t.Run("with a uuid that's already taken", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		object := &models.Object{
			ID:    id,
			Class: "Foo",
		}

		vectorRepo.On("Exists", "Foo", id).Return(true, nil).Once()
		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		_, err := manager.AddObject(ctx, nil, object, nil)
		assert.Equal(t, NewErrInvalidUserInput("id '%s' already exists", id), err)
	})

	t.Run("with a uuid that's malformed", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-4f00000002ae-bd52-ee09cb5f31cc")
		object := &models.Object{
			ID:    id,
			Class: "Foo",
		}

		vectorRepo.On("Exists", "Foo", id).Return(false, nil).Once()
		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(nil, nil)

		_, err := manager.AddObject(ctx, nil, object, nil)
		assert.Equal(t, NewErrInvalidUserInput("invalid object: invalid UUID length: %d", len(id)), err)
	})
}

func Test_Add_Object_OverrideVectorizer(t *testing.T) {
	var (
		vectorRepo      *fakeVectorRepo
		modulesProvider *fakeModulesProvider
		manager         *Manager
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "FooOverride",
					Vectorizer:        config.VectorizerModuleText2VecContextionary,
					VectorIndexConfig: hnsw.UserConfig{},
				},
			},
		},
	}

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		vectorRepo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		modulesProvider = getFakeModulesProvider()
		metrics := &fakeMetrics{}
		manager = NewManager(locks, schemaManager, cfg, logger,
			authorizer, vectorRepo, modulesProvider, metrics)
	}

	t.Run("overriding the vector by explicitly specifying it", func(t *testing.T) {
		reset()

		ctx := context.Background()
		object := &models.Object{
			Class:  "FooOverride",
			Vector: []float32{9, 9, 9},
		}

		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(object.Vector, nil)

		_, err := manager.AddObject(ctx, nil, object, nil)
		require.Nil(t, err)

		vec := vectorRepo.Mock.Calls[0].Arguments.Get(1).([]float32)

		assert.Equal(t, []float32{9, 9, 9}, vec, "check that vector was overridden")
	})
}

func Test_AddObjectEmptyProperties(t *testing.T) {
	var (
		vectorRepo      *fakeVectorRepo
		modulesProvider *fakeModulesProvider
		manager         *Manager
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
		vectorRepo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		modulesProvider = getFakeModulesProvider()
		metrics := &fakeMetrics{}
		manager = NewManager(locks, schemaManager, cfg, logger,
			authorizer, vectorRepo, modulesProvider, metrics)
	}
	reset()
	ctx := context.Background()
	object := &models.Object{
		Class:  "TestClass",
		Vector: []float32{9, 9, 9},
	}
	assert.Nil(t, object.Properties)
	modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
		Return(nil, nil)
	addedObject, err := manager.AddObject(ctx, nil, object, nil)
	assert.Nil(t, err)
	assert.NotNil(t, addedObject.Properties)
}

func Test_AddObjectWithUUIDProps(t *testing.T) {
	var (
		vectorRepo      *fakeVectorRepo
		modulesProvider *fakeModulesProvider
		manager         *Manager
	)
	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "TestClass",
					VectorIndexConfig: hnsw.UserConfig{},

					Properties: []*models.Property{
						{
							Name:     "my_id",
							DataType: []string{"uuid"},
						},
						{
							Name:     "my_idz",
							DataType: []string{"uuid[]"},
						},
					},
				},
			},
		},
	}
	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		vectorRepo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		modulesProvider = getFakeModulesProvider()
		metrics := &fakeMetrics{}
		manager = NewManager(locks, schemaManager, cfg, logger,
			authorizer, vectorRepo, modulesProvider, metrics)
	}
	reset()
	ctx := context.Background()
	object := &models.Object{
		Class:  "TestClass",
		Vector: []float32{9, 9, 9},
		Properties: map[string]interface{}{
			"my_id":  "28bafa1e-7956-4c58-8a02-4499a9d15253",
			"my_idz": []any{"28bafa1e-7956-4c58-8a02-4499a9d15253"},
		},
	}
	modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
		Return(nil, nil)
	addedObject, err := manager.AddObject(ctx, nil, object, nil)
	require.Nil(t, err)
	require.NotNil(t, addedObject.Properties)

	expectedID := uuid.MustParse("28bafa1e-7956-4c58-8a02-4499a9d15253")
	expectedIDz := []uuid.UUID{uuid.MustParse("28bafa1e-7956-4c58-8a02-4499a9d15253")}

	assert.Equal(t, expectedID, addedObject.Properties.(map[string]interface{})["my_id"])
	assert.Equal(t, expectedIDz, addedObject.Properties.(map[string]interface{})["my_idz"])
}
