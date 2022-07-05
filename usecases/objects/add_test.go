//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_Add_Object_WithNoVectorizerModule(t *testing.T) {
	var (
		vectorRepo *fakeVectorRepo
		manager    *Manager
	)

	schema := schema.Schema{
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
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{
			Config: config.Config{
				AutoSchema: config.AutoSchema{
					Enabled:       autoSchemaEnabled,
					DefaultString: "string",
				},
			},
		}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vecProvider := &fakeVectorizerProvider{vectorizer}
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer, vecProvider, vectorRepo, getFakeModulesProvider())
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

		res, err := manager.AddObject(ctx, nil, class)
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

		res, err := manager.AddObject(ctx, nil, object)
		require.Nil(t, err)
		uuidDuringCreation := vectorRepo.Mock.Calls[1].Arguments.Get(0).(*models.Object).ID

		assert.Equal(t, id, uuidDuringCreation, "check that a uuid is the user specified one")
		assert.Equal(t, res.ID, uuidDuringCreation, "check that connector add ID and user response match")
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

		res, err := manager.AddObject(ctx, nil, object)
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

		_, err := manager.AddObject(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("id '%s' already exists", id), err)
	})

	t.Run("with a uuid that's malformed", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-4FOOOOOOO2ae-bd52-ee09cb5f31cc")
		class := &models.Object{
			ID:    id,
			Class: "Foo",
		}

		vectorRepo.On("Exists", "Foo", id).Return(false, nil).Once()

		_, err := manager.AddObject(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("invalid object: invalid UUID length: %d", len(id)), err)
	})

	t.Run("without a vector", func(t *testing.T) {
		// Note that this was an invalid case before v1.10 which added this
		// functionality, as part of
		// https://github.com/semi-technologies/weaviate/issues/1800
		reset()

		ctx := context.Background()
		class := &models.Object{
			Class: "Foo",
		}

		_, err := manager.AddObject(ctx, nil, class)
		assert.Nil(t, err)
	})

	t.Run("without a vector, but indexing skipped", func(t *testing.T) {
		reset()

		ctx := context.Background()
		class := &models.Object{
			Class: "FooSkipped",
		}

		_, err := manager.AddObject(ctx, nil, class)
		assert.Nil(t, err)
	})
}

func Test_Add_Object_WithExternalVectorizerModule(t *testing.T) {
	var (
		vectorRepo *fakeVectorRepo
		manager    *Manager
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
		vectorizer := &fakeVectorizer{}
		vecProvider := &fakeVectorizerProvider{vectorizer}
		vectorizer.On("UpdateObject", mock.Anything).Return([]float32{0, 1, 2}, nil)
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer, vecProvider, vectorRepo, getFakeModulesProvider())
	}

	t.Run("without an id set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		object := &models.Object{
			Class: "Foo",
		}

		res, err := manager.AddObject(ctx, nil, object)
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

		res, err := manager.AddObject(ctx, nil, object)
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

		_, err := manager.AddObject(ctx, nil, object)
		assert.Equal(t, NewErrInvalidUserInput("id '%s' already exists", id), err)
	})

	t.Run("with a uuid that's malformed", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-4FOOOOOOO2ae-bd52-ee09cb5f31cc")
		object := &models.Object{
			ID:    id,
			Class: "Foo",
		}

		vectorRepo.On("Exists", "Foo", id).Return(false, nil).Once()

		_, err := manager.AddObject(ctx, nil, object)
		assert.Equal(t, NewErrInvalidUserInput("invalid object: invalid UUID length: %d", len(id)), err)
	})
}

func Test_Add_Object_OverrideVectorizer(t *testing.T) {
	var (
		vectorRepo *fakeVectorRepo
		manager    *Manager
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
		vectorizer := &fakeVectorizer{}
		vecProvider := &fakeVectorizerProvider{vectorizer}
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer, vecProvider, vectorRepo, getFakeModulesProvider())
	}

	t.Run("overriding the vector by explicitly specifying it", func(t *testing.T) {
		reset()

		ctx := context.Background()
		object := &models.Object{
			Class:  "FooOverride",
			Vector: []float32{9, 9, 9},
		}

		_, err := manager.AddObject(ctx, nil, object)
		require.Nil(t, err)

		vec := vectorRepo.Mock.Calls[0].Arguments.Get(1).([]float32)

		assert.Equal(t, []float32{9, 9, 9}, vec, "check that vector was overridden")
	})
}
