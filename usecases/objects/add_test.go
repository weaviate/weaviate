//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
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
		class := &models.Object{
			Vector: []float32{0.1, 0.2, 0.3},
			ID:     id,
			Class:  "Foo",
		}
		vectorRepo.On("Exists", id).Return(false, nil).Once()

		res, err := manager.AddObject(ctx, nil, class)
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

		vectorRepo.On("Exists", id).Return(true, nil).Once()

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

		vectorRepo.On("Exists", id).Return(false, nil).Once()

		_, err := manager.AddObject(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("invalid object: invalid UUID length: %d", len(id)), err)
	})

	t.Run("without a vector", func(t *testing.T) {
		reset()

		ctx := context.Background()
		class := &models.Object{
			Class: "Foo",
		}

		_, err := manager.AddObject(ctx, nil, class)
		_, ok := err.(ErrInvalidUserInput)
		assert.True(t, ok)
		assert.Contains(t, err.Error(), "vector must be present")
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

// TODO: This currently always assumes the text2vec-vectorizer, but this could
// be any module. Needs to be actually made modular
func Test_Add_Object_WithExternalVectorizerModule(t *testing.T) {
	var (
		vectorRepo *fakeVectorRepo
		manager    *Manager
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:      "Foo",
					Vectorizer: config.VectorizerModuleText2VecContextionary,
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
		class := &models.Object{
			Class: "Foo",
		}

		res, err := manager.AddObject(ctx, nil, class)
		uuidDuringCreation := vectorRepo.Mock.Calls[0].Arguments.Get(0).(*models.Object).ID

		assert.Nil(t, err)
		assert.Len(t, uuidDuringCreation, 36, "check that a uuid was assigned")
		assert.Equal(t, uuidDuringCreation, res.ID, "check that connector add ID and user response match")
	})

	t.Run("with an explicit (correct) ID set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		class := &models.Object{
			ID:    id,
			Class: "Foo",
		}
		vectorRepo.On("Exists", id).Return(false, nil).Once()

		res, err := manager.AddObject(ctx, nil, class)
		uuidDuringCreation := vectorRepo.Mock.Calls[1].Arguments.Get(0).(*models.Object).ID

		assert.Nil(t, err)
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

		vectorRepo.On("Exists", id).Return(true, nil).Once()

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

		vectorRepo.On("Exists", id).Return(false, nil).Once()

		_, err := manager.AddObject(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("invalid object: invalid UUID length: %d", len(id)), err)
	})
}
