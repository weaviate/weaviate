//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_BatchManager_AddActions(t *testing.T) {
	var (
		vectorRepo *fakeVectorRepo
		manager    *BatchManager
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
				},
			},
		},
	}

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		config := &config.WeaviateConfig{}
		locks := &fakeLocks{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		logger, _ := test.NewNullLogger()
		authorizer := &fakeAuthorizer{}
		vectorizer := &fakeVectorizer{}
		vectorizer.On("Object", mock.Anything).Return([]float32{0, 1, 2}, nil)
		manager = NewBatchManager(vectorRepo, vectorizer, locks,
			schemaManager, config, logger, authorizer)
	}

	ctx := context.Background()

	t.Run("without any actions", func(t *testing.T) {
		reset()
		expectedErr := NewErrInvalidUserInput("invalid param 'objects': cannot be empty, need at least" +
			" one object for batching")

		_, err := manager.AddObjects(ctx, nil, []*models.Object{}, []*string{})

		assert.Equal(t, expectedErr, err)
	})

	t.Run("with actions without IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		actions := []*models.Object{
			&models.Object{
				Class: "Foo",
			},
			&models.Object{
				Class: "Foo",
			},
		}

		_, err := manager.AddObjects(ctx, nil, actions, []*string{})
		repoCalledWithActions := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithActions, 2)
		assert.Len(t, repoCalledWithActions[0].UUID, 36, "a uuid was set for the first action")
		assert.Len(t, repoCalledWithActions[1].UUID, 36, "a uuid was set for the second action")
	})

	t.Run("with user-specified IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("2d3942c3-b412-4d80-9dfa-99a646629cd2")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		actions := []*models.Object{
			&models.Object{
				ID:    id1,
				Class: "Foo",
			},
			&models.Object{
				ID:    id2,
				Class: "Foo",
			},
		}

		_, err := manager.AddObjects(ctx, nil, actions, []*string{})
		repoCalledWithActions := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithActions, 2)
		assert.Equal(t, id1, repoCalledWithActions[0].UUID, "the user-specified uuid was used")
		assert.Equal(t, id2, repoCalledWithActions[1].UUID, "the user-specified uuid was used")
	})

	t.Run("with an invalid user-specified IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("invalid")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		actions := []*models.Object{
			&models.Object{
				ID:    id1,
				Class: "Foo",
			},
			&models.Object{
				ID:    id2,
				Class: "Foo",
			},
		}

		_, err := manager.AddObjects(ctx, nil, actions, []*string{})
		repoCalledWithActions := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithActions, 2)
		assert.Equal(t, repoCalledWithActions[0].Err.Error(), "uuid: incorrect UUID length: invalid")
		assert.Equal(t, id2, repoCalledWithActions[1].UUID, "the user-specified uuid was used")
	})
}

func Test_BatchManager_AddThings(t *testing.T) {
	var (
		vectorRepo *fakeVectorRepo
		manager    *BatchManager
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
				},
			},
		},
	}

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		config := &config.WeaviateConfig{}
		locks := &fakeLocks{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		logger, _ := test.NewNullLogger()
		authorizer := &fakeAuthorizer{}
		vectorizer := &fakeVectorizer{}
		vectorizer.On("Object", mock.Anything).Return([]float32{0, 1, 2}, nil)
		manager = NewBatchManager(vectorRepo, vectorizer,
			locks, schemaManager, config, logger, authorizer)
	}

	ctx := context.Background()

	t.Run("without any objects", func(t *testing.T) {
		reset()
		expectedErr := NewErrInvalidUserInput("invalid param 'objects': cannot be empty, need at least" +
			" one object for batching")

		_, err := manager.AddObjects(ctx, nil, []*models.Object{}, []*string{})

		assert.Equal(t, expectedErr, err)
	})

	t.Run("with objects without IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		objects := []*models.Object{
			&models.Object{
				Class: "Foo",
			},
			&models.Object{
				Class: "Foo",
			},
		}

		_, err := manager.AddObjects(ctx, nil, objects, []*string{})
		vectorRepoCalledWithObjects := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, vectorRepoCalledWithObjects, 2)
		assert.Len(t, vectorRepoCalledWithObjects[0].UUID, 36, "a uuid was set for the first object")
		assert.Len(t, vectorRepoCalledWithObjects[1].UUID, 36, "a uuid was set for the second object")
	})

	t.Run("with user-specified IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("2d3942c3-b412-4d80-9dfa-99a646629cd2")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		things := []*models.Object{
			&models.Object{
				ID:    id1,
				Class: "Foo",
			},
			&models.Object{
				ID:    id2,
				Class: "Foo",
			},
		}

		_, err := manager.AddObjects(ctx, nil, things, []*string{})
		vectorRepoCalledWithThings := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, vectorRepoCalledWithThings, 2)
		assert.Equal(t, id1, vectorRepoCalledWithThings[0].UUID, "the user-specified uuid was used")
		assert.Equal(t, id2, vectorRepoCalledWithThings[1].UUID, "the user-specified uuid was used")
	})

	t.Run("with an invalid user-specified IDs", func(t *testing.T) {
		reset()
		vectorRepo.On("BatchPutObjects", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("invalid")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		things := []*models.Object{
			&models.Object{
				ID:    id1,
				Class: "Foo",
			},
			&models.Object{
				ID:    id2,
				Class: "Foo",
			},
		}

		_, err := manager.AddObjects(ctx, nil, things, []*string{})
		vectorRepoCalledWithThings := vectorRepo.Calls[0].Arguments[0].(BatchObjects)

		assert.Nil(t, err)
		require.Len(t, vectorRepoCalledWithThings, 2)
		assert.Equal(t, vectorRepoCalledWithThings[0].Err.Error(), "uuid: incorrect UUID length: invalid")
		assert.Equal(t, id2, vectorRepoCalledWithThings[1].UUID, "the user-specified uuid was used")
	})
}
