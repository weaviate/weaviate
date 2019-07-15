/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package kinds

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
		repo    *fakeRepo
		manager *BatchManager
	)

	schema := schema.Schema{
		Actions: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
				},
			},
		},
	}

	reset := func() {
		repo = &fakeRepo{}
		config := &config.WeaviateConfig{}
		locks := &fakeLocks{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		logger, _ := test.NewNullLogger()
		authorizer := &fakeAuthorizer{}
		manager = NewBatchManager(repo, locks, schemaManager, nil,
			config, logger, authorizer)
	}

	ctx := context.Background()

	t.Run("without any actions", func(t *testing.T) {
		reset()
		expectedErr := NewErrInvalidUserInput("invalid param 'actions': cannot be empty, need at least" +
			" one action for batching")

		_, err := manager.AddActions(ctx, nil, []*models.Action{}, []*string{})

		assert.Equal(t, expectedErr, err)
	})

	t.Run("with actions without IDs", func(t *testing.T) {
		reset()
		repo.On("AddActionsBatch", mock.Anything).Return(nil).Once()
		actions := []*models.Action{
			&models.Action{
				Class: "Foo",
			},
			&models.Action{
				Class: "Foo",
			},
		}

		_, err := manager.AddActions(ctx, nil, actions, []*string{})
		repoCalledWithActions := repo.Calls[0].Arguments[0].(BatchActions)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithActions, 2)
		assert.Len(t, repoCalledWithActions[0].UUID, 36, "a uuid was set for the first action")
		assert.Len(t, repoCalledWithActions[1].UUID, 36, "a uuid was set for the second action")
	})

	t.Run("with user-specified IDs", func(t *testing.T) {
		reset()
		repo.On("AddActionsBatch", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("2d3942c3-b412-4d80-9dfa-99a646629cd2")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		actions := []*models.Action{
			&models.Action{
				ID:    id1,
				Class: "Foo",
			},
			&models.Action{
				ID:    id2,
				Class: "Foo",
			},
		}

		_, err := manager.AddActions(ctx, nil, actions, []*string{})
		repoCalledWithActions := repo.Calls[0].Arguments[0].(BatchActions)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithActions, 2)
		assert.Equal(t, id1, repoCalledWithActions[0].UUID, "the user-specified uuid was used")
		assert.Equal(t, id2, repoCalledWithActions[1].UUID, "the user-specified uuid was used")
	})

	t.Run("with an invalid user-specified IDs", func(t *testing.T) {
		reset()
		repo.On("AddActionsBatch", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("invalid")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		actions := []*models.Action{
			&models.Action{
				ID:    id1,
				Class: "Foo",
			},
			&models.Action{
				ID:    id2,
				Class: "Foo",
			},
		}

		_, err := manager.AddActions(ctx, nil, actions, []*string{})
		repoCalledWithActions := repo.Calls[0].Arguments[0].(BatchActions)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithActions, 2)
		assert.Equal(t, repoCalledWithActions[0].Err.Error(), "uuid: incorrect UUID length: invalid")
		assert.Equal(t, id2, repoCalledWithActions[1].UUID, "the user-specified uuid was used")
	})
}

func Test_BatchManager_AddThings(t *testing.T) {
	var (
		repo    *fakeRepo
		manager *BatchManager
	)

	schema := schema.Schema{
		Things: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
				},
			},
		},
	}

	reset := func() {
		repo = &fakeRepo{}
		config := &config.WeaviateConfig{}
		locks := &fakeLocks{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		logger, _ := test.NewNullLogger()
		authorizer := &fakeAuthorizer{}
		manager = NewBatchManager(repo, locks, schemaManager, nil,
			config, logger, authorizer)
	}

	ctx := context.Background()

	t.Run("without any things", func(t *testing.T) {
		reset()
		expectedErr := NewErrInvalidUserInput("invalid param 'things': cannot be empty, need at least" +
			" one thing for batching")

		_, err := manager.AddThings(ctx, nil, []*models.Thing{}, []*string{})

		assert.Equal(t, expectedErr, err)
	})

	t.Run("with things without IDs", func(t *testing.T) {
		reset()
		repo.On("AddThingsBatch", mock.Anything).Return(nil).Once()
		things := []*models.Thing{
			&models.Thing{
				Class: "Foo",
			},
			&models.Thing{
				Class: "Foo",
			},
		}

		_, err := manager.AddThings(ctx, nil, things, []*string{})
		repoCalledWithThings := repo.Calls[0].Arguments[0].(BatchThings)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithThings, 2)
		assert.Len(t, repoCalledWithThings[0].UUID, 36, "a uuid was set for the first thing")
		assert.Len(t, repoCalledWithThings[1].UUID, 36, "a uuid was set for the second thing")
	})

	t.Run("with user-specified IDs", func(t *testing.T) {
		reset()
		repo.On("AddThingsBatch", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("2d3942c3-b412-4d80-9dfa-99a646629cd2")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		things := []*models.Thing{
			&models.Thing{
				ID:    id1,
				Class: "Foo",
			},
			&models.Thing{
				ID:    id2,
				Class: "Foo",
			},
		}

		_, err := manager.AddThings(ctx, nil, things, []*string{})
		repoCalledWithThings := repo.Calls[0].Arguments[0].(BatchThings)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithThings, 2)
		assert.Equal(t, id1, repoCalledWithThings[0].UUID, "the user-specified uuid was used")
		assert.Equal(t, id2, repoCalledWithThings[1].UUID, "the user-specified uuid was used")
	})

	t.Run("with an invalid user-specified IDs", func(t *testing.T) {
		reset()
		repo.On("AddThingsBatch", mock.Anything).Return(nil).Once()
		id1 := strfmt.UUID("invalid")
		id2 := strfmt.UUID("cf918366-3d3b-4b90-9bc6-bc5ea8762ff6")
		things := []*models.Thing{
			&models.Thing{
				ID:    id1,
				Class: "Foo",
			},
			&models.Thing{
				ID:    id2,
				Class: "Foo",
			},
		}

		_, err := manager.AddThings(ctx, nil, things, []*string{})
		repoCalledWithThings := repo.Calls[0].Arguments[0].(BatchThings)

		assert.Nil(t, err)
		require.Len(t, repoCalledWithThings, 2)
		assert.Equal(t, repoCalledWithThings[0].Err.Error(), "uuid: incorrect UUID length: invalid")
		assert.Equal(t, id2, repoCalledWithThings[1].UUID, "the user-specified uuid was used")
	})
}
