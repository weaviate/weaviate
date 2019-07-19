//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

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
)

func Test_Add_Action(t *testing.T) {
	var (
		repo    *fakeRepo
		manager *Manager
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
		repo.On("AddAction", mock.Anything, mock.Anything).Return(nil).Once()
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		network := &fakeNetwork{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorRepo := &fakeVectorRepo{}
		manager = NewManager(repo, locks, schemaManager, network, cfg, logger, authorizer, vectorizer, vectorRepo)
	}

	t.Run("without an id set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		class := &models.Action{
			Class: "Foo",
		}

		res, err := manager.AddAction(ctx, nil, class)
		uuidDuringCreation := repo.Mock.Calls[0].Arguments.Get(1).(strfmt.UUID)

		assert.Nil(t, err)
		assert.Len(t, uuidDuringCreation, 36, "check that a uuid was assigned")
		assert.Equal(t, uuidDuringCreation, res.ID, "check that connector add ID and user response match")
	})

	t.Run("with an explicit (correct) ID set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		class := &models.Action{
			ID:    id,
			Class: "Foo",
		}
		repo.On("ClassExists", id).Return(false, nil).Once()

		res, err := manager.AddAction(ctx, nil, class)
		uuidDuringCreation := repo.Mock.Calls[1].Arguments.Get(1).(strfmt.UUID)

		assert.Nil(t, err)
		assert.Equal(t, id, uuidDuringCreation, "check that a uuid is the user specified one")
		assert.Equal(t, res.ID, uuidDuringCreation, "check that connector add ID and user response match")
	})

	t.Run("with a uuid that's already taken", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		class := &models.Action{
			ID:    id,
			Class: "Foo",
		}

		repo.On("ClassExists", id).Return(true, nil).Once()

		_, err := manager.AddAction(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("id '%s' already exists", id), err)
	})

	t.Run("with a uuid that's malformed", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-4FOOOOOOO2ae-bd52-ee09cb5f31cc")
		class := &models.Action{
			ID:    id,
			Class: "Foo",
		}

		repo.On("ClassExists", id).Return(false, nil).Once()

		_, err := manager.AddAction(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("invalid action: uuid: incorrect UUID length: %s", id), err)
	})
}

func Test_Add_Thing(t *testing.T) {
	var (
		repo    *fakeRepo
		manager *Manager
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
		repo.On("AddThing", mock.Anything, mock.Anything).Return(nil).Once()
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		network := &fakeNetwork{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorRepo := &fakeVectorRepo{}
		manager = NewManager(repo, locks, schemaManager, network, cfg, logger, authorizer, vectorizer, vectorRepo)
	}

	t.Run("without an id set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		class := &models.Thing{
			Class: "Foo",
		}

		res, err := manager.AddThing(ctx, nil, class)
		uuidDuringCreation := repo.Mock.Calls[0].Arguments.Get(1).(strfmt.UUID)

		assert.Nil(t, err)
		assert.Len(t, uuidDuringCreation, 36, "check that a uuid was assigned")
		assert.Equal(t, uuidDuringCreation, res.ID, "check that connector add ID and user response match")
	})

	t.Run("with an explicit (correct) ID set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		class := &models.Thing{
			ID:    id,
			Class: "Foo",
		}
		repo.On("ClassExists", id).Return(false, nil).Once()

		res, err := manager.AddThing(ctx, nil, class)
		uuidDuringCreation := repo.Mock.Calls[1].Arguments.Get(1).(strfmt.UUID)

		assert.Nil(t, err)
		assert.Equal(t, id, uuidDuringCreation, "check that a uuid is the user specified one")
		assert.Equal(t, res.ID, uuidDuringCreation, "check that connector add ID and user response match")
	})

	t.Run("with a uuid that's already taken", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		class := &models.Thing{
			ID:    id,
			Class: "Foo",
		}

		repo.On("ClassExists", id).Return(true, nil).Once()

		_, err := manager.AddThing(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("id '%s' already exists", id), err)
	})

	t.Run("with a uuid that's malformed", func(t *testing.T) {
		reset()

		ctx := context.Background()
		id := strfmt.UUID("5a1cd361-1e0d-4FOOOOOOO2ae-bd52-ee09cb5f31cc")
		class := &models.Thing{
			ID:    id,
			Class: "Foo",
		}

		repo.On("ClassExists", id).Return(false, nil).Once()

		_, err := manager.AddThing(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("invalid thing: uuid: incorrect UUID length: %s", id), err)
	})
}
