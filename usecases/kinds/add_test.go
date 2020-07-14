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
		vectorRepo *fakeVectorRepo
		manager    *Manager
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
		vectorRepo = &fakeVectorRepo{}
		vectorRepo.On("PutAction", mock.Anything, mock.Anything).Return(nil).Once()
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		network := &fakeNetwork{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		vectorizer := &fakeVectorizer{}
		vectorizer.On("Action", mock.Anything).Return([]float32{0, 1, 2}, nil)
		manager = NewManager(locks, schemaManager, network, cfg, logger, authorizer, vectorizer, vectorRepo, extender, projector)
	}

	t.Run("without an id set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		class := &models.Action{
			Class: "Foo",
		}

		res, err := manager.AddAction(ctx, nil, class)
		uuidDuringCreation := vectorRepo.Mock.Calls[0].Arguments.Get(0).(*models.Action).ID

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
		vectorRepo.On("Exists", id).Return(false, nil).Once()

		res, err := manager.AddAction(ctx, nil, class)
		uuidDuringCreation := vectorRepo.Mock.Calls[1].Arguments.Get(0).(*models.Action).ID

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

		vectorRepo.On("Exists", id).Return(true, nil).Once()

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

		vectorRepo.On("Exists", id).Return(false, nil).Once()

		_, err := manager.AddAction(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("invalid action: uuid: incorrect UUID length: %s", id), err)
	})
}

func Test_Add_Thing(t *testing.T) {
	var (
		vectorRepo *fakeVectorRepo
		manager    *Manager
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
		vectorRepo = &fakeVectorRepo{}
		vectorRepo.On("PutThing", mock.Anything, mock.Anything).Return(nil).Once()
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		network := &fakeNetwork{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		vectorizer := &fakeVectorizer{}
		vectorizer.On("Thing", mock.Anything).Return([]float32{0, 1, 2}, nil)
		manager = NewManager(locks, schemaManager, network, cfg, logger, authorizer, vectorizer, vectorRepo, extender, projector)
	}

	t.Run("without an id set", func(t *testing.T) {
		reset()

		ctx := context.Background()
		class := &models.Thing{
			Class: "Foo",
		}

		res, err := manager.AddThing(ctx, nil, class)
		uuidDuringCreation := vectorRepo.Mock.Calls[0].Arguments.Get(0).(*models.Thing).ID

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
		vectorRepo.On("Exists", id).Return(false, nil).Once()

		res, err := manager.AddThing(ctx, nil, class)
		uuidDuringCreation := vectorRepo.Mock.Calls[1].Arguments.Get(0).(*models.Thing).ID

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

		vectorRepo.On("Exists", id).Return(true, nil).Once()

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

		vectorRepo.On("Exists", id).Return(false, nil).Once()

		_, err := manager.AddThing(ctx, nil, class)
		assert.Equal(t, NewErrInvalidUserInput("invalid thing: uuid: incorrect UUID length: %s", id), err)
	})
}
