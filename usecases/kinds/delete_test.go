package kinds

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func Test_Delete_Action(t *testing.T) {
	var (
		repo       *fakeRepo
		manager    *Manager
		vectorRepo *fakeVectorRepo
	)

	reset := func() {
		repo = &fakeRepo{
			GetActionResponse: &models.Action{
				Class: "MyAction",
			},
		}
		schemaManager := &fakeSchemaManager{}
		locks := &fakeLocks{}
		network := &fakeNetwork{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorRepo = &fakeVectorRepo{}
		manager = NewManager(repo, locks, schemaManager, network, cfg, logger, authorizer, vectorizer, vectorRepo)
	}

	reset()

	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")

	repo.On("DeleteAction", (*models.Action)(nil), id).Return(nil).Once()
	vectorRepo.On("DeleteAction", "MyAction", id).Return(nil).Once()

	ctx := context.Background()
	err := manager.DeleteAction(ctx, nil, id)

	assert.Nil(t, err)

	repo.AssertExpectations(t)
	vectorRepo.AssertExpectations(t)
}

func Test_Delete_Thing(t *testing.T) {
	var (
		repo       *fakeRepo
		manager    *Manager
		vectorRepo *fakeVectorRepo
	)

	reset := func() {
		repo = &fakeRepo{
			GetThingResponse: &models.Thing{
				Class: "MyThing",
			},
		}
		schemaManager := &fakeSchemaManager{}
		locks := &fakeLocks{}
		network := &fakeNetwork{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vectorRepo = &fakeVectorRepo{}
		manager = NewManager(repo, locks, schemaManager, network, cfg, logger, authorizer, vectorizer, vectorRepo)
	}

	reset()

	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")

	repo.On("DeleteThing", (*models.Thing)(nil), id).Return(nil).Once()
	vectorRepo.On("DeleteThing", "MyThing", id).Return(nil).Once()

	ctx := context.Background()
	err := manager.DeleteThing(ctx, nil, id)

	assert.Nil(t, err)

	repo.AssertExpectations(t)
	vectorRepo.AssertExpectations(t)
}
