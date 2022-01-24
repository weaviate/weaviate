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
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_Delete_Action(t *testing.T) {
	var (
		manager    *Manager
		vectorRepo *fakeVectorRepo
	)

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		vectorRepo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything).Return(&search.Result{
			ClassName: "MyAction",
		}, nil).Once()
		schemaManager := &fakeSchemaManager{}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vecProvider := &fakeVectorizerProvider{vectorizer}
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer, vecProvider,
			vectorRepo, getFakeModulesProvider())
	}

	reset()

	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")

	vectorRepo.On("DeleteObject", "MyAction", id).Return(nil).Once()

	ctx := context.Background()
	err := manager.DeleteObject(ctx, nil, id)

	assert.Nil(t, err)

	vectorRepo.AssertExpectations(t)
}

func Test_Delete_Thing(t *testing.T) {
	var (
		manager    *Manager
		vectorRepo *fakeVectorRepo
	)

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		vectorRepo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything).Return(&search.Result{
			ClassName: "MyThing",
		}, nil).Once()
		schemaManager := &fakeSchemaManager{}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		vectorizer := &fakeVectorizer{}
		vecProvider := &fakeVectorizerProvider{vectorizer}
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer, vecProvider,
			vectorRepo, getFakeModulesProvider())
	}

	reset()

	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")

	vectorRepo.On("DeleteObject", "MyThing", id).Return(nil).Once()

	ctx := context.Background()
	err := manager.DeleteObject(ctx, nil, id)

	assert.Nil(t, err)

	vectorRepo.AssertExpectations(t)
}
