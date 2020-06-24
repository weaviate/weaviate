//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

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
		vectorRepo.On("ActionByID", mock.Anything, mock.Anything, mock.Anything).Return(&search.Result{
			ClassName: "MyAction",
		}, nil).Once()
		schemaManager := &fakeSchemaManager{}
		locks := &fakeLocks{}
		network := &fakeNetwork{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		vectorizer := &fakeVectorizer{}
		manager = NewManager(locks, schemaManager, network, cfg, logger, authorizer, vectorizer, vectorRepo, extender)
	}

	reset()

	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")

	vectorRepo.On("DeleteAction", "MyAction", id).Return(nil).Once()

	ctx := context.Background()
	err := manager.DeleteAction(ctx, nil, id)

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
		vectorRepo.On("ThingByID", mock.Anything, mock.Anything, mock.Anything).Return(&search.Result{
			ClassName: "MyThing",
		}, nil).Once()
		schemaManager := &fakeSchemaManager{}
		locks := &fakeLocks{}
		network := &fakeNetwork{}
		cfg := &config.WeaviateConfig{}
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		vectorizer := &fakeVectorizer{}
		manager = NewManager(locks, schemaManager, network, cfg, logger, authorizer, vectorizer, vectorRepo, extender)
	}

	reset()

	id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")

	vectorRepo.On("DeleteThing", "MyThing", id).Return(nil).Once()

	ctx := context.Background()
	err := manager.DeleteThing(ctx, nil, id)

	assert.Nil(t, err)

	vectorRepo.AssertExpectations(t)
}
