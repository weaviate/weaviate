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
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_DeleteObjectsWithSameId(t *testing.T) {
	var (
		cls = "MyClass"
		id  = strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	)

	manager, vectorRepo := newDeleteDependency()
	vectorRepo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything).Return(&search.Result{
		ClassName: cls,
	}, nil).Once()
	vectorRepo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Once()
	vectorRepo.On("DeleteObject", cls, id, mock.Anything).Return(nil).Once()

	err := manager.DeleteObject(context.Background(), nil, "", id, nil, "")
	assert.Nil(t, err)
	vectorRepo.AssertExpectations(t)
}

func Test_DeleteObject(t *testing.T) {
	var (
		cls         = "MyClass"
		id          = strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		errNotFound = errors.New("object not found")
	)

	manager, repo := newDeleteDependency()

	repo.On("DeleteObject", cls, id, mock.Anything).Return(nil).Once()
	err := manager.DeleteObject(context.Background(), nil, cls, id, nil, "")
	assert.Nil(t, err)
	repo.AssertExpectations(t)

	// return internal error if deleteObject() fails
	repo.On("DeleteObject", cls, id, mock.Anything).Return(errNotFound).Once()
	err = manager.DeleteObject(context.Background(), nil, cls, id, nil, "")
	if !errors.As(err, &ErrInternal{}) {
		t.Errorf("error type got: %T want: ErrInternal", err)
	}
	repo.AssertExpectations(t)
}

func newDeleteDependency() (*Manager, *fakeVectorRepo) {
	vectorRepo := new(fakeVectorRepo)
	logger, _ := test.NewNullLogger()
	manager := NewManager(
		new(fakeSchemaManager),
		new(config.WeaviateConfig),
		logger,
		mocks.NewMockAuthorizer(),
		vectorRepo,
		getFakeModulesProvider(),
		new(fakeMetrics), nil,
		NewAutoSchemaManager(new(fakeSchemaManager), vectorRepo, new(config.WeaviateConfig), mocks.NewMockAuthorizer(), logger, prometheus.NewPedanticRegistry()))
	return manager, vectorRepo
}
