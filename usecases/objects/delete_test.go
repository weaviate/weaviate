//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_DeleteObjectsWithSameId(t *testing.T) {
	var (
		cls = "MyClass"
		id  = strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
	)

	manager, vectorRepo, _, _ := newDeleteDependency()
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

	manager, repo, _, _ := newDeleteDependency()

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

// TestDeleteObject_RbacResolveAlias is to make sure alias is resolved to correct
// collection before doing RBAC check on original class.
func TestDeleteObject_RbacResolveAlias(t *testing.T) {
	manager, repo, auth, schema := newDeleteDependency()

	var (
		class = "SomeClass"
		alias = "SomeAlias"
		id    = strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		ctx   = context.Background()
	)

	repo.On("DeleteObject", class, id, mock.Anything).Return(nil).Once()

	// we mock `resolveAlias`.
	// And we make sure the "resource" name we got in rbac's Authorize is
	// what returned from `resolveAlias`, so that we can confirm, resolveAlias
	// always happens before rbac authorize.
	schema.resolveAliasTo = class

	err := manager.DeleteObject(ctx, nil, alias, id, nil, "")
	require.NoError(t, err)
	assert.Len(t, auth.Calls(), 1)
	assert.Contains(t, auth.Calls()[0].Resources[0], class) // make sure rbac is called with "resolved class" name
}

func newDeleteDependency() (*Manager, *fakeVectorRepo, *mocks.FakeAuthorizer, *fakeSchemaManager) {
	vectorRepo := new(fakeVectorRepo)
	logger, _ := test.NewNullLogger()
	authorizer := mocks.NewMockAuthorizer()
	smanager := new(fakeSchemaManager)
	manager := NewManager(
		smanager,
		new(config.WeaviateConfig),
		logger,
		authorizer,
		vectorRepo,
		getFakeModulesProvider(),
		new(fakeMetrics), nil,
		NewAutoSchemaManager(new(fakeSchemaManager), vectorRepo, new(config.WeaviateConfig), mocks.NewMockAuthorizer(), logger, prometheus.NewPedanticRegistry()))
	return manager, vectorRepo, authorizer, smanager
}
