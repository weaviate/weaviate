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
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_HeadObject(t *testing.T) {
	var (
		manager    *Manager
		vectorRepo *fakeVectorRepo
	)

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
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

	t.Run("object exists", func(t *testing.T) {
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31cc")
		vectorRepo.On("Exists", id).Return(true, nil).Once()

		exists, err := manager.HeadObject(context.Background(), nil, id)

		assert.Nil(t, err)
		assert.True(t, exists)

		vectorRepo.AssertExpectations(t)
	})

	t.Run("object doesn't exist", func(t *testing.T) {
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31ee")
		vectorRepo.On("Exists", id).Return(false, nil).Once()

		exists, err := manager.HeadObject(context.Background(), nil, id)

		assert.Nil(t, err)
		assert.False(t, exists)

		vectorRepo.AssertExpectations(t)
	})

	t.Run("should throw an error", func(t *testing.T) {
		id := strfmt.UUID("5a1cd361-1e0d-42ae-bd52-ee09cb5f31dd")
		throwErr := errors.New("something went wrong")
		vectorRepo.On("Exists", id).Return(false, throwErr).Once()

		exists, err := manager.HeadObject(context.Background(), nil, id)

		require.NotNil(t, err)
		assert.EqualError(t, err, "could not check object's existence: something went wrong")
		assert.False(t, exists)

		vectorRepo.AssertExpectations(t)
	})
}
