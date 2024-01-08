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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func Test_UpdateAction(t *testing.T) {
	var (
		db              *fakeVectorRepo
		modulesProvider *fakeModulesProvider
		manager         *Manager
		extender        *fakeExtender
		projectorFake   *fakeProjector
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "ActionClass",
					VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
					Properties: []*models.Property{
						{
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
							Name:         "foo",
						},
					},
				},
			},
		},
	}

	reset := func() {
		db = &fakeVectorRepo{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{}
		cfg.Config.QueryDefaults.Limit = 20
		cfg.Config.QueryMaximumResults = 200
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		extender = &fakeExtender{}
		projectorFake = &fakeProjector{}
		metrics := &fakeMetrics{}
		modulesProvider = getFakeModulesProviderWithCustomExtenders(extender, projectorFake)
		manager = NewManager(locks, schemaManager, cfg,
			logger, authorizer, db, modulesProvider, metrics)
	}

	t.Run("ensure creation timestamp persists", func(t *testing.T) {
		reset()

		beforeUpdate := time.Now().UnixNano() / int64(time.Millisecond)
		id := strfmt.UUID("34e9df15-0c3b-468d-ab99-f929662834c7")
		vec := []float32{0, 1, 2}

		result := &search.Result{
			ID:        id,
			ClassName: "ActionClass",
			Schema:    map[string]interface{}{"foo": "bar"},
			Created:   beforeUpdate,
			Updated:   beforeUpdate,
		}
		db.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()
		modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
			Return(vec, nil)
		db.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()

		payload := &models.Object{
			Class:      "ActionClass",
			ID:         id,
			Properties: map[string]interface{}{"foo": "baz"},
		}
		res, err := manager.UpdateObject(context.Background(), &models.Principal{}, "", id, payload, nil)
		require.Nil(t, err)
		expected := &models.Object{
			Class:            "ActionClass",
			ID:               id,
			Properties:       map[string]interface{}{"foo": "baz"},
			CreationTimeUnix: beforeUpdate,
		}

		afterUpdate := time.Now().UnixNano() / int64(time.Millisecond)

		assert.Equal(t, expected.Class, res.Class)
		assert.Equal(t, expected.ID, res.ID)
		assert.Equal(t, expected.Properties, res.Properties)
		assert.Equal(t, expected.CreationTimeUnix, res.CreationTimeUnix)
		assert.GreaterOrEqual(t, res.LastUpdateTimeUnix, beforeUpdate)
		assert.LessOrEqual(t, res.LastUpdateTimeUnix, afterUpdate)
	})
}

func Test_UpdateObject(t *testing.T) {
	var (
		cls          = "MyClass"
		id           = strfmt.UUID("34e9df15-0c3b-468d-ab99-f929662834c7")
		beforeUpdate = (time.Now().UnixNano() - 2*int64(time.Millisecond)) / int64(time.Millisecond)
		vec          = []float32{0, 1, 2}
		anyErr       = errors.New("any error")
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             cls,
					VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
					Properties: []*models.Property{
						{
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
							Name:         "foo",
						},
					},
				},
			},
		},
	}

	m := newFakeGetManager(schema)
	payload := &models.Object{
		Class:      cls,
		ID:         id,
		Properties: map[string]interface{}{"foo": "baz"},
	}
	// the object might not exist
	m.repo.On("Object", cls, id, mock.Anything, mock.Anything, "").Return(nil, anyErr).Once()
	_, err := m.UpdateObject(context.Background(), &models.Principal{}, cls, id, payload, nil)
	if err == nil {
		t.Fatalf("must return an error if object() fails")
	}

	result := &search.Result{
		ID:        id,
		ClassName: cls,
		Schema:    map[string]interface{}{"foo": "bar"},
		Created:   beforeUpdate,
		Updated:   beforeUpdate,
	}
	m.repo.On("Object", cls, id, mock.Anything, mock.Anything, "").Return(result, nil).Once()
	m.modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
		Return(vec, nil)
	m.repo.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()

	expected := &models.Object{
		Class:            cls,
		ID:               id,
		Properties:       map[string]interface{}{"foo": "baz"},
		CreationTimeUnix: beforeUpdate,
		Vector:           vec,
	}
	res, err := m.UpdateObject(context.Background(), &models.Principal{}, cls, id, payload, nil)
	require.Nil(t, err)
	if res.LastUpdateTimeUnix <= beforeUpdate {
		t.Error("time after update must be greater than time before update ")
	}
	res.LastUpdateTimeUnix = 0 // to allow for equality
	assert.Equal(t, expected, res)
}
