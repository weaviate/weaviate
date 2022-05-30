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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_UpdateAction(t *testing.T) {
	var (
		db            *fakeVectorRepo
		vectorizer    *fakeVectorizer
		manager       *Manager
		extender      *fakeExtender
		projectorFake *fakeProjector
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "ActionClass",
					VectorIndexConfig: hnsw.NewDefaultUserConfig(),
					Properties: []*models.Property{
						{
							DataType: []string{"string"},
							Name:     "foo",
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
		vectorizer = &fakeVectorizer{}
		vecProvider := &fakeVectorizerProvider{vectorizer}
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer,
			vecProvider, db, getFakeModulesProviderWithCustomExtenders(extender, projectorFake))
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
		vectorizer.On("UpdateObject", mock.Anything).Return(vec, nil).Once()
		db.On("PutObject", mock.Anything, mock.Anything).Return(nil).Once()

		payload := &models.Object{
			Class:      "ActionClass",
			ID:         id,
			Properties: map[string]interface{}{"foo": "baz"},
		}
		res, err := manager.UpdateObject(context.Background(), &models.Principal{}, id, payload)
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
