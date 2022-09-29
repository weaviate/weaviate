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

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	enthnsw "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCRUD_NoIndexProp(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger, _ := test.NewNullLogger()
	thingclass := &models.Class{
		Class:               "ThingClassWithNoIndexProps",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{{
			Name:         "stringProp",
			DataType:     []string{string(schema.DataTypeString)},
			Tokenization: "word",
		}, {
			Name:          "hiddenStringProp",
			DataType:      []string{string(schema.DataTypeString)},
			Tokenization:  "word",
			IndexInverted: ptBool(false),
		}},
	}
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		FlushIdleAfter:            60,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(repo, logger)

	t.Run("creating the thing class", func(t *testing.T) {
		require.Nil(t,
			migrator.AddClass(context.Background(), thingclass, schemaGetter.shardState))

		// update schema getter so it's in sync with class
		schemaGetter.schema = schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{thingclass},
			},
		}
	})

	thingID := strfmt.UUID("9f119c4f-80da-4ae5-bfd1-e4b63054125f")

	t.Run("adding a thing", func(t *testing.T) {
		thing := &models.Object{
			CreationTimeUnix:   1565612833955,
			LastUpdateTimeUnix: 1000001,
			ID:                 thingID,
			Class:              "ThingClassWithNoIndexProps",
			Properties: map[string]interface{}{
				"stringProp":       "some value",
				"hiddenStringProp": "some hidden value",
			},
		}
		vector := []float32{1, 3, 5, 0.4}
		err := repo.PutObject(context.Background(), thing, vector)

		assert.Nil(t, err)
	})

	t.Run("all props are present when getting by id", func(t *testing.T) {
		res, err := repo.ObjectByID(context.Background(), thingID,
			search.SelectProperties{}, additional.Properties{})
		expectedSchema := map[string]interface{}{
			"stringProp":       "some value",
			"hiddenStringProp": "some hidden value",
			"id":               thingID,
		}

		require.Nil(t, err)
		assert.Equal(t, expectedSchema, res.Schema)
	})

	// Same as above, but with Object()
	t.Run("all props are present when getting by id and class", func(t *testing.T) {
		res, err := repo.Object(context.Background(), "ThingClassWithNoIndexProps", thingID,
			search.SelectProperties{}, additional.Properties{})
		expectedSchema := map[string]interface{}{
			"stringProp":       "some value",
			"hiddenStringProp": "some hidden value",
			"id":               thingID,
		}

		require.Nil(t, err)
		assert.Equal(t, expectedSchema, res.Schema)
	})

	t.Run("class search on the noindex prop errors", func(t *testing.T) {
		_, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName: "ThingClassWithNoIndexProps",
			Pagination: &filters.Pagination{
				Limit: 10,
			},
			Filters: buildFilter("hiddenStringProp", "hidden", eq, dtString),
		})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(),
			"bucket for prop hiddenStringProp not found - is it indexed?")
	})

	t.Run("class search on timestamp prop with no timestamp indexing error", func(t *testing.T) {
		_, err := repo.ClassSearch(context.Background(), traverser.GetParams{
			ClassName: "ThingClassWithNoIndexProps",
			Pagination: &filters.Pagination{
				Limit: 10,
			},
			Filters: buildFilter("_creationTimeUnix", "1234567891011", eq, dtString),
		})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(),
			"timestamps must be indexed to be filterable! "+
				"add `indexTimestaps: true` to the invertedIndexConfig")
	})
}

func ptBool(in bool) *bool {
	return &in
}
