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

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestCRUD_NoIndexProp(t *testing.T) {
	dirName := t.TempDir()

	vFalse := false
	logger, _ := test.NewNullLogger()
	thingclass := &models.Class{
		Class:               "ThingClassWithNoIndexProps",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{{
			Name:         "stringProp",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		}, {
			Name:            "hiddenStringProp",
			DataType:        schema.DataTypeText.PropString(),
			Tokenization:    models.PropertyTokenizationWhitespace,
			IndexFilterable: &vFalse,
			IndexSearchable: &vFalse,
		}},
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		MemtablesFlushIdleAfter:   60,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())

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
		err := repo.PutObject(context.Background(), thing, vector, nil)

		assert.Nil(t, err)
	})

	t.Run("all props are present when getting by id", func(t *testing.T) {
		res, err := repo.ObjectByID(context.Background(), thingID, search.SelectProperties{}, additional.Properties{}, "")
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
			search.SelectProperties{}, additional.Properties{}, nil, "")
		expectedSchema := map[string]interface{}{
			"stringProp":       "some value",
			"hiddenStringProp": "some hidden value",
			"id":               thingID,
		}

		require.Nil(t, err)
		assert.Equal(t, expectedSchema, res.Schema)
	})

	t.Run("class search on the noindex prop errors", func(t *testing.T) {
		_, err := repo.Search(context.Background(), dto.GetParams{
			ClassName: "ThingClassWithNoIndexProps",
			Pagination: &filters.Pagination{
				Limit: 10,
			},
			Filters: buildFilter("hiddenStringProp", "hidden", eq, schema.DataTypeText),
		})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "Filtering by property 'hiddenStringProp' requires inverted index. "+
			"Is `indexFilterable` option of property 'hiddenStringProp' enabled? "+
			"Set it to `true` or leave empty")
	})

	t.Run("class search on timestamp prop with no timestamp indexing error", func(t *testing.T) {
		_, err := repo.Search(context.Background(), dto.GetParams{
			ClassName: "ThingClassWithNoIndexProps",
			Pagination: &filters.Pagination{
				Limit: 10,
			},
			Filters: buildFilter("_creationTimeUnix", "1234567891011", eq, schema.DataTypeText),
		})

		require.NotNil(t, err)
		assert.Contains(t, err.Error(),
			"Timestamps must be indexed to be filterable! Add `IndexTimestamps: true` to the InvertedIndexConfig in")
	})
}
