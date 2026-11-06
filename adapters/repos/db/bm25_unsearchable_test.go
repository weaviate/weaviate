//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestBM25ErrorsWhenIndexSearchableDisabled is the #5888 reproduction:
// a collection has a searchable title and a text property with
// indexSearchable disabled. BM25 without a property list used to search
// only title and return nothing for a query that lives on text.
func TestBM25ErrorsWhenIndexSearchableDisabled(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()
	repo, schemaGetter := newBM25BlockTestRepo(t, logger)
	defer repo.Shutdown(ctx)

	off, on := false, true
	class := &models.Class{
		Class:               "TestCollection",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
		Properties: []*models.Property{
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &on,
				IndexSearchable: &on,
			},
			{
				Name:            "text",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &on,
				IndexSearchable: &off,
			},
		},
	}
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
	require.NoError(t, NewMigrator(repo, logger, "node1").AddClass(ctx, class))

	for i := 0; i < 2; i++ {
		obj := &models.Object{
			Class: class.Class,
			ID:    strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String()),
			Properties: map[string]interface{}{
				"title": fmt.Sprintf("Title %d", i),
				"text":  fmt.Sprintf("Text %d", i),
			},
		}
		require.NoError(t, repo.PutObject(ctx, obj, []float32{1, 3, 5, 0.4}, nil, nil, nil, 0))
	}

	idx := repo.GetIndex(schema.ClassName(class.Class))
	require.NotNil(t, idx)
	addit := additional.Properties{}

	search := func(props []string, query string) ([]*storobj.Object, error) {
		t.Helper()
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: props, Query: query}
		res, _, err := idx.objectSearch(ctx, 5, nil, kwr, nil, nil, addit, nil, "", 0, nil)
		return res, err
	}

	t.Run("no properties", func(t *testing.T) {
		_, err := search(nil, "text")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "indexSearchable")
		assert.Contains(t, err.Error(), `"text"`)
	})

	t.Run("explicit disabled property", func(t *testing.T) {
		_, err := search([]string{"text"}, "text")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "indexSearchable")
		assert.Contains(t, err.Error(), "text")
	})

	t.Run("boosted disabled property names the property without the boost", func(t *testing.T) {
		_, err := search([]string{"text^2"}, "text")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "indexSearchable")
		assert.NotContains(t, err.Error(), "text^2")
	})

	t.Run("explicit searchable property still matches", func(t *testing.T) {
		res, err := search([]string{"title"}, "title")
		require.NoError(t, err)
		require.NotEmpty(t, res)
	})

	t.Run("unknown property is not reported as indexSearchable", func(t *testing.T) {
		_, err := search([]string{"titel"}, "title")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no such prop")
		assert.NotContains(t, err.Error(), "indexSearchable")
	})
}
