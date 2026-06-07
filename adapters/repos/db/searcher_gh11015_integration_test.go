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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

// Regression test for https://github.com/weaviate/weaviate/issues/11015
//
// With IndexPropertyLength enabled globally but indexFilterable disabled on an
// array property, the property has no inverted index, so its length bucket is
// never created (Shard.createPropertyLengthIndex is gated by HasAnyInvertedIndex).
// Filtering by len() of that property must return a clear, actionable error
// rather than reading a non-existent bucket.
func TestFilterPropertyLengthNonFilterable_GH11015(t *testing.T) {
	notFilterable := false
	class := &models.Class{
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexPropertyLength:    true,
			IndexNullState:         true,
			UsingBlockMaxWAND:      config.DefaultUsingBlockMaxWAND,
		},
		Class: "GH11015Class",
		Properties: []*models.Property{
			{
				Name:            "ints_non_filterable",
				DataType:        []string{"int[]"},
				IndexFilterable: &notFilterable,
			},
		},
	}

	migrator, repo, schemaGetter := createRepo(t)
	defer repo.Shutdown(context.Background())
	require.Nil(t, migrator.AddClass(context.Background(), class))
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{Classes: []*models.Class{class}},
	}

	search := func(propertyPath string, op filters.Operator, value interface{}, valueType schema.DataType) error {
		params := dto.GetParams{
			ClassName:  class.Class,
			Pagination: &filters.Pagination{Limit: 5},
			Filters: &filters.LocalFilter{Root: &filters.Clause{
				Operator: op,
				On: &filters.Path{
					Class:    schema.ClassName(class.Class),
					Property: schema.PropertyName(propertyPath),
				},
				Value: &filters.Value{Value: value, Type: valueType},
			}},
		}
		_, err := repo.Search(context.Background(), params)
		return err
	}

	t.Run("filter by length", func(t *testing.T) {
		err := search("len(ints_non_filterable)", filters.OperatorEqual, 2, schema.DataTypeInt)
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires inverted index")
	})

	t.Run("filter by null state", func(t *testing.T) {
		err := search("ints_non_filterable", filters.OperatorIsNull, true, schema.DataTypeBoolean)
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires inverted index")
	})
}
