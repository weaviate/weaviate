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

package schema

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/require"
)

func TestAddClass(t *testing.T) {
	t.Run("with empty class name", func(t *testing.T) {
		err := newSchemaManager().AddClass(context.Background(),
			nil, &models.Class{})
		require.EqualError(t, err, "'' is not a valid class name")
	})

	t.Run("with default BM25 params", func(t *testing.T) {
		mgr := newSchemaManager()

		expectedBM25Config := &models.BM25Config{
			K1: config.DefaultBM25k1,
			B:  config.DefaultBM25b,
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{Class: "NewClass"})
		require.Nil(t, err)

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedBM25Config, mgr.state.ObjectSchema.Classes[0].InvertedIndexConfig.Bm25)
	})

	t.Run("with customized BM25 params", func(t *testing.T) {
		mgr := newSchemaManager()

		expectedBM25Config := &models.BM25Config{
			K1: 1.88,
			B:  0.44,
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{
					Bm25: expectedBM25Config,
				},
			})
		require.Nil(t, err)

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedBM25Config, mgr.state.ObjectSchema.Classes[0].InvertedIndexConfig.Bm25)
	})

	t.Run("with default Stopwords config", func(t *testing.T) {
		mgr := newSchemaManager()

		expectedStopwordConfig := &models.StopwordConfig{
			Preset: stopwords.EnglishPreset,
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{Class: "NewClass"})
		require.Nil(t, err)

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedStopwordConfig, mgr.state.ObjectSchema.Classes[0].InvertedIndexConfig.Stopwords)
	})

	t.Run("with customized Stopwords config", func(t *testing.T) {
		mgr := newSchemaManager()

		expectedStopwordConfig := &models.StopwordConfig{
			Preset:    "none",
			Additions: []string{"monkey", "zebra", "octopus"},
			Removals:  []string{"are"},
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{
					Stopwords: expectedStopwordConfig,
				},
			})
		require.Nil(t, err)

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedStopwordConfig, mgr.state.ObjectSchema.Classes[0].InvertedIndexConfig.Stopwords)
	})
}
