//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright (c) 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package traverser

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func TestExtractHighlights(t *testing.T) {
	schema := models.PropertySchema(map[string]interface{}{
		"title": "The Quick brown fox",
		"body": []interface{}{
			"slow text",
			"quick dog and QUICK cat",
		},
	})

	params := dto.GetParams{
		AdditionalProperties: additional.Properties{
			Highlight: &additional.HighlightProperties{
				FragmentCount: 2,
				FragmentSize:  80,
				PreTag:        "<mark>",
				PostTag:       "</mark>",
			},
		},
		KeywordRanking: &searchparams.KeywordRanking{
			Query:      "quick",
			Properties: []string{"title", "body^2", "title"},
		},
	}

	highlights := (&Explorer{}).extractHighlights(schema, params)

	require.Equal(t, []additional.Highlight{
		{
			Property:  "title",
			Fragments: []string{"The <mark>Quick</mark> brown fox"},
		},
		{
			Property:  "body",
			Fragments: []string{"<mark>quick</mark> dog and <mark>QUICK</mark> cat"},
		},
	}, highlights)
}

func TestExtractHighlightsWithRequestedProperties(t *testing.T) {
	schema := models.PropertySchema(map[string]interface{}{
		"title": "important keyword",
		"body":  "body keyword",
	})

	params := dto.GetParams{
		AdditionalProperties: additional.Properties{
			Highlight: &additional.HighlightProperties{
				Properties:    []string{"body"},
				FragmentCount: 1,
				FragmentSize:  80,
				PreTag:        "<em>",
				PostTag:       "</em>",
			},
		},
		HybridSearch: &searchparams.HybridSearch{
			Query:      "keyword",
			Properties: []string{"title"},
		},
	}

	highlights := (&Explorer{}).extractHighlights(schema, params)

	require.Equal(t, []additional.Highlight{
		{
			Property:  "body",
			Fragments: []string{"body <em>keyword</em>"},
		},
	}, highlights)
}

func TestExtractHighlightsWithoutKeywordQuery(t *testing.T) {
	schema := models.PropertySchema(map[string]interface{}{
		"title": "keyword",
	})

	params := dto.GetParams{
		AdditionalProperties: additional.Properties{
			Highlight: additional.DefaultHighlightProperties(),
		},
	}

	require.Empty(t, (&Explorer{}).extractHighlights(schema, params))
}
