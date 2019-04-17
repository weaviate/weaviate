/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package schema

import (
	"errors"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
)

func Test__SchemaSearch_Classes(t *testing.T) {
	tests := schemaSearchTests{
		{
			name: "className exactly in the contextionary, no keywords, no close other results",
			words: map[string][]float32{
				"$THING[Car]": {5, 5, 5},
				"car":         {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "Car",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
			},
			expectedError: nil,
			expectedResult: SearchResults{
				Type: SearchTypeClass,
				Results: []SearchResult{
					SearchResult{
						Name:      "Car",
						Kind:      kind.THING_KIND,
						Certainty: 0.9699532,
					},
				},
			},
		},
		{
			name: "with a camel cased seach string",
			words: map[string][]float32{
				"$THING[Driver]": {3, 3, 5},
				"car":            {4.7, 5.2, 5},
				"driver":         {3, 3, 3},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "CarDriver",
				Kind:       kind.THING_KIND,
				Certainty:  0.5,
			},
			expectedError: nil,
			expectedResult: SearchResults{
				Type: SearchTypeClass,
				Results: []SearchResult{
					SearchResult{
						Name:      "Driver",
						Kind:      kind.THING_KIND,
						Certainty: 0.8572954,
					},
				},
			},
		},
		{
			name: "with another class, that's somewhat related, but not enough the meet the desired certainty",
			words: map[string][]float32{
				"$THING[Car]":   {5, 5, 5},
				"$THING[Train]": {2, 2, 2},
				"car":           {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "Car",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
			},
			expectedError: nil,
			expectedResult: SearchResults{
				Type: SearchTypeClass,
				Results: []SearchResult{
					SearchResult{
						Name:      "Car",
						Kind:      kind.THING_KIND,
						Certainty: 0.9699532,
					},
				},
			},
		},
		{
			name: "with an action instead of a thing",
			words: map[string][]float32{
				"$ACTION[Drive]": {5, 5, 5},
				"drive":          {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "Drive",
				Kind:       kind.ACTION_KIND,
				Certainty:  0.9,
			},
			expectedError: nil,
			expectedResult: SearchResults{
				Type: SearchTypeClass,
				Results: []SearchResult{
					SearchResult{
						Name:      "Drive",
						Kind:      kind.ACTION_KIND,
						Certainty: 0.9699532,
					},
				},
			},
		},
		{
			name: "className is not a word in the contextionary",
			words: map[string][]float32{
				"$THING[Car]": {5, 5, 5},
				"car":         {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "Spaceship",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
			},
			expectedError: errors.New("could not build centroid from name and keywords: " +
				"invalid name in search: " +
				"the word 'spaceship' is not present in the contextionary and therefore not a valid search term"),
		},
		{
			name: "with additional keywords, all contained",
			words: map[string][]float32{
				"$THING[Car]":    {5, 5, 5},
				"car":            {4.7, 5.2, 5},
				"transportation": {5, 3, 3},
				"automobile":     {5.2, 5.0, 4},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "Car",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
				Keywords: models.SemanticSchemaKeywords{
					{
						Keyword: "automobile",
						Weight:  0.8,
					},
					{
						Keyword: "transportation",
						Weight:  0.5,
					},
				},
			},
			expectedError: nil,
			expectedResult: SearchResults{
				Type: SearchTypeClass,
				Results: []SearchResult{
					SearchResult{
						Name:      "Car",
						Kind:      kind.THING_KIND,
						Certainty: 0.9284513,
					},
				},
			},
		},
		{
			name: "with a keyword that's not in the contextinary",
			words: map[string][]float32{
				"$THING[Car]": {5, 5, 5},
				"car":         {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "Car",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
				Keywords: models.SemanticSchemaKeywords{
					{
						Keyword: "bicycle",
						Weight:  0.8,
					},
				},
			},
			expectedError: errors.New("could not build centroid from name and keywords: " +
				"invalid keyword in search: " +
				"the word 'bicycle' is not present in the contextionary and therefore not a valid search term"),
		},
	}

	tests.Assert(t)
}

func Test__SchemaSearch_Properties(t *testing.T) {
	tests := schemaSearchTests{
		{
			name: "property name exactly in the contextionary, no keywords, no close other results",
			words: map[string][]float32{
				"$THING[Car][horsepower]": {5, 5, 5},
				"horsepower":              {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeProperty,
				Name:       "horsepower",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
			},
			expectedError: nil,
			expectedResult: SearchResults{
				Type: SearchTypeProperty,
				Results: []SearchResult{
					SearchResult{
						Name:      "horsepower",
						Kind:      kind.THING_KIND,
						Certainty: 0.9699532,
					},
				},
			},
		},
		{
			name: "with two potential properties, that are somewhat related, " +
				"but not close enough to meet the certainty threshold",
			words: map[string][]float32{
				"$THING[Car][horsepower]":   {5, 5, 5},
				"$THING[Powerdrill][power]": {2, 2, 2},
				"horsepower":                {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeProperty,
				Name:       "horsepower",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
			},
			expectedError: nil,
			expectedResult: SearchResults{
				Type: SearchTypeProperty,
				Results: []SearchResult{
					SearchResult{
						Name:      "horsepower",
						Kind:      kind.THING_KIND,
						Certainty: 0.9699532,
					},
				},
			},
		},
		{
			name: "with duplicated property names", // this occurs when several classes have potential properties
			words: map[string][]float32{
				"$THING[Car][horsepower]":        {5, 5, 5},
				"$THING[Automobile][horsepower]": {5.1, 5.1, 5.1},
				"horsepower":                     {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeProperty,
				Name:       "horsepower",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
			},
			expectedError: nil,
			expectedResult: SearchResults{
				Type: SearchTypeProperty,
				Results: []SearchResult{
					SearchResult{
						Name:      "horsepower",
						Kind:      kind.THING_KIND,
						Certainty: 0.9672985, // note: this is the mean certainty of Car.horsepower and Autombile.horsepower
					},
				},
			},
		},
		{
			name: "propertyName is not a word in the contextionary",
			words: map[string][]float32{
				"$THING[Car][horsepower]": {5, 5, 5},
				"horsepower":              {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeProperty,
				Name:       "Spaceship",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
			},
			expectedError: errors.New("could not build centroid from name and keywords: " +
				"invalid name in search: " +
				"the word 'spaceship' is not present in the contextionary and therefore not a valid search term"),
		},
		{
			name: "with additional keywords, all contained",
			words: map[string][]float32{
				"$THING[Car][horsepower]": {5, 5, 5},
				"horsepower":              {4.7, 5.2, 5},
				"transportation":          {5, 3, 3},
				"automobile":              {5.2, 5.0, 4},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeProperty,
				Name:       "horsepower",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
				Keywords: models.SemanticSchemaKeywords{
					{
						Keyword: "automobile",
						Weight:  0.8,
					},
					{
						Keyword: "transportation",
						Weight:  0.5,
					},
				},
			},
			expectedError: nil,
			expectedResult: SearchResults{
				Type: SearchTypeProperty,
				Results: []SearchResult{
					SearchResult{
						Name:      "horsepower",
						Kind:      kind.THING_KIND,
						Certainty: 0.9284513,
					},
				},
			},
		},
		{
			name: "with a keyword that's not in the contextinary",
			words: map[string][]float32{
				"$THING[Car][horsepower]": {5, 5, 5},
				"horsepower":              {4.7, 5.2, 5},
			},
			searchParams: SearchParams{
				SearchType: SearchTypeProperty,
				Name:       "horsepower",
				Kind:       kind.THING_KIND,
				Certainty:  0.9,
				Keywords: models.SemanticSchemaKeywords{
					{
						Keyword: "bicycle",
						Weight:  0.8,
					},
				},
			},
			expectedError: errors.New("could not build centroid from name and keywords: " +
				"invalid keyword in search: " +
				"the word 'bicycle' is not present in the contextionary and therefore not a valid search term"),
		},
	}

	tests.Assert(t)
}

type schemaSearchTest struct {
	name           string
	words          map[string][]float32
	searchParams   SearchParams
	expectedResult SearchResults
	expectedError  error
}

type schemaSearchTests []schemaSearchTest

func (s schemaSearchTests) Assert(t *testing.T) {
	for _, test := range s {
		t.Run(test.name, func(t *testing.T) {
			// build c11y
			builder := contextionary.InMemoryBuilder(3)
			for word, vectors := range test.words {
				builder.AddWord(word, contextionary.NewVector(vectors))
			}

			rawC11y := builder.Build(3)
			schemaC11y := New(rawC11y)

			// perform search
			res, err := schemaC11y.SchemaSearch(test.searchParams)

			// assert error
			assert.Equal(t, test.expectedError, err, "should match the expected error")

			// assert result
			assert.Equal(t, test.expectedResult, res, "should match the expected result")
		})
	}
}
