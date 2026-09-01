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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func titleEqualsClause(token string) filters.Clause {
	return filters.Clause{
		Operator: filters.OperatorEqual,
		On: &filters.Path{
			Class:    schema.ClassName("MyClass"),
			Property: schema.PropertyName("title"),
		},
		Value: &filters.Value{
			Value: token,
			Type:  schema.DataTypeText,
		},
	}
}

// titleEquals matches the objects whose word-tokenized title contains any of
// the given tokens.
func titleEquals(tokens ...string) *filters.LocalFilter {
	if len(tokens) == 1 {
		clause := titleEqualsClause(tokens[0])
		return &filters.LocalFilter{Root: &clause}
	}

	operands := make([]filters.Clause, len(tokens))
	for i, token := range tokens {
		operands[i] = titleEqualsClause(token)
	}
	return &filters.LocalFilter{
		Root: &filters.Clause{Operator: filters.OperatorOr, Operands: operands},
	}
}

// TestAggregateHybridAppliesFilter pins that both hybrid legs honor the filter.
// Fusion unions the sparse and dense result sets, so a leg that skips the filter
// puts non-matching objects into the counts.
func TestAggregateHybridAppliesFilter(t *testing.T) {
	_, repo, schemaGetter := createRepo(t)
	defer repo.Shutdown(context.Background())

	logger, _ := test.NewNullLogger()
	SetupFusionClass(t, repo, schemaGetter, logger, 1.2, 0.75)

	// "BM25F" appears in the title and description of "Our journey to BM25F" and
	// "Our peanuts to BM25F", but not in "Elephant Parade".
	const query = "BM25F"

	titlePath := &filters.Path{
		Class:    schema.ClassName("MyClass"),
		Property: schema.PropertyName("title"),
	}
	objectLimit := 10
	topOccurrencesLimit := 10

	tests := []struct {
		name            string
		alpha           float64
		filter          *filters.LocalFilter
		groupBy         *filters.Path
		fusionAlgorithm int
		omitObjectLimit bool
		expectedCount   int
		expectedTitles  []string
	}{
		{
			name:           "sparse only, filter keeps one of the two keyword hits",
			alpha:          0,
			filter:         titleEquals("journey"),
			expectedCount:  1,
			expectedTitles: []string{"Our journey to BM25F"},
		},
		{
			name:           "sparse and dense, filter keeps one of the two keyword hits",
			alpha:          0.5,
			filter:         titleEquals("journey"),
			expectedCount:  1,
			expectedTitles: []string{"Our journey to BM25F"},
		},
		{
			name:            "sparse and dense, relative score fusion",
			alpha:           0.5,
			filter:          titleEquals("journey"),
			fusionAlgorithm: common_filters.HybridRelativeScoreFusion,
			expectedCount:   1,
			expectedTitles:  []string{"Our journey to BM25F"},
		},
		{
			name:           "sparse only, filter keeps both keyword hits",
			alpha:          0,
			filter:         titleEquals("journey", "peanuts"),
			expectedCount:  2,
			expectedTitles: []string{"Our journey to BM25F", "Our peanuts to BM25F"},
		},
		{
			name:          "sparse only, filter excludes every keyword hit",
			alpha:         0,
			filter:        titleEquals("elephant"),
			expectedCount: 0,
		},
		{
			name:           "sparse and dense, filter excludes every keyword hit",
			alpha:          0.5,
			filter:         titleEquals("elephant"),
			expectedCount:  1,
			expectedTitles: []string{"Elephant Parade"},
		},
		{
			name:          "sparse only, filter matches nothing",
			alpha:         0,
			filter:        titleEquals("zebra"),
			expectedCount: 0,
		},
		{
			name:          "sparse and dense, filter matches nothing",
			alpha:         0.5,
			filter:        titleEquals("zebra"),
			expectedCount: 0,
		},
		{
			name:           "dense only, filter keeps one object",
			alpha:          1,
			filter:         titleEquals("journey"),
			expectedCount:  1,
			expectedTitles: []string{"Our journey to BM25F"},
		},
		{
			name:            "dense only, no object limit",
			alpha:           1,
			filter:          titleEquals("journey"),
			omitObjectLimit: true,
			expectedCount:   1,
			expectedTitles:  []string{"Our journey to BM25F"},
		},
		{
			name:           "sparse only, no filter",
			alpha:          0,
			expectedCount:  2,
			expectedTitles: []string{"Our journey to BM25F", "Our peanuts to BM25F"},
		},
		{
			name:           "sparse and dense, no filter",
			alpha:          0.5,
			expectedCount:  3,
			expectedTitles: []string{"Our journey to BM25F", "Our peanuts to BM25F", "Elephant Parade"},
		},
		{
			name:           "grouped, sparse only, filter keeps one of the two keyword hits",
			alpha:          0,
			filter:         titleEquals("journey"),
			groupBy:        titlePath,
			expectedCount:  1,
			expectedTitles: []string{"Our journey to BM25F"},
		},
		{
			name:           "grouped, sparse and dense, filter keeps one of the two keyword hits",
			alpha:          0.5,
			filter:         titleEquals("journey"),
			groupBy:        titlePath,
			expectedCount:  1,
			expectedTitles: []string{"Our journey to BM25F"},
		},
		{
			name:           "grouped, sparse only, filter keeps both keyword hits",
			alpha:          0,
			filter:         titleEquals("journey", "peanuts"),
			groupBy:        titlePath,
			expectedCount:  2,
			expectedTitles: []string{"Our journey to BM25F", "Our peanuts to BM25F"},
		},
		{
			name:          "grouped, sparse only, filter excludes every keyword hit",
			alpha:         0,
			filter:        titleEquals("elephant"),
			groupBy:       titlePath,
			expectedCount: 0,
		},
		{
			name:           "grouped, sparse and dense, filter excludes every keyword hit",
			alpha:          0.5,
			filter:         titleEquals("elephant"),
			groupBy:        titlePath,
			expectedCount:  1,
			expectedTitles: []string{"Elephant Parade"},
		},
		{
			name:          "grouped, sparse only, filter matches nothing",
			alpha:         0,
			filter:        titleEquals("zebra"),
			groupBy:       titlePath,
			expectedCount: 0,
		},
		{
			name:           "grouped, sparse only, no filter",
			alpha:          0,
			groupBy:        titlePath,
			expectedCount:  2,
			expectedTitles: []string{"Our journey to BM25F", "Our peanuts to BM25F"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			params := aggregation.Params{
				ClassName:        schema.ClassName("MyClass"),
				Filters:          tc.filter,
				GroupBy:          tc.groupBy,
				IncludeMetaCount: true,
				Properties: []aggregation.ParamProperty{{
					Name:        schema.PropertyName("title"),
					Aggregators: []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(&topOccurrencesLimit)},
				}},
				Hybrid: &searchparams.HybridSearch{
					Query:           query,
					Alpha:           tc.alpha,
					Vector:          JourneyVector(),
					FusionAlgorithm: tc.fusionAlgorithm,
				},
			}
			if !tc.omitObjectLimit {
				params.ObjectLimit = &objectLimit
			}

			res, err := repo.Aggregate(context.Background(), params, nil)
			require.NoError(t, err)

			var count int
			var titles []string
			for _, group := range res.Groups {
				count += group.Count
				for _, occ := range group.Properties["title"].TextAggregation.Items {
					titles = append(titles, occ.Value)
				}
			}
			require.Equal(t, tc.expectedCount, count)
			require.ElementsMatch(t, tc.expectedTitles, titles)
		})
	}
}
